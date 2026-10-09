// SPDX-FileCopyrightText: 2026 Spark Arena
// SPDX-License-Identifier: Apache-2.0

package peer

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/spark-arena/oci-relay/internal/image"
	"github.com/spark-arena/oci-relay/internal/transfer"
)

func twoPaths(t *testing.T, leaf *Client) *Client {
	t.Helper()
	c, err := NewPathClient([]Path{{leaf.Endpoint, "127.0.0.1"}, {leaf.Endpoint, "127.0.0.2"}}, leaf.Credentials)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func TestVerifyTwoBoundPaths(t *testing.T) {
	ctx, s, leaf := fixtureServer(t)
	c := twoPaths(t, leaf)
	r, err := Verify(ctx, c, PullOptions{Memory: 16 * transfer.FrameSize, Parallel: 2})
	if err != nil {
		t.Fatal(err)
	}
	if r.State != "VERIFIED" || r.ImageID != "" || r.Tag != "" || r.Metrics.VerifiedBlobs != 2 || r.Metrics.PeakBuffers > 16*transfer.FrameSize {
		t.Fatalf("bad verification: %+v", r)
	}
	var size int64
	for _, d := range s.Image.Descriptors {
		size += d.Size
	}
	if r.Metrics.VerifiedBytes != size {
		t.Fatal("wrong verified size")
	}
	for _, p := range r.Paths {
		if p.Bytes == 0 || p.Requests != 1 || p.Active != 0 || p.Disabled {
			t.Fatalf("unused/failed path: %+v", p)
		}
	}
	wrong := r
	wrong.State = "COMPLETE"
	if err := c.Report(ctx, wrong); err == nil {
		t.Fatal("verification accepted as imported image")
	}
	wrong = r
	wrong.Metrics.VerifiedBytes--
	if err := c.Report(ctx, wrong); err == nil {
		t.Fatal("partial verification accepted")
	}
	if err := c.Report(ctx, r); err != nil {
		t.Fatal(err)
	}
	if s.Results()["receiver"].State != "VERIFIED" {
		t.Fatal("missing verification result")
	}
}

func TestFailedPathFallsBackBeforeWriting(t *testing.T) {
	ctx, s, leaf := fixtureServer(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	endpoint := "https://" + listener.Addr().String()
	listener.Close()
	c, err := NewPathClient([]Path{{endpoint, "127.0.0.1"}, {leaf.Endpoint, "127.0.0.2"}}, leaf.Credentials)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	d := s.Image.Descriptors[1]
	var data bytes.Buffer
	if err := c.Fetch(ctx, d, &data); err != nil {
		t.Fatal(err)
	}
	if err := image.Verify(data.Bytes(), d); err != nil {
		t.Fatal(err)
	}
	p := c.PathMetrics()
	if !p[0].Disabled || p[0].Failures != 1 || p[0].Bytes != 0 || p[1].Bytes != d.Size {
		t.Fatalf("fallback metrics: %+v", p)
	}
}

func TestMultipleConnectionsShareVerificationBudget(t *testing.T) {
	ctx, _, leaf := fixtureServer(t)
	c, err := NewPathClientConnections([]Path{{leaf.Endpoint, "127.0.0.1"}}, leaf.Credentials, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	r, err := Verify(ctx, c, PullOptions{Memory: 16 * transfer.FrameSize, Parallel: 2})
	if err != nil {
		t.Fatal(err)
	}
	if r.Metrics.PeakBuffers > 16*transfer.FrameSize || len(r.Paths) != 2 {
		t.Fatalf("unbounded pool: %+v", r)
	}
	for i, p := range r.Paths {
		if p.Connection != i+1 || p.Bytes == 0 {
			t.Fatalf("connection unused: %+v", p)
		}
	}
}

func TestVerifyRestartsPartialBlobOnSurvivingConnection(t *testing.T) {
	ctx, _, leaf := fixtureServer(t)
	c := twoPaths(t, leaf)
	original := c.paths.paths[0].client.HTTP.Transport
	c.paths.paths[0].client.HTTP.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		response, err := original.RoundTrip(r)
		if err == nil && strings.Contains(r.URL.Path, "/blobs/") {
			response.Body.Close()
			response.Body = io.NopCloser(brokenBody{strings.NewReader("broken prefix")})
		}
		return response, err
	})
	r, err := Verify(ctx, c, PullOptions{Memory: 16 * transfer.FrameSize, Parallel: 2, Retries: 2})
	if err != nil {
		t.Fatal(err)
	}
	if r.State != "VERIFIED" || r.Metrics.VerifiedBlobs != 2 || r.Metrics.Replays != 1 || !r.Paths[0].Disabled || r.Paths[1].Failures != 0 {
		t.Fatalf("whole blob retry failed: %+v", r)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type brokenBody struct{ io.Reader }

func (r brokenBody) Read(b []byte) (int, error) {
	n, err := r.Reader.Read(b)
	if err == io.EOF {
		return n, io.ErrUnexpectedEOF
	}
	return n, err
}

func TestPartialPathFailureNeverAppendsRestart(t *testing.T) {
	ctx, s, leaf := fixtureServer(t)
	c := twoPaths(t, leaf)
	d := s.Image.Descriptors[1]
	c.paths.paths[0].client.HTTP.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{ProtoMajor: 2, StatusCode: 200, ContentLength: d.Size,
			Body: io.NopCloser(brokenBody{strings.NewReader("prefix")})}, nil
	})
	var data bytes.Buffer
	if err := c.Fetch(ctx, d, &data); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("missing failure: %v", err)
	}
	if data.String() != "prefix" || c.PathMetrics()[1].Requests != 0 {
		t.Fatal("appended another attempt after partial body")
	}
	data.Reset()
	if err := c.Fetch(ctx, d, &data); err != nil {
		t.Fatal(err)
	}
	if err := image.Verify(data.Bytes(), d); err != nil {
		t.Fatal(err)
	}
}

func TestWrongCertificateDoesNotFallback(t *testing.T) {
	ctx, s, leaf := fixtureServer(t)
	c := twoPaths(t, leaf)
	c.paths.paths[0].client.transport.TLSClientConfig.ServerName = "wrong-session"
	if err := c.Fetch(ctx, s.Image.Descriptors[1], io.Discard); err == nil {
		t.Fatal("accepted incorrect TLS identity")
	}
	if p := c.PathMetrics(); p[0].Disabled || p[1].Requests != 0 {
		t.Fatalf("TLS failure triggered fallback: %+v", p)
	}
}

func TestVerifyRejectsCorruptionAndCancellation(t *testing.T) {
	ctx, _, leaf := fixtureServer(t)
	c := twoPaths(t, leaf)
	for _, p := range c.paths.paths {
		original := p.client.HTTP.Transport
		p.client.HTTP.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
			response, err := original.RoundTrip(r)
			if err == nil && strings.Contains(r.URL.Path, "/blobs/") {
				response.Body.Close()
				response.Body = io.NopCloser(io.LimitReader(zeroReader{}, response.ContentLength))
			}
			return response, err
		})
	}
	r, err := Verify(ctx, c, PullOptions{Memory: 16 * transfer.FrameSize, Parallel: 2, Retries: 2})
	if err == nil || r.State != "FAILED" || r.Metrics.VerifiedBlobs != 0 {
		t.Fatalf("corruption accepted: %+v %v", r, err)
	}
	for _, p := range c.PathMetrics() {
		if p.Disabled || p.Failures != 0 {
			t.Fatal("hash mismatch treated as network fallback")
		}
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	r, err = Verify(cancelled, c, PullOptions{})
	if err == nil || r.State != "CANCELLED" {
		t.Fatal("cancellation lost")
	}
}

type zeroReader struct{}

func (zeroReader) Read(b []byte) (int, error) { clear(b); return len(b), nil }
