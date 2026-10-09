// SPDX-FileCopyrightText: 2026 Spark Arena
// SPDX-License-Identifier: Apache-2.0

package peer

import (
	"bytes"
	"context"
	"github.com/spark-arena/oci-relay/internal/engine"
	"github.com/spark-arena/oci-relay/internal/image"
	"github.com/spark-arena/oci-relay/internal/source"
	"github.com/spark-arena/oci-relay/internal/testutil"
	"github.com/spark-arena/oci-relay/internal/transfer"
	"io"
	"net"
	"os"
	"testing"
	"time"
)

func fixtureServer(t *testing.T) (context.Context, *Server, *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	root, im := testutil.Layout(t, 2<<20)
	cache, err := transfer.New(ctx, &source.Layout{Root: root}, 8*transfer.FrameSize, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cache.Close)
	session, err := NewSession([]string{"receiver"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewServer(ctx, session, im, cache, "127.0.0.1:0", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	c, err := NewClient("https://"+s.Listener.Addr().String(), session.Peers["receiver"], nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return ctx, s, c
}
func TestAuthenticatedHTTP2(t *testing.T) {
	ctx, s, c := fixtureServer(t)
	im, err := c.Image(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(im.Manifest, s.Image.Manifest) {
		t.Fatal("manifest changed")
	}
	for _, d := range im.Descriptors {
		var data bytes.Buffer
		if err = c.Fetch(ctx, d, &data); err != nil {
			t.Fatal(err)
		}
		if err = image.Verify(data.Bytes(), d); err != nil {
			t.Fatal(err)
		}
	}
	if s.Cache.Metrics().PeakBuffers > 8*transfer.FrameSize {
		t.Fatal("source exceeded cache budget")
	}
	other, err := NewSession([]string{"receiver"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	stranger, err := NewClient(c.Endpoint, other.Peers["receiver"], nil)
	if err != nil {
		t.Fatal(err)
	}
	defer stranger.Close()
	if _, err = stranger.Image(ctx); err == nil {
		t.Fatal("accepted unrelated transfer credential")
	}
}
func TestHTTP2ThroughPipe(t *testing.T) {
	ctx, s, c := fixtureServer(t)
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	// Model a bounded SSH byte bridge. TLS remains between the receiver and source.
	go func() {
		remote, err := net.Dial("tcp", s.Listener.Addr().String())
		if err != nil {
			return
		}
		defer remote.Close()
		go func() { _, _ = io.Copy(remote, right) }()
		_, _ = io.Copy(right, remote)
	}()
	piped, err := NewClient(c.Endpoint, c.Credentials, left)
	if err != nil {
		t.Fatal(err)
	}
	defer piped.Close()
	im, err := piped.Image(ctx)
	if err != nil {
		t.Fatal(err)
	}
	d := im.Descriptors[1]
	var got bytes.Buffer
	if err = piped.Fetch(ctx, d, &got); err != nil {
		t.Fatal(err)
	}
	if err = image.Verify(got.Bytes(), d); err != nil {
		t.Fatal(err)
	}
}
func TestRealDockerPull(t *testing.T) {
	if os.Getenv("OCI_RELAY_DOCKER_TESTS") != "1" {
		t.Skip("set OCI_RELAY_DOCKER_TESTS=1")
	}
	ctx, s, c := fixtureServer(t)
	tag := "oci-relay-receiver-test:" + s.Session.Source.Transfer
	e, err := engine.New("")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := e.RemoveTag(cleanup, tag); err != nil {
			t.Log(err)
		}
	}()
	result, err := Pull(ctx, c, PullOptions{Tag: tag, Memory: 8 * transfer.FrameSize, Parallel: 1})
	if err != nil {
		t.Fatal(err)
	}
	if result.State != "COMPLETE" || result.ImageID != string(s.Image.Descriptors[0].Digest) {
		t.Fatalf("unexpected result %+v", result)
	}
	if s.Cache.Metrics().Acquired != s.Image.Descriptors[1].Size {
		t.Fatalf("source byte count: %+v", s.Cache.Metrics())
	}
	if err = c.Report(ctx, result); err != nil {
		t.Fatal(err)
	}
	if s.Results()["receiver"].State != "COMPLETE" {
		t.Fatal("completion not recorded")
	}
	t.Logf("real Docker pull: %s, acquired=%d peak=%d", result.ImageID, s.Cache.Metrics().Acquired, s.Cache.Metrics().PeakBuffers)
}

func TestLeaseExpiresAndControlPrivilegesAreSeparated(t *testing.T) {
	ctx, s, c := fixtureServer(t)
	if err := c.Heartbeat(ctx); err == nil {
		t.Fatal("receiver renewed manager lease")
	}
	manager, err := NewClient(c.Endpoint, s.Session.Peers["manager"], nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	if err = manager.Heartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	if err = manager.Report(ctx, Result{Version: 1, Transfer: s.Session.Source.Transfer, Peer: "manager", State: "FAILED"}); err == nil {
		t.Fatal("manager finalized a receiver")
	}
	invalid := Result{Version: 1, Transfer: s.Session.Source.Transfer, Peer: "receiver", State: "COMPLETE", Manifest: string(s.Image.Digest), ImageID: "sha256:wrong", Tag: "fixture:tag"}
	if err = c.Report(ctx, invalid); err == nil {
		t.Fatal("accepted wrong completed image ID")
	}
	leased, err := NewServer(ctx, s.Session, s.Image, s.Cache, "127.0.0.1:0", "", 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer leased.Close()
	select {
	case <-leased.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("abandoned manager lease did not expire")
	}
}
