// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only
// Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.

package source

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scitrera/oci-relay/internal/transfer"
)

func testRangeOptions(server *httptest.Server) RegistryOptions {
	o := registryOptions(server)
	o.RangeConcurrency, o.RangeChunkBytes, o.RangeThresholdBytes, o.RangeBufferBytes = 4, 64<<10, 64<<10, 256<<10
	return o
}

func fixtureInterval(t testing.TB, f *registryFixture, w http.ResponseWriter, req *http.Request) (int64, int64) {
	t.Helper()
	var start, end int64
	if n, err := fmt.Sscanf(req.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil || n != 2 || start < 0 || end < start || end >= int64(len(f.blob)) {
		t.Errorf("invalid client range %q", req.Header.Get("Range"))
		return 0, 0
	}
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(f.blob)))
	w.Header().Set("Content-Length", fmt.Sprint(end-start+1))
	return start, end
}

func assertRangeReleased(t *testing.T, r *Registry) {
	t.Helper()
	m := r.Metrics()
	if m.ActiveRangeRequests != 0 || m.RangeBufferBytes != 0 {
		t.Fatalf("range resources leaked: %+v", m)
	}
}

func TestRegistryRangesParallelOrderedAndSharedSHA(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	f := newRegistryFixture(t)
	joined := make(chan struct{}, 4)
	releaseInitial := make(chan struct{})
	lastStarted, releaseLast := make(chan struct{}), make(chan struct{})
	f.handler = func(w http.ResponseWriter, req *http.Request) bool {
		if !strings.HasSuffix(req.URL.Path, f.blobDigest.String()) {
			return false
		}
		start, end := fixtureInterval(t, f, w, req)
		w.WriteHeader(206)
		w.(http.Flusher).Flush()
		if start < 4*(64<<10) {
			joined <- struct{}{}
			if start == 0 {
				for range 4 {
					select {
					case <-joined:
					case <-req.Context().Done():
						return true
					}
				}
				close(releaseInitial)
			}
			select {
			case <-releaseInitial:
			case <-req.Context().Done():
				return true
			}
		} else {
			close(lastStarted)
			select {
			case <-releaseLast:
			case <-req.Context().Done():
				return true
			}
		}
		_, _ = w.Write(f.blob[start : end+1])
		return true
	}
	srv := httptest.NewServer(f)
	defer srv.Close()
	o := testRangeOptions(srv)
	o.MaxCacheBytes, o.SpoolDir = int64(len(f.blob)), t.TempDir()
	r, err := NewRegistry(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	c, err := transfer.NewSource(ctx, r, 1<<20, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	reader, err := c.Open(ctx, r.Image.Descriptors[1])
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	// A prefix is available while the final range is still in flight.
	prefix := make([]byte, 64<<10)
	if _, err = io.ReadFull(reader, prefix); err != nil {
		t.Fatal(err)
	}
	select {
	case <-lastStarted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	close(releaseLast)
	rest, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(append(prefix, rest...), f.blob) {
		t.Fatalf("reassembly: %v", err)
	}
	var replay bytes.Buffer
	if err = r.Fetch(ctx, r.Image.Descriptors[1], &replay); err != nil || !bytes.Equal(replay.Bytes(), f.blob) {
		t.Fatalf("disk replay: %v", err)
	}
	m := r.Metrics()
	if m.RangeRequests != 5 || m.RangeDownloads != 1 || m.Downloads != 1 || m.UpstreamBytes != int64(len(f.blob)) || m.CacheHits != 1 || m.PeakRangeRequests != 4 || m.PeakRangeBufferBytes != 256<<10 || m.SharedVerifications != 1 || m.LocalVerifications != 1 {
		t.Fatalf("range accounting/verification: %+v", m)
	}
	assertRangeReleased(t, r)
}

func TestRegistryRangeFallbackAndDisabled(t *testing.T) {
	for _, mode := range []string{"ignored", "416", "disabled", "small", "budget"} {
		t.Run(mode, func(t *testing.T) {
			f := newRegistryFixture(t)
			f.handler = func(w http.ResponseWriter, req *http.Request) bool {
				if !strings.HasSuffix(req.URL.Path, f.blobDigest.String()) {
					return false
				}
				if mode == "416" && req.Header.Get("Range") != "" {
					w.WriteHeader(416)
					return true
				}
				if mode != "ignored" && mode != "416" && req.Header.Get("Range") != "" {
					t.Error("unexpected range")
				}
				return false // Default fixture returns the whole 200 response.
			}
			srv := httptest.NewServer(f)
			defer srv.Close()
			o := testRangeOptions(srv)
			switch mode {
			case "disabled":
				o.RangeConcurrency = 1
			case "small":
				o.RangeThresholdBytes = 256 << 20
			case "budget":
				o.RangeBufferBytes = 64 << 10
			}
			r, err := NewRegistry(context.Background(), o)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			var got bytes.Buffer
			if err = r.Fetch(context.Background(), r.Image.Descriptors[1], &got); err != nil || !bytes.Equal(got.Bytes(), f.blob) {
				t.Fatalf("fallback: %v", err)
			}
			m := r.Metrics()
			wantRequests := int64(1)
			if mode == "416" {
				wantRequests = 2
			}
			if f.requests.Load() != wantRequests || m.UpstreamBytes != int64(len(f.blob)) || m.Downloads != 1 || m.RangeDownloads != 0 {
				t.Fatal(m, f.requests.Load())
			}
			if mode == "ignored" || mode == "416" {
				if m.RangeRequests != 1 || m.RangeFallbacks != 1 {
					t.Fatal(m)
				}
			} else if m.RangeRequests != 0 {
				t.Fatal(m)
			}
			assertRangeReleased(t, r)
		})
	}
}

func TestRegistryRangesRejectInvalidResponsesBeforePublication(t *testing.T) {
	for _, mode := range []string{"missing", "offset", "total", "length", "multipart", "encoding", "digest-header", "short", "excess", "corrupt", "later-200"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			f := newRegistryFixture(t)
			f.handler = func(w http.ResponseWriter, req *http.Request) bool {
				if !strings.HasSuffix(req.URL.Path, f.blobDigest.String()) {
					return false
				}
				start, end := fixtureInterval(t, f, w, req)
				body := bytes.Clone(f.blob[start : end+1])
				if start == 0 && mode != "later-200" || start != 0 && mode == "later-200" {
					switch mode {
					case "missing":
						w.Header().Del("Content-Range")
					case "offset":
						w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start+1, end+1, len(f.blob)))
					case "total":
						w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/*", start, end))
					case "length":
						w.Header().Set("Content-Length", fmt.Sprint(len(body)+1))
					case "multipart":
						w.Header().Set("Content-Type", "multipart/byteranges")
					case "encoding":
						w.Header().Set("Content-Encoding", "gzip")
					case "digest-header":
						w.Header().Set("Docker-Content-Digest", "sha256:"+strings.Repeat("0", 64))
					case "short":
						body = body[:len(body)-1]
					case "excess":
						body = append(body, 1)
						w.Header().Del("Content-Length")
					case "corrupt":
						body[0] ^= 1
					case "later-200":
						w.Header().Del("Content-Range")
						w.Header().Del("Content-Length")
						w.WriteHeader(200)
						_, _ = w.Write(f.blob)
						return true
					}
				}
				w.WriteHeader(206)
				w.(http.Flusher).Flush()
				_, _ = w.Write(body)
				return true
			}
			srv := httptest.NewServer(f)
			defer srv.Close()
			o := testRangeOptions(srv)
			o.MaxCacheBytes, o.SpoolDir = int64(len(f.blob)), t.TempDir()
			r, err := NewRegistry(ctx, o)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			c, err := transfer.NewSource(ctx, r, 1<<20, 1)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := c.Open(ctx, r.Image.Descriptors[1])
			if err != nil {
				t.Fatal(err)
			}
			n, err := io.Copy(io.Discard, reader)
			reader.Close()
			c.Close()
			if err == nil || n >= int64(len(f.blob)) {
				t.Fatalf("invalid range published: %d %v", n, err)
			}
			if r.Metrics().CacheReservedBytes != 0 {
				t.Fatal("invalid disk entry retained")
			}
			files, err := os.ReadDir(r.dir)
			if err != nil || len(files) != 0 {
				t.Fatalf("partial files: %v %v", files, err)
			}
			assertRangeReleased(t, r)
		})
	}
}

func TestRegistryRangesCancelActiveAndQueuedFetches(t *testing.T) {
	f := newRegistryFixture(t)
	started := make(chan struct{})
	var once sync.Once
	f.handler = func(w http.ResponseWriter, req *http.Request) bool {
		if !strings.HasSuffix(req.URL.Path, f.blobDigest.String()) {
			return false
		}
		fixtureInterval(t, f, w, req)
		w.WriteHeader(206)
		w.(http.Flusher).Flush()
		once.Do(func() { close(started) })
		<-req.Context().Done()
		return true
	}
	srv := httptest.NewServer(f)
	defer srv.Close()
	r, err := NewRegistry(context.Background(), testRangeOptions(srv))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 2)
	go func() { done <- r.Fetch(ctx, r.Image.Descriptors[1], io.Discard) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	go func() { done <- r.Fetch(ctx, r.Image.Descriptors[1], io.Discard) }()
	// Closing the source cancels both HTTP bodies and memory-budget waiters.
	closed := make(chan error, 1)
	go func() { closed <- r.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Close blocked")
	}
	for range 2 {
		if err := <-done; err == nil {
			t.Fatal("cancelled fetch succeeded")
		}
	}
	assertRangeReleased(t, r)
}

func TestRegistryRangesSharedBudgetAndWriterFailure(t *testing.T) {
	f := newRegistryFixture(t)
	f.handler = func(w http.ResponseWriter, req *http.Request) bool {
		if !strings.HasSuffix(req.URL.Path, f.blobDigest.String()) {
			return false
		}
		start, end := fixtureInterval(t, f, w, req)
		w.WriteHeader(206)
		_, _ = w.Write(f.blob[start : end+1])
		return true
	}
	srv := httptest.NewServer(f)
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := NewRegistry(ctx, testRangeOptions(srv))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.Fetch(ctx, r.Image.Descriptors[1], io.Discard); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	m := r.Metrics()
	if m.PeakRangeBufferBytes > 256<<10 || m.PeakRangeRequests > 4 || m.RangeDownloads != 8 {
		t.Fatal(m)
	}
	want := errors.New("destination failed")
	if err := r.Fetch(ctx, r.Image.Descriptors[1], rangeFailWriter{want}); !errors.Is(err, want) {
		t.Fatalf("lost writer error: %v", err)
	}
	assertRangeReleased(t, r)
}

type rangeFailWriter struct{ err error }

func (w rangeFailWriter) Write([]byte) (int, error) { return 0, w.err }

func TestRegistryRangeRedirectPreservesRangeWithoutCredentials(t *testing.T) {
	f := newRegistryFixture(t)
	var authenticated atomic.Int64
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") != "" || req.Header.Get("Cookie") != "" {
			t.Error("credentials leaked to CDN")
		}
		start, end := fixtureInterval(t, f, w, req)
		w.WriteHeader(206)
		_, _ = w.Write(f.blob[start : end+1])
	}))
	defer cdn.Close()
	f.handler = func(w http.ResponseWriter, req *http.Request) bool {
		if req.Header.Get("Authorization") != "Basic dXNlcjpwYXNz" {
			w.Header().Set("WWW-Authenticate", `Basic realm="fixture"`)
			w.WriteHeader(401)
			return true
		}
		if !strings.HasSuffix(req.URL.Path, f.blobDigest.String()) {
			return false
		}
		authenticated.Add(1)
		http.Redirect(w, req, cdn.URL+"/blob?token=secret", 307)
		return true
	}
	srv := httptest.NewServer(f)
	defer srv.Close()
	o := testRangeOptions(srv)
	o.Credentials = &RegistryCredentials{Username: "user", Password: "pass"}
	r, err := NewRegistry(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var got bytes.Buffer
	if err = r.Fetch(context.Background(), r.Image.Descriptors[1], &got); err != nil || !bytes.Equal(got.Bytes(), f.blob) {
		t.Fatalf("redirect ranges: %v", err)
	}
	if authenticated.Load() != 5 {
		t.Fatal(authenticated.Load())
	}
}

func TestRegistryRangeLimits(t *testing.T) {
	for _, o := range []RegistryOptions{
		{RangeConcurrency: -1}, {RangeConcurrency: 17}, {RangeChunkBytes: -1}, {RangeChunkBytes: (64 << 20) + 1},
		{RangeThresholdBytes: -1}, {RangeThresholdBytes: (1 << 50) + 1}, {RangeBufferBytes: -1}, {RangeBufferBytes: (1 << 30) + 1},
	} {
		var r registryRanges
		if err := r.configure(o); err == nil {
			t.Fatalf("invalid range options accepted: %+v", o)
		}
	}
	var r registryRanges
	if err := r.configure(RegistryOptions{}); err != nil {
		t.Fatal(err)
	}
	if r.streams != 4 || r.chunk != 16<<20 || r.threshold != 256<<20 || r.budget != 128<<20 {
		t.Fatalf("unexpected defaults: %+v", &r)
	}
	if err := r.configure(RegistryOptions{RangeBufferBytes: 32 << 20}); err != nil || r.streams != 2 {
		t.Fatalf("budget did not cap streams: %d %v", r.streams, err)
	}
}

func TestRegistryRangesRefreshTokenDuringParallelRequests(t *testing.T) {
	f := newRegistryFixture(t)
	var tokenCalls atomic.Int64
	var srv *httptest.Server
	f.handler = func(w http.ResponseWriter, req *http.Request) bool {
		if req.URL.Path == "/token" {
			if req.URL.Query().Get("scope") != "repository:test/image:pull" {
				t.Error("wrong range token scope")
			}
			tokenCalls.Add(1)
			fmt.Fprint(w, `{"token":"refreshed","expires_in":3600}`)
			return true
		}
		if !strings.HasSuffix(req.URL.Path, f.blobDigest.String()) {
			return false
		}
		start, end := fixtureInterval(t, f, w, req)
		if start > 0 && req.Header.Get("Authorization") != "Bearer refreshed" {
			w.Header().Del("Content-Length")
			w.Header().Del("Content-Range")
			w.Header().Set("WWW-Authenticate", `Bearer realm="`+srv.URL+`/token",service="fixture"`)
			w.WriteHeader(401)
			return true
		}
		w.WriteHeader(206)
		_, _ = w.Write(f.blob[start : end+1])
		return true
	}
	srv = httptest.NewServer(f)
	defer srv.Close()
	o := testRangeOptions(srv)
	o.Credentials = &RegistryCredentials{RegistryToken: "original"}
	r, err := NewRegistry(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err = r.Fetch(context.Background(), r.Image.Descriptors[1], io.Discard); err != nil {
		t.Fatal(err)
	}
	if tokenCalls.Load() != 1 || r.Metrics().RangeRequests != 5 {
		t.Fatalf("range token refresh not coalesced: calls=%d metrics=%+v", tokenCalls.Load(), r.Metrics())
	}
	assertRangeReleased(t, r)
}

func TestRegistryRangesOverVerifiedHTTP2(t *testing.T) {
	f := newRegistryFixture(t)
	f.handler = func(w http.ResponseWriter, req *http.Request) bool {
		if !strings.HasSuffix(req.URL.Path, f.blobDigest.String()) {
			return false
		}
		if req.ProtoMajor != 2 || req.Header.Get("Accept-Encoding") != "identity" {
			t.Error("expected HTTP/2 identity request")
		}
		start, end := fixtureInterval(t, f, w, req)
		w.WriteHeader(206)
		_, _ = w.Write(f.blob[start : end+1])
		return true
	}
	srv := httptest.NewUnstartedServer(f)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	o := testRangeOptions(srv)
	o.Reference = strings.TrimPrefix(srv.URL, "https://") + "/test/image:latest"
	o.PlainHTTP = false
	o.Transport = srv.Client().Transport.(*http.Transport)
	r, err := NewRegistry(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err = r.Fetch(context.Background(), r.Image.Descriptors[1], io.Discard); err != nil {
		t.Fatal(err)
	}
	assertRangeReleased(t, r)
}

// Models an upstream service limiting each response stream, not a LAN/network
// capacity benchmark. Every iteration fetches and SHA-verifies the same blob;
// no retained disk cache or Docker daemon is involved.
func BenchmarkRegistryRangeDownload(b *testing.B) {
	f := newRegistryFixtureSize(b, 16<<20)
	f.handler = func(w http.ResponseWriter, req *http.Request) bool {
		if !strings.HasSuffix(req.URL.Path, f.blobDigest.String()) {
			return false
		}
		start, end := int64(0), int64(len(f.blob)-1)
		if req.Header.Get("Range") != "" {
			start, end = fixtureInterval(b, f, w, req)
			w.WriteHeader(206)
		} else {
			w.Header().Set("Content-Length", fmt.Sprint(len(f.blob)))
		}
		for offset := start; offset <= end; offset += 64 << 10 {
			if _, err := w.Write(f.blob[offset:min(end+1, offset+(64<<10))]); err != nil {
				return true
			}
			w.(http.Flusher).Flush()
			timer := time.NewTimer(time.Millisecond)
			select {
			case <-timer.C:
			case <-req.Context().Done():
				timer.Stop()
				return true
			}
		}
		return true
	}
	srv := httptest.NewServer(f)
	defer srv.Close()
	for _, streams := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("streams-%d", streams), func(b *testing.B) {
			o := testRangeOptions(srv)
			o.RangeConcurrency, o.RangeChunkBytes, o.RangeBufferBytes = streams, 1<<20, 8<<20
			r, err := NewRegistry(context.Background(), o)
			if err != nil {
				b.Fatal(err)
			}
			defer r.Close()
			b.SetBytes(int64(len(f.blob)))
			b.ResetTimer()
			for range b.N {
				if err := r.Fetch(context.Background(), r.Image.Descriptors[1], io.Discard); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(r.Metrics().PeakRangeRequests), "peak-requests")
		})
	}
}
