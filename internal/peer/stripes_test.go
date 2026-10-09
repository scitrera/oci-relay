// SPDX-FileCopyrightText: 2026 Spark Arena
// SPDX-License-Identifier: Apache-2.0

package peer

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/spark-arena/oci-relay/internal/engine"
	"github.com/spark-arena/oci-relay/internal/source"
	"github.com/spark-arena/oci-relay/internal/testutil"
	"github.com/spark-arena/oci-relay/internal/transfer"
)

func stripeFixture(t *testing.T) (context.Context, *Server, *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	root, im := testutil.Layout(t, 19<<20)
	cache, err := transfer.New(ctx, &source.Layout{Root: root}, 4*transfer.FrameSize, 1)
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
	c, err := NewPathClientConnections([]Path{{"https://" + s.Listener.Addr().String(), "127.0.0.1"}}, session.Peers["receiver"], 4)
	if err != nil {
		t.Fatal(err)
	}
	c.StripeThreshold = 8 << 20
	t.Cleanup(c.Close)
	return ctx, s, c
}

func TestStripeVerifyAndSourceAcquisitionBudget(t *testing.T) {
	ctx, s, c := stripeFixture(t)
	r, err := Verify(ctx, c, PullOptions{Memory: 4 * transfer.FrameSize, Parallel: 1})
	if err != nil {
		t.Fatal(err)
	}
	if r.State != "VERIFIED" || r.Metrics.VerifiedBlobs != 2 || r.Metrics.PeakBuffers > 4*transfer.FrameSize {
		t.Fatalf("bad result: %+v", r)
	}
	for _, p := range r.Paths {
		if p.StripeRequests != 1 || p.Bytes == 0 || p.Active != 0 || p.Disabled {
			t.Fatalf("unused/failed stripe: %+v", p)
		}
	}
	m := s.Cache.Metrics()
	if m.VerifiedBlobs != 2 || m.Replays != 0 || m.Acquired != r.Metrics.VerifiedBytes || m.PeakBuffers > 4*transfer.FrameSize {
		t.Fatalf("stripe refetched or exceeded budget: %+v", m)
	}
}

func TestStripePieceNegotiation(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		ctx, s, c := stripeFixture(t)
		c.StripePieceBytes = 8 << 20
		if legacy {
			for _, p := range c.paths.paths {
				original := p.client.HTTP.Transport
				p.client.HTTP.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
					if r.URL.Query().Has("piece") {
						t.Error("sent piece override to legacy peer")
					}
					response, err := original.RoundTrip(r)
					if err == nil {
						response.Header.Del(stripePieceHeader)
					}
					return response, err
				})
			}
		}
		r, err := Verify(ctx, c, PullOptions{Memory: 4 * transfer.FrameSize, Parallel: 1})
		if err != nil {
			t.Fatal(err)
		}
		wantPiece, wantLanes := int64(8<<20), 2 // 19 MiB fixture has two full 8 MiB pieces.
		if legacy {
			wantPiece, wantLanes = stripePiece, 4
		}
		used := 0
		for _, p := range r.Paths {
			if p.StripeRequests != 0 {
				used++
				if p.StripePieceBytes != wantPiece {
					t.Fatalf("wrong negotiated size: %+v", p)
				}
			}
		}
		if used != wantLanes || r.State != "VERIFIED" || s.Cache.Metrics().Replays != 0 {
			t.Fatalf("piece negotiation: %+v", r)
		}
	}
}

func TestStripeGroupRejectsGeometryChanges(t *testing.T) {
	ctx, s, _ := stripeFixture(t)
	d := s.Image.Descriptors[1]
	g, err := s.stripeGroup(ctx, "geometry-test", d, 2, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer g.cancel()
	if _, err := s.stripeGroup(ctx, "geometry-test", d, 2, 8<<20); err == nil {
		t.Fatal("accepted different piece size in one group")
	}
}

func TestStripePieceLengthCoverage(t *testing.T) {
	for _, piece := range []int64{1 << 20, 8 << 20, 16 << 20, 32 << 20, 64 << 20} {
		for count := 2; count <= 8; count++ {
			for _, size := range []int64{piece * int64(count), piece*int64(count) + 1, piece*int64(count*3) - 7} {
				want := make([]int64, count)
				for offset, index := int64(0), 0; offset < size; index++ {
					n := min(piece, size-offset)
					want[index%count] += n
					offset += n
				}
				for lane := range count {
					if got := stripeLength(size, count, lane, piece); got != want[lane] {
						t.Fatalf("piece %d count %d lane %d: got %d want %d", piece, count, lane, got, want[lane])
					}
				}
			}
		}
	}
}

func TestHTTP2LargeReceiveWindow(t *testing.T) {
	ctx, s, c := stripeFixture(t)
	if err := c.ConfigureHTTP2Window(16 << 20); err != nil {
		t.Fatal(err)
	}
	r, err := c.request(ctx, "GET", "/relay/v1/blobs/"+string(s.Image.Descriptors[1].Digest), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	// Prove the configured window reaches the wire, not just the config struct:
	// without reading the body, a default 4 MiB window cannot acquire 8 MiB
	// beyond this fixture's 256 KiB source ring.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s.Cache.Metrics().Acquired >= 8<<20 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("large window was not advertised/applied: %+v", s.Cache.Metrics())
}

func TestStripeFailureRetriesWholeBlobOnSurvivors(t *testing.T) {
	ctx, _, c := stripeFixture(t)
	// Force the failed connection to be ready before the final lane joins.
	// Without the server join barrier, this yields a setup HTTP 503 instead
	// of the retryable body failure on the broken connection.
	late := c.paths.paths[len(c.paths.paths)-1]
	lateTransport := late.client.HTTP.Transport
	late.client.HTTP.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.Path, "/stripes/") {
			time.Sleep(25 * time.Millisecond)
		}
		return lateTransport.RoundTrip(r)
	})
	path := c.paths.paths[0]
	original := path.client.HTTP.Transport
	path.client.HTTP.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		response, err := original.RoundTrip(r)
		if err == nil && strings.Contains(r.URL.Path, "/stripes/") {
			_ = response.Body.Close()
			response.Body = io.NopCloser(brokenBody{strings.NewReader("partial stripe")})
		}
		return response, err
	})
	r, err := Verify(ctx, c, PullOptions{Memory: 4 * transfer.FrameSize, Parallel: 1, Retries: 2})
	if err != nil {
		t.Fatal(err)
	}
	if r.State != "VERIFIED" || r.Metrics.Replays != 1 || !r.Paths[0].Disabled || r.Paths[0].Failures != 1 {
		t.Fatalf("bad recovery: %+v", r)
	}
}

func TestStripeProtocolAndIntegrityFailures(t *testing.T) {
	for _, failure := range []string{"metadata", "corrupt", "certificate"} {
		t.Run(failure, func(t *testing.T) {
			ctx, _, c := stripeFixture(t)
			if failure == "certificate" {
				c.paths.paths[1].client.transport.TLSClientConfig.ServerName = "wrong-session"
			}
			for _, p := range c.paths.paths {
				original := p.client.HTTP.Transport
				p.client.HTTP.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
					response, err := original.RoundTrip(r)
					if err == nil && strings.Contains(r.URL.Path, "/stripes/") {
						if failure == "metadata" {
							response.Header.Set(stripingHeader, "wrong")
						}
						if failure == "corrupt" {
							_ = response.Body.Close()
							response.Body = io.NopCloser(io.LimitReader(zeroReader{}, response.ContentLength))
						}
					}
					return response, err
				})
			}
			r, err := Verify(ctx, c, PullOptions{Memory: 4 * transfer.FrameSize, Parallel: 1, Retries: 2})
			if err == nil || r.State != "FAILED" || r.Metrics.VerifiedBlobs != 0 {
				t.Fatalf("accepted %s: %+v %v", failure, r, err)
			}
			for _, p := range r.Paths {
				if p.Disabled {
					t.Fatal("integrity/protocol error triggered failover")
				}
			}
		})
	}
}

func TestStripeOldSourceAndDisabledFallback(t *testing.T) {
	for _, mode := range []string{"old", "disabled", "below-threshold"} {
		ctx, _, c := stripeFixture(t)
		if mode == "old" {
			for _, p := range c.paths.paths {
				original := p.client.HTTP.Transport
				p.client.HTTP.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
					response, err := original.RoundTrip(r)
					if err == nil {
						response.Header.Del(stripingHeader)
					}
					return response, err
				})
			}
		} else if mode == "disabled" {
			c.StripeThreshold = 0
		} else {
			c.StripeThreshold = 256 << 20
		}
		r, err := Verify(ctx, c, PullOptions{Memory: 4 * transfer.FrameSize, Parallel: 1})
		if err != nil || r.State != "VERIFIED" {
			t.Fatalf("fallback: %v", err)
		}
		for _, p := range r.Paths {
			if p.StripeRequests != 0 {
				t.Fatal("unexpected striping")
			}
		}
	}
}

func TestStripeCancellationAndWriterFailure(t *testing.T) {
	ctx, s, c := stripeFixture(t)
	if _, err := c.Image(ctx); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(ctx)
	err := c.Fetch(ctx, s.Image.Descriptors[1], writerFunc(func(b []byte) (int, error) { cancel(); return 0, io.ErrClosedPipe }))
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("writer error lost: %v", err)
	}
	for _, p := range c.PathMetrics() {
		if p.Active != 0 || p.Disabled {
			t.Fatalf("writer failure disabled route: %+v", p)
		}
	}
}

func TestStripeDeadlineDoesNotDisableConnections(t *testing.T) {
	ctx, s, c := stripeFixture(t)
	if _, err := c.Image(ctx); err != nil {
		t.Fatal(err)
	}
	for _, path := range c.paths.paths {
		path.client.HTTP.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
			<-r.Context().Done()
			return nil, r.Context().Err()
		})
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
	defer cancel()
	if err := c.Fetch(ctx, s.Image.Descriptors[1], io.Discard); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline lost: %v", err)
	}
	for _, path := range c.PathMetrics() {
		if path.Active != 0 || path.Disabled || path.Failures != 0 {
			t.Fatalf("deadline disabled healthy route: %+v", path)
		}
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(b []byte) (int, error) { return f(b) }

func TestRealDockerPullStriped(t *testing.T) {
	if os.Getenv("OCI_RELAY_DOCKER_TESTS") != "1" {
		t.Skip("set OCI_RELAY_DOCKER_TESTS=1")
	}
	ctx, s, c := stripeFixture(t)
	tag := "oci-relay-stripe-test:" + s.Session.Source.Transfer
	e, err := engine.New("")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := e.RemoveTag(ctx, tag); err != nil {
			t.Error(err)
		}
	}()
	r, err := Pull(ctx, c, PullOptions{Tag: tag, Memory: 8 * transfer.FrameSize, Parallel: 1})
	if err != nil {
		t.Fatal(err)
	}
	if r.State != "COMPLETE" || r.ImageID != string(s.Image.Descriptors[0].Digest) {
		t.Fatalf("bad import: %+v", r)
	}
	for _, p := range r.Paths {
		if p.StripeRequests != 1 {
			t.Fatal("import did not stripe")
		}
	}
	if err := c.Report(ctx, r); err != nil {
		t.Fatal(err)
	}
	// A warm target must still avoid all layer transfers.
	warm, err := Pull(ctx, c, PullOptions{Tag: tag, SkipPresent: true, Memory: 8 * transfer.FrameSize, Parallel: 1})
	if err != nil || !warm.AlreadyPresent || warm.Metrics.Acquired != 0 {
		t.Fatalf("warm import: %+v %v", warm, err)
	}
}

func TestStripeAbandonedGroupReleasesReaders(t *testing.T) {
	ctx, s, c := stripeFixture(t)
	d := s.Image.Descriptors[1]
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		response, err := c.request(ctx, "GET", "/relay/v1/stripes/"+string(d.Digest)+"?group=00000000000000000000000000000001&lanes=4&lane=0", nil)
		if response != nil {
			_ = response.Body.Close()
		}
		done <- err
	}()
	// An incomplete group cannot return headers. Abandon the request after its
	// lane joins, then verify that cancellation releases retained source data.
	joined := false
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && !joined {
		s.mu.Lock()
		for _, g := range s.stripeGroups {
			g.mu.Lock()
			joined = g.joined > 0
			g.mu.Unlock()
		}
		s.mu.Unlock()
		if !joined {
			time.Sleep(time.Millisecond)
		}
	}
	if !joined {
		t.Fatal("stripe request did not join")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("abandoned stripe: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("abandoned stripe did not cancel")
	}
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		count := len(s.stripeGroups)
		s.mu.Unlock()
		if count == 0 && s.Cache.Metrics().ActiveAcquisitions == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("abandoned group retained readers/acquisition")
}
