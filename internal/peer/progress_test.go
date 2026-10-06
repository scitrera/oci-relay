// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only
// Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.

package peer

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/scitrera/oci-relay/internal/transfer"
)

type progressFixtureSource func(context.Context, v1.Descriptor, io.Writer) error

func (f progressFixtureSource) Fetch(ctx context.Context, d v1.Descriptor, w io.Writer) error {
	return f(ctx, d, w)
}

func TestProgressCountsUniqueOffsetsAcrossConcurrentReplays(t *testing.T) {
	ctx, s, c := fixtureServer(t)
	p := newReceiverProgress(ctx, c)
	defer p.close()
	layer := s.Image.Descriptors[1]
	p.plan([]v1.Descriptor{layer, layer}, nil, 0)
	src := progressSource{source: c, progress: p}
	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := src.Fetch(ctx, layer, io.Discard); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	// Config metadata must not inflate layer progress.
	if err := src.Fetch(ctx, s.Image.Descriptors[0], io.Discard); err != nil {
		t.Fatal(err)
	}
	got := p.snapshot()
	if got.ExpectedBytes != layer.Size || got.ReceivedBytes != layer.Size || got.WireBytes != 3*layer.Size || got.ActiveStreams != 0 {
		t.Fatalf("replay/metadata inflated progress: %+v", got)
	}
	if len(s.Results()) != 0 {
		t.Fatal("byte progress finalized image import")
	}
}

func TestProgressPartialRetryAndLocalLayers(t *testing.T) {
	ctx, s, c := fixtureServer(t)
	p := newReceiverProgress(ctx, c)
	defer p.close()
	layer := s.Image.Descriptors[1]
	local := v1.Descriptor{Digest: digest.FromString("local"), Size: 1234}
	p.plan([]v1.Descriptor{local, layer}, map[digest.Digest]transfer.Source{local.Digest: c}, 1)
	src := progressSource{source: progressFixtureSource(func(_ context.Context, _ v1.Descriptor, w io.Writer) error {
		_, err := w.Write([]byte("partial"))
		if err != nil {
			return err
		}
		return io.ErrUnexpectedEOF
	}), progress: p}
	if err := src.Fetch(ctx, layer, io.Discard); err != io.ErrUnexpectedEOF {
		t.Fatal(err)
	}
	src.source = c
	if err := src.Fetch(ctx, layer, io.Discard); err != nil {
		t.Fatal(err)
	}
	got := p.snapshot()
	if got.ExpectedBytes != layer.Size || got.ReceivedBytes != layer.Size || got.WireBytes != layer.Size+7 || got.ReusedLayers != 1 {
		t.Fatalf("incorrect partial retry/cache accounting: %+v", got)
	}
}

func TestProgressAuthenticationBoundsOrderingAndFinalization(t *testing.T) {
	ctx, s, c := fixtureServer(t)
	p := ReceiverProgress{Version: 1, Transfer: c.Credentials.Transfer, Peer: c.Credentials.Peer, Sequence: 2, Phase: "discovering"}
	if err := c.reportProgress(ctx, p); err != nil {
		t.Fatal(err)
	}
	old := p
	old.Sequence = 1
	old.Phase = "checking"
	if err := c.reportProgress(ctx, old); err != nil {
		t.Fatal(err)
	}
	if s.Progress()[p.Peer].Phase != "discovering" {
		t.Fatal("stale event overwrote newer progress")
	}
	for _, mutate := range []func(*ReceiverProgress){
		func(p *ReceiverProgress) { p.Peer = "other" },
		func(p *ReceiverProgress) { p.Transfer = "other" },
		func(p *ReceiverProgress) { p.Phase = "complete" },
		func(p *ReceiverProgress) { p.ExpectedBytes = -1 },
		func(p *ReceiverProgress) { p.ActiveStreams = 99999 },
		func(p *ReceiverProgress) { p.ReceivedBytes = 1 },
	} {
		bad := p
		mutate(&bad)
		if c.reportProgress(ctx, bad) == nil {
			t.Fatalf("accepted invalid progress: %+v", bad)
		}
	}
	if _, err := c.request(ctx, http.MethodPost, "/relay/v1/progress", strings.NewReader(strings.Repeat(" ", 4097))); err == nil {
		t.Fatal("accepted oversized progress")
	}
	manager, err := NewClient(c.Endpoint, s.Session.Peers["manager"], nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	if manager.reportProgress(ctx, p) == nil {
		t.Fatal("manager impersonated receiver progress")
	}
	if err := c.Report(ctx, Result{Version: 1, Transfer: p.Transfer, Peer: p.Peer, State: "FAILED"}); err != nil {
		t.Fatal(err)
	}
	p.Sequence = 3
	p.Phase = "verifying"
	if err := c.reportProgress(ctx, p); err != nil {
		t.Fatal(err)
	}
	if s.Progress()[p.Peer].Phase != "discovering" || s.Results()[p.Peer].State != "FAILED" {
		t.Fatal("late progress changed finalized receiver")
	}
}

type stalledProgressTransport struct{ entered chan struct{} }

func (t stalledProgressTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	select {
	case t.entered <- struct{}{}:
	default:
	}
	<-r.Context().Done()
	return nil, r.Context().Err()
}

func TestProgressCancellationDoesNotWaitForReportTimeout(t *testing.T) {
	entered := make(chan struct{}, 1)
	c := &Client{Endpoint: "https://fixture", HTTP: &http.Client{Transport: stalledProgressTransport{entered}}}
	p := newReceiverProgress(context.Background(), c)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("report did not start")
	}
	done := make(chan struct{})
	go func() { p.close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("progress blocked cancellation")
	}
}
