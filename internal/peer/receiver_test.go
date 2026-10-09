// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package peer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/spark-arena/oci-relay/internal/image"
	"github.com/spark-arena/oci-relay/internal/source"
	"github.com/spark-arena/oci-relay/internal/testutil"
	"github.com/spark-arena/oci-relay/internal/transfer"
)

func saturatedRegistry(t *testing.T) *Registry {
	t.Helper()
	root, im := testutil.Layout(t, 4096)
	cache, err := transfer.New(t.Context(), &source.Layout{Root: root}, 2*transfer.FrameSize, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cache.Close)
	reg := NewRegistry(im, cache, "test")
	// Hold every active response slot, as a burst of Docker blob requests does.
	for n := 0; n < cap(reg.streams); n++ {
		reg.streams <- struct{}{}
	}
	return reg
}

func TestRegistryQueuesBlobRequestsAtStreamLimit(t *testing.T) {
	reg := saturatedRegistry(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	d := reg.Image.Descriptors[1]
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		reg.ServeHTTP(response, httptest.NewRequestWithContext(ctx, http.MethodGet, "/v2/test/blobs/"+d.Digest.String(), nil))
	}()
	select {
	case <-done:
		t.Fatalf("busy receiver rejected blob: %d", response.Code)
	case <-time.After(50 * time.Millisecond):
	}
	// Metadata remains available even while blob bodies wait for admission.
	for _, path := range []string{"/v2/test/manifests/transfer", "/v2/test/blobs/" + reg.Image.Descriptors[0].Digest.String()} {
		meta := httptest.NewRecorder()
		reg.ServeHTTP(meta, httptest.NewRequestWithContext(ctx, http.MethodGet, path, nil))
		if meta.Code != http.StatusOK {
			t.Fatalf("metadata blocked: %d", meta.Code)
		}
	}
	<-reg.streams
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("queued blob did not resume")
	}
	if response.Code != http.StatusOK {
		t.Fatalf("resumed request: %d", response.Code)
	}
	if err := image.Verify(response.Body.Bytes(), d); err != nil {
		t.Fatal(err)
	}
	if len(reg.streams) != cap(reg.streams)-1 {
		t.Fatal("stream slot leaked")
	}
	if reg.Cache.Metrics().PeakBuffers > 2*transfer.FrameSize {
		t.Fatal("exceeded cache budget")
	}
}

func TestRegistryCancelsQueuedBlobRequest(t *testing.T) {
	reg := saturatedRegistry(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		reg.ServeHTTP(response, httptest.NewRequestWithContext(ctx, http.MethodGet, "/v2/test/blobs/"+reg.Image.Descriptors[1].Digest.String(), nil))
	}()
	select {
	case <-done:
		t.Fatalf("busy receiver rejected request: %d", response.Code)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancelled request remained queued")
	}
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("cancelled request: %d", response.Code)
	}
	if len(reg.streams) != cap(reg.streams) || reg.Cache.Metrics().Acquired != 0 {
		t.Fatal("cancelled waiter acquired a slot or payload")
	}
}
