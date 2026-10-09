// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type registryRanges struct {
	streams                                                            int
	chunk, threshold, budget                                           int64
	mu                                                                 sync.Mutex
	changed                                                            chan struct{}
	used, peakUsed, active, peakActive, requests, downloads, fallbacks atomic.Int64
}

func (r *registryRanges) configure(o RegistryOptions) error {
	r.streams, r.chunk, r.threshold, r.budget = o.RangeConcurrency, o.RangeChunkBytes, o.RangeThresholdBytes, o.RangeBufferBytes
	if r.streams == 0 {
		r.streams = 4
	}
	if r.chunk == 0 {
		r.chunk = 16 << 20
	}
	if r.threshold == 0 {
		r.threshold = 256 << 20
	}
	if r.budget == 0 {
		r.budget = 128 << 20
	}
	if r.streams < 1 || r.streams > 16 || r.chunk < 64<<10 || r.chunk > 64<<20 ||
		r.threshold < 64<<10 || r.threshold > 1<<50 || r.budget < 64<<10 || r.budget > 1<<30 {
		return errors.New("invalid registry range limits: streams 1..16, chunk 64 KiB..64 MiB, threshold 64 KiB..1 PiB, buffer 64 KiB..1 GiB")
	}
	r.streams = min(r.streams, int(r.budget/r.chunk))
	if r.streams < 2 {
		r.streams, r.budget = 1, 0
	}
	r.changed = make(chan struct{})
	return nil
}

// RangeBufferBudget is the maximum extra payload memory used for ordered range
// read-ahead. CLI callers reserve this from their total source memory budget.
func (r *Registry) RangeBufferBudget() int64 { return r.ranges.budget }

func recordPeak(peak *atomic.Int64, value int64) {
	for old := peak.Load(); value > old; old = peak.Load() {
		if peak.CompareAndSwap(old, value) {
			return
		}
	}
}

// Reserve a whole per-blob window atomically. Reserving individual pieces could
// deadlock when later offsets occupy all memory ahead of a missing first piece.
func (r *registryRanges) reserve(ctx context.Context, size int64) (func(), error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		r.mu.Lock()
		if size <= r.budget-r.used.Load() {
			recordPeak(&r.peakUsed, r.used.Add(size))
			r.mu.Unlock()
			return func() {
				r.mu.Lock()
				r.used.Add(-size)
				close(r.changed)
				r.changed = make(chan struct{})
				r.mu.Unlock()
			}, nil
		}
		changed := r.changed
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

type rangeBody struct {
	io.ReadCloser
	once   sync.Once
	active *atomic.Int64
	err    error
}

func (b *rangeBody) Close() error {
	b.once.Do(func() {
		b.err = b.ReadCloser.Close()
		b.active.Add(-1)
	})
	return b.err
}

func (r *Registry) openRange(ctx context.Context, d v1.Descriptor, offset, length int64) (*http.Response, error) {
	r.ranges.requests.Add(1)
	recordPeak(&r.ranges.peakActive, r.ranges.active.Add(1))
	resp, err := r.http.getRange(ctx, r.http.ref.BlobURL(d.Digest.String()), "", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	if err != nil {
		r.ranges.active.Add(-1)
		return nil, err
	}
	resp.Body = &rangeBody{ReadCloser: resp.Body, active: &r.ranges.active}
	return resp, nil
}

// A digest-addressed blob plus full SHA verification pins representation across
// requests. Accept exactly the requested interval and known total, never a
// wildcard total, overlapping subset, multipart response, or rewritten body.
func validateRegistryRange(resp *http.Response, d v1.Descriptor, offset, length int64) error {
	if resp.StatusCode != http.StatusPartialContent {
		return errors.New("registry stopped honoring byte ranges")
	}
	value := resp.Header.Get("Content-Range")
	if !strings.HasPrefix(value, "bytes ") {
		return errors.New("registry range missing byte Content-Range")
	}
	interval, total, ok := strings.Cut(strings.TrimPrefix(value, "bytes "), "/")
	first, last, split := strings.Cut(interval, "-")
	start, e1 := strconv.ParseInt(first, 10, 64)
	end, e2 := strconv.ParseInt(last, 10, 64)
	size, e3 := strconv.ParseInt(total, 10, 64)
	if !ok || !split || e1 != nil || e2 != nil || e3 != nil || start != offset || end != offset+length-1 || size != d.Size ||
		(resp.ContentLength >= 0 && resp.ContentLength != length) || strings.HasPrefix(resp.Header.Get("Content-Type"), "multipart/") {
		return errors.New("registry range interval or length mismatch")
	}
	if hash := resp.Header.Get("Docker-Content-Digest"); hash != "" && hash != d.Digest.String() {
		return errors.New("registry range digest header mismatch")
	}
	return nil
}

func (r *Registry) readRange(ctx context.Context, d v1.Descriptor, offset int64, data []byte, resp *http.Response) error {
	var err error
	if resp == nil {
		resp, err = r.openRange(ctx, d, offset, int64(len(data)))
		if err != nil {
			return err
		}
	}
	defer resp.Body.Close()
	if err = validateRegistryRange(resp, d, offset, int64(len(data))); err != nil {
		return err
	}
	reader := &countRegistryReader{r: resp.Body, count: &r.upstream}
	if _, err = io.ReadFull(reader, data); err != nil {
		return safeRequestError(ctx, err)
	}
	var extra [1]byte
	if n, err := io.ReadFull(reader, extra[:]); n != 0 || err != io.EOF {
		return errors.New("registry range has excess bytes or no verified end")
	}
	return nil
}

// Complete bounded ranges out of order, but feed the existing cache/hash writer
// in blob order. Reuse each slot only after its bytes have been consumed. There
// is no full-layer staging, second SHA pass, or unbounded completed-piece queue.
func (r *Registry) downloadRanges(parent context.Context, d v1.Descriptor, w io.Writer, verify func() error) (returnErr error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	chunk := r.ranges.chunk
	lanes := min(r.ranges.streams, int((d.Size+chunk-1)/chunk))
	release, err := r.ranges.reserve(ctx, int64(lanes)*chunk)
	if err != nil {
		return err
	}
	defer func() {
		if release != nil {
			release()
		}
	}()
	first, err := r.openRange(ctx, d, 0, min(chunk, d.Size))
	if err != nil {
		return err
	}
	defer first.Body.Close()
	r.downloads.Add(1)
	// Probe only the first piece. A 200 is already the complete blob stream;
	// consume it directly instead of making a redundant request. A 416 permits
	// one ordinary GET before any bytes have reached the destination writer.
	if first.StatusCode == http.StatusOK || first.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		r.ranges.fallbacks.Add(1)
		// Whole-body fallback needs no reordering window. Free the reservation
		// while this stream runs so other layers can still use parallel ranges.
		release()
		release = nil
		if first.StatusCode == http.StatusRequestedRangeNotSatisfiable {
			first.Body.Close()
			first, err = r.http.get(ctx, r.http.ref.BlobURL(d.Digest.String()), "")
			if err != nil {
				return err
			}
		}
		return r.downloadResponse(ctx, d, w, verify, first)
	}
	if err = validateRegistryRange(first, d, 0, min(chunk, d.Size)); err != nil {
		return err
	}
	r.ranges.downloads.Add(1)
	type slot struct {
		data  []byte
		ready chan error
	}
	slots := make([]slot, lanes)
	var wg sync.WaitGroup
	var once sync.Once
	var firstErr error
	defer func() {
		cancel()
		wg.Wait()
		if firstErr != nil && (returnErr == nil || errors.Is(returnErr, context.Canceled)) {
			returnErr = firstErr
		}
	}()
	start := func(index int64, response *http.Response) {
		s := &slots[index%int64(lanes)]
		offset := index * chunk
		length := min(chunk, d.Size-offset)
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := r.readRange(ctx, d, offset, s.data[:length], response)
			if err != nil {
				once.Do(func() { firstErr = err; cancel() })
			}
			s.ready <- err
		}()
	}
	for i := range slots {
		slots[i] = slot{data: make([]byte, chunk), ready: make(chan error, 1)}
		var response *http.Response
		if i == 0 {
			response = first
		}
		start(int64(i), response)
	}
	dst, finish := r.verifyingWriter(d, w, verify)
	var written int64
	for index := int64(0); written < d.Size; index++ {
		s := &slots[index%int64(lanes)]
		select {
		case err := <-s.ready:
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		length := min(chunk, d.Size-written)
		n, err := (&contextWriter{ctx: ctx, w: dst}).Write(s.data[:length])
		if err != nil {
			return err
		}
		if int64(n) != length {
			return io.ErrShortWrite
		}
		written += int64(n)
		if next := index + int64(lanes); next*chunk < d.Size {
			start(next, nil)
		}
	}
	return finish(written)
}
