// SPDX-FileCopyrightText: 2026 Spark Arena
// SPDX-License-Identifier: Apache-2.0

package transfer

import (
	"bytes"
	"context"
	"errors"
	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fixtureSource struct {
	data  []byte
	calls atomic.Int64
	gate  chan struct{}
}

func (s *fixtureSource) Fetch(ctx context.Context, d v1.Descriptor, w io.Writer) error {
	s.calls.Add(1)
	if s.gate != nil {
		select {
		case <-s.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	_, err := io.Copy(w, bytes.NewReader(s.data))
	return err
}
func TestSharedLargeBlobAndBudget(t *testing.T) {
	payload := bytes.Repeat([]byte("image bytes"), 100000)
	src := &fixtureSource{data: payload, gate: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := New(ctx, src, 4*FrameSize, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	d := v1.Descriptor{Digest: digest.FromBytes(payload), Size: int64(len(payload))}
	a, err := c.Open(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.Open(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for _, r := range []io.ReadCloser{a, b} {
		wg.Add(1)
		go func(r io.ReadCloser) {
			defer wg.Done()
			defer r.Close()
			got, err := io.ReadAll(r)
			if err != nil {
				t.Error(err)
			} else if !bytes.Equal(got, payload) {
				t.Error("wrong bytes")
			}
		}(r)
	}
	close(src.gate)
	wg.Wait()
	if src.calls.Load() != 1 {
		t.Fatalf("source read %d times", src.calls.Load())
	}
	if c.Metrics().PeakBuffers > 4*FrameSize {
		t.Fatal("exceeded budget")
	}
}
func TestDigestFailureWithholdsTail(t *testing.T) {
	src := &fixtureSource{data: []byte("bad")}
	c, err := New(context.Background(), src, 2*FrameSize, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	r, err := c.Open(context.Background(), v1.Descriptor{Digest: digest.FromBytes([]byte("yes")), Size: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err == nil || len(got) == 3 {
		t.Fatalf("corrupt response appeared complete: %q %v", got, err)
	}
}
func TestSlowReaderDetachedWithoutBlockingHealthyReader(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 16*FrameSize)
	src := &fixtureSource{data: payload, gate: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := New(ctx, src, 4*FrameSize, 1)
	if err != nil {
		t.Fatal(err)
	}
	c.LagTimeout = 30 * time.Millisecond
	defer c.Close()
	d := v1.Descriptor{Digest: digest.FromBytes(payload), Size: int64(len(payload))}
	slow, err := c.Open(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Close()
	fast, err := c.Open(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	defer fast.Close()
	close(src.gate)
	got, err := io.ReadAll(fast)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("fast receiver lost bytes")
	}
	_, err = slow.Read(make([]byte, 1))
	if !errors.Is(err, ErrLagged) {
		t.Fatalf("expected lag error, got %v", err)
	}
}

func TestLastReaderCancellationStopsSourceAndAllowsRetry(t *testing.T) {
	payload := []byte("retry me")
	src := &fixtureSource{data: payload, gate: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := New(ctx, src, 2*FrameSize, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	d := v1.Descriptor{Digest: digest.FromBytes(payload), Size: int64(len(payload))}
	r, err := c.Open(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	close(src.gate)
	r, err = c.Open(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("retry: %q %v", got, err)
	}
	if c.Metrics().Replays != 1 {
		t.Fatal("cancelled acquisition not counted as replay")
	}
}

type seekableFixture struct {
	data       []byte
	calls      atomic.Int64
	firstReady chan struct{}
	release    chan struct{}
}

func (s *seekableFixture) Fetch(ctx context.Context, d v1.Descriptor, w io.Writer) error {
	if s.calls.Add(1) == 1 {
		if _, err := w.Write(s.data[:4*FrameSize]); err != nil {
			return err
		}
		close(s.firstReady)
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
		_, err := w.Write(s.data[4*FrameSize:])
		return err
	}
	_, err := w.Write(s.data)
	return err
}

func TestSeekableLateReaderDoesNotWaitForWholeBlob(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	src := &seekableFixture{data: bytes.Repeat([]byte("x"), 8*FrameSize), firstReady: make(chan struct{}), release: make(chan struct{})}
	c, err := New(ctx, src, 4*FrameSize, 2)
	if err != nil {
		t.Fatal(err)
	}
	c.ConcurrentReplay = true
	defer c.Close()
	d := v1.Descriptor{Digest: digest.FromBytes(src.data), Size: int64(len(src.data))}
	first, err := c.Open(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, err = io.CopyN(io.Discard, first, 4*FrameSize); err != nil {
		t.Fatal(err)
	}
	<-src.firstReady // First acquisition is unfinished and its prefix was evicted.
	late, err := c.Open(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(late)
	_ = late.Close()
	if err != nil || !bytes.Equal(got, src.data) {
		t.Fatalf("late reader: %v", err)
	}
	close(src.release)
	if _, err = io.Copy(io.Discard, first); err != nil {
		t.Fatal(err)
	}
	_ = first.Close()
	// Reclaim both windows without losing the fixed aggregate memory bound.
	again, err := c.Open(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = io.Copy(io.Discard, again); err != nil {
		t.Fatal(err)
	}
	_ = again.Close()
	if c.Metrics().Replays != 2 || c.Metrics().PeakBuffers > 4*FrameSize {
		t.Fatalf("incorrect replay accounting/budget: %+v", c.Metrics())
	}
}

func TestJoinWindowRetainsPrefixForLateReceiver(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	payload := bytes.Repeat([]byte("x"), 16*FrameSize)
	src := &fixtureSource{data: payload}
	c, err := New(ctx, src, 4*FrameSize, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.ConcurrentReplay, c.JoinReaders, c.JoinWindow = true, 2, time.Second
	d := v1.Descriptor{Digest: digest.FromBytes(payload), Size: int64(len(payload))}
	first, err := c.Open(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	// Consume the entire per-entry prefix before the other receiver requests it.
	if _, err = io.CopyN(io.Discard, first, 2*FrameSize); err != nil {
		t.Fatal(err)
	}
	late, err := c.Open(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	defer late.Close()
	finished := make(chan error, 1)
	go func() { _, err := io.Copy(io.Discard, first); finished <- err }()
	got, err := io.ReadAll(late)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("late receiver: %v", err)
	}
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if src.calls.Load() != 1 || c.Metrics().Replays != 0 || c.Metrics().PeakBuffers > 4*FrameSize {
		t.Fatalf("not coalesced within budget: %+v", c.Metrics())
	}
}

func TestJoinWindowExpiresWhenOtherReceiverHasCachedLayer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	payload := bytes.Repeat([]byte("x"), 8*FrameSize)
	c, err := New(ctx, &fixtureSource{data: payload}, 2*FrameSize, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.JoinReaders, c.JoinWindow = 3, 20*time.Millisecond
	r, err := c.Open(ctx, v1.Descriptor{Digest: digest.FromBytes(payload), Size: int64(len(payload))})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err = io.Copy(io.Discard, r); err != nil {
		t.Fatal(err)
	}
	if c.Metrics().JoinWaits != 1 || c.Metrics().JoinWaitSeconds <= 0 {
		t.Fatalf("missing bounded wait: %+v", c.Metrics())
	}
}

func TestCancellationInterruptsJoinWindow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	payload := bytes.Repeat([]byte("x"), 8*FrameSize)
	c, err := New(ctx, &fixtureSource{data: payload}, 2*FrameSize, 1)
	if err != nil {
		t.Fatal(err)
	}
	c.JoinReaders, c.JoinWindow = 2, time.Hour
	r, err := c.Open(ctx, v1.Descriptor{Digest: digest.FromBytes(payload), Size: int64(len(payload))})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = io.CopyN(io.Discard, r, 2*FrameSize); err != nil {
		t.Fatal(err)
	}
	_ = r.Close()
	finished := make(chan struct{})
	go func() { c.Close(); close(finished) }()
	select {
	case <-finished:
	case <-ctx.Done():
		t.Fatal("join prevented cancellation")
	}
}
