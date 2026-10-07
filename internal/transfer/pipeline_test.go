// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only
// Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.

package transfer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type sourceFunc func(context.Context, v1.Descriptor, io.Writer) error

func (f sourceFunc) Fetch(ctx context.Context, d v1.Descriptor, w io.Writer) error {
	return f(ctx, d, w)
}

func TestSourcePipelineIntegrityAndBudget(t *testing.T) {
	payload := bytes.Repeat([]byte("pipeline"), 400000)
	d := v1.Descriptor{Digest: digest.FromBytes(payload), Size: int64(len(payload))}
	for _, mode := range []string{"good", "bad-digest", "short", "excess", "late-error"} {
		for _, budget := range []int64{2 * FrameSize, 1 << 20} {
			t.Run(fmt.Sprintf("%s/budget_%d", mode, budget), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				src := sourceFunc(func(ctx context.Context, desc v1.Descriptor, w io.Writer) error {
					verify := Verifier(w, desc)
					if verify == nil || Verifier(w, v1.Descriptor{Digest: desc.Digest, Size: desc.Size + 1}) != nil {
						return errors.New("invalid verification ownership")
					}
					data := bytes.Clone(payload)
					switch mode {
					case "bad-digest":
						data[len(data)-1] ^= 1
					case "short":
						data = data[:len(data)-1]
					case "excess":
						data = append(data, 0)
					}
					if _, err := w.Write(data); err != nil {
						return err
					}
					if err := verify(); err != nil {
						return err
					}
					if mode == "late-error" {
						return errors.New("source EOF validation failed after SHA")
					}
					return nil
				})
				c, err := NewSource(ctx, src, budget, 1)
				if err != nil {
					t.Fatal(err)
				}
				defer c.Close()
				r, err := c.Open(ctx, d)
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				got, err := io.ReadAll(r)
				if mode == "good" {
					if err != nil || !bytes.Equal(got, payload) {
						t.Fatalf("valid stream: %d %v", len(got), err)
					}
				} else if err == nil || int64(len(got)) >= d.Size {
					t.Fatalf("invalid stream released full tail: %d %v", len(got), err)
				}
				if c.Metrics().PeakBuffers > budget {
					t.Fatalf("exceeded budget: %+v", c.Metrics())
				}
			})
		}
	}
}

func TestSourceVerificationDoesNotReleaseTailBeforeFetchCompletes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	verified, finish := make(chan struct{}), make(chan struct{})
	payload := bytes.Repeat([]byte("x"), 3*FrameSize)
	src := sourceFunc(func(ctx context.Context, d v1.Descriptor, w io.Writer) error {
		if _, err := w.Write(payload); err != nil {
			return err
		}
		if err := Verifier(w, d)(); err != nil {
			return err
		}
		close(verified)
		select {
		case <-finish:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	c, err := NewSource(ctx, src, 1<<20, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	r, err := c.Open(ctx, v1.Descriptor{Digest: digest.FromBytes(payload), Size: int64(len(payload))})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	done := make(chan error, 1)
	go func() { _, err := io.Copy(io.Discard, r); done <- err }()
	select {
	case <-verified:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-done:
		t.Fatalf("tail released before source EOF: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(finish)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestSourcePipelineCancellationDrainsWorkers(t *testing.T) {
	for _, verify := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		started := make(chan struct{})
		payload := bytes.Repeat([]byte("x"), 2<<20)
		if verify {
			// Fits in the retained window plus queue, but cannot drain without
			// a consumer. Exercise cancellation of the verification barrier.
			payload = payload[:14*FrameSize]
		}
		src := sourceFunc(func(ctx context.Context, d v1.Descriptor, w io.Writer) error {
			if !verify {
				close(started)
			}
			if _, err := w.Write(payload); err != nil {
				return err
			}
			if verify {
				close(started)
				return Verifier(w, d)()
			}
			return nil
		})
		c, err := NewSource(ctx, src, 1<<20, 1)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		r, err := c.Open(ctx, v1.Descriptor{Digest: digest.FromBytes(payload), Size: int64(len(payload))})
		if err != nil {
			cancel()
			c.Close()
			t.Fatal(err)
		}
		<-started
		cancel()
		done := make(chan struct{})
		go func() { c.Close(); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("pipeline did not stop")
		}
		r.Close()
	}
}

func TestSourcePipelineErrorStopsQueuedWritesWithoutConsumer(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 14*FrameSize)
	want := errors.New("source failed at EOF")
	src := sourceFunc(func(ctx context.Context, d v1.Descriptor, w io.Writer) error {
		if _, err := w.Write(payload); err != nil {
			return err
		}
		return want
	})
	c, err := NewSource(context.Background(), src, 1<<20, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	r, err := c.Open(context.Background(), v1.Descriptor{Digest: digest.FromBytes(payload), Size: int64(len(payload))})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	done := make(chan struct{})
	go func() { c.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("source error waited for consumer/lag timeout")
	}
	e := r.(*reader).e
	if !errors.Is(e.err, want) {
		t.Fatalf("source error lost: %v", e.err)
	}
}
