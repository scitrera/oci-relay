// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package transfer

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

func TestStripesShareAcquisitionAndSmallRing(t *testing.T) {
	for _, count := range []int{2, 4, 8} {
		t.Run(string(rune('0'+count)), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			payload := make([]byte, 37*FrameSize+317)
			for i := range payload {
				payload[i] = byte(i/FrameSize + i%251)
			}
			src := &fixtureSource{data: payload}
			c, err := New(ctx, src, 2*FrameSize, 1) // Smaller than even one full stripe cycle.
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			d := v1.Descriptor{Digest: digest.FromBytes(payload), Size: int64(len(payload))}
			lanes, err := c.OpenStripes(ctx, d, count, 2*FrameSize)
			if err != nil {
				t.Fatal(err)
			}
			other, err := c.OpenStripes(ctx, d, count, 2*FrameSize)
			if err != nil {
				t.Fatal(err)
			}
			lanes = append(lanes, other...)
			ordinary, err := c.Open(ctx, d)
			if err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer ordinary.Close()
				got, err := io.ReadAll(ordinary)
				if err != nil || !bytes.Equal(got, payload) {
					t.Errorf("ordinary reader alongside stripes: %v", err)
				}
			}()
			for lane, reader := range lanes {
				lane := lane % count
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer reader.Close()
					got, err := io.ReadAll(reader)
					var want []byte
					for offset := lane * 2 * FrameSize; offset < len(payload); offset += count * 2 * FrameSize {
						want = append(want, payload[offset:min(offset+2*FrameSize, len(payload))]...)
					}
					if err != nil || !bytes.Equal(got, want) {
						t.Errorf("lane %d: %v, got %d bytes want %d", lane, err, len(got), len(want))
					}
				}()
			}
			wg.Wait()
			if src.calls.Load() != 1 || c.Metrics().VerifiedBlobs != 1 || c.Metrics().PeakBuffers > 2*FrameSize {
				t.Fatalf("unshared/unbounded stripes: %+v calls=%d", c.Metrics(), src.calls.Load())
			}
		})
	}
}

func TestStripeFailureNeverCompletesCorruptLayer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	payload := bytes.Repeat([]byte("x"), 9*FrameSize+1)
	c, err := New(ctx, &fixtureSource{data: payload}, 2*FrameSize, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	lanes, err := c.OpenStripes(ctx, v1.Descriptor{Digest: digest.FromBytes([]byte("wrong")), Size: int64(len(payload))}, 4, FrameSize)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for _, r := range lanes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer r.Close()
			if _, err := io.Copy(io.Discard, r); err == nil {
				t.Error("corrupt stripe completed")
			}
		}()
	}
	wg.Wait()
	if c.Metrics().VerifiedBlobs != 0 {
		t.Fatal("corruption verified")
	}
}
