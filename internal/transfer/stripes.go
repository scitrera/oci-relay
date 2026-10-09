// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package transfer

import (
	"context"
	"errors"
	"io"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

// OpenStripes atomically attaches lanes to ONE ordinary, sequential acquisition.
// Lane i reads pieces i, i+count, ... without copying/discarding intervening bytes.
// All lanes share the existing ring and its backpressure, lag and SHA-256 checks.
// The caller must consume lanes concurrently and close all of them on failure.
func (c *Cache) OpenStripes(ctx context.Context, d v1.Descriptor, count int, piece int64) ([]io.ReadCloser, error) {
	if count < 2 || count > 8 || piece < FrameSize || piece > 64<<20 || piece%FrameSize != 0 || d.Size < int64(count)*piece {
		return nil, errors.New("invalid stripe geometry")
	}
	first, err := c.Open(ctx, d)
	if err != nil {
		return nil, err
	}
	r := first.(*reader)
	e := r.e
	e.mu.Lock()
	defer e.mu.Unlock()
	// The initial reader has held offset zero, so the producer cannot evict the
	// prefix while the other lanes are registered. No per-lane payload buffers.
	r.stripeSize, r.stripeCount = piece, count
	lanes := []io.ReadCloser{r}
	for i := 1; i < count; i++ {
		lane := &reader{ctx: ctx, e: e, offset: int64(i) * piece, stripeSize: piece, stripeCount: count}
		e.active[lane] = true
		e.readers++
		lanes = append(lanes, lane)
	}
	e.notify()
	return lanes, nil
}
