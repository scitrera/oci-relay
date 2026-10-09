// SPDX-FileCopyrightText: 2026 Spark Arena
// SPDX-License-Identifier: Apache-2.0

package transfer

import (
	"context"
	"errors"
	"io"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

// Verifier returns a barrier for this cache's own writers, or nil for any other
// writer/descriptor. A source can use it instead of hashing the same bytes twice.
// Call it after all writes and the source's EOF checks, before publishing a disk
// cache entry. Like Write, it must not be called concurrently or after Fetch
// returns. It does not release the last frame: Fetch must also succeed.
// The private method prevents arbitrary writers from claiming verification.
func Verifier(w io.Writer, d v1.Descriptor) func() error {
	if v, ok := w.(interface {
		verifier(v1.Descriptor) func() error
	}); ok {
		return v.verifier(d)
	}
	return nil
}

type pipelineItem struct {
	data   []byte
	verify chan error
}

type pipelineWriter struct {
	ctx     context.Context
	pool    chan []byte
	pending chan pipelineItem
	desc    v1.Descriptor
}

func (c *Cache) fetchPipelined(e *entry, w *producer) error {
	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()
	p := &pipelineWriter{ctx: ctx, pool: make(chan []byte, c.pipelineFrames), pending: make(chan pipelineItem, c.pipelineFrames), desc: e.desc}
	for i := 0; i < c.pipelineFrames; i++ {
		p.pool <- make([]byte, FrameSize)
	}
	allocated := int64(c.pipelineFrames * FrameSize)
	resident := c.resident.Add(allocated)
	for old := c.peak.Load(); resident > old; old = c.peak.Load() {
		if c.peak.CompareAndSwap(old, resident) {
			break
		}
	}
	defer c.resident.Add(-allocated)
	done := make(chan struct{})
	var writeErr error
	go func() {
		defer close(done)
		for item := range p.pending {
			if item.verify != nil {
				if writeErr == nil {
					writeErr = w.verify()
				}
				item.verify <- writeErr
			} else {
				if writeErr == nil {
					var n int
					n, writeErr = w.Write(item.data)
					if writeErr == nil && n != len(item.data) {
						writeErr = io.ErrShortWrite
					}
				}
				p.pool <- item.data[:FrameSize]
			}
			if writeErr != nil {
				cancel()
			}
		}
	}()
	fetchErr := c.source.Fetch(ctx, e.desc, p)
	if fetchErr != nil {
		// A source failure is terminal even if queued data is still blocked by
		// a slow consumer. Stop producer.Write's entry-level waits immediately.
		e.cancel()
	}
	close(p.pending)
	<-done
	return errors.Join(fetchErr, writeErr)
}

func (p *pipelineWriter) Write(b []byte) (int, error) {
	total := len(b)
	for len(b) > 0 {
		if err := p.ctx.Err(); err != nil {
			return total - len(b), err
		}
		var buf []byte
		select {
		case buf = <-p.pool:
		case <-p.ctx.Done():
			return total - len(b), p.ctx.Err()
		}
		n := copy(buf, b)
		select {
		case p.pending <- pipelineItem{data: buf[:n]}:
			b = b[n:]
		case <-p.ctx.Done():
			p.pool <- buf
			return total - len(b), p.ctx.Err()
		}
	}
	return total, nil
}

func (p *pipelineWriter) verifier(d v1.Descriptor) func() error {
	if d.Digest != p.desc.Digest || d.Size != p.desc.Size {
		return nil
	}
	return func() error {
		result := make(chan error, 1)
		select {
		case p.pending <- pipelineItem{verify: result}:
		case <-p.ctx.Done():
			return p.ctx.Err()
		}
		select {
		case err := <-result:
			return err
		case <-p.ctx.Done():
			return p.ctx.Err()
		}
	}
}
