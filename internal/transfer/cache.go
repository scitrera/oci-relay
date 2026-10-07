// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only
// Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.

package transfer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"d7y.io/dragonfly/v2/pkg/stats"
	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	backpressure "github.com/scitrera/go-backpressure"
)

const FrameSize = 64 << 10

var ErrLagged = errors.New("receiver fell behind retained window; retry the blob")

type Source interface {
	Fetch(context.Context, v1.Descriptor, io.Writer) error
}
type Metrics struct {
	Acquired             int64   `json:"acquired_bytes"`
	Replays              int64   `json:"replays"`
	PeakBuffers          int64   `json:"peak_buffer_bytes"`
	RecentBytesPerSecond float64 `json:"recent_bytes_per_second"`
	ActiveAcquisitions   int64   `json:"active_acquisitions"`
	PeakAcquisitions     int64   `json:"peak_acquisitions"`
	VerifiedBytes        int64   `json:"verified_bytes"`
	VerifiedBlobs        int64   `json:"verified_blobs"`
	LastVerifiedSeconds  float64 `json:"last_verified_seconds"`
	JoinWaits            int64   `json:"join_waits"`
	JoinWaitSeconds      float64 `json:"join_wait_seconds"`
}
type Cache struct {
	ctx                                                            context.Context
	cancel                                                         context.CancelFunc
	source                                                         Source
	slots                                                          *backpressure.Semaphore
	maxSlots                                                       int
	chunks                                                         int
	pipelineFrames                                                 int
	mu                                                             sync.Mutex
	entries                                                        map[digest.Digest]*entry
	live                                                           map[*entry]bool
	seen                                                           map[digest.Digest]bool
	used                                                           int
	peak                                                           atomic.Int64
	resident                                                       atomic.Int64
	acquired                                                       atomic.Int64
	replays                                                        atomic.Int64
	active, peakActive, verifiedBytes, verifiedBlobs, lastVerified atomic.Int64
	started                                                        time.Time
	rate                                                           *stats.RollingWindow
	LagTimeout                                                     time.Duration
	// Enable only for cheap, independently readable sources such as local files.
	// A late reader may use another bounded slot instead of waiting for an
	// entire large acquisition whose first bytes have already been evicted.
	ConcurrentReplay bool
	// Retain a new stream's prefix until this many readers join or the bounded
	// window from creation expires. Zero disables coalescing waits. Set before use.
	JoinReaders          int
	JoinWindow           time.Duration
	joinWaits, joinNanos atomic.Int64
	wg                   sync.WaitGroup
}
type entry struct {
	cache    *Cache
	ctx      context.Context
	cancel   context.CancelFunc
	desc     v1.Descriptor
	mu       sync.Mutex
	changed  chan struct{}
	chunks   [][]byte
	base     int64
	produced int64
	done     bool
	err      error
	readers  int
	joined   int // Logical consumers; stripe lanes do not count as extra receivers.
	active   map[*reader]bool
	created  time.Time
}

func New(ctx context.Context, source Source, memory int64, parallel int) (*Cache, error) {
	if parallel < 1 || parallel > 64 || memory < int64(parallel*FrameSize*2) || memory > 1<<40 {
		return nil, errors.New("invalid buffer budget or acquisition limit")
	}
	ctx, cancel := context.WithCancel(ctx)
	return &Cache{ctx: ctx, cancel: cancel, source: source, maxSlots: parallel, chunks: int(memory / int64(parallel*FrameSize)), entries: map[digest.Digest]*entry{}, live: map[*entry]bool{}, seen: map[digest.Digest]bool{}, rate: stats.NewRollingWindow(64), LagTimeout: 30 * time.Second, started: time.Now(),
		slots: backpressure.NewSemaphore(1, parallel, backpressure.SemaphoreShortTimeout(time.Minute), backpressure.SemaphoreLongTimeout(10*time.Minute))}, nil
}

// NewSource overlaps source reads/reconstruction with hashing and cache writes.
// Its queue is reserved from the same memory budget, never added to it. Very
// small budgets keep the synchronous path to preserve a useful retained window.
func NewSource(ctx context.Context, source Source, memory int64, parallel int) (*Cache, error) {
	c, err := New(ctx, source, memory, parallel)
	if err == nil && c.chunks >= 16 {
		c.pipelineFrames = 4
		c.chunks -= c.pipelineFrames
	}
	return c, err
}
func (c *Cache) Close() { c.mu.Lock(); c.cancel(); c.mu.Unlock(); c.wg.Wait(); c.slots.Close() }
func (c *Cache) Metrics() Metrics {
	return Metrics{Acquired: c.acquired.Load(), Replays: c.replays.Load(), PeakBuffers: c.peak.Load(), RecentBytesPerSecond: c.rate.Snapshot().Mean,
		ActiveAcquisitions: c.active.Load(), PeakAcquisitions: c.peakActive.Load(), VerifiedBytes: c.verifiedBytes.Load(), VerifiedBlobs: c.verifiedBlobs.Load(), LastVerifiedSeconds: float64(c.lastVerified.Load()) / float64(time.Second),
		JoinWaits: c.joinWaits.Load(), JoinWaitSeconds: float64(c.joinNanos.Load()) / float64(time.Second)}
}
func (e *entry) notify() { close(e.changed); e.changed = make(chan struct{}) }

// Caller holds c.mu and e.mu; only completed, unreferenced entries are retired.
func (c *Cache) retire(e *entry) {
	delete(c.live, e)
	if c.entries[e.desc.Digest] == e {
		delete(c.entries, e.desc.Digest)
	}
	c.used--
	c.resident.Add(-int64(len(e.chunks) * FrameSize))
	e.chunks = nil
}
func (c *Cache) Open(ctx context.Context, d v1.Descriptor) (io.ReadCloser, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := c.ctx.Err(); err != nil {
			return nil, err
		}
		c.mu.Lock()
		if err := c.ctx.Err(); err != nil {
			c.mu.Unlock()
			return nil, err
		}
		if e := c.entries[d.Digest]; e != nil {
			e.mu.Lock()
			if e.desc.Size != d.Size {
				e.mu.Unlock()
				c.mu.Unlock()
				return nil, errors.New("descriptor size conflict")
			}
			if e.base == 0 && e.err == nil && (e.done || e.ctx.Err() == nil) {
				r := &reader{ctx: ctx, e: e}
				e.readers++
				e.joined++
				e.active[r] = true
				e.notify()
				e.mu.Unlock()
				c.mu.Unlock()
				return r, nil
			}
			if !e.done && !c.ConcurrentReplay {
				ch := e.changed
				e.mu.Unlock()
				c.mu.Unlock()
				select {
				case <-ch:
					continue
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-c.ctx.Done():
					return nil, c.ctx.Err()
				}
			}
			if e.done && e.readers == 0 {
				c.retire(e)
			}
			e.mu.Unlock()
		}
		// Completed entries can be discarded without affecting consumers that still hold them.
		for e := range c.live {
			if c.used < c.maxSlots {
				break
			}
			e.mu.Lock()
			if e.done && e.readers == 0 {
				c.retire(e)
			}
			e.mu.Unlock()
		}
		if c.used >= c.maxSlots {
			c.mu.Unlock()
			timer := time.NewTimer(10 * time.Millisecond)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-c.ctx.Done():
				timer.Stop()
				return nil, c.ctx.Err()
			}
			continue
		}
		// A previous errored entry with active readers must not be overwritten.
		if c.entries[d.Digest] != nil && !c.ConcurrentReplay {
			c.mu.Unlock()
			select {
			case <-time.After(time.Millisecond):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		e := &entry{cache: c, desc: d, changed: make(chan struct{}), readers: 1, joined: 1, active: map[*reader]bool{}, created: time.Now()}
		e.ctx, e.cancel = context.WithCancel(c.ctx)
		r := &reader{ctx: ctx, e: e}
		e.active[r] = true
		c.entries[d.Digest] = e
		c.live[e] = true
		c.used++
		if c.seen[d.Digest] {
			c.replays.Add(1)
		}
		c.seen[d.Digest] = true
		// Descriptor cardinality is bounded by validated image metadata.
		c.wg.Add(1)
		c.mu.Unlock()
		go c.produce(e)
		return r, nil
	}
}
func (c *Cache) produce(e *entry) {
	defer c.wg.Done()
	defer e.cancel()
	err := c.slots.Acquire(e.ctx, 0, 1)
	if err == nil {
		active := c.active.Add(1)
		for old := c.peakActive.Load(); active > old; old = c.peakActive.Load() {
			if c.peakActive.CompareAndSwap(old, active) {
				break
			}
		}
		start := time.Now()
		w := &producer{e: e, hash: sha256.New()}
		if c.pipelineFrames > 0 {
			err = c.fetchPipelined(e, w)
		} else {
			err = c.source.Fetch(e.ctx, e.desc, w)
		}
		if err == nil {
			err = w.verify()
		}
		if err == nil {
			c.rate.Add(float64(w.n) / time.Since(start).Seconds())
			c.verifiedBytes.Add(w.n)
			c.verifiedBlobs.Add(1)
			// Serialize the timestamp maximum across simultaneous acquisitions.
			now := time.Since(c.started).Nanoseconds()
			for old := c.lastVerified.Load(); now > old; old = c.lastVerified.Load() {
				if c.lastVerified.CompareAndSwap(old, now) {
					break
				}
			}
		}
		c.active.Add(-1)
		c.slots.Release(1)
	}
	e.mu.Lock()
	e.done = true
	e.err = err
	e.notify()
	e.mu.Unlock()
}

type producer struct {
	e    *entry
	hash hash.Hash
	n    int64
}

func (p *producer) verify() error {
	if err := p.e.ctx.Err(); err != nil {
		return err
	}
	if p.n != p.e.desc.Size {
		return fmt.Errorf("blob length mismatch: got %d, expected %d", p.n, p.e.desc.Size)
	}
	if "sha256:"+hex.EncodeToString(p.hash.Sum(nil)) != string(p.e.desc.Digest) {
		return errors.New("blob digest mismatch")
	}
	return nil
}

func (p *producer) verifier(d v1.Descriptor) func() error {
	if d.Digest != p.e.desc.Digest || d.Size != p.e.desc.Size {
		return nil
	}
	return p.verify
}

func (p *producer) Write(b []byte) (int, error) {
	e := p.e
	if err := e.ctx.Err(); err != nil {
		return 0, err
	}
	if int64(len(b)) > e.desc.Size-p.n {
		return 0, errors.New("source exceeded blob length")
	}
	total := len(b)
	for len(b) > 0 {
		if err := e.ctx.Err(); err != nil {
			return total - len(b), err
		}
		n := min(len(b), FrameSize)
		e.mu.Lock()
		// Fill the last fixed-size chunk before appending the next one.
		if len(e.chunks) > 0 && len(e.chunks[len(e.chunks)-1]) < FrameSize {
			last := len(e.chunks) - 1
			n = min(n, FrameSize-len(e.chunks[last]))
			e.chunks[last] = append(e.chunks[last], b[:n]...)
		} else {
			var chunk []byte
			if len(e.chunks) == e.cache.chunks {
				// Start fetching immediately, but briefly preserve the initial window
				// for peers that have not requested this layer yet. Cached peers need
				// not join: this always expires, without holding additional payload.
				if err := e.join(); err != nil {
					e.mu.Unlock()
					return total - len(b), err
				}
				deadline := time.Now().Add(e.cache.LagTimeout)
				for {
					blocked := false
					for r := range e.active {
						if !r.closed && r.err == nil && r.offset < e.base+int64(len(e.chunks[0])) {
							blocked = true
						}
					}
					if !blocked {
						break
					}
					remaining := time.Until(deadline)
					if remaining <= 0 {
						for r := range e.active {
							if r.offset < e.base+int64(len(e.chunks[0])) {
								r.err = ErrLagged
								delete(e.active, r)
							}
						}
						e.notify()
						if len(e.active) == 0 {
							e.cancel()
							e.mu.Unlock()
							return total - len(b), ErrLagged
						}
						break
					}
					ch := e.changed
					e.mu.Unlock()
					timer := time.NewTimer(remaining)
					select {
					case <-ch:
						timer.Stop()
					case <-timer.C:
					case <-e.ctx.Done():
						timer.Stop()
						return total - len(b), e.ctx.Err()
					}
					e.mu.Lock()
				}
				chunk = e.chunks[0][:0]
				e.base += int64(len(e.chunks[0]))
				copy(e.chunks, e.chunks[1:])
				e.chunks = e.chunks[:len(e.chunks)-1]
			} else {
				chunk = make([]byte, 0, FrameSize)
				resident := e.cache.resident.Add(FrameSize)
				for old := e.cache.peak.Load(); resident > old; old = e.cache.peak.Load() {
					if e.cache.peak.CompareAndSwap(old, resident) {
						break
					}
				}
			}
			e.chunks = append(e.chunks, append(chunk, b[:n]...))
		}
		e.produced += int64(n)
		e.notify()
		e.mu.Unlock()
		_, _ = p.hash.Write(b[:n])
		p.n += int64(n)
		e.cache.acquired.Add(int64(n))
		b = b[n:]
	}
	return total, nil
}

// Caller holds e.mu; the lock is reacquired after every wait, including errors.
func (e *entry) join() error {
	if e.base != 0 || e.cache.JoinReaders <= 1 || e.cache.JoinWindow <= 0 {
		return nil
	}
	deadline := e.created.Add(e.cache.JoinWindow)
	var started time.Time
	defer func() {
		if !started.IsZero() {
			e.cache.joinNanos.Add(time.Since(started).Nanoseconds())
		}
	}()
	for e.joined < e.cache.JoinReaders && time.Until(deadline) > 0 {
		if started.IsZero() {
			started = time.Now()
			e.cache.joinWaits.Add(1)
		}
		ch := e.changed
		e.mu.Unlock()
		timer := time.NewTimer(time.Until(deadline))
		select {
		case <-ch:
		case <-timer.C:
		case <-e.ctx.Done():
		}
		timer.Stop()
		e.mu.Lock()
		if err := e.ctx.Err(); err != nil {
			return err
		}
	}
	return nil
}

type reader struct {
	ctx         context.Context
	e           *entry
	offset      int64
	closed      bool
	err         error
	stripeSize  int64
	stripeCount int
}

func (r *reader) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	e := r.e
	for {
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
		e.mu.Lock()
		if r.closed {
			e.mu.Unlock()
			return 0, io.ErrClosedPipe
		}
		if r.err != nil {
			err := r.err
			e.mu.Unlock()
			return 0, err
		}
		if e.err != nil {
			err := e.err
			e.mu.Unlock()
			return 0, err
		}
		if r.offset < e.base {
			e.mu.Unlock()
			return 0, ErrLagged
		}
		if r.offset < e.produced {
			index := int((r.offset - e.base) / FrameSize)
			offset := int((r.offset - e.base) % FrameSize)
			available := e.chunks[index][offset:]
			n := min(len(b), len(available))
			if r.stripeCount > 1 {
				n = min(n, int(r.stripeSize-r.offset%r.stripeSize))
			}
			// Withhold the tail until the entire descriptor is verified.
			if r.offset+int64(n) == e.desc.Size && !e.done {
				ch := e.changed
				e.mu.Unlock()
				select {
				case <-ch:
					continue
				case <-r.ctx.Done():
					return 0, r.ctx.Err()
				case <-e.cache.ctx.Done():
					return 0, e.cache.ctx.Err()
				}
			}
			copy(b, available[:n])
			r.offset += int64(n)
			if r.stripeCount > 1 && r.offset%r.stripeSize == 0 {
				r.offset = min(e.desc.Size, r.offset+int64(r.stripeCount-1)*r.stripeSize)
			}
			e.notify()
			e.mu.Unlock()
			return n, nil
		}
		if e.done {
			e.mu.Unlock()
			return 0, io.EOF
		}
		ch := e.changed
		e.mu.Unlock()
		select {
		case <-ch:
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		case <-e.cache.ctx.Done():
			return 0, e.cache.ctx.Err()
		}
	}
}
func (r *reader) Close() error {
	r.e.mu.Lock()
	defer r.e.mu.Unlock()
	if !r.closed {
		r.closed = true
		r.e.readers--
		delete(r.e.active, r)
		if len(r.e.active) == 0 && !r.e.done {
			r.e.cancel()
		}
		r.e.notify()
	}
	return nil
}
