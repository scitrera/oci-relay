// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only
// Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.

package peer

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

// Path pins both ends of a TCP route. Distinct IPs alone do not prove distinct
// physical links; the orchestrator qualifies routes before starting receivers.
type Path struct {
	Endpoint     string `json:"endpoint"`
	LocalAddress string `json:"local_address"`
}

type PathMetrics struct {
	Path
	Connection int   `json:"connection"`
	Bytes      int64 `json:"bytes"`
	Requests   int64 `json:"requests"`
	Failures   int64 `json:"failures"`
	Active     int64 `json:"active"`
	PeakActive int64 `json:"peak_active"`
	Disabled   bool  `json:"disabled"`
}
type dataPath struct {
	client   *Client
	metrics  PathMetrics
	inflight int64
}
type pathPool struct {
	mu    sync.Mutex
	paths []*dataPath
	next  int
}
type pathError struct{ error }

func (e *pathError) Unwrap() error { return e.error }

// Certificate/authentication/protocol errors are never route fallback signals.
func transportError(err error) error {
	var cert *tls.CertificateVerificationError
	var op *net.OpError
	var timeout net.Error
	if !errors.As(err, &cert) && (errors.As(err, &op) || errors.As(err, &timeout) && timeout.Timeout() || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)) {
		return &pathError{err}
	}
	return err
}

func NewPathClient(paths []Path, credentials Credentials) (*Client, error) {
	return NewPathClientConnections(paths, credentials, 1)
}

// Connections per path separates HTTP/2 connection contention from NIC capacity.
// All transports still share one receiver cache and acquisition budget.
func NewPathClientConnections(paths []Path, credentials Credentials, connections int) (*Client, error) {
	if len(paths) < 1 || len(paths) > 8 {
		return nil, errors.New("require between 1 and 8 data paths")
	}
	if connections < 1 || connections > 4 {
		return nil, errors.New("connections per path must be between 1 and 4")
	}
	c := &Client{Credentials: credentials, Endpoint: paths[0].Endpoint, paths: &pathPool{}}
	seen := map[Path]bool{}
	for _, path := range paths {
		ip := net.ParseIP(path.LocalAddress)
		if ip == nil || ip.IsUnspecified() || ip.IsMulticast() || seen[path] {
			c.Close()
			return nil, errors.New("data paths require unique endpoint/local IP pairs and a concrete local IP")
		}
		seen[path] = true
		for connection := 1; connection <= connections; connection++ {
			leaf, err := NewClient(path.Endpoint, credentials, nil)
			if err != nil {
				c.Close()
				return nil, err
			}
			dialer := &net.Dialer{LocalAddr: &net.TCPAddr{IP: ip}, Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
			leaf.transport.DialContext = dialer.DialContext
			// Exactly one HTTP/2 connection per pool member makes metrics and
			// explicit connection-count experiments meaningful.
			leaf.transport.MaxConnsPerHost = 1
			leaf.transport.MaxIdleConnsPerHost = 1
			c.paths.paths = append(c.paths.paths, &dataPath{client: leaf, metrics: PathMetrics{Path: path, Connection: connection}})
		}
	}
	return c, nil
}

func (c *Client) PathMetrics() []PathMetrics {
	if c.paths == nil {
		return nil
	}
	c.paths.mu.Lock()
	defer c.paths.mu.Unlock()
	result := make([]PathMetrics, 0, len(c.paths.paths))
	for _, p := range c.paths.paths {
		result = append(result, p.metrics)
	}
	return result
}

func (p *pathPool) choose(size int64) *dataPath {
	p.mu.Lock()
	defer p.mu.Unlock()
	var chosen *dataPath
	for offset := range p.paths {
		i := (p.next + offset) % len(p.paths)
		candidate := p.paths[i]
		if !candidate.metrics.Disabled && (chosen == nil || candidate.inflight < chosen.inflight) {
			chosen = candidate
		}
	}
	if chosen != nil {
		p.next = (p.next + 1) % len(p.paths)
		chosen.inflight += size
		chosen.metrics.Active++
		chosen.metrics.PeakActive = max(chosen.metrics.PeakActive, chosen.metrics.Active)
		chosen.metrics.Requests++
	}
	return chosen
}

type countWriter struct {
	writer io.Writer
	n      int64
	err    error
}

func (w *countWriter) Write(b []byte) (int, error) {
	n, err := w.writer.Write(b)
	w.n += int64(n)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	w.err = err
	return n, err
}

func (c *Client) fetchPath(ctx context.Context, d v1.Descriptor, w io.Writer) error {
	for range c.paths.paths {
		if err := ctx.Err(); err != nil {
			return err
		}
		p := c.paths.choose(d.Size)
		if p == nil {
			return errors.New("all data paths failed")
		}
		counter := &countWriter{writer: w}
		err := p.client.fetch(ctx, d, counter)
		var network *pathError
		failed := ctx.Err() == nil && counter.err == nil && errors.As(err, &network)
		c.paths.mu.Lock()
		p.inflight -= d.Size
		p.metrics.Active--
		p.metrics.Bytes += counter.n
		if failed {
			p.metrics.Failures++
			p.metrics.Disabled = true
		}
		c.paths.mu.Unlock()
		// Never append a restarted blob after a partial write. The caller must
		// discard the failed acquisition and retry the entire verified blob.
		if !failed || counter.n != 0 {
			return err
		}
	}
	return errors.New("all data paths failed")
}

func (c *Client) pathRequest(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	for _, p := range c.paths.paths {
		c.paths.mu.Lock()
		disabled := p.metrics.Disabled
		c.paths.mu.Unlock()
		if disabled {
			continue
		}
		r, err := p.client.request(ctx, method, path, body)
		var network *pathError
		if method != http.MethodGet || ctx.Err() != nil || !errors.As(transportError(err), &network) {
			return r, err
		}
		c.paths.mu.Lock()
		p.metrics.Failures++
		p.metrics.Disabled = true
		c.paths.mu.Unlock()
	}
	return nil, errors.New("all data paths failed")
}
