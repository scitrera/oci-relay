// SPDX-FileCopyrightText: 2026 Spark Arena
// SPDX-License-Identifier: Apache-2.0

package peer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

// Reserve distinct connections. Prefer distinct endpoints before adding another
// connection on an endpoint, so a four-lane cap does not crowd out a second NIC.
func (p *pathPool) reserveStripes(size int64, limit int, piece int64) []*dataPath {
	p.mu.Lock()
	defer p.mu.Unlock()
	var chosen []*dataPath
	used := map[*dataPath]bool{}
	endpoints := map[string]int{}
	for len(chosen) < min(limit, 8) {
		var best *dataPath
		for offset := range p.paths {
			candidate := p.paths[(p.next+offset)%len(p.paths)]
			if candidate.metrics.Disabled || used[candidate] {
				continue
			}
			endpoint := candidate.metrics.Endpoint
			if best == nil || endpoints[endpoint] < endpoints[best.metrics.Endpoint] || endpoints[endpoint] == endpoints[best.metrics.Endpoint] && candidate.inflight < best.inflight {
				best = candidate
			}
		}
		if best == nil {
			break
		}
		chosen = append(chosen, best)
		used[best] = true
		endpoints[best.metrics.Endpoint]++
	}
	if len(chosen) < 2 {
		return nil
	}
	p.next = (p.next + 1) % len(p.paths)
	for i, path := range chosen {
		path.inflight += stripeLength(size, len(chosen), i, piece)
		path.metrics.Active++
		path.metrics.PeakActive = max(path.metrics.PeakActive, path.metrics.Active)
		path.metrics.Requests++
		path.metrics.StripeRequests++
		path.metrics.StripePieceBytes = piece
	}
	return chosen
}

type stripeResponse struct {
	response *http.Response
	err      error
	read     int64
}

// Stripe bodies are consumed in piece order directly into the existing receiver
// cache/hash writer. There is no full-layer spool or extra reassembly ring.
// HTTP/2's bounded receive windows provide read-ahead on the other connections.
func (c *Client) fetchStripes(ctx context.Context, d v1.Descriptor, w io.Writer, paths []*dataPath, pieceSize int64) error {
	parent := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	responses := make([]stripeResponse, len(paths))
	defer func() {
		cancel()
		for i, path := range paths {
			r := &responses[i]
			if r.response != nil {
				_ = r.response.Body.Close()
			}
			var network *pathError
			c.paths.mu.Lock()
			path.inflight -= stripeLength(d.Size, len(paths), i, pieceSize)
			path.metrics.Active--
			path.metrics.Bytes += r.read
			if parent.Err() == nil && errors.As(r.err, &network) {
				path.metrics.Failures++
				path.metrics.Disabled = true
			}
			c.paths.mu.Unlock()
		}
	}()
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return err
	}
	var wg sync.WaitGroup
	var once sync.Once
	var firstError error
	for i, path := range paths {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := &responses[i]
			url := fmt.Sprintf("/relay/v1/stripes/%s?group=%s&lanes=%d&lane=%d", d.Digest, hex.EncodeToString(token[:]), len(paths), i)
			if pieceSize != stripePiece {
				url += fmt.Sprintf("&piece=%d", pieceSize)
			}
			r.response, r.err = path.client.request(ctx, http.MethodGet, url, nil)
			if r.err != nil {
				r.err = transportError(r.err)
			} else if r.response.StatusCode != http.StatusOK || r.response.ContentLength != stripeLength(d.Size, len(paths), i, pieceSize) || r.response.Header.Get(stripingHeader) != stripeFormat(len(paths), i, pieceSize) || r.response.Header.Get("Docker-Content-Digest") != string(d.Digest) {
				r.err = errors.New("relay stripe metadata mismatch")
			}
			if r.err != nil {
				once.Do(func() { firstError = r.err; cancel() })
			}
		}()
	}
	wg.Wait()
	if firstError != nil {
		return firstError
	}
	buffer := make([]byte, 64<<10)
	for offset, piece := int64(0), 0; offset < d.Size; piece++ {
		r := &responses[piece%len(paths)]
		length := min(pieceSize, d.Size-offset)
		writer := &countWriter{writer: w}
		n, err := io.CopyBuffer(writer, io.LimitReader(r.response.Body, length), buffer)
		r.read += n
		if writer.err != nil {
			return writer.err
		}
		if err != nil || n != length {
			if err == nil {
				err = io.ErrUnexpectedEOF
			}
			r.err = &pathError{err}
			return r.err // A partial acquisition is discarded, never appended to a retry.
		}
		offset += length
	}
	for i := range responses {
		r := &responses[i]
		n, err := r.response.Body.Read(buffer[:1])
		r.read += int64(n)
		if n != 0 {
			return errors.New("relay stripe exceeded expected length")
		}
		if err != io.EOF {
			if err == nil {
				err = io.ErrNoProgress
			}
			r.err = &pathError{err}
			return r.err
		}
	}
	return nil
}
