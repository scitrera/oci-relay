// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only
// Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.

package peer

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

const stripingHeader = "X-Oci-Relay-Striping"
const stripePiece = int64(1 << 20)
const maxStripePiece = int64(64 << 20)
const stripePieceHeader = "X-Oci-Relay-Stripe-Piece-Max"

func validStripePiece(piece int64) bool {
	return piece >= stripePiece && piece <= maxStripePiece && piece&(piece-1) == 0
}

func stripeLength(size int64, count, lane int, piece int64) int64 {
	cycle := piece * int64(count)
	return size/cycle*piece + max(0, min(piece, size%cycle-int64(lane)*piece))
}

func stripeFormat(count, lane int, piece int64) string {
	return fmt.Sprintf("1;%d;%d;%d", count, lane, piece)
}

type stripeGroup struct {
	ctx              context.Context
	cancel           context.CancelFunc
	ready            chan struct{}
	mu               sync.Mutex
	digest           digest.Digest
	count            int
	piece            int64
	readers          []io.ReadCloser
	claimed          []bool
	joined, finished int
	err              error
	joinTimer        *time.Timer
}

// A group reserves every lane before serving any payload. This prevents a fast
// first connection from evicting the pieces needed by a later connection.
func (s *Server) stripeGroup(ctx context.Context, key string, d v1.Descriptor, count int, piece int64) (*stripeGroup, error) {
	s.mu.Lock()
	g := s.stripeGroups[key]
	created := g == nil
	if created {
		if len(s.stripeGroups) >= 32 {
			s.mu.Unlock()
			return nil, errors.New("stripe group limit reached")
		}
		groupCtx, cancel := context.WithTimeout(s.ctx, 30*time.Minute)
		g = &stripeGroup{ctx: groupCtx, cancel: cancel, ready: make(chan struct{}), digest: d.Digest, count: count, piece: piece, claimed: make([]bool, count)}
		s.stripeGroups[key] = g
	}
	s.mu.Unlock()
	if g.digest != d.Digest || g.count != count || g.piece != piece {
		return nil, errors.New("stripe group descriptor/geometry conflict")
	}
	if created {
		// Never hold the server mutex while waiting for a source acquisition slot.
		stopOpen := context.AfterFunc(ctx, g.cancel)
		g.readers, g.err = s.Cache.OpenStripes(g.ctx, d, count, piece)
		stopOpen()
		// Time spent queued for an acquisition is not a slow lane joining. Start
		// this bound only once readers actually retain source memory.
		g.joinTimer = time.AfterFunc(30*time.Second, g.cancel)
		context.AfterFunc(g.ctx, func() {
			g.joinTimer.Stop()
			for _, r := range g.readers {
				_ = r.Close()
			}
			s.mu.Lock()
			if s.stripeGroups[key] == g {
				delete(s.stripeGroups, key)
			}
			s.mu.Unlock()
		})
		close(g.ready)
		if g.err != nil {
			g.cancel()
		}
	}
	select {
	case <-g.ready:
		return g, g.err
	case <-g.ctx.Done():
		return nil, g.ctx.Err()
	case <-ctx.Done():
		g.cancel()
		return nil, ctx.Err()
	}
}

func (s *Server) serveStripe(w http.ResponseWriter, r *http.Request, peer string) {
	if r.Method != http.MethodGet || peer == "manager" {
		http.Error(w, "receiver GET required", http.StatusForbidden)
		return
	}
	d, ok := s.Image.Descriptor(digest.Digest(strings.TrimPrefix(r.URL.Path, "/relay/v1/stripes/")))
	q := r.URL.Query()
	piece := stripePiece
	var pieceErr error
	if q.Has("piece") {
		piece, pieceErr = strconv.ParseInt(q.Get("piece"), 10, 64)
	}
	count, countErr := strconv.Atoi(q.Get("lanes"))
	lane, laneErr := strconv.Atoi(q.Get("lane"))
	token, tokenErr := hex.DecodeString(q.Get("group"))
	if !ok || pieceErr != nil || !validStripePiece(piece) || countErr != nil || laneErr != nil || tokenErr != nil || len(token) != 16 || count < 2 || count > 8 || lane < 0 || lane >= count || d.Size < int64(count)*piece {
		http.Error(w, "invalid stripe request", http.StatusBadRequest)
		return
	}
	select {
	case s.stripeStreams <- struct{}{}:
		defer func() { <-s.stripeStreams }()
	default:
		http.Error(w, "stream limit reached", http.StatusTooManyRequests)
		return
	}
	g, err := s.stripeGroup(r.Context(), peer+":"+hex.EncodeToString(token), d, count, piece)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	g.mu.Lock()
	if g.claimed[lane] {
		g.mu.Unlock()
		http.Error(w, "stripe already claimed", http.StatusConflict)
		return
	}
	g.claimed[lane] = true
	g.joined++
	if g.joined == count {
		g.joinTimer.Stop()
	}
	g.mu.Unlock()
	// Cancelling any lane terminates the group, including lanes not yet joined.
	stop := context.AfterFunc(r.Context(), g.cancel)
	defer stop()
	defer func() {
		_ = g.readers[lane].Close()
		g.mu.Lock()
		g.finished++
		finished := g.finished == count
		g.mu.Unlock()
		if finished || err != nil {
			g.cancel()
		}
	}()
	w.Header().Set("Content-Length", formatSize(stripeLength(d.Size, count, lane, piece)))
	w.Header().Set("Docker-Content-Digest", string(d.Digest))
	w.Header().Set(stripingHeader, stripeFormat(count, lane, piece))
	w.WriteHeader(http.StatusOK)
	// Clients open every lane before consuming bodies; flush headers even when
	// this lane's first piece has not been produced yet.
	if err = http.NewResponseController(w).Flush(); err == nil {
		err = CopyResponse(g.ctx, w, g.readers[lane])
	}
	if err != nil {
		panic(http.ErrAbortHandler)
	}
}
