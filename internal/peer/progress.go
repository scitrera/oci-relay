// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package peer

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"sync"
	"time"

	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/scitrera/oci-relay/internal/engine"
	"github.com/scitrera/oci-relay/internal/image"
	"github.com/scitrera/oci-relay/internal/transfer"
)

// ReceiverProgress is advisory, never proof of import or integrity. ExpectedBytes
// is an upper bound: Docker may discover additional cached layers during import.
// WireBytes includes retries/replays; ReceivedBytes counts each blob offset once.
// Both count layer payloads, excluding metadata and transport overhead.
type ReceiverProgress struct {
	Version        int     `json:"version"`
	Transfer       string  `json:"transfer"`
	Peer           string  `json:"peer"`
	Sequence       uint64  `json:"sequence"`
	Phase          string  `json:"phase"`
	Elapsed        float64 `json:"elapsed_seconds"`
	InventoryReady bool    `json:"inventory_ready"`
	ExpectedBytes  int64   `json:"expected_bytes"`
	ReceivedBytes  int64   `json:"received_bytes"`
	WireBytes      int64   `json:"wire_bytes"`
	ReusedLayers   int     `json:"reused_layers"`
	ActiveStreams  int     `json:"active_streams"`
	BytesPerSecond float64 `json:"bytes_per_second"`
	// Docker's extraction counters describe its input stream, not filesystem
	// bytes or a time estimate. Keep one active layer so each report stays bounded.
	ExtractingLayer  string  `json:"extracting_layer,omitempty"`
	ExtractCurrent   int64   `json:"extract_current,omitempty"`
	ExtractTotal     int64   `json:"extract_total,omitempty"`
	ExtractSeconds   float64 `json:"extract_seconds,omitempty"`
	ExtractUpdateAge float64 `json:"extract_update_age_seconds,omitempty"`
	ExtractedLayers  int     `json:"extracted_layers,omitempty"`
}

func validLayerID(id string) bool {
	if len(id) == 0 || len(id) > 64 {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil
}

func validProgress(p ReceiverProgress) bool {
	switch p.Phase {
	case "checking", "discovering", "decoding", "transferring", "importing", "verifying", "cleanup":
	default:
		return false
	}
	return (p.ExtractingLayer == "" || validLayerID(p.ExtractingLayer)) &&
		p.ExtractCurrent >= 0 && p.ExtractCurrent <= 1<<60 && p.ExtractTotal >= 0 && p.ExtractTotal <= 1<<60 &&
		p.ExtractedLayers >= 0 && p.ExtractedLayers <= 4095 &&
		p.ExtractSeconds >= 0 && !math.IsNaN(p.ExtractSeconds) && !math.IsInf(p.ExtractSeconds, 0) &&
		p.ExtractUpdateAge >= 0 && !math.IsNaN(p.ExtractUpdateAge) && !math.IsInf(p.ExtractUpdateAge, 0) &&
		p.Sequence > 0 && p.ExpectedBytes >= 0 && p.ExpectedBytes <= 1<<60 &&
		p.ReceivedBytes >= 0 && p.ReceivedBytes <= 1<<60 && p.WireBytes >= p.ReceivedBytes &&
		p.ReusedLayers >= 0 && p.ReusedLayers <= 4095 && p.ActiveStreams >= 0 && p.ActiveStreams <= 256 &&
		p.Elapsed >= 0 && !math.IsNaN(p.Elapsed) && !math.IsInf(p.Elapsed, 0) &&
		p.BytesPerSecond >= 0 && !math.IsNaN(p.BytesPerSecond) && !math.IsInf(p.BytesPerSecond, 0)
}

func (s *Server) progress(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodPost || id == "manager" {
		http.Error(w, "receiver POST required", http.StatusForbidden)
		return
	}
	raw, err := image.ReadBounded(r.Body, 4096)
	if err != nil {
		http.Error(w, "progress exceeds limit", http.StatusRequestEntityTooLarge)
		return
	}
	var p ReceiverProgress
	if json.Unmarshal(raw, &p) != nil || p.Version != ProtocolVersion || p.Transfer != s.Session.Source.Transfer || p.Peer != id || !validProgress(p) {
		http.Error(w, "invalid progress", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	// Late or reordered observations cannot overwrite final outcomes or newer data.
	if _, finished := s.results[id]; !finished && p.Sequence > s.receiverProgress[id].Sequence {
		if s.receiverProgress == nil {
			s.receiverProgress = map[string]ReceiverProgress{}
		}
		s.receiverProgress[id] = p
	}
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) Progress() map[string]ReceiverProgress {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]ReceiverProgress, len(s.receiverProgress))
	for id, p := range s.receiverProgress {
		out[id] = p
	}
	return out
}

func (c *Client) reportProgress(ctx context.Context, p ReceiverProgress) error {
	raw, err := json.Marshal(p)
	if err != nil {
		return err
	}
	r, err := c.request(ctx, http.MethodPost, "/relay/v1/progress", bytes.NewReader(raw))
	if err != nil {
		return err
	}
	return r.Body.Close()
}

type receiverProgress struct {
	mu               sync.Mutex
	value            ReceiverProgress
	started, sampled time.Time
	lastWire         int64
	offsets          map[digest.Digest]int64
	sizes            map[digest.Digest]int64
	extracts         map[string]*layerExtraction
	cancel           context.CancelFunc
	done             chan struct{}
}

type layerExtraction struct {
	started, updated time.Time
	order            int
	current, total   int64
	done             bool
}

// Status is advisory and bounded independently of the Docker JSON stream.
func (p *receiverProgress) docker(event engine.Progress) {
	if !validLayerID(event.ID) || (event.Status != "Extracting" && event.Status != "Pull complete") {
		return
	}
	current, total := event.ProgressDetail.Current, event.ProgressDetail.Total
	if current < 0 || total < 0 || current > 1<<60 || total > 1<<60 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.extracts == nil {
		p.extracts = map[string]*layerExtraction{}
	}
	layer := p.extracts[event.ID]
	now := time.Now()
	if layer == nil {
		if len(p.extracts) >= image.MaxDescriptors-1 {
			return
		}
		layer = &layerExtraction{started: now, updated: now, order: len(p.extracts)}
		p.extracts[event.ID] = layer
	}
	if event.Status == "Pull complete" {
		if !layer.done {
			p.value.ExtractedLayers++
			layer.done = true
		}
		return
	}
	if layer.done { // A retried pull may emit a layer again; do not double count.
		return
	}
	p.value.Phase = "importing"
	if layer.current != current || layer.total != total {
		layer.updated = now
	}
	layer.current, layer.total = current, total
}

func newReceiverProgress(ctx context.Context, c *Client) *receiverProgress {
	ctx, cancel := context.WithCancel(ctx)
	now := time.Now()
	p := &receiverProgress{
		value:   ReceiverProgress{Version: ProtocolVersion, Transfer: c.Credentials.Transfer, Peer: c.Credentials.Peer, Phase: "checking"},
		started: now, sampled: now, offsets: map[digest.Digest]int64{}, sizes: map[digest.Digest]int64{},
		cancel: cancel, done: make(chan struct{}),
	}
	go func() {
		defer close(p.done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			// Best effort and bounded: progress failure must not fail a transfer,
			// occupy a data worker, or delay cancellation. No queued snapshots.
			reportCtx, stop := context.WithTimeout(ctx, 2*time.Second)
			_ = c.reportProgress(reportCtx, p.snapshot())
			stop()
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return p
}

func (p *receiverProgress) close() { p.cancel(); <-p.done }

func (p *receiverProgress) phase(phase string) {
	p.mu.Lock()
	p.value.Phase = phase
	p.mu.Unlock()
}

func (p *receiverProgress) plan(layers []v1.Descriptor, local map[digest.Digest]transfer.Source, reused int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.value.ExpectedBytes = 0
	for _, d := range layers {
		if _, exists := p.sizes[d.Digest]; !exists {
			p.sizes[d.Digest] = d.Size
			if local[d.Digest] == nil {
				p.value.ExpectedBytes += d.Size
			}
		}
	}
	p.value.InventoryReady = true
	p.value.ReusedLayers = reused
	p.value.Phase = "transferring"
}

func (p *receiverProgress) snapshot() ReceiverProgress {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	p.value.ExtractingLayer = ""
	p.value.ExtractCurrent, p.value.ExtractTotal = 0, 0
	p.value.ExtractSeconds, p.value.ExtractUpdateAge = 0, 0
	var oldest *layerExtraction
	for id, layer := range p.extracts {
		// Clock resolution may give adjacent events identical timestamps.
		if !layer.done && (oldest == nil || layer.order < oldest.order) {
			oldest = layer
			p.value.ExtractingLayer = id
		}
	}
	if oldest != nil {
		p.value.ExtractCurrent, p.value.ExtractTotal = oldest.current, oldest.total
		p.value.ExtractSeconds = now.Sub(oldest.started).Seconds()
		p.value.ExtractUpdateAge = now.Sub(oldest.updated).Seconds()
	}
	p.value.Sequence++
	p.value.Elapsed = now.Sub(p.started).Seconds()
	if dt := now.Sub(p.sampled).Seconds(); dt > 0 {
		p.value.BytesPerSecond = float64(p.value.WireBytes-p.lastWire) / dt
	}
	p.sampled, p.lastWire = now, p.value.WireBytes
	return p.value
}

type progressSource struct {
	source   transfer.Source
	progress *receiverProgress
}

func (s progressSource) Fetch(ctx context.Context, d v1.Descriptor, w io.Writer) error {
	p := s.progress
	p.mu.Lock()
	_, tracked := p.sizes[d.Digest]
	if tracked {
		p.value.ActiveStreams++
	}
	p.mu.Unlock()
	if !tracked { // Config is supplied as metadata for Docker imports.
		return s.source.Fetch(ctx, d, w)
	}
	defer func() { p.mu.Lock(); p.value.ActiveStreams--; p.mu.Unlock() }()
	return s.source.Fetch(ctx, d, &progressWriter{Writer: w, progress: p, digest: d.Digest})
}

type progressWriter struct {
	io.Writer
	progress *receiverProgress
	digest   digest.Digest
	offset   int64
}

func (w *progressWriter) Write(b []byte) (int, error) {
	n, err := w.Writer.Write(b)
	w.offset += int64(n)
	p := w.progress
	p.mu.Lock()
	p.value.WireBytes += int64(n)
	end := min(w.offset, p.sizes[w.digest])
	if end > p.offsets[w.digest] {
		p.value.ReceivedBytes += end - p.offsets[w.digest]
		p.offsets[w.digest] = end
	}
	p.mu.Unlock()
	return n, err
}
