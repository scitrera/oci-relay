// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package peer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"

	digest "github.com/opencontainers/go-digest"
	"github.com/spark-arena/oci-relay/internal/image"
)

type negotiation struct {
	SourceManifest digest.Digest `json:"source_manifest"`
	Manifest       []byte        `json:"manifest"`
	Store          string        `json:"store"`
	CachedLayers   int           `json:"cached_layers"`
	CacheVersion   int           `json:"cache_version,omitempty"`
	Availability   []string      `json:"availability,omitempty"`
}

type agreement struct {
	Manifest     digest.Digest `json:"manifest"`
	MissingBlobs int           `json:"missing_blobs"`
	MissingBytes int64         `json:"missing_bytes"`
	CacheVersion int           `json:"cache_version,omitempty"`
}

func agree(source *image.Image, n negotiation) (agreement, error) {
	if n.SourceManifest != source.Digest || len(n.Store) > 128 || n.CachedLayers < 0 || n.CachedLayers >= len(source.Descriptors) {
		return agreement{}, errors.New("invalid receiver inventory")
	}
	view, err := image.Parse(n.Manifest, source.Config, "", source.Platform)
	if err != nil {
		return agreement{}, err
	}
	if len(view.Descriptors) != len(source.Descriptors) {
		return agreement{}, errors.New("receiver layer count changed")
	}
	states := n.Availability
	switch n.CacheVersion {
	case 0:
		if len(states) != 0 {
			return agreement{}, errors.New("unversioned layer availability")
		}
		states = make([]string, len(source.Descriptors)-1)
		for i := range states {
			states[i] = layerMissing
			if i < n.CachedLayers {
				states[i] = layerUnpacked
			}
		}
	case 2:
		if len(states) != len(source.Descriptors)-1 {
			return agreement{}, errors.New("wrong layer availability count")
		}
	default:
		return agreement{}, errors.New("unsupported cache negotiation version")
	}
	cached := map[digest.Digest]bool{}
	for i, state := range states {
		if state != layerMissing && state != layerBlob && state != layerUnpacked {
			return agreement{}, errors.New("unknown layer availability")
		}
		if (i < n.CachedLayers) != (state == layerUnpacked) {
			return agreement{}, errors.New("unpacked layer requires complete parent prefix")
		}
		if state != layerMissing {
			cached[view.Descriptors[i+1].Digest] = true
		}
	}
	a := agreement{Manifest: view.Digest, CacheVersion: n.CacheVersion}
	for i, d := range source.Descriptors {
		if i > 0 && states[i-1] != layerMissing {
			continue
		}
		v := view.Descriptors[i]
		if d.Digest != v.Digest || d.Size != v.Size || d.MediaType != v.MediaType {
			return agreement{}, errors.New("receiver changed config or a missing layer")
		}
		if i == 0 || cached[d.Digest] {
			continue
		}
		cached[d.Digest] = true
		if d.Size > (1<<60)-a.MissingBytes {
			return agreement{}, errors.New("receiver byte total exceeds limit")
		}
		a.MissingBlobs++
		a.MissingBytes += d.Size
	}
	return a, nil
}

func (s *Server) negotiate(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodPost || id == "manager" {
		http.Error(w, "receiver POST required", 403)
		return
	}
	raw, err := image.ReadBounded(r.Body, 8<<20)
	if err != nil {
		http.Error(w, "inventory exceeds limit", 413)
		return
	}
	var n negotiation
	if json.Unmarshal(raw, &n) != nil {
		http.Error(w, "invalid inventory", 400)
		return
	}
	a, err := agree(s.Image, n)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	s.mu.Lock()
	if _, exists := s.results[id]; exists {
		s.mu.Unlock()
		http.Error(w, "receiver already finalized", 409)
		return
	}
	if s.agreements == nil {
		s.agreements = map[string]agreement{}
	}
	s.agreements[id] = a
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(a)
}

func (c *Client) Negotiate(ctx context.Context, source, view *image.Image, store string, count int) error {
	return c.negotiate(ctx, source, view, negotiation{SourceManifest: source.Digest, Manifest: view.Manifest, Store: store, CachedLayers: count})
}

func (c *Client) NegotiateLayers(ctx context.Context, source, view *image.Image, store string, count int, availability []string) error {
	return c.negotiate(ctx, source, view, negotiation{SourceManifest: source.Digest, Manifest: view.Manifest, Store: store, CachedLayers: count, CacheVersion: 2, Availability: availability})
}

func (c *Client) negotiate(ctx context.Context, source, view *image.Image, n negotiation) error {
	expected, err := agree(source, n)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(n)
	if err != nil {
		return err
	}
	r, err := c.request(ctx, "POST", "/relay/v1/negotiate", bytes.NewReader(raw))
	if err != nil {
		return err
	}
	defer r.Body.Close()
	raw, err = image.ReadBounded(r.Body, 4096)
	if err != nil {
		return err
	}
	var a agreement
	if json.Unmarshal(raw, &a) != nil || a != expected {
		return errors.New("source inventory agreement mismatch")
	}
	return nil
}
