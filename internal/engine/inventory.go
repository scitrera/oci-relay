// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/client"
	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/scitrera/oci-relay/internal/image"
)

type InventoryRequest struct {
	DiffIDs  []digest.Digest `json:"diff_ids"`
	Platform v1.Platform     `json:"platform"`
}

// Inventory contains hints only. Receivers pin candidates and revalidate native
// metadata before advertising availability. No payload is read during discovery.
type Inventory struct {
	InventoryRequest
	Candidates  []string `json:"candidates"`
	HelperImage string   `json:"helper_image"`
	Base        string   `json:"base"`
	Prefix      int      `json:"prefix"`
	Listed      int      `json:"images_listed"`
	Inspected   int      `json:"images_inspected"`
	Complete    bool     `json:"complete"`
	StopReason  string   `json:"stop_reason"`
	Seconds     float64  `json:"seconds"`
}

func (r InventoryRequest) Validate() error {
	if r.Platform.OS != "linux" || r.Platform.Architecture == "" || len(r.DiffIDs) >= image.MaxDescriptors {
		return errors.New("invalid cache inventory platform/layer count")
	}
	for _, d := range r.DiffIDs {
		if d.Validate() != nil || d.Algorithm() != digest.SHA256 {
			return errors.New("invalid inventory diffID")
		}
	}
	return nil
}

// Discover searches all image records, including dangling/intermediate images,
// with eight in-flight inspections in batches of 32. A deadline returns partial
// hints explicitly; daemon errors are never disguised as cache misses.
func (e *Engine) Discover(ctx context.Context, request InventoryRequest, budget time.Duration, prefixOnly bool) (out Inventory, err error) {
	out.InventoryRequest = request
	start := time.Now()
	defer func() { out.Seconds = time.Since(start).Seconds() }()
	if err = request.Validate(); err != nil {
		return
	}
	probe, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	wanted := map[string]bool{}
	for _, d := range request.DiffIDs {
		wanted[string(d)] = true
	}
	owners := map[string]string{}
	baseLayers := map[string]bool{}
	defer func() {
		seen := map[string]bool{}
		add := func(id string) {
			if id != "" && !seen[id] {
				out.Candidates = append(out.Candidates, id)
				seen[id] = true
			}
		}
		add(out.Base)
		for _, d := range request.DiffIDs {
			if !baseLayers[string(d)] {
				add(owners[string(d)])
			}
		}
		if len(out.Candidates) > 0 {
			out.HelperImage = out.Candidates[0]
		}
	}()
	partial := func(cause error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if probe.Err() != nil {
			out.StopReason = "time_budget"
			return nil
		}
		return cause
	}
	if len(wanted) == 0 {
		out.Complete, out.StopReason = true, "no_layers"
		return
	}
	ids, listErr := e.ListImages(probe)
	if listErr != nil {
		err = partial(listErr)
		return
	}
	unique, seen := []string{}, map[string]bool{}
	for _, id := range ids {
		d := digest.Digest(id)
		if d.Validate() != nil || d.Algorithm() != digest.SHA256 {
			err = errors.New("Docker returned invalid inventory image ID")
			return
		}
		if !seen[id] {
			unique = append(unique, id)
			seen[id] = true
		}
	}
	ids = unique
	out.Listed = len(ids)
	for offset := 0; offset < len(ids); offset += 32 {
		batch := ids[offset:min(offset+32, len(ids))]
		type inspected struct {
			info client.ImageInspectResult
			err  error
		}
		results := make([]inspected, len(batch))
		var wg sync.WaitGroup
		for worker := 0; worker < min(8, len(batch)); worker++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				for i := worker; i < len(batch); i += 8 {
					results[i].info, results[i].err = e.InspectPlatform(probe, batch[i], request.Platform)
				}
			}(worker)
		}
		wg.Wait()
		for i, result := range results {
			if result.err != nil {
				if errdefs.IsNotFound(result.err) {
					out.Inspected++
					continue
				}
				if probe.Err() != nil {
					continue
				}
				err = result.err
				return
			}
			out.Inspected++
			info := result.info
			if !image.Matches(request.Platform, v1.Platform{OS: info.Os, Architecture: info.Architecture, Variant: info.Variant}) {
				continue
			}
			if out.HelperImage == "" {
				out.HelperImage = batch[i]
			}
			prefix := 0
			for j, layer := range info.RootFS.Layers {
				if j == prefix && j < len(request.DiffIDs) && layer == string(request.DiffIDs[j]) {
					prefix++
				}
				if wanted[layer] && owners[layer] == "" {
					owners[layer] = batch[i]
				}
			}
			if prefix > out.Prefix {
				out.Prefix, out.Base = prefix, batch[i]
				baseLayers = map[string]bool{}
				for _, layer := range info.RootFS.Layers {
					baseLayers[layer] = true
				}
			}
		}
		if probe.Err() != nil {
			err = partial(probe.Err())
			return
		}
		if out.Prefix == len(request.DiffIDs) || (!prefixOnly && len(owners) == len(wanted)) {
			out.Complete, out.StopReason = true, "all_layers_found"
			return
		}
	}
	out.Complete, out.StopReason = true, "all_images_scanned"
	return
}
