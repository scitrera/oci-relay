// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package peer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"sync/atomic"
	"time"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/spark-arena/oci-relay/internal/engine"
	"github.com/spark-arena/oci-relay/internal/image"
	"github.com/spark-arena/oci-relay/internal/source"
	"github.com/spark-arena/oci-relay/internal/transfer"
)

type localImage struct {
	im    *image.Image
	src   transfer.Source
	close func() error
}

func openLocal(ctx context.Context, o PullOptions, id string, p v1.Platform) (*localImage, error) {
	switch o.NativeStore {
	case "overlay2":
		n, err := source.NewNative(ctx, o.NativeRoot, id, o.EngineVersion, p)
		if err != nil {
			return nil, err
		}
		return &localImage{n.Image, n, n.Close}, nil
	case "containerd":
		n, err := source.NewContainerd(ctx, o.NativeRoot, id, o.EngineVersion, p)
		if err != nil {
			return nil, err
		}
		return &localImage{n.Image, n, n.Close}, nil
	default:
		return nil, errors.New("unknown native receiver store")
	}
}

func qualifyReceiver(ctx context.Context, e *engine.Engine, o PullOptions) (string, error) {
	if o.NativeStore == "" && (o.NativeRoot != "" || o.NativeBase != "" || o.EngineVersion != "") {
		return "", errors.New("native receiver options require --native-store")
	}
	info, err := e.Client.Info(ctx, client.InfoOptions{})
	if err != nil {
		return "", err
	}
	store := info.Info.Driver
	for _, pair := range info.Info.DriverStatus {
		if pair[0] == "driver-type" && pair[1] == "io.containerd.snapshotter.v1" {
			store = "containerd"
		}
	}
	if o.NativeStore != "" && (o.NativeRoot == "" || store != o.NativeStore || info.Info.ServerVersion != o.EngineVersion || info.Info.OSType != "linux" || !engine.SupportsStoreVersion(info.Info.ServerVersion) || (store == "containerd" && info.Info.Driver != "overlayfs")) {
		return "", errors.New("native receiver facts do not match the running Docker daemon")
	}
	return store, nil
}

// A view changes only representations of a proven ordered prefix. Config bytes,
// DiffIDs, layer order and every missing descriptor remain exactly as supplied.
func reuseView(incoming, local *image.Image, count int) (*image.Image, error) {
	if err := incoming.Validate(); err != nil {
		return nil, err
	}
	if err := local.Validate(); err != nil {
		return nil, err
	}
	var want, have v1.Image
	_ = json.Unmarshal(incoming.Config, &want)
	_ = json.Unmarshal(local.Config, &have)
	if count < 0 || count > len(want.RootFS.DiffIDs) || count > len(have.RootFS.DiffIDs) || !image.Matches(incoming.Platform, local.Platform) {
		return nil, errors.New("invalid local layer prefix")
	}
	for i := 0; i < count; i++ {
		if want.RootFS.DiffIDs[i] != have.RootFS.DiffIDs[i] {
			return nil, errors.New("local layer parent chain mismatch")
		}
	}
	var m v1.Manifest
	_ = json.Unmarshal(incoming.Manifest, &m)
	changed := false
	for i := 0; i < count; i++ {
		d := local.Descriptors[i+1]
		if m.Layers[i].Digest != d.Digest || m.Layers[i].Size != d.Size || m.Layers[i].MediaType != d.MediaType {
			changed = true
			m.Layers[i] = d
		}
	}
	if !changed {
		return incoming, nil
	}
	// Representations may cross Docker/OCI media types; emit an OCI manifest.
	m.MediaType = v1.MediaTypeImageManifest
	raw, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return image.Parse(raw, incoming.Config, "", incoming.Platform)
}

// Availability describes network avoidance separately from unpack avoidance.
const (
	layerMissing  = "missing"
	layerUnpacked = "chain"
	layerBlob     = "blob"
)

type reuseSource struct {
	remote       transfer.Source
	local        map[digest.Digest]transfer.Source
	availability []string
	localBytes   atomic.Int64
}

func (s *reuseSource) Fetch(ctx context.Context, d v1.Descriptor, w io.Writer) error {
	if local := s.local[d.Digest]; local != nil {
		counter := &countWriter{writer: w}
		err := local.Fetch(ctx, d, counter)
		s.localBytes.Add(counter.n)
		return err
	}
	return s.remote.Fetch(ctx, d, w)
}

func inventoryMetrics(result *Result, inv engine.Inventory) {
	result.DiscoverySeconds = inv.Seconds
	result.DiscoveryImages = inv.Listed
	result.DiscoveryInspected = inv.Inspected
	result.DiscoveryComplete = inv.Complete
	result.DiscoveryStopReason = inv.StopReason
}

func inventoryFor(im *image.Image) engine.InventoryRequest {
	var cfg v1.Image
	_ = json.Unmarshal(im.Config, &cfg)
	return engine.InventoryRequest{DiffIDs: cfg.RootFS.DiffIDs, Platform: im.Platform}
}

func validateInventory(inv *engine.Inventory, im *image.Image) error {
	request := inventoryFor(im)
	if err := inv.InventoryRequest.Validate(); err != nil {
		return err
	}
	if !slices.Equal(inv.DiffIDs, request.DiffIDs) || !image.Matches(request.Platform, inv.Platform) || !image.Matches(inv.Platform, request.Platform) || len(inv.Candidates) > len(inv.DiffIDs)+1 {
		return errors.New("cache inventory does not match source image")
	}
	seen := map[string]bool{}
	for _, id := range inv.Candidates {
		d := digest.Digest(id)
		if d.Validate() != nil || d.Algorithm() != digest.SHA256 || seen[id] {
			return errors.New("invalid cache inventory candidate")
		}
		seen[id] = true
	}
	return nil
}

func receiverView(ctx context.Context, e *engine.Engine, c *Client, im *image.Image, o PullOptions, result *Result) (*image.Image, *reuseSource, func(), error) {
	s := &reuseSource{remote: c, local: map[digest.Digest]transfer.Source{}, availability: make([]string, len(im.Descriptors)-1)}
	for i := range s.availability {
		s.availability[i] = layerMissing
	}
	cleanup := func() {}
	if o.NativeStore == "" {
		return im, s, cleanup, nil
	}
	request := inventoryFor(im)
	var inv engine.Inventory
	var err error
	switch {
	case o.Inventory != nil:
		if err = validateInventory(o.Inventory, im); err != nil {
			return im, s, cleanup, err
		}
		inv = *o.Inventory
	case o.NativeBase != "":
		inv = engine.Inventory{InventoryRequest: request, Candidates: []string{o.NativeBase}, StopReason: "explicit_base"}
	default:
		inv, err = e.Discover(ctx, request, 10*time.Second, false)
		if err != nil {
			return im, s, cleanup, err
		}
	}
	inventoryMetrics(result, inv)
	start := time.Now()
	defer func() { result.CacheProbeSeconds = time.Since(start).Seconds() }()
	var native *source.Native
	var content *source.ContentCache
	pins := []string{}
	cleanup = func() {
		var cleanupErr error
		if native != nil {
			cleanupErr = errors.Join(cleanupErr, native.Close())
		}
		if content != nil {
			cleanupErr = errors.Join(cleanupErr, content.Close())
		}
		for _, name := range pins {
			cleanCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_, err := e.Client.ContainerRemove(cleanCtx, name, client.ContainerRemoveOptions{Force: true, RemoveVolumes: true})
			cancel()
			if err != nil && !errdefs.IsNotFound(err) {
				cleanupErr = errors.Join(cleanupErr, err)
			}
		}
		if cleanupErr != nil {
			result.CleanupError = cleanupErr.Error()
		}
		result.LocalBytes = s.localBytes.Load()
	}
	var manifest v1.Manifest
	_ = json.Unmarshal(im.Manifest, &manifest)
	if o.NativeStore == "containerd" {
		content, err = source.NewContentCache(o.NativeRoot)
		if err != nil {
			return im, s, cleanup, err
		}
		// Exact incoming blobs need no image record, config rewrite or GC lease:
		// open descriptors retain the bytes even when the CAS path is unlinked.
		for i, d := range manifest.Layers {
			hit, probeErr := content.Probe(ctx, d)
			if probeErr != nil {
				return im, s, cleanup, probeErr
			}
			if hit {
				s.local[d.Digest], s.availability[i] = content, layerBlob
			}
		}
	}
	for number, id := range inv.Candidates {
		if err = ctx.Err(); err != nil {
			return im, s, cleanup, err
		}
		// overlay2 files require image retention. Created containers never run,
		// and avoid the last-tag removal conflict on dangling/busy images.
		if o.NativeStore == "overlay2" && id != o.NativeBase {
			name := fmt.Sprintf("oci-relay-cache-%s-%s-%d", c.Credentials.Transfer, c.Credentials.Peer, number)
			pins = append(pins, name) // covers an indeterminate create result
			_, err = e.Client.ContainerCreate(ctx, client.ContainerCreateOptions{Name: name, Image: id, Platform: &im.Platform,
				Config:     &container.Config{Entrypoint: []string{"/oci-relay-cache-pin"}, Cmd: []string{}, Labels: map[string]string{"com.scitrera.oci-relay.cache-pin": "true"}},
				HostConfig: &container.HostConfig{NetworkMode: "none", ReadonlyRootfs: true, Runtime: "runc", SecurityOpt: []string{"no-new-privileges"}},
			})
			if errdefs.IsNotFound(err) {
				result.DiscoveryStale++
				continue
			}
			if err != nil {
				return im, s, cleanup, err
			}
		}
		localDescriptors := map[digest.Digest]v1.Descriptor{}
		var diffs []digest.Digest
		if o.NativeStore == "overlay2" {
			wanted := map[digest.Digest]bool{}
			for i, diff := range request.DiffIDs {
				if s.availability[i] == layerMissing {
					wanted[diff] = true
				}
			}
			selected, openErr := source.NewNativeLayers(ctx, o.NativeRoot, id, o.EngineVersion, im.Platform, wanted)
			if openErr != nil {
				return im, s, cleanup, openErr
			}
			diffs = selected.DiffIDs
			for diff := range wanted {
				if d, ok := selected.Layer(diff); ok {
					localDescriptors[diff] = d
				}
			}
			if native == nil {
				native = selected
			} else {
				mergeErr := native.MergeLayers(selected)
				closeErr := selected.Close()
				if err = errors.Join(mergeErr, closeErr); err != nil {
					return im, s, cleanup, err
				}
			}
		} else {
			local, openErr := source.NewContainerd(ctx, o.NativeRoot, id, o.EngineVersion, im.Platform)
			// A discovery hint may disappear before metadata can be opened.
			if errors.Is(openErr, os.ErrNotExist) {
				result.DiscoveryStale++
				continue
			}
			if openErr != nil {
				return im, s, cleanup, openErr
			}
			var cfg v1.Image
			_ = json.Unmarshal(local.Image.Config, &cfg)
			diffs = cfg.RootFS.DiffIDs
			for j, diff := range diffs {
				localDescriptors[diff] = local.Image.Descriptors[j+1]
			}
			if err = local.Close(); err != nil {
				return im, s, cleanup, err
			}
		}
		prefix := 0
		for prefix < len(diffs) && prefix < len(request.DiffIDs) && diffs[prefix] == request.DiffIDs[prefix] {
			prefix++
		}
		result.ReusedLayers = max(result.ReusedLayers, prefix)
		for i, diff := range request.DiffIDs {
			if s.availability[i] == layerMissing {
				d, ok := localDescriptors[diff]
				if !ok {
					continue
				}
				if content != nil {
					hit, probeErr := content.Probe(ctx, d)
					if probeErr != nil {
						return im, s, cleanup, probeErr
					}
					if !hit {
						continue
					}
					s.local[d.Digest] = content
				} else {
					s.local[d.Digest] = native
				}
				manifest.Layers[i], s.availability[i] = d, layerBlob
			}
			if i < prefix {
				s.availability[i] = layerUnpacked
			}
		}
	}
	result.ReusedLayers = 0
	for i, state := range s.availability {
		if state == layerUnpacked && i == result.ReusedLayers {
			result.ReusedLayers++
		} else if state == layerUnpacked {
			s.availability[i] = layerBlob
		}
		if s.availability[i] == layerBlob {
			result.CachedBlobLayers++
		}
	}
	if content != nil {
		result.CacheProbeLimited = content.Limited
	}
	manifest.MediaType = v1.MediaTypeImageManifest
	raw, err := json.Marshal(manifest)
	if err != nil {
		return im, s, cleanup, err
	}
	// Preserve exact incoming metadata if no layer representation changed.
	changed := false
	for i, d := range manifest.Layers {
		old := im.Descriptors[i+1]
		if d.Digest != old.Digest || d.Size != old.Size || d.MediaType != old.MediaType {
			changed = true
		}
	}
	if !changed {
		return im, s, cleanup, nil
	}
	view, err := image.Parse(raw, im.Config, "", im.Platform)
	return view, s, cleanup, err
}
