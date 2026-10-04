// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only
// Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.

package peer

import (
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/client"
	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/scitrera/oci-relay/internal/engine"
	"github.com/scitrera/oci-relay/internal/image"
	"github.com/scitrera/oci-relay/internal/transfer"
)

// A cache hit must match the entire parent chain, not an isolated diffID.
func commonPrefix(want []digest.Digest, have []string) int {
	for i := 0; i < len(want) && i < len(have); i++ {
		if string(want[i]) != have[i] {
			return i
		}
	}
	return min(len(want), len(have))
}

func cachedBase(ctx context.Context, e *engine.Engine, im *image.Image) (string, int, error) {
	var config v1.Image
	if err := json.Unmarshal(im.Config, &config); err != nil {
		return "", 0, err
	}
	listed, err := e.Client.ImageList(ctx, client.ImageListOptions{})
	if err != nil {
		return "", 0, err
	}
	best, id := 0, ""
	// Bound public-API discovery on busy hosts. Unexamined images simply cannot
	// contribute a cache hit; this never permits skipping unproven layers.
	for _, candidate := range listed.Items[:min(len(listed.Items), 128)] {
		info, err := e.Inspect(ctx, candidate.ID)
		if errdefs.IsNotFound(err) {
			continue
		}
		if err != nil {
			return "", 0, err
		}
		if info.Os != im.Platform.OS || info.Architecture != im.Platform.Architecture {
			continue
		}
		count := commonPrefix(config.RootFS.DiffIDs, info.RootFS.Layers)
		if count > best {
			best, id = count, info.ID
		}
		if best == len(config.RootFS.DiffIDs) {
			break
		}
	}
	return id, best, nil
}

type archiveEntry struct {
	name string
	size int64
	data []byte
	desc *v1.Descriptor
}

func loadEntries(im *image.Image, skip int, tag string, budget int64) ([]archiveEntry, int64, error) {
	if err := im.Validate(); err != nil {
		return nil, 0, err
	}
	if skip < 0 || skip >= len(im.Descriptors) || budget < 1 || budget > 1<<50 {
		return nil, 0, errors.New("invalid load prefix or archive budget")
	}
	if err := engine.ValidateTag(tag); err != nil {
		return nil, 0, err
	}
	configName := "config.json"
	entries := []archiveEntry{{name: configName, size: int64(len(im.Config)), data: im.Config}}
	layers := make([]string, len(im.Descriptors)-1)
	seen := map[digest.Digest]bool{}
	for i, d := range im.Descriptors[1:] {
		// This experiment accepts only exact uncompressed descriptors. Compressed
		// source representations continue through the normal registry importer.
		if d.MediaType != v1.MediaTypeImageLayer {
			return nil, 0, errors.New("load importer requires uncompressed OCI layers")
		}
		if i < skip {
			// These are archive placeholders, NOT blobs claiming a false digest.
			// Docker must find the previously verified exact chain; if it vanished,
			// loading this empty placeholder fails its required DiffID check.
			layers[i] = fmt.Sprintf("cached/%d.tar", i)
			entries = append(entries, archiveEntry{name: layers[i]})
			continue
		}
		layers[i] = "layers/" + d.Digest.Encoded() + ".tar"
		if !seen[d.Digest] {
			entries = append(entries, archiveEntry{name: layers[i], size: d.Size, desc: &d})
			seen[d.Digest] = true
		}
	}
	manifest, err := json.Marshal([]struct {
		Config   string
		RepoTags []string
		Layers   []string
	}{{configName, []string{tag}, layers}})
	if err != nil {
		return nil, 0, err
	}
	entries = append(entries, archiveEntry{name: "manifest.json", size: int64(len(manifest)), data: manifest})
	size := int64(1024) // final tar blocks
	for _, entry := range entries {
		if entry.size > budget {
			return nil, 0, errors.New("load archive exceeds explicit import budget")
		}
		n := 512 + (entry.size+511)/512*512
		if n > budget-size {
			return nil, 0, errors.New("load archive exceeds explicit import budget")
		}
		size += n
	}
	return entries, size, nil
}

func writeLoadArchive(ctx context.Context, w io.Writer, cache *transfer.Cache, entries []archiveEntry) error {
	tw := tar.NewWriter(w)
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := tw.WriteHeader(&tar.Header{Name: entry.name, Mode: 0600, Size: entry.size, Format: tar.FormatGNU}); err != nil {
			return err
		}
		if entry.desc == nil {
			if _, err := tw.Write(entry.data); err != nil {
				return err
			}
			continue
		}
		r, err := cache.Open(ctx, *entry.desc)
		if err != nil {
			return err
		}
		n, copyErr := io.Copy(tw, r)
		closeErr := r.Close()
		if err := errors.Join(copyErr, closeErr); err != nil {
			return err
		}
		if n != entry.size {
			return errors.New("load layer size mismatch")
		}
	}
	// No valid archive terminator on failed verification or cancelled input.
	return tw.Close()
}

// loadImage can decline cache-only selection before transferring data when
// the store, representation or cached chain is unsuitable. Import errors never
// trigger a fallback after data transfer starts.
func loadImage(ctx context.Context, e *engine.Engine, im *image.Image, cache *transfer.Cache, tag string, o PullOptions, result *Result) (bool, error) {
	info, err := e.Client.Info(ctx, client.InfoOptions{})
	if err != nil {
		return true, err
	}
	qualified := info.Info.OSType == "linux" && info.Info.Driver == "overlay2" &&
		(info.Info.ServerVersion == "29.1.3" || info.Info.ServerVersion == "29.2.1")
	if !qualified {
		if o.Import == "load-cached" {
			return false, nil
		}
		return true, errors.New("load importer is qualified only on Docker 29.1.3/29.2.1 Linux overlay2")
	}
	for _, d := range im.Descriptors[1:] {
		if d.MediaType != v1.MediaTypeImageLayer {
			if o.Import == "load-cached" {
				return false, nil
			}
			return true, errors.New("load importer requires uncompressed OCI layers")
		}
	}
	skip, base := 0, ""
	if o.Import == "load-cached" {
		base, skip, err = cachedBase(ctx, e, im)
		if err != nil {
			return true, err
		}
		if skip == 0 {
			return false, nil
		}
	}
	entries, size, err := loadEntries(im, skip, tag, o.MaxImport)
	if err != nil {
		return true, err
	}
	if base != "" {
		pin := tag + "-base"
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := e.RemoveTag(cleanup, pin); err != nil && !errdefs.IsNotFound(err) {
				result.CleanupError = err.Error()
			}
		}()
		if err := e.Tag(ctx, base, pin); err != nil {
			return true, err
		}
		pinned, err := e.Inspect(ctx, pin)
		if err != nil {
			return true, err
		}
		if pinned.ID != base {
			return true, errors.New("cached base changed while pinning")
		}
	}
	result.ImportMethod, result.ReusedLayers, result.ImportArchiveBytes = "load", skip, size
	loadCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	reader, writer := io.Pipe()
	defer reader.Close()
	finished := make(chan error, 1)
	start := time.Now()
	go func() {
		err := writeLoadArchive(loadCtx, writer, cache, entries)
		_ = writer.CloseWithError(err)
		finished <- err
	}()
	err = e.Load(loadCtx, reader)
	cancel()
	_ = reader.CloseWithError(err)
	writeErr := <-finished
	result.LoadSeconds = time.Since(start).Seconds()
	return true, errors.Join(err, writeErr)
}
