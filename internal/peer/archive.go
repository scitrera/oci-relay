// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package peer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/distribution/reference"
	digest "github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/scitrera/oci-relay/internal/engine"
	"github.com/scitrera/oci-relay/internal/image"
	"github.com/scitrera/oci-relay/internal/transfer"
)

// portableEntries emits an OCI layout plus Docker's manifest.json. Both stores
// consume the same verified blobs; OCI import retains the original manifest,
// while classic import verifies the config's ordered uncompressed DiffIDs.
func portableEntries(im *image.Image, tag string, budget int64) ([]archiveEntry, int64, error) {
	if err := im.Validate(); err != nil {
		return nil, 0, err
	}
	if err := engine.ValidateTag(tag); err != nil {
		return nil, 0, err
	}
	if budget < 4<<20 || budget > 1<<50 {
		return nil, 0, errors.New("invalid archive import budget")
	}
	blob := func(d digest.Digest) string { return "blobs/sha256/" + d.Encoded() }
	entries := []archiveEntry{}
	add := func(name string, data []byte) {
		entries = append(entries, archiveEntry{name: name, size: int64(len(data)), data: data})
	}
	add("oci-layout", []byte(`{"imageLayoutVersion":"1.0.0"}`))
	named, err := reference.ParseNormalizedNamed(tag)
	if err != nil {
		return nil, 0, err
	}
	index, err := json.Marshal(v1.Index{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: v1.MediaTypeImageIndex, Manifests: []v1.Descriptor{{MediaType: im.MediaType, Digest: im.Digest, Size: int64(len(im.Manifest)), Platform: &im.Platform, Annotations: map[string]string{"io.containerd.image.name": named.String(), v1.AnnotationRefName: tag}}}})
	if err != nil {
		return nil, 0, err
	}
	add("index.json", index)
	add(blob(im.Digest), im.Manifest)
	add(blob(im.Descriptors[0].Digest), im.Config)
	layers := make([]string, len(im.Descriptors)-1)
	seen := map[digest.Digest]bool{im.Digest: true, im.Descriptors[0].Digest: true}
	for i, d := range im.Descriptors[1:] {
		layers[i] = blob(d.Digest)
		if !seen[d.Digest] {
			entries = append(entries, archiveEntry{name: layers[i], size: d.Size, desc: &d})
			seen[d.Digest] = true
		}
	}
	manifest, err := json.Marshal([]struct {
		Config   string
		RepoTags []string
		Layers   []string
	}{{blob(im.Descriptors[0].Digest), []string{tag}, layers}})
	if err != nil {
		return nil, 0, err
	}
	add("manifest.json", manifest)
	size := int64(1024)
	for _, entry := range entries {
		if entry.size > budget {
			return nil, 0, errors.New("archive exceeds explicit import budget")
		}
		n := 512 + (entry.size+511)/512*512
		if n > budget-size {
			return nil, 0, errors.New("archive exceeds explicit import budget")
		}
		size += n
	}
	return entries, size, nil
}

func importArchive(ctx context.Context, e *engine.Engine, im *image.Image, cache *transfer.Cache, tag string, o PullOptions, result *Result) error {
	entries, size, err := portableEntries(im, tag, o.MaxImport)
	if err != nil {
		return err
	}
	result.ImportMethod, result.ImportArchiveBytes = "archive", size
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	reader, writer := io.Pipe()
	defer reader.Close()
	stop := context.AfterFunc(ctx, func() { _ = reader.CloseWithError(ctx.Err()) })
	defer stop()
	done := make(chan error, 1)
	start := time.Now()
	go func() {
		err := writeLoadArchive(ctx, writer, cache, entries)
		_ = writer.CloseWithError(err)
		done <- err
	}()
	err = e.LoadArchive(ctx, reader, size, o.ImportDirectory)
	cancel()
	_ = reader.CloseWithError(err)
	writeErr := <-done
	result.LoadSeconds = time.Since(start).Seconds()
	return errors.Join(err, writeErr)
}
