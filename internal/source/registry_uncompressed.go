// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	digest "github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/scitrera/oci-relay/internal/image"
)

// ExportUncompressed is an opt-in benchmark path, not registry source policy.
// It stages a complete layout before serving and therefore does not skip cached
// receiver layers. Both compressed digests and uncompressed DiffIDs must verify.
// The caller owns the returned private directory; every failed export removes it.
// maxBytes bounds payload plus metadata; workers bounds simultaneous decoders.
func (r *Registry) ExportUncompressed(ctx context.Context, spool string, maxBytes int64, workers int) (root string, returnErr error) {
	if maxBytes < 8<<20 || maxBytes > 1<<50 || workers < 1 || workers > 32 {
		return "", errors.New("export requires an 8 MiB..1 PiB disk budget and 1..32 workers")
	}
	root, err := os.MkdirTemp(spool, "oci-relay-uncompressed-")
	if err != nil {
		return "", err
	}
	dir := root
	defer func() {
		if returnErr != nil {
			returnErr = errors.Join(returnErr, os.RemoveAll(dir))
		}
	}()
	blobs := filepath.Join(root, "blobs", "sha256")
	if err = os.MkdirAll(blobs, 0700); err != nil {
		return "", err
	}
	var manifest v1.Manifest
	var cfg v1.Image
	_ = json.Unmarshal(r.Image.Manifest, &manifest)
	_ = json.Unmarshal(r.Image.Config, &cfg)
	for _, d := range manifest.Layers {
		switch d.MediaType {
		case v1.MediaTypeImageLayer, v1.MediaTypeImageLayerGzip, "application/vnd.docker.image.rootfs.diff.tar.gzip":
		default:
			return "", fmt.Errorf("uncompressed experiment does not support %s", d.MediaType)
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var used atomic.Int64
	// Reserve the maximum manifest/config/index footprint before writing layers.
	used.Store(int64(len(r.Image.Config)) + 2*image.MaxMetadata + 1024)
	if used.Load() > maxBytes {
		return "", errors.New("export metadata exceeds disk budget")
	}
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	var firstErr error
	var once sync.Once
	for i, d := range manifest.Layers {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			break
		}
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		go func(i int, d v1.Descriptor) {
			defer wg.Done()
			defer func() { <-sem }()
			size, err := r.exportLayer(ctx, d, cfg.RootFS.DiffIDs[i], blobs, &used, maxBytes)
			if err != nil {
				once.Do(func() { firstErr = err; cancel() })
				return
			}
			manifest.Layers[i] = v1.Descriptor{MediaType: v1.MediaTypeImageLayer, Digest: cfg.RootFS.DiffIDs[i], Size: size}
		}(i, d)
	}
	wg.Wait()
	if firstErr != nil {
		return "", firstErr
	}
	if err = ctx.Err(); err != nil {
		return "", err
	}
	manifest.MediaType = v1.MediaTypeImageManifest
	raw, err := json.Marshal(manifest)
	if err != nil {
		return "", err
	}
	im, err := image.Parse(raw, r.Image.Config, "", r.Image.Platform)
	if err != nil {
		return "", err
	}
	index, err := json.Marshal(v1.Index{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: v1.MediaTypeImageIndex,
		Manifests: []v1.Descriptor{{MediaType: im.MediaType, Digest: im.Digest, Size: int64(len(raw)), Platform: &im.Platform}}})
	if err != nil {
		return "", err
	}
	for name, data := range map[string][]byte{
		"oci-layout": []byte(`{"imageLayoutVersion":"1.0.0"}`), "index.json": index,
		filepath.Join("blobs", "sha256", im.Digest.Encoded()):                raw,
		filepath.Join("blobs", "sha256", im.Descriptors[0].Digest.Encoded()): im.Config,
	} {
		if err = os.WriteFile(filepath.Join(root, name), data, 0600); err != nil {
			return "", err
		}
	}
	return root, nil
}

func (r *Registry) exportLayer(ctx context.Context, d v1.Descriptor, diffID digest.Digest, blobs string, used *atomic.Int64, limit int64) (int64, error) {
	f, err := os.CreateTemp(blobs, "partial-")
	if err != nil {
		return 0, err
	}
	defer f.Close()
	defer os.Remove(f.Name())
	reader, writer := io.Pipe()
	done := make(chan error, 1)
	go func() {
		err := r.Fetch(ctx, d, writer)
		_ = writer.CloseWithError(err)
		done <- err
	}()
	var decoded io.Reader = reader
	var gz *gzip.Reader
	if d.MediaType != v1.MediaTypeImageLayer {
		gz, err = gzip.NewReader(reader)
		if err == nil {
			decoded = gz
		}
	}
	hash := digest.SHA256.Digester()
	var size int64
	if err == nil {
		size, err = io.CopyBuffer(io.MultiWriter(&exportBudgetWriter{ctx, f, used, limit}, hash.Hash()), decoded, make([]byte, 64<<10))
	}
	if gz != nil {
		err = errors.Join(err, gz.Close())
	}
	_ = reader.CloseWithError(err)
	err = errors.Join(err, <-done, f.Close())
	if err != nil {
		return 0, err
	}
	if hash.Digest() != diffID {
		return 0, errors.New("registry layer uncompressed DiffID mismatch")
	}
	if err = os.Rename(f.Name(), filepath.Join(blobs, diffID.Encoded())); err != nil {
		return 0, err
	}
	return size, nil
}

type exportBudgetWriter struct {
	ctx   context.Context
	w     io.Writer
	used  *atomic.Int64
	limit int64
}

func (w *exportBudgetWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	if w.used.Add(int64(len(p))) > w.limit {
		return 0, errors.New("uncompressed export exceeds disk budget")
	}
	return w.w.Write(p)
}
