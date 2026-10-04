// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only
// Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.

package source

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	digest "github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/scitrera/oci-relay/internal/image"
	"github.com/vbatts/tar-split/tar/asm"
	"github.com/vbatts/tar-split/tar/storage"
)

// Native is an explicitly selected, read-only adapter for qualified classic
// overlay2 stores. The coordinator must hold a container reference to the image
// for its lifetime and mount the metadata and layer trees read-only.
type Native struct {
	Image              *image.Image
	PreparationSeconds float64
	MetadataBytes      int64
	metadata, layers   *os.Root
	blobs              map[digest.Digest]nativeLayer
}

type nativeLayer struct {
	metadataPath, cacheID string
	size                  int64
}

func NewNative(ctx context.Context, root, id, engineVersion string, platform v1.Platform) (_ *Native, returnErr error) {
	if engineVersion != "29.1.3" && engineVersion != "29.2.1" {
		return nil, errors.New("native source supports only qualified Docker 29.1.3/29.2.1 classic overlay2 stores")
	}
	dg := digest.Digest(id)
	if dg.Validate() != nil || dg.Algorithm() != digest.SHA256 {
		return nil, errors.New("native source requires an immutable sha256 image ID")
	}
	start := time.Now()
	n := &Native{blobs: map[digest.Digest]nativeLayer{}}
	defer func() {
		if returnErr != nil {
			returnErr = errors.Join(returnErr, n.Close())
		}
	}()
	var err error
	n.metadata, err = os.OpenRoot(path.Join(root, "image/overlay2"))
	if err != nil {
		return nil, err
	}
	n.layers, err = os.OpenRoot(path.Join(root, "overlay2"))
	if err != nil {
		return nil, err
	}
	config, err := readRoot(n.metadata, "imagedb/content/sha256/"+dg.Encoded(), image.MaxMetadata)
	if err != nil {
		return nil, err
	}
	if digest.FromBytes(config) != dg {
		return nil, errors.New("native config digest does not match pinned image ID")
	}
	var cfg v1.Image
	if err = json.Unmarshal(config, &cfg); err != nil {
		return nil, err
	}
	if len(cfg.RootFS.DiffIDs) > image.MaxDescriptors-1 {
		return nil, errors.New("too many native layers")
	}
	manifest := v1.Manifest{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: v1.MediaTypeImageManifest,
		Config: v1.Descriptor{Digest: dg, Size: int64(len(config)), MediaType: v1.MediaTypeImageConfig}}
	var parent digest.Digest
	for _, diff := range cfg.RootFS.DiffIDs {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		if diff.Validate() != nil || diff.Algorithm() != digest.SHA256 {
			return nil, errors.New("invalid native diffID")
		}
		chain := diff
		if parent != "" {
			chain = digest.FromString(string(parent) + " " + string(diff))
		}
		directory := "layerdb/sha256/" + chain.Encoded() + "/"
		storedDiff, err := readRoot(n.metadata, directory+"diff", 128)
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(string(storedDiff)) != string(diff) {
			return nil, errors.New("native layer diffID mismatch")
		}
		if parent != "" {
			storedParent, err := readRoot(n.metadata, directory+"parent", 128)
			if err != nil {
				return nil, err
			}
			if strings.TrimSpace(string(storedParent)) != string(parent) {
				return nil, errors.New("native layer parent mismatch")
			}
		}
		cache, err := readRoot(n.metadata, directory+"cache-id", 128)
		if err != nil {
			return nil, err
		}
		cacheID := strings.TrimSpace(string(cache))
		// BuildKit and classic registration use different opaque cache IDs.
		// Accept a bounded single component, never a path or a dot component.
		if len(cacheID) < 1 || len(cacheID) > 128 || strings.Trim(cacheID, "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_-") != "" {
			return nil, errors.New("invalid overlay2 cache ID")
		}
		layer := nativeLayer{metadataPath: directory + "tar-split.json.gz", cacheID: cacheID}
		up, closeMetadata, err := n.unpacker(ctx, layer)
		if err != nil {
			return nil, err
		}
		for {
			e, nextErr := up.Next()
			if nextErr == io.EOF {
				break
			}
			if nextErr != nil {
				closeMetadata()
				return nil, nextErr
			}
			length := e.Size
			if e.Type == storage.SegmentType {
				length = int64(len(e.Payload))
			}
			if length > (1<<50)-layer.size {
				closeMetadata()
				return nil, errors.New("native layer exceeds size limit")
			}
			layer.size += length
		}
		n.MetadataBytes += up.reader.used
		closeMetadata()
		if previous, ok := n.blobs[diff]; ok && previous.size != layer.size {
			return nil, errors.New("conflicting native layer lengths")
		}
		n.blobs[diff] = layer
		manifest.Layers = append(manifest.Layers, v1.Descriptor{Digest: diff, Size: layer.size, MediaType: v1.MediaTypeImageLayer})
		parent = chain
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	n.Image, err = image.Parse(raw, config, "", platform)
	if err != nil {
		return nil, err
	}
	n.PreparationSeconds = time.Since(start).Seconds()
	return n, nil
}

func readRoot(root *os.Root, name string, limit int64) ([]byte, error) {
	f, err := root.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return image.ReadBounded(f, limit)
}

// Bound metadata decoding and check context between entries. No layer body is
// read during preparation. Full SHA-256 verification remains in transfer.Cache.
type nativeMetadataReader struct {
	ctx  context.Context
	r    io.Reader
	used int64
}

func (r *nativeMetadataReader) Read(b []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	const maximum = 512 << 20
	if r.used >= maximum {
		return 0, errors.New("native tar-split metadata exceeds 512 MiB per layer")
	}
	b = b[:min(int64(len(b)), maximum-r.used)]
	n, err := r.r.Read(b)
	r.used += int64(n)
	return n, err
}

type nativeUnpacker struct {
	storage.Unpacker
	reader   *nativeMetadataReader
	position int
}

func (u *nativeUnpacker) Next() (*storage.Entry, error) {
	if err := u.reader.ctx.Err(); err != nil {
		return nil, err
	}
	e, err := u.Unpacker.Next()
	if err != nil {
		return nil, err
	}
	if e.Position != u.position || u.position >= 1<<20 {
		return nil, errors.New("invalid tar-split entry order/count")
	}
	u.position++
	switch e.Type {
	case storage.FileType:
		name := path.Clean(e.GetName())
		if e.Size < 0 || e.Size > 1<<50 || name == ".." || strings.HasPrefix(name, "../") || path.IsAbs(name) || (e.Size > 0 && (name == "." || len(e.Payload) != 8)) {
			return nil, errors.New("invalid tar-split file entry")
		}
	case storage.SegmentType:
		if len(e.Payload) > 8<<20 {
			return nil, errors.New("tar-split segment exceeds 8 MiB")
		}
	default:
		return nil, errors.New("unknown tar-split entry type")
	}
	return e, nil
}

func (n *Native) unpacker(ctx context.Context, layer nativeLayer) (*nativeUnpacker, func(), error) {
	f, err := n.metadata.Open(layer.metadataPath)
	if err != nil {
		return nil, nil, err
	}
	gz, err := gzip.NewReader(f)
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	r := &nativeMetadataReader{ctx: ctx, r: gz}
	return &nativeUnpacker{Unpacker: storage.NewJSONUnpacker(r), reader: r}, func() { _ = gz.Close(); _ = f.Close() }, nil
}

type nativeGetter struct {
	root *os.Root
	ctx  context.Context
}

func (g nativeGetter) Get(name string) (io.ReadCloser, error) {
	if err := g.ctx.Err(); err != nil {
		return nil, err
	}
	f, err := g.root.Open(name) // Rejects symlinks escaping the layer directory.
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		f.Close()
		return nil, errors.New("native payload is not a regular file")
	}
	return f, nil
}

type contextWriter struct {
	ctx context.Context
	w   io.Writer
}

func (w contextWriter) Write(b []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return w.w.Write(b)
}

func (n *Native) Fetch(ctx context.Context, d v1.Descriptor, w io.Writer) error {
	known, ok := n.Image.Descriptor(d.Digest)
	if !ok || known.Size != d.Size {
		return errors.New("descriptor not in native image")
	}
	if d.Digest == n.Image.Descriptors[0].Digest {
		_, err := io.Copy(contextWriter{ctx, w}, bytes.NewReader(n.Image.Config))
		return err
	}
	layer := n.blobs[d.Digest]
	root, err := n.layers.OpenRoot(layer.cacheID + "/diff")
	if err != nil {
		return err
	}
	defer root.Close()
	up, closeMetadata, err := n.unpacker(ctx, layer)
	if err != nil {
		return err
	}
	defer closeMetadata()
	if err = asm.WriteOutputTarStream(nativeGetter{root, ctx}, up, contextWriter{ctx, w}); err != nil {
		return fmt.Errorf("reconstruct layer %s: %w", d.Digest, err)
	}
	return nil
}

func (n *Native) Close() error {
	var err error
	if n.metadata != nil {
		err = n.metadata.Close()
		n.metadata = nil
	}
	if n.layers != nil {
		err = errors.Join(err, n.layers.Close())
		n.layers = nil
	}
	return err
}
