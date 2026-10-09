// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"runtime"
	"time"

	digest "github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/scitrera/oci-relay/internal/engine"
	"github.com/scitrera/oci-relay/internal/fileio"
	"github.com/scitrera/oci-relay/internal/image"
)

// Containerd reads retained OCI content, never snapshotter files or databases.
// Its caller must pin the Docker image and provide a read-only content root.
// Compressed bytes keep their original digest; no export or recompression occurs.
type Containerd struct {
	Image              *image.Image
	PreparationSeconds float64
	MetadataBytes      int64
	root               *os.Root
}

func NewContainerd(ctx context.Context, root, id, version string, platform v1.Platform) (_ *Containerd, returnErr error) {
	if !engine.SupportsStoreVersion(version) {
		return nil, errors.New("native containerd source requires Docker 29 or newer")
	}
	dg := digest.Digest(id)
	if dg.Validate() != nil || dg.Algorithm() != digest.SHA256 {
		return nil, errors.New("containerd source requires a pinned sha256 manifest/index ID")
	}
	start := time.Now()
	r, err := os.OpenRoot(root)
	if err != nil {
		return nil, err
	}
	n := &Containerd{root: r}
	defer func() {
		if returnErr != nil {
			returnErr = errors.Join(returnErr, n.Close())
		}
	}()
	if platform.OS == "" {
		platform.OS = runtime.GOOS
	}
	if platform.Architecture == "" {
		platform.Architecture = runtime.GOARCH
	}
	read := func(name string, limit int64) ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		f, err := r.Open(name)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		st, err := f.Stat()
		if err != nil || !st.Mode().IsRegular() {
			return nil, errors.New("containerd content is not a regular file")
		}
		const metadataBudget = 64 << 20
		if st.Size() > metadataBudget-n.MetadataBytes {
			return nil, errors.New("containerd metadata traversal exceeds 64 MiB")
		}
		b, err := image.ReadBounded(f, min(limit, metadataBudget-n.MetadataBytes))
		n.MetadataBytes += int64(len(b))
		return b, err
	}
	raw, err := read("blobs/sha256/"+dg.Encoded(), image.MaxMetadata)
	if err != nil {
		return nil, err
	}
	if digest.FromBytes(raw) != dg {
		return nil, errors.New("containerd root digest mismatch")
	}
	var header struct {
		MediaType string `json:"mediaType"`
	}
	if err := json.Unmarshal(raw, &header); err != nil {
		return nil, err
	}
	index, err := json.Marshal(v1.Index{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: v1.MediaTypeImageIndex,
		Manifests: []v1.Descriptor{{Digest: dg, Size: int64(len(raw)), MediaType: header.MediaType}}})
	if err != nil {
		return nil, err
	}
	n.Image, err = image.LoadLayoutReader(func(name string, limit int64) ([]byte, error) {
		switch name {
		case "oci-layout":
			return []byte(`{"imageLayoutVersion":"1.0.0"}`), nil
		case "index.json":
			return index, nil
		default:
			return read(name, limit)
		}
	}, "", platform)
	if err != nil {
		return nil, err
	}
	for _, d := range n.Image.Descriptors {
		st, err := r.Stat("blobs/sha256/" + d.Digest.Encoded())
		if err != nil {
			return nil, err
		}
		if !st.Mode().IsRegular() || st.Size() != d.Size {
			return nil, errors.New("containerd blob type/size mismatch")
		}
	}
	n.PreparationSeconds = time.Since(start).Seconds()
	return n, nil
}

func (n *Containerd) Fetch(ctx context.Context, d v1.Descriptor, w io.Writer) error {
	known, ok := n.Image.Descriptor(d.Digest)
	if !ok || known.Size != d.Size || known.MediaType != d.MediaType {
		return errors.New("descriptor not in containerd image")
	}
	f, err := n.root.Open("blobs/sha256/" + d.Digest.Encoded())
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Size() != d.Size {
		return errors.New("containerd blob type/size changed")
	}
	// transfer.Cache verifies length and full SHA-256 on every acquired blob.
	r := fileio.NewReader(f, 0, d.Size+1)
	defer r.Close()
	_, err = io.Copy(contextWriter{ctx, w}, r)
	return err
}

func (n *Containerd) Close() error { return n.root.Close() }
