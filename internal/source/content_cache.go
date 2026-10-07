// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only
// Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.

package source

import (
	"context"
	"errors"
	"io"
	"os"

	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/scitrera/oci-relay/internal/fileio"
)

// ContentCache probes only requested CAS paths, retaining open file descriptors
// so a concurrent Docker GC unlink cannot remove advertised bytes. Acquisition
// still passes through transfer.Cache's full size/SHA verification.
type ContentCache struct {
	root        *os.Root
	files       map[digest.Digest]*os.File
	descriptors map[digest.Digest]v1.Descriptor
	Limited     bool
}

func NewContentCache(root string) (*ContentCache, error) {
	r, err := os.OpenRoot(root)
	if err != nil {
		return nil, err
	}
	return &ContentCache{root: r, files: map[digest.Digest]*os.File{}, descriptors: map[digest.Digest]v1.Descriptor{}}, nil
}

func (c *ContentCache) Probe(ctx context.Context, d v1.Descriptor) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if d.Digest.Validate() != nil || d.Digest.Algorithm() != digest.SHA256 || d.Size < 0 || d.Size > 1<<50 {
		return false, errors.New("invalid content cache descriptor")
	}
	if old, ok := c.descriptors[d.Digest]; ok {
		if old.Size != d.Size || old.MediaType != d.MediaType {
			return false, errors.New("conflicting content cache descriptor")
		}
		return true, nil
	}
	// Bound retained FDs, independently of image inventory size.
	if len(c.files) >= 256 {
		c.Limited = true
		return false, nil
	}
	f, err := c.root.Open("blobs/sha256/" + d.Digest.Encoded())
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Size() != d.Size {
		f.Close()
		return false, errors.New("cached content type/size mismatch")
	}
	c.files[d.Digest], c.descriptors[d.Digest] = f, d
	return true, nil
}

func (c *ContentCache) Fetch(ctx context.Context, d v1.Descriptor, w io.Writer) error {
	known, ok := c.descriptors[d.Digest]
	if !ok || known.Size != d.Size || known.MediaType != d.MediaType {
		return errors.New("unprobed content cache descriptor")
	}
	r := fileio.NewReader(c.files[d.Digest], 0, d.Size+1)
	defer r.Close()
	_, err := io.Copy(contextWriter{ctx, w}, r)
	return err
}

func (c *ContentCache) Close() error {
	var err error
	for _, f := range c.files {
		err = errors.Join(err, f.Close())
	}
	c.files = nil
	return errors.Join(err, c.root.Close())
}
