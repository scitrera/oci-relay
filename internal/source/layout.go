// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"context"
	"io"
	"os"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/scitrera/oci-relay/internal/fileio"
	"github.com/scitrera/oci-relay/internal/image"
)

type Layout struct{ Root string }

func (l *Layout) Fetch(ctx context.Context, d v1.Descriptor, w io.Writer) error {
	path, err := image.BlobPath(l.Root, d.Digest)
	if err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	r := fileio.ReadFile(f)
	defer r.Close()
	buf := make([]byte, 64<<10)
	for {
		if err = ctx.Err(); err != nil {
			return err
		}
		n, re := r.Read(buf)
		if n > 0 {
			if _, err = w.Write(buf[:n]); err != nil {
				return err
			}
		}
		if re == io.EOF {
			return nil
		}
		if re != nil {
			return re
		}
	}
}
