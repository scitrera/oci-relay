// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only
// Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.

//go:build !linux

package fileio

import (
	"io"
	"os"
)

func newReader(f *os.File, offset, size int64) io.ReadCloser {
	return io.NopCloser(io.NewSectionReader(f, offset, size))
}
