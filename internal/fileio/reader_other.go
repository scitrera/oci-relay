// SPDX-FileCopyrightText: 2026 Spark Arena
// SPDX-License-Identifier: Apache-2.0

//go:build !linux

package fileio

import (
	"io"
	"os"
)

func newReader(f *os.File, offset, size int64) io.ReadCloser {
	return io.NopCloser(io.NewSectionReader(f, offset, size))
}
