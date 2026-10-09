// SPDX-FileCopyrightText: 2026 Spark Arena
// SPDX-License-Identifier: Apache-2.0

// Package fileio provides bounded sequential reads of already-open local data
// files. Linux prefers direct I/O; other platforms use buffered reads.
package fileio

import (
	"errors"
	"io"
	"math"
	"os"
)

const BufferSize = 1 << 20

// NewReader reads a section without changing f's offset or flags. It borrows f:
// callers must keep f open until this reader is closed. Separate readers may
// concurrently read the same pinned FD, including after the file is unlinked.
func NewReader(f *os.File, offset, size int64) io.ReadCloser {
	if offset < 0 || size < 0 || offset > math.MaxInt64-size {
		return invalidReader{}
	}
	return newReader(f, offset, size)
}

// ReadFile takes ownership of f and reads from its beginning, including bytes
// beyond a previously observed size so callers can detect unexpected growth.
func ReadFile(f *os.File) io.ReadCloser {
	return &ownedReader{ReadCloser: NewReader(f, 0, math.MaxInt64), file: f}
}

type ownedReader struct {
	io.ReadCloser
	file *os.File
}

func (r *ownedReader) Close() error { return errors.Join(r.ReadCloser.Close(), r.file.Close()) }

type invalidReader struct{}

func (invalidReader) Read([]byte) (int, error) { return 0, errors.New("invalid file section") }
func (invalidReader) Close() error             { return nil }
