// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package fileio

import (
	"errors"
	"io"
	"os"
	"strconv"
	"unsafe"

	"golang.org/x/sys/unix"
)

func newReader(f *os.File, offset, size int64) io.ReadCloser {
	fallback := func() io.ReadCloser { return io.NopCloser(io.NewSectionReader(f, offset, size)) }
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || size == 0 {
		return fallback()
	}
	// Reopen the already validated inode, not its original pathname. Unlike dup,
	// this creates an independent open-file description: no shared flags/offset
	// changes, and it works for pinned content files removed by concurrent GC.
	direct, err := os.OpenFile("/proc/self/fd/"+strconv.FormatUint(uint64(f.Fd()), 10), os.O_RDONLY|unix.O_DIRECT, 0)
	if err != nil {
		return fallback()
	}
	memAlign, offsetAlign := 4096, 4096
	var sx unix.Statx_t
	if unix.Statx(int(direct.Fd()), "", unix.AT_EMPTY_PATH, unix.STATX_DIOALIGN, &sx) == nil && sx.Mask&unix.STATX_DIOALIGN != 0 {
		memAlign, offsetAlign = int(sx.Dio_mem_align), int(sx.Dio_offset_align)
	}
	if memAlign < 1 || memAlign > 64<<10 || offsetAlign < 1 || offsetAlign > 64<<10 || BufferSize%offsetAlign != 0 {
		direct.Close()
		return fallback()
	}
	// Small files need only one aligned block, rather than a 1 MiB allocation.
	length := min(int64(BufferSize), max(int64(offsetAlign), st.Size()-offset+offset%int64(offsetAlign)))
	length = (length + int64(offsetAlign) - 1) / int64(offsetAlign) * int64(offsetAlign)
	return &directReader{file: direct, original: f, pos: offset, limit: offset + size, alignment: offsetAlign, memAlignment: memAlign, bufferSize: int(length)}
}

type directReader struct {
	file, original                      *os.File
	pos, limit                          int64
	alignment, memAlignment, bufferSize int
	buffer, available                   []byte
	readErr                             error
	closed                              bool
	// Per-instance hook also lets tests exercise a filesystem rejecting direct
	// reads after successful initial reads, without relying on a particular FS.
	readAt func([]byte, int64) (int, error)
}

func unsupported(err error) bool {
	return errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOSYS)
}

func (r *directReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, os.ErrClosed
	}
	if len(p) == 0 {
		return 0, nil
	}
	if r.pos >= r.limit {
		return 0, io.EOF
	}
	p = p[:min(int64(len(p)), r.limit-r.pos)]
	if r.file == nil {
		n, err := r.original.ReadAt(p, r.pos)
		r.pos += int64(n)
		return n, err
	}
	if len(r.available) == 0 {
		if r.readErr != nil {
			return 0, r.readErr
		}
		if r.buffer == nil {
			backing := make([]byte, r.bufferSize+r.memAlignment-1)
			start := int((uintptr(r.memAlignment) - uintptr(unsafe.Pointer(&backing[0]))%uintptr(r.memAlignment)) % uintptr(r.memAlignment))
			r.buffer = backing[start : start+r.bufferSize]
		}
		offset := r.pos - r.pos%int64(r.alignment)
		read := r.readAt
		if read == nil {
			read = r.readDirect
		}
		n, err := read(r.buffer, offset)
		if unsupported(err) {
			// Resume at the exact consumer offset, not the aligned/readahead offset.
			// Real I/O errors propagate and never trigger a buffered retry.
			r.file.Close()
			r.file, r.buffer, r.available = nil, nil, nil
			return r.Read(p)
		}
		r.readErr = err
		skip := int(r.pos - offset)
		if n <= skip {
			if err == nil {
				// An aligned read can include the final partial block we already
				// consumed. No bytes at/after the logical offset means EOF.
				err = io.EOF
			}
			return 0, err
		}
		r.available = r.buffer[skip:n]
	}
	n := copy(p, r.available)
	r.available = r.available[n:]
	r.pos += int64(n)
	return n, nil
}

func (r *directReader) readDirect(b []byte, offset int64) (int, error) {
	// os.File.ReadAt retries a short read with the remaining subslice. At an
	// unaligned EOF that retry has an invalid address/offset for O_DIRECT and
	// can report EINVAL instead of EOF. Issue one aligned pread at a time; Read
	// realigns subsequent requests and skips any already-consumed prefix.
	for {
		n, err := unix.Pread(int(r.file.Fd()), b, offset)
		if n < 0 {
			n = 0
		}
		if n == 0 && errors.Is(err, unix.EINTR) {
			continue
		}
		return n, err
	}
}

func (r *directReader) Close() error {
	r.closed = true
	r.buffer, r.available = nil, nil
	if r.file != nil {
		err := r.file.Close()
		r.file = nil
		return err
	}
	return nil
}
