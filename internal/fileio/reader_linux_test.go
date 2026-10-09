// SPDX-FileCopyrightText: 2026 Spark Arena
// SPDX-License-Identifier: Apache-2.0

package fileio

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"unsafe"

	"golang.org/x/sys/unix"
)

func TestDirectAlignmentFallbackAndIOErrors(t *testing.T) {
	for _, failure := range []error{nil, unix.EINVAL, unix.EOPNOTSUPP, unix.EIO} {
		f, data := fixture(t, 3*BufferSize+37)
		r := NewReader(f, 123, int64(len(data)-123))
		defer r.Close()
		d, ok := r.(*directReader)
		if !ok {
			t.Skip("test filesystem does not support direct I/O")
		}
		flags, err := unix.FcntlInt(d.file.Fd(), unix.F_GETFL, 0)
		if err != nil || flags&unix.O_DIRECT == 0 {
			t.Fatalf("direct FD: %x %v", flags, err)
		}
		flags, err = unix.FcntlInt(f.Fd(), unix.F_GETFL, 0)
		if err != nil || flags&unix.O_DIRECT != 0 {
			t.Fatal("modified original descriptor")
		}
		calls := 0
		d.readAt = func(b []byte, offset int64) (int, error) {
			calls++
			if uintptr(unsafe.Pointer(&b[0]))%uintptr(d.memAlignment) != 0 || offset%int64(d.alignment) != 0 || len(b)%d.alignment != 0 {
				t.Fatal("unaligned direct read")
			}
			if calls == 2 && failure != nil {
				return 0, failure
			}
			return d.readDirect(b, offset)
		}
		got, err := io.ReadAll(r)
		if failure == unix.EIO {
			if !errors.Is(err, unix.EIO) || d.file == nil {
				t.Fatalf("I/O error incorrectly masked: %v", err)
			}
		} else if err != nil || !bytes.Equal(got, data[123:]) {
			t.Fatalf("fallback lost/duplicated data: %d %v", len(got), err)
		}
		if (failure == unix.EINVAL || failure == unix.EOPNOTSUPP) && d.file != nil {
			t.Fatal("unsupported direct I/O did not fall back")
		}
		if failure == nil && d.file == nil {
			t.Fatal("partial tail unnecessarily fell back to buffered reads")
		}
	}
}

func TestDirectSmallPartialTailDoesNotFallBack(t *testing.T) {
	for _, size := range []int{1, 17, 511, 513, 4097} {
		f, data := fixture(t, size)
		r := NewReader(f, 0, int64(size+1))
		d, ok := r.(*directReader)
		if !ok {
			r.Close()
			t.Skip("test filesystem does not support direct I/O")
		}
		got, err := io.ReadAll(r)
		if err != nil || !bytes.Equal(got, data) || d.file == nil {
			t.Fatalf("size=%d: direct tail failed: %v", size, err)
		}
		r.Close()
	}
}
