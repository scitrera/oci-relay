// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only
// Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.

package fileio

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func fixture(t *testing.T, size int) (*os.File, []byte) {
	t.Helper()
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251)
	}
	path := filepath.Join(t.TempDir(), "data")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f, data
}

func TestSectionsAndPartialTails(t *testing.T) {
	for _, size := range []int{0, 1, 511, 512, 4095, 4096, BufferSize + 17, 2*BufferSize + 123} {
		f, data := fixture(t, size)
		for _, offset := range []int{0, 1, 127, 4097} {
			if offset > size {
				continue
			}
			for _, length := range []int{size - offset, (size - offset) / 2} {
				r := NewReader(f, int64(offset), int64(length))
				var out bytes.Buffer
				_, err := io.CopyBuffer(&out, struct{ io.Reader }{r}, make([]byte, 173))
				r.Close()
				if err != nil || !bytes.Equal(out.Bytes(), data[offset:offset+length]) {
					t.Fatalf("size=%d offset=%d length=%d: %v", size, offset, length, err)
				}
			}
		}
	}
}

func TestPinnedFileConcurrentSections(t *testing.T) {
	f, data := fixture(t, 2*BufferSize+213)
	// Keep the original descriptor pinned while a Docker-style GC unlink happens.
	if err := os.Remove(f.Name()); err != nil {
		t.Skipf("platform cannot unlink open file: %v", err)
	}
	if _, err := f.Seek(17, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			r := NewReader(f, int64(offset), int64(len(data)-offset))
			defer r.Close()
			got, err := io.ReadAll(r)
			if err != nil || !bytes.Equal(got, data[offset:]) {
				t.Errorf("pinned read: %v", err)
			}
		}(i * 123)
	}
	wg.Wait()
	if pos, err := f.Seek(0, io.SeekCurrent); err != nil || pos != 17 {
		t.Fatalf("changed shared offset: %d %v", pos, err)
	}
}

func TestReadFileSeesGrowthAndOwnsFD(t *testing.T) {
	f, data := fixture(t, 100)
	r := ReadFile(f)
	appendFile, err := os.OpenFile(f.Name(), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = appendFile.Write([]byte("extra"))
	appendFile.Close()
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(got, append(data, []byte("extra")...)) {
		t.Fatalf("growth hidden: %v", err)
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = f.Stat(); err == nil {
		t.Fatal("owned file left open")
	}
}
