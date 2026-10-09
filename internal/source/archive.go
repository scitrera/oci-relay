// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/spark-arena/oci-relay/internal/engine"
	"github.com/spark-arena/oci-relay/internal/fileio"
	"github.com/spark-arena/oci-relay/internal/image"
	"github.com/spark-arena/oci-relay/internal/privatefs"
)

type ArchiveOptions struct {
	Reference string
	Platform  v1.Platform
	MaxSpool  int64
	SpoolDir  string
}

// Archive explicitly stages one Docker export. It serves independent sections
// of that file; layer bodies are never extracted to a second set of files.
// The export's OCI representation may differ from the registry manifest.
type Archive struct {
	Image              *image.Image
	Bytes              int64
	PreparationSeconds float64
	FirstByteSeconds   float64
	file               *os.File
	dir                string
	entries            map[string]archiveSection
}

type archiveSection struct{ offset, size int64 }

func NewArchive(ctx context.Context, eng *engine.Engine, o ArchiveOptions) (_ *Archive, returnErr error) {
	if o.MaxSpool < image.MaxMetadata || o.MaxSpool > 1<<50 {
		return nil, errors.New("docker-save requires an explicit full-archive spool budget between 4 MiB and 1 PiB")
	}
	start := time.Now()
	inspected, err := eng.Inspect(ctx, o.Reference)
	if err != nil {
		return nil, err
	}
	// Pin the immutable Docker image ID so a moving tag cannot change the export.
	id := inspected.ID
	if inspected.Descriptor != nil {
		if o.Platform.OS == "" {
			o.Platform = v1.Platform{OS: inspected.Os, Architecture: inspected.Architecture, Variant: inspected.Variant}
		}
		inspected, err = eng.InspectPlatform(ctx, id, o.Platform)
		if err != nil {
			return nil, err
		}
	}
	dir, err := os.MkdirTemp(o.SpoolDir, "oci-relay-archive-")
	if err != nil {
		return nil, err
	}
	if err = privatefs.Ensure(dir); err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	a := &Archive{dir: dir}
	defer func() {
		if returnErr != nil {
			returnErr = errors.Join(returnErr, a.Close())
		}
	}()
	a.file, err = os.OpenFile(filepath.Join(dir, "image.tar"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	r, err := eng.Save(ctx, id, o.Platform)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	stop := context.AfterFunc(ctx, func() { _ = r.Close() })
	defer stop()
	// Check the cap before each disk write; never grow the owned file beyond it.
	buf := make([]byte, 256<<10)
	for {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		n, readErr := r.Read(buf)
		if n > 0 {
			if a.Bytes == 0 {
				a.FirstByteSeconds = time.Since(start).Seconds()
			}
			if int64(n) > o.MaxSpool-a.Bytes {
				return nil, errors.New("Docker export exceeds full-archive spool budget")
			}
			if _, err = a.file.Write(buf[:n]); err != nil {
				return nil, err
			}
			a.Bytes += int64(n)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, readErr
		}
	}
	if err = a.index(ctx); err != nil {
		return nil, err
	}
	if inspected.Descriptor == nil {
		if _, ok := a.entries["oci-layout"]; ok {
			a.Image, err = image.LoadLayoutReader(a.read, "", o.Platform)
		} else {
			a.Image, err = a.classic(o.Platform, id)
		}
	} else {
		// Exported indexes can retain attestations and other platforms. Resolve
		// only the exact platform manifest already pinned by Docker inspection.
		name, pathErr := image.BlobPath("", inspected.Descriptor.Digest)
		if pathErr != nil {
			return nil, pathErr
		}
		raw, readErr := a.read(name, image.MaxMetadata)
		if readErr != nil {
			return nil, readErr
		}
		if err = image.Verify(raw, *inspected.Descriptor); err != nil {
			return nil, err
		}
		var m v1.Manifest
		if err = json.Unmarshal(raw, &m); err != nil {
			return nil, err
		}
		name, pathErr = image.BlobPath("", m.Config.Digest)
		if pathErr != nil {
			return nil, pathErr
		}
		config, readErr := a.read(name, image.MaxMetadata)
		if readErr != nil {
			return nil, readErr
		}
		a.Image, err = image.Parse(raw, config, inspected.Descriptor.Digest, o.Platform)
	}
	if err != nil {
		return nil, fmt.Errorf("Docker export must contain a single-platform OCI layout: %w", err)
	}
	if (inspected.Descriptor == nil && string(a.Image.Descriptors[0].Digest) != id) || (inspected.Descriptor != nil && !engine.MatchesImage(inspected, a.Image)) {
		return nil, errors.New("Docker export identity does not match pinned image")
	}
	for _, d := range a.Image.Descriptors {
		name, _ := image.BlobPath("", d.Digest)
		s, ok := a.entries[filepath.ToSlash(name)]
		if !ok || s.size != d.Size {
			return nil, fmt.Errorf("archive blob missing or wrong size: %s", d.Digest)
		}
	}
	a.PreparationSeconds = time.Since(start).Seconds()
	return a, nil
}

func (a *Archive) index(ctx context.Context) error {
	if _, err := a.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	tr := tar.NewReader(a.file)
	a.entries = make(map[string]archiveSection)
	for count := 0; ; count++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		h, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if count >= image.MaxDescriptors*4 {
			return errors.New("archive has too many entries")
		}
		name := strings.TrimSuffix(h.Name, "/")
		if name == "." || path.IsAbs(name) || path.Clean(name) != name || strings.HasPrefix(name, "../") {
			return errors.New("archive contains an invalid path")
		}
		if h.Typeflag == tar.TypeDir {
			continue
		}
		if h.Typeflag != tar.TypeReg {
			return errors.New("archive links and special files are unsupported")
		}
		if _, exists := a.entries[filepath.ToSlash(name)]; exists {
			return errors.New("archive contains duplicate file names")
		}
		offset, err := a.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		if h.Size < 0 || offset > a.Bytes || h.Size > a.Bytes-offset {
			return errors.New("truncated archive entry")
		}
		a.entries[name] = archiveSection{offset: offset, size: h.Size}
	}
}

func (a *Archive) read(name string, limit int64) ([]byte, error) {
	name = filepath.ToSlash(name)
	s, ok := a.entries[filepath.ToSlash(name)]
	if !ok {
		return nil, fmt.Errorf("archive entry %q missing", name)
	}
	if s.size > limit {
		return nil, errors.New("archive metadata exceeds limit")
	}
	return image.ReadBounded(io.NewSectionReader(a.file, s.offset, s.size), limit)
}

func (a *Archive) Fetch(ctx context.Context, d v1.Descriptor, w io.Writer) error {
	known, ok := a.Image.Descriptor(d.Digest)
	if !ok || known.Size != d.Size {
		return errors.New("descriptor not in prepared archive")
	}
	name, _ := image.BlobPath("", d.Digest)
	s := a.entries[filepath.ToSlash(name)]
	r := fileio.NewReader(a.file, s.offset, s.size)
	defer r.Close()
	buf := make([]byte, 256<<10)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := r.Read(buf)
		if n > 0 {
			written, writeErr := w.Write(buf[:n])
			if writeErr != nil {
				return writeErr
			}
			if written != n {
				return io.ErrShortWrite
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// Close runs only after transfer readers have stopped.
func (a *Archive) Close() error {
	var err error
	if a.file != nil {
		err = a.file.Close()
		a.file = nil
	}
	if a.dir != "" {
		err = errors.Join(err, os.RemoveAll(a.dir))
		a.dir = ""
	}
	return err
}
