// SPDX-FileCopyrightText: 2026 Spark Arena
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/client"
	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/spark-arena/oci-relay/internal/engine"
	"github.com/spark-arena/oci-relay/internal/image"
	"github.com/spark-arena/oci-relay/internal/testutil"
)

func archiveFixture(t *testing.T) ([]byte, *image.Image) {
	t.Helper()
	root, im := testutil.Layout(t, 5<<20)
	var data bytes.Buffer
	tw := tar.NewWriter(&data)
	err := filepath.WalkDir(root, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		b, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, name)
		if err = tw.WriteHeader(&tar.Header{Name: rel, Mode: 0600, Size: int64(len(b))}); err != nil {
			return err
		}
		_, err = tw.Write(b)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = tw.Close(); err != nil {
		t.Fatal(err)
	}
	return data.Bytes(), im
}

func archiveEngine(t *testing.T, id string, export http.HandlerFunc) *engine.Engine {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/images/fixture/json") {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"Id": id})
		} else if strings.HasSuffix(r.URL.Path, "/images/get") {
			if r.URL.Query().Get("names") != id {
				t.Error("export did not pin the inspected ID")
			}
			export(w, r)
		} else {
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(s.Close)
	c, err := client.New(client.WithHost(s.URL), client.WithAPIVersion("1.52"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return &engine.Engine{Client: c}
}

func TestArchiveParallelReadsAndCleanup(t *testing.T) {
	data, im := archiveFixture(t)
	e := archiveEngine(t, string(im.Descriptors[0].Digest), func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(data) })
	parent := t.TempDir()
	a, err := NewArchive(context.Background(), e, ArchiveOptions{Reference: "fixture", MaxSpool: 8 << 20, SpoolDir: parent})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	if a.Bytes != int64(len(data)) || a.Image.Digest != im.Digest {
		t.Fatal("changed archive metadata")
	}
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, d := range im.Descriptors {
				var b bytes.Buffer
				if err := a.Fetch(context.Background(), d, &b); err != nil {
					t.Error(err)
					return
				}
				if err := image.Verify(b.Bytes(), d); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	if err = a.Close(); err != nil {
		t.Fatal(err)
	}
	remaining, _ := os.ReadDir(parent)
	if len(remaining) != 0 {
		t.Fatal("staged archive leaked")
	}
	// Failure cannot leave a partially staged image behind either.
	_, err = NewArchive(context.Background(), e, ArchiveOptions{Reference: "fixture", MaxSpool: 4 << 20, SpoolDir: parent})
	if err == nil || !strings.Contains(err.Error(), "spool budget") {
		t.Fatalf("budget: %v", err)
	}
	remaining, _ = os.ReadDir(parent)
	if len(remaining) != 0 {
		t.Fatal("failed staging leaked")
	}
}

func TestArchiveCancellationClosesExport(t *testing.T) {
	started := make(chan struct{})
	e := archiveEngine(t, "sha256:"+strings.Repeat("a", 64), func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(make([]byte, 512))
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	parent := t.TempDir()
	finished := make(chan error, 1)
	go func() {
		_, err := NewArchive(ctx, e, ArchiveOptions{Reference: "fixture", MaxSpool: 4 << 20, SpoolDir: parent})
		finished <- err
	}()
	select {
	case <-started:
		cancel()
	case <-ctx.Done():
		t.Fatal("export did not start")
	}
	if err := <-finished; err == nil {
		t.Fatal("cancelled export succeeded")
	}
	remaining, _ := os.ReadDir(parent)
	if len(remaining) != 0 {
		t.Fatal("cancelled archive leaked")
	}
}

func TestArchiveRejectsUnsafeAndDuplicateEntries(t *testing.T) {
	for _, h := range []tar.Header{
		{Name: "../outside", Typeflag: tar.TypeReg},
		{Name: "/outside", Typeflag: tar.TypeReg},
		{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "outside"},
		{Name: "duplicate", Typeflag: tar.TypeReg},
	} {
		t.Run(h.Name, func(t *testing.T) {
			f, err := os.CreateTemp(t.TempDir(), "archive")
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			tw := tar.NewWriter(f)
			if err = tw.WriteHeader(&h); err != nil {
				t.Fatal(err)
			}
			if h.Name == "duplicate" {
				if err = tw.WriteHeader(&h); err != nil {
					t.Fatal(err)
				}
			}
			if err = tw.Close(); err != nil {
				t.Fatal(err)
			}
			size, _ := f.Seek(0, io.SeekCurrent)
			a := &Archive{file: f, Bytes: size}
			if err = a.index(context.Background()); err == nil {
				t.Fatal("invalid archive accepted")
			}
		})
	}
}

func TestContainerdArchivePinsPlatformManifestInsteadOfConfigID(t *testing.T) {
	data, im := archiveFixture(t)
	rootID := string(digest.FromString("pinned-index"))
	var cfg v1.Image
	_ = json.Unmarshal(im.Config, &cfg)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/json") {
			id := rootID
			desc := v1.Descriptor{Digest: digest.Digest(id), MediaType: v1.MediaTypeImageIndex, Size: 100}
			if r.URL.Query().Get("platform") != "" {
				id = string(im.Digest)
				desc = v1.Descriptor{Digest: im.Digest, MediaType: im.MediaType, Size: int64(len(im.Manifest))}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"Id": id, "Descriptor": desc, "Os": im.Platform.OS, "Architecture": im.Platform.Architecture, "RootFS": map[string]any{"Type": "layers", "Layers": cfg.RootFS.DiffIDs}})
		} else if strings.HasSuffix(r.URL.Path, "/images/get") {
			if r.URL.Query().Get("names") != rootID {
				t.Error("export lost the pinned index reference")
			}
			_, _ = w.Write(data)
		} else {
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	cli, err := client.New(client.WithHost(server.URL), client.WithAPIVersion("1.52"))
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	archive, err := NewArchive(context.Background(), &engine.Engine{Client: cli}, ArchiveOptions{Reference: "fixture", MaxSpool: 8 << 20, SpoolDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	if archive.Image.Digest != im.Digest || archive.Image.Descriptors[0].Digest != im.Descriptors[0].Digest {
		t.Fatal("wrong containerd export identity")
	}
}
