// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package testutil

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"encoding/json"
	digest "github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/scitrera/oci-relay/internal/image"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func Layout(t testing.TB, size int) (string, *image.Image) {
	t.Helper()
	dir := t.TempDir()
	payload := make([]byte, size)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	tw := tar.NewWriter(&archive)
	if err := tw.WriteHeader(&tar.Header{Name: "payload", Mode: 0600, Size: int64(len(payload))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	diff := digest.FromBytes(archive.Bytes())
	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	if _, err := gz.Write(archive.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	put := func(b []byte, media string) v1.Descriptor {
		d := v1.Descriptor{Digest: digest.FromBytes(b), Size: int64(len(b)), MediaType: media}
		path, err := image.BlobPath(dir, d.Digest)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(path, b, 0600); err != nil {
			t.Fatal(err)
		}
		return d
	}
	layer := put(compressed.Bytes(), v1.MediaTypeImageLayerGzip)
	created := time.Now().UTC()
	cfg := v1.Image{Created: &created, Platform: v1.Platform{OS: "linux", Architecture: runtime.GOARCH}, RootFS: v1.RootFS{Type: "layers", DiffIDs: []digest.Digest{diff}}}
	config, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	configDesc := put(config, v1.MediaTypeImageConfig)
	raw, err := json.Marshal(v1.Manifest{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: v1.MediaTypeImageManifest, Config: configDesc, Layers: []v1.Descriptor{layer}})
	if err != nil {
		t.Fatal(err)
	}
	manifest := put(raw, v1.MediaTypeImageManifest)
	manifest.Platform = &cfg.Platform
	manifest.Annotations = map[string]string{v1.AnnotationRefName: "fixture"}
	index, err := json.Marshal(v1.Index{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: v1.MediaTypeImageIndex, Manifests: []v1.Descriptor{manifest}})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "index.json"), index, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "oci-layout"), []byte(`{"imageLayoutVersion":"1.0.0"}`), 0600); err != nil {
		t.Fatal(err)
	}
	im, err := image.LoadLayout(dir, "fixture", cfg.Platform)
	if err != nil {
		t.Fatal(err)
	}
	return dir, im
}
