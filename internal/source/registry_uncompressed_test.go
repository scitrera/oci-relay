// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only
// Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.
package source

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/scitrera/oci-relay/internal/image"
)

func TestUncompressedRegistryExportVerifiesBothRepresentations(t *testing.T) {
	for _, mode := range []string{"valid", "bad-blob", "bad-diffid", "budget", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			f := newRegistryFixture(t)
			if mode == "bad-diffid" {
				var cfg v1.Image
				_ = json.Unmarshal(f.config, &cfg)
				cfg.RootFS.DiffIDs[0] = digest.FromString("incorrect")
				f.config, _ = json.Marshal(cfg)
				f.configDigest = digest.FromBytes(f.config)
				var m v1.Manifest
				_ = json.Unmarshal(f.manifest, &m)
				m.Config.Digest, m.Config.Size = f.configDigest, int64(len(f.config))
				f.manifest, _ = json.Marshal(m)
				f.manifestDigest = digest.FromBytes(f.manifest)
			}
			if mode == "bad-blob" {
				f.handler = func(w http.ResponseWriter, r *http.Request) bool {
					if !strings.HasSuffix(r.URL.Path, f.blobDigest.String()) {
						return false
					}
					bad := bytes.Clone(f.blob)
					bad[len(bad)-1] ^= 1
					_, _ = w.Write(bad)
					return true
				}
			}
			srv := httptest.NewServer(f)
			defer srv.Close()
			opts := registryOptions(srv)
			opts.Reference = strings.TrimSuffix(opts.Reference, ":latest") + "@" + f.manifestDigest.String()
			r, err := NewRegistry(context.Background(), opts)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "cancel" {
				cancel()
			}
			budget := int64(32 << 20)
			if mode == "budget" {
				budget = 8<<20 + 4096
			}
			spool := t.TempDir()
			root, err := r.ExportUncompressed(ctx, spool, budget, 3)
			if mode != "valid" {
				if err == nil {
					t.Fatal("invalid export succeeded")
				}
				entries, _ := os.ReadDir(spool)
				if len(entries) != 0 {
					t.Fatal("failed export left staging data")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			im, err := image.LoadLayout(root, "", r.Image.Platform)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(im.Config, r.Image.Config) || im.Digest == r.Image.Digest || r.RootDigest != f.manifestDigest {
				t.Fatal("export changed config or lost source pin")
			}
			layer := im.Descriptors[1]
			var data bytes.Buffer
			if err = (&Layout{root}).Fetch(ctx, layer, &data); err != nil {
				t.Fatal(err)
			}
			if layer.MediaType != v1.MediaTypeImageLayer || image.Verify(data.Bytes(), layer) != nil || layer.Size != 256<<10 {
				t.Fatal("unverified uncompressed content")
			}
		})
	}
}

func TestUncompressedExportCancelsBlockedUpstreamAndRemovesPartialFiles(t *testing.T) {
	f := newRegistryFixture(t)
	entered := make(chan struct{})
	f.handler = func(w http.ResponseWriter, req *http.Request) bool {
		if !strings.HasSuffix(req.URL.Path, f.blobDigest.String()) {
			return false
		}
		_, _ = w.Write(f.blob[:65536])
		w.(http.Flusher).Flush()
		close(entered)
		<-req.Context().Done()
		return true
	}
	srv := httptest.NewServer(f)
	defer srv.Close()
	r, err := NewRegistry(context.Background(), registryOptions(srv))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	spool := t.TempDir()
	done := make(chan error, 1)
	go func() { _, err := r.ExportUncompressed(ctx, spool, 32<<20, 3); done <- err }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("export did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled export succeeded")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("export did not cancel")
	}
	entries, err := os.ReadDir(spool)
	if err != nil || len(entries) != 0 {
		t.Fatalf("cancelled export left files: %v %v", entries, err)
	}
}
