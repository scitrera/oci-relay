// SPDX-FileCopyrightText: 2026 Spark Arena
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/spark-arena/oci-relay/internal/transfer"
)

func TestRegistrySharesSHAWithSourceCache(t *testing.T) {
	for _, pipelined := range []bool{false, true} {
		for _, retention := range []bool{false, true} {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			f := newRegistryFixture(t)
			srv := httptest.NewServer(f)
			opts := registryOptions(srv)
			if retention {
				opts.MaxCacheBytes = int64(len(f.blob))
				opts.SpoolDir = t.TempDir()
			}
			r, err := NewRegistry(ctx, opts)
			if err != nil {
				cancel()
				srv.Close()
				t.Fatal(err)
			}
			layer := r.Image.Descriptors[1]
			// New transfer caches force a disk replay / second upstream request.
			for range 2 {
				newCache := transfer.New
				if pipelined {
					newCache = transfer.NewSource
				}
				c, err := newCache(ctx, r, 1<<20, 1)
				if err != nil {
					t.Fatal(err)
				}
				reader, err := c.Open(ctx, layer)
				if err != nil {
					t.Fatal(err)
				}
				got, err := io.ReadAll(reader)
				reader.Close()
				c.Close()
				if err != nil || !bytes.Equal(got, f.blob) {
					t.Fatalf("pipelined=%t retention=%t: %v", pipelined, retention, err)
				}
			}
			m := r.Metrics()
			if m.SharedVerifications != 2 || m.LocalVerifications != 0 {
				t.Fatalf("duplicate/missing SHA: %+v", m)
			}
			if retention && (m.Downloads != 1 || m.CacheHits != 1) {
				t.Fatal(m)
			}
			// Ordinary callers still hash, including disk cache hits.
			if err := r.Fetch(ctx, layer, io.Discard); err != nil {
				t.Fatal(err)
			}
			if r.Metrics().LocalVerifications != 1 {
				t.Fatal("standalone fetch skipped SHA")
			}
			r.Close()
			srv.Close()
			cancel()
		}
	}
}

func TestRegistrySharedVerificationRejectsCorruptionBeforePublication(t *testing.T) {
	for _, mode := range []string{"corrupt-upstream", "excess-upstream", "short-upstream", "corrupt-disk", "short-disk", "excess-disk"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			f := newRegistryFixture(t)
			if strings.HasSuffix(mode, "upstream") {
				f.handler = func(w http.ResponseWriter, r *http.Request) bool {
					if !strings.HasSuffix(r.URL.Path, f.blobDigest.String()) {
						return false
					}
					corrupt := bytes.Clone(f.blob)
					switch mode {
					case "corrupt-upstream":
						corrupt[len(corrupt)-1] ^= 1
					case "excess-upstream":
						corrupt = append(corrupt, 0)
					case "short-upstream":
						corrupt = corrupt[:len(corrupt)-1]
					}
					w.WriteHeader(200)
					w.(http.Flusher).Flush()
					_, _ = w.Write(corrupt)
					return true
				}
			}
			srv := httptest.NewServer(f)
			defer srv.Close()
			opts := registryOptions(srv)
			opts.MaxCacheBytes = int64(len(f.blob))
			opts.SpoolDir = t.TempDir()
			r, err := NewRegistry(ctx, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			layer := r.Image.Descriptors[1]
			if !strings.HasSuffix(mode, "upstream") {
				if err := r.Fetch(ctx, layer, io.Discard); err != nil {
					t.Fatal(err)
				}
				data := bytes.Clone(f.blob)
				switch mode {
				case "corrupt-disk":
					data[len(data)-1] ^= 1
				case "short-disk":
					data = data[:len(data)-1]
				case "excess-disk":
					data = append(data, 0)
				}
				if err := os.WriteFile(r.blobs[layer.Digest].path, data, 0600); err != nil {
					t.Fatal(err)
				}
				if err := r.Fetch(ctx, layer, io.Discard); err == nil {
					t.Fatal("standalone disk replay accepted corruption")
				}
			}
			c, err := transfer.NewSource(ctx, r, 1<<20, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			reader, err := c.Open(ctx, layer)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			n, err := io.Copy(io.Discard, reader)
			if err == nil || n >= layer.Size {
				t.Fatalf("corruption accepted: %d %v", n, err)
			}
			if strings.HasSuffix(mode, "upstream") {
				if r.Metrics().CacheReservedBytes != 0 {
					t.Fatal("corrupt blob published")
				}
				files, err := os.ReadDir(r.dir)
				if err != nil || len(files) != 0 {
					t.Fatalf("corrupt cache file retained: %v %v", files, err)
				}
			}
		})
	}
}
