// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only
// Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.
package peer

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	digest "github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/scitrera/oci-relay/internal/image"
	"github.com/scitrera/oci-relay/internal/transfer"
	"io"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"
	"time"
)

type decoderBytes []byte

func (b decoderBytes) Fetch(ctx context.Context, d v1.Descriptor, w io.Writer) error {
	_, e := w.Write(b)
	return e
}
func TestBundledDecoderIntegrity(t *testing.T) {
	gzipPath, e := exec.LookPath("gzip")
	if e != nil {
		t.Fatal(e)
	}
	var tarBytes bytes.Buffer
	tw := tar.NewWriter(&tarBytes)
	_ = tw.WriteHeader(&tar.Header{Name: "hello", Mode: 0600, Size: 5})
	_, _ = tw.Write([]byte("hello"))
	_ = tw.Close()
	raw := tarBytes.Bytes()
	raw = append(raw, make([]byte, 5<<20)...)
	var zipped bytes.Buffer
	z := gzip.NewWriter(&zipped)
	_, _ = z.Write(raw)
	_ = z.Close()
	for _, mode := range []string{"valid", "bad-compressed-sha", "bad-diff-id", "failed-decoder", "bad-gzip", "budget", "cancelled", "cached"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			options := DecodeOptions{Mode: "auto", Helper: gzipPath, Directory: dir, Workers: 2, MaxBytes: 16 << 20}
			if mode == "failed-decoder" {
				options.Helper = "/does/not/exist"
			}
			if mode == "budget" {
				options.MaxBytes = 4 << 20
			}
			zippedData := append([]byte(nil), zipped.Bytes()...)
			if mode == "bad-gzip" {
				zippedData[len(zippedData)-8] ^= 1
			}
			diff := digest.FromBytes(raw)
			if mode == "bad-diff-id" {
				diff = digest.FromString("wrong")
			}
			cfg := v1.Image{Platform: v1.Platform{OS: "linux", Architecture: "arm64"}, RootFS: v1.RootFS{Type: "layers", DiffIDs: []digest.Digest{diff}}}
			cb, _ := json.Marshal(cfg)
			cd := v1.Descriptor{MediaType: v1.MediaTypeImageConfig, Digest: digest.FromBytes(cb), Size: int64(len(cb))}
			ld := v1.Descriptor{MediaType: v1.MediaTypeImageLayerGzip, Digest: digest.FromBytes(zippedData), Size: int64(zipped.Len())}
			if mode == "bad-compressed-sha" {
				ld.Digest = digest.FromString("wrong")
			}
			m := v1.Manifest{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: v1.MediaTypeImageManifest, Config: cd, Layers: []v1.Descriptor{ld}}
			mb, _ := json.Marshal(m)
			im, e := image.Parse(mb, cb, "", cfg.Platform)
			if e != nil {
				t.Fatal(e)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			c, e := transfer.New(ctx, decoderBytes(zippedData), 4<<20, 2)
			if e != nil {
				t.Fatal(e)
			}
			defer c.Close()
			availability := []string{layerMissing}
			if mode == "cached" {
				availability[0] = layerUnpacked
			}
			if mode == "cancelled" {
				cancel()
			}
			decoded, e := prepareDecoded(ctx, im, c, availability, options)
			defer decoded.close()
			if mode == "failed-decoder" || mode == "budget" || mode == "cached" {
				if e != nil || decoded.image.Digest != im.Digest || len(decoded.files) != 0 {
					t.Fatalf("bad fallback/skip: %+v %v", decoded, e)
				}
				if mode != "cached" && decoded.metrics.Fallback == "" {
					t.Fatal("missing fallback reason")
				}
				entries, _ := os.ReadDir(dir)
				if len(entries) != 0 {
					t.Fatal("scratch leaked")
				}
				return
			}
			if mode != "valid" {
				if e == nil {
					t.Fatal("accepted corrupt payload or cancellation")
				}
				if mode == "cancelled" && !errors.Is(e, context.Canceled) {
					t.Fatal(e)
				}
				entries, _ := os.ReadDir(dir)
				if len(entries) != 0 {
					t.Fatal("scratch leaked")
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			out := decoded.image
			h := decoded.registry(c, "bench")
			if _, e := agree(im, negotiation{SourceManifest: im.Digest, Manifest: out.Manifest, Store: "overlay2", CacheVersion: 2, Availability: []string{layerBlob}}); e != nil {
				t.Fatal(e)
			}
			if out.Descriptors[1].Digest != diff || out.Descriptors[1].Size != int64(len(raw)) {
				t.Fatal("bad raw descriptor")
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest("GET", "/v2/bench/blobs/"+string(diff), nil))
			if w.Code != 200 || !bytes.Equal(w.Body.Bytes(), raw) {
				t.Fatal("raw blob response mismatch")
			}
		})
	}
}
