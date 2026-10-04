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
	"github.com/containerd/errdefs"
	"github.com/scitrera/oci-relay/internal/engine"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/scitrera/oci-relay/internal/image"
	"github.com/scitrera/oci-relay/internal/source"
	"github.com/scitrera/oci-relay/internal/testutil"
	"github.com/scitrera/oci-relay/internal/transfer"
)

func rawLayout(t *testing.T, size int) (string, *image.Image) {
	t.Helper()
	root, im := testutil.Layout(t, size)
	p, _ := image.BlobPath(root, im.Descriptors[1].Digest)
	compressed, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	gz, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(gz)
	if err != nil {
		t.Fatal(err)
	}
	_ = gz.Close()
	d := v1.Descriptor{Digest: digest.FromBytes(raw), Size: int64(len(raw)), MediaType: v1.MediaTypeImageLayer}
	p, _ = image.BlobPath(root, d.Digest)
	if err = os.WriteFile(p, raw, 0600); err != nil {
		t.Fatal(err)
	}
	var m v1.Manifest
	if err = json.Unmarshal(im.Manifest, &m); err != nil {
		t.Fatal(err)
	}
	m.Layers[0] = d
	manifest, _ := json.Marshal(m)
	im, err = image.Parse(manifest, im.Config, "", im.Platform)
	if err != nil {
		t.Fatal(err)
	}
	return root, im
}

func TestCachedPrefixRequiresEveryParent(t *testing.T) {
	a, b, c := digest.FromString("a"), digest.FromString("b"), digest.FromString("c")
	want := []digest.Digest{a, b, c}
	for _, tc := range []struct {
		have []string
		n    int
	}{
		{nil, 0}, {[]string{string(b), string(c)}, 0}, {[]string{string(a), string(c)}, 1},
		{[]string{string(a), string(b)}, 2}, {[]string{string(a), string(b), string(c)}, 3},
	} {
		if got := commonPrefix(want, tc.have); got != tc.n {
			t.Fatalf("prefix %v: %d", tc.have, got)
		}
	}
}

func TestLoadArchivePreservesBytesBudgetAndCachedSkips(t *testing.T) {
	root, im := rawLayout(t, 1<<20)
	for _, skip := range []int{0, 1} {
		c, err := transfer.New(context.Background(), &source.Layout{Root: root}, 8*transfer.FrameSize, 1)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		entries, size, err := loadEntries(im, skip, "oci-relay-test:archive", 4<<20)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err = loadEntries(im, skip, "oci-relay-test:archive", size-1); err == nil {
			t.Fatal("archive cap ignored")
		}
		var output bytes.Buffer
		if err = writeLoadArchive(context.Background(), &output, c, entries); err != nil {
			t.Fatal(err)
		}
		if int64(output.Len()) != size {
			t.Fatalf("archive size %d expected %d", output.Len(), size)
		}
		tr := tar.NewReader(&output)
		found := false
		for {
			h, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(tr)
			if err != nil {
				t.Fatal(err)
			}
			if h.Name == "config.json" && !bytes.Equal(data, im.Config) {
				t.Fatal("config changed")
			}
			if strings.HasPrefix(h.Name, "layers/") {
				found = true
				if err = image.Verify(data, im.Descriptors[1]); err != nil {
					t.Fatal(err)
				}
			}
		}
		if found != (skip == 0) {
			t.Fatal("wrong layer inclusion")
		}
		if skip == 1 && c.Metrics().Acquired != 0 {
			t.Fatal("read cached layer")
		}
	}
}

func TestLoadArchiveRefusesCorruptLayer(t *testing.T) {
	root, im := rawLayout(t, 1<<20)
	p, _ := image.BlobPath(root, im.Descriptors[1].Digest)
	data, _ := os.ReadFile(p)
	data[800] ^= 1
	if err := os.WriteFile(p, data, 0600); err != nil {
		t.Fatal(err)
	}
	c, err := transfer.New(context.Background(), &source.Layout{Root: root}, 8*transfer.FrameSize, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	entries, _, err := loadEntries(im, 0, "oci-relay-test:corrupt", 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err = writeLoadArchive(context.Background(), &out, c, entries); err == nil {
		t.Fatal("accepted corrupt layer")
	}
	tr := tar.NewReader(&out)
	for {
		_, err := tr.Next()
		if err != nil {
			if err == io.EOF {
				t.Fatal("corrupt archive appeared complete")
			}
			break
		}
		if _, err = io.Copy(io.Discard, tr); err != nil {
			break
		}
	}
}

func TestRealDockerLoadCachedPrefix(t *testing.T) {
	if os.Getenv("OCI_RELAY_DOCKER_TESTS") != "1" {
		t.Skip("set OCI_RELAY_DOCKER_TESTS=1")
	}
	ctx, s, c := fixtureServer(t)
	e, err := engine.New("")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	baseTag := "oci-relay-test:compressed-" + s.Session.Source.Transfer
	childTag := "oci-relay-test:child-" + s.Session.Source.Transfer
	defer e.RemoveTag(context.Background(), childTag)
	defer e.RemoveTag(context.Background(), baseTag)
	if _, err = Pull(ctx, c, PullOptions{Tag: baseTag}); err != nil {
		t.Fatal(err)
	}
	root, tail := rawLayout(t, 128<<10)
	var compressed bytes.Buffer
	if err = c.Fetch(ctx, s.Image.Descriptors[1], &compressed); err != nil {
		t.Fatal(err)
	}
	gz, err := gzip.NewReader(&compressed)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(gz)
	if err != nil {
		t.Fatal(err)
	}
	_ = gz.Close()
	base := v1.Descriptor{Digest: digest.FromBytes(raw), Size: int64(len(raw)), MediaType: v1.MediaTypeImageLayer}
	path, _ := image.BlobPath(root, base.Digest)
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	var cfg v1.Image
	if err = json.Unmarshal(tail.Config, &cfg); err != nil {
		t.Fatal(err)
	}
	cfg.RootFS.DiffIDs = append([]digest.Digest{base.Digest}, cfg.RootFS.DiffIDs...)
	config, _ := json.Marshal(cfg)
	var manifest v1.Manifest
	if err = json.Unmarshal(tail.Manifest, &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.Config.Digest, manifest.Config.Size = digest.FromBytes(config), int64(len(config))
	manifest.Layers = append([]v1.Descriptor{base}, manifest.Layers...)
	encoded, _ := json.Marshal(manifest)
	child, err := image.Parse(encoded, config, "", tail.Platform)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := transfer.New(ctx, &source.Layout{Root: root}, 8*transfer.FrameSize, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	session, err := NewSession([]string{"receiver"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ctx, session, child, cache, "127.0.0.1:0", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	receiver, err := NewClient("https://"+server.Listener.Addr().String(), session.Peers["receiver"], nil)
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	baseID, prefix, probeErr := cachedBase(ctx, e, child)
	if probeErr != nil || baseID == "" || prefix != 1 {
		t.Fatalf("cached base selection: %s %d %v", baseID, prefix, probeErr)
	}
	result, err := Pull(ctx, receiver, PullOptions{Tag: childTag, Import: "load-cached", MaxImport: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if result.ReusedLayers != 1 || result.ImportMethod != "load" || result.ImageID != string(child.Descriptors[0].Digest) {
		t.Fatalf("wrong result: %+v", result)
	}
	if cache.Metrics().Acquired != tail.Descriptors[1].Size {
		t.Fatalf("cached base transferred: %+v", cache.Metrics())
	}
	if result.CleanupError != "" {
		t.Fatal(result.CleanupError)
	}
}

func TestRealDockerLoadMissingCachedChainFails(t *testing.T) {
	if os.Getenv("OCI_RELAY_DOCKER_TESTS") != "1" {
		t.Skip("set OCI_RELAY_DOCKER_TESTS=1")
	}
	_, im := rawLayout(t, 1<<20)
	e, err := engine.New("")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	tag := "oci-relay-test:missing-cache-" + im.Digest.Encoded()[:20]
	defer e.RemoveTag(context.Background(), tag)
	entries, _, err := loadEntries(im, 1, tag, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	var data bytes.Buffer
	if err = writeLoadArchive(context.Background(), &data, nil, entries); err != nil {
		t.Fatal(err)
	}
	if err = e.Load(context.Background(), &data); err == nil {
		t.Fatal("missing cached chain accepted")
	}
	if _, err = e.Inspect(context.Background(), tag); !errdefs.IsNotFound(err) {
		t.Fatalf("failed load created destination: %v", err)
	}
}
