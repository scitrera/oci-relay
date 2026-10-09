// SPDX-FileCopyrightText: 2026 Spark Arena
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"bytes"
	"context"
	"fmt"
	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/spark-arena/oci-relay/internal/testutil"
	"github.com/spark-arena/oci-relay/internal/transfer"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestContentCacheRetainsUnlinkedBlobAndVerifiesCorruption(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(map[bool]string{false: "unlink", true: "corrupt"}[corrupt], func(t *testing.T) {
			root, im := testutil.Layout(t, 4096)
			d := im.Descriptors[1]
			path := filepath.Join(root, "blobs/sha256", d.Digest.Encoded())
			original, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			local, err := NewContentCache(root)
			if err != nil {
				t.Fatal(err)
			}
			defer local.Close()
			hit, err := local.Probe(context.Background(), d)
			if err != nil || !hit {
				t.Fatal(hit, err)
			}
			if corrupt {
				damaged := bytes.Clone(original)
				damaged[0] ^= 1
				if err = os.WriteFile(path, damaged, 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				if err = os.Remove(path); err != nil {
					t.Fatal(err)
				}
			}
			c, err := transfer.New(context.Background(), local, 8<<20, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			r, err := c.Open(context.Background(), d)
			var b []byte
			if err == nil {
				b, err = io.ReadAll(r)
				r.Close()
			}
			if corrupt && err == nil {
				t.Fatal("accepted corrupt local content")
			}
			if !corrupt && (err != nil || !bytes.Equal(b, original)) {
				t.Fatal("lost unlinked content", err)
			}
		})
	}
}

func TestNativeLayerSelectionSkipsUnneededMetadata(t *testing.T) {
	root, id, _, metadata, _ := nativeFixture(t)
	if err := os.Remove(metadata); err != nil {
		t.Fatal(err)
	}
	n, err := NewNativeLayers(context.Background(), root, id, "29.2.1", v1.Platform{}, map[digest.Digest]bool{})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	if len(n.DiffIDs) != 1 || n.MetadataBytes != 0 {
		t.Fatal("scanned unused layer metadata")
	}
	if _, ok := n.Layer(n.DiffIDs[0]); ok {
		t.Fatal("advertised unselected layer")
	}
	if _, err := NewNativeLayers(context.Background(), root, id, "29.2.1", v1.Platform{}, map[digest.Digest]bool{n.DiffIDs[0]: true}); err == nil {
		t.Fatal("ignored missing selected metadata")
	}
}

func TestContentCacheProbeBoundsAndConfinement(t *testing.T) {
	root, im := testutil.Layout(t, 1024)
	c, err := NewContentCache(root)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	d := im.Descriptors[1]
	path := filepath.Join(root, "blobs/sha256", d.Digest.Encoded())
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if hit, err := c.Probe(context.Background(), d); hit || err != nil {
		t.Fatal("missing blob was not a miss", hit, err)
	}
	outside := filepath.Join(t.TempDir(), "outside")
	if err = os.WriteFile(outside, make([]byte, d.Size), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(outside, path); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Probe(context.Background(), d); err == nil {
		t.Fatal("followed path outside content root")
	}
	for i := range 257 {
		data := []byte(fmt.Sprint(i))
		dg := digest.FromBytes(data)
		if err = os.WriteFile(filepath.Join(root, "blobs/sha256", dg.Encoded()), data, 0600); err != nil {
			t.Fatal(err)
		}
		hit, err := c.Probe(context.Background(), v1.Descriptor{Digest: dg, Size: int64(len(data)), MediaType: v1.MediaTypeImageLayer})
		if err != nil || hit != (i < 256) {
			t.Fatal(i, hit, err)
		}
	}
	if !c.Limited || len(c.files) != 256 {
		t.Fatal("unbounded descriptor retention")
	}
}

func TestNativeSelectedLayersMergeAndVerify(t *testing.T) {
	root, id, _, _, raw := nativeFixture(t)
	diff := digest.FromBytes(raw)
	n, err := NewNativeLayers(context.Background(), root, id, "29.2.1", v1.Platform{}, map[digest.Digest]bool{diff: true})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	other, err := NewNativeLayers(context.Background(), root, id, "29.2.1", v1.Platform{}, map[digest.Digest]bool{diff: true})
	if err != nil {
		t.Fatal(err)
	}
	mergeErr := n.MergeLayers(other)
	closeErr := other.Close()
	if mergeErr != nil || closeErr != nil {
		t.Fatal(mergeErr, closeErr)
	}
	c, err := transfer.New(context.Background(), n, 8<<20, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	d, ok := n.Layer(diff)
	if !ok {
		t.Fatal("lost merged layer")
	}
	r, err := c.Open(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	r.Close()
	if err != nil || !bytes.Equal(raw, got) {
		t.Fatal("selected layer changed", err)
	}
	if c.Metrics().VerifiedBlobs != 1 {
		t.Fatal("merged layer was not SHA-verified")
	}
}
