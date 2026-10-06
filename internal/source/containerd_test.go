// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only
// Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.

package source

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	digest "github.com/opencontainers/go-digest"
	"github.com/scitrera/oci-relay/internal/testutil"
	"github.com/scitrera/oci-relay/internal/transfer"
)

func TestContainerdPreservesCompressedContentAndIndex(t *testing.T) {
	root, im := testutil.Layout(t, 2<<20)
	index, err := os.ReadFile(filepath.Join(root, "index.json"))
	if err != nil {
		t.Fatal(err)
	}
	id := digest.FromBytes(index)
	if err := os.WriteFile(filepath.Join(root, "blobs/sha256", id.Encoded()), index, 0600); err != nil {
		t.Fatal(err)
	}
	n, err := NewContainerd(context.Background(), root, string(id), "29.2.1", im.Platform)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	if n.Image.Digest != im.Digest || n.Image.Descriptors[1].Digest != im.Descriptors[1].Digest || n.MetadataBytes > 32<<10 {
		t.Fatal("native source changed content or read payload during preparation")
	}
	c, err := transfer.New(context.Background(), n, 8<<20, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, d := range im.Descriptors {
		r, err := c.Open(context.Background(), d)
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.Copy(io.Discard, r)
		_ = r.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	if c.Metrics().VerifiedBlobs != 2 {
		t.Fatal("native bytes were not verified")
	}
}

func TestContainerdAcceptsDocker29AndLater(t *testing.T) {
	root, im := testutil.Layout(t, 4096)
	for _, version := range []string{"29.0.0", "29.1.3", "29.3.0", "30.0.0", "29.1.3-0ubuntu1~24.04.1"} {
		t.Run(version, func(t *testing.T) {
			n, err := NewContainerd(context.Background(), root, string(im.Digest), version, im.Platform)
			if err != nil {
				t.Fatal(err)
			}
			if err := n.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestContainerdRejectsCorruptionEscapeAndWrongStoreVersion(t *testing.T) {
	for _, change := range []string{"version", "root", "size", "payload", "escape", "cancel", "platform"} {
		t.Run(change, func(t *testing.T) {
			root, im := testutil.Layout(t, 1<<20)
			version := "29.2.1"
			p := im.Platform
			blob := filepath.Join(root, "blobs/sha256", im.Descriptors[1].Digest.Encoded())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch change {
			case "version":
				version = "28.5.2"
			case "root":
				_ = os.WriteFile(filepath.Join(root, "blobs/sha256", im.Digest.Encoded()), []byte("wrong"), 0600)
			case "size":
				_ = os.Truncate(blob, 4)
			case "payload":
				f, err := os.OpenFile(blob, os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				_, _ = f.Write([]byte("bad"))
				_ = f.Close()
			case "escape":
				outside := filepath.Join(t.TempDir(), "blob")
				b, _ := os.ReadFile(blob)
				_ = os.WriteFile(outside, b, 0600)
				_ = os.Remove(blob)
				_ = os.Symlink(outside, blob)
			case "cancel":
				cancel()
			case "platform":
				p.Architecture = "unsupported"
			}
			n, err := NewContainerd(ctx, root, string(im.Digest), version, p)
			if change != "payload" {
				if err == nil {
					n.Close()
					t.Fatal("accepted invalid store")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer n.Close()
			c, err := transfer.New(ctx, n, 8<<20, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			r, err := c.Open(ctx, im.Descriptors[1])
			if err == nil {
				_, err = io.Copy(io.Discard, r)
				_ = r.Close()
			}
			if err == nil {
				t.Fatal("accepted corrupt payload")
			}
		})
	}
}
