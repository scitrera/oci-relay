// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0
package source

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/spark-arena/oci-relay/internal/transfer"
	"github.com/vbatts/tar-split/tar/asm"
	"github.com/vbatts/tar-split/tar/storage"
)

func TestNativePayloadReadPolicyAndBytes(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	g := nativeGetter{root: root, ctx: context.Background()}
	for _, size := range []int{nativeDirectReadMin - 1, nativeDirectReadMin, nativeDirectReadMin + 17} {
		data := bytes.Repeat([]byte("x"), size)
		name := fmt.Sprint(size)
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
		r, err := g.Get(name)
		if err != nil {
			t.Fatal(err)
		}
		_, buffered := r.(*os.File)
		if buffered != (size < nativeDirectReadMin) {
			r.Close()
			t.Fatalf("unexpected reader for %d bytes", size)
		}
		got, err := io.ReadAll(r)
		r.Close()
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("payload changed at cutoff: %v", err)
		}
	}
}

func nativeFixture(t *testing.T) (root, id, payloadPath, metadataPath string, raw []byte) {
	t.Helper()
	root = t.TempDir()
	payload := bytes.Repeat([]byte("layer payload"), 15000)
	var b bytes.Buffer
	tw := tar.NewWriter(&b)
	for _, h := range []*tar.Header{
		{Name: "files/", Typeflag: tar.TypeDir, Mode: 0755},
		{Name: "files/payload", Size: int64(len(payload)), Mode: 0644, ModTime: time.Unix(1234, 0)},
		{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "files/payload", Mode: 0777},
		{Name: "hardlink", Typeflag: tar.TypeLink, Linkname: "files/payload", Mode: 0644},
	} {
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if h.Size > 0 {
			if _, err := tw.Write(payload); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	b.Write(make([]byte, 2048)) // Include padding beyond the ordinary tar EOF.
	raw = append([]byte(nil), b.Bytes()...)
	diff := digest.FromBytes(raw)
	cfg := v1.Image{Platform: v1.Platform{OS: "linux", Architecture: "arm64"}, RootFS: v1.RootFS{Type: "layers", DiffIDs: []digest.Digest{diff}}}
	config, _ := json.Marshal(cfg)
	id = string(digest.FromBytes(config))
	write := func(name string, data []byte) {
		if err := os.MkdirAll(filepath.Dir(name), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(name, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(root, "image/overlay2/imagedb/content/sha256", digest.Digest(id).Encoded()), config)
	dir := filepath.Join(root, "image/overlay2/layerdb/sha256", diff.Encoded())
	write(dir+"/diff", []byte(diff))
	cache := strings.Repeat("a", 64)
	write(dir+"/cache-id", []byte(cache))
	payloadPath = filepath.Join(root, "overlay2", cache, "diff/files/payload")
	write(payloadPath, payload)
	var metadata bytes.Buffer
	gz := gzip.NewWriter(&metadata)
	in, done, err := asm.NewInputTarStreamWithDone(bytes.NewReader(raw), storage.NewJSONPacker(gz), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = io.Copy(io.Discard, in); err != nil {
		t.Fatal(err)
	}
	in.Close()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if err = gz.Close(); err != nil {
		t.Fatal(err)
	}
	metadataPath = dir + "/tar-split.json.gz"
	write(metadataPath, metadata.Bytes())
	return
}

func TestNativeExactReconstructionAndDeferredPayload(t *testing.T) {
	root, id, payload, _, raw := nativeFixture(t)
	if err := os.Rename(payload, payload+".held"); err != nil {
		t.Fatal(err)
	}
	n, err := NewNative(context.Background(), root, id, "29.1.3", v1.Platform{})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	if err = os.Rename(payload+".held", payload); err != nil {
		t.Fatal(err)
	}
	if n.Image.Descriptors[1].Size != int64(len(raw)) {
		t.Fatal("wrong reconstructed size")
	}
	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var out bytes.Buffer
			if err := n.Fetch(context.Background(), n.Image.Descriptors[1], &out); err != nil {
				t.Error(err)
				return
			}
			if !bytes.Equal(out.Bytes(), raw) {
				t.Error("tar bytes changed")
			}
		}()
	}
	wg.Wait()
}

func TestNativeAcceptsDocker29AndLater(t *testing.T) {
	root, id, _, _, _ := nativeFixture(t)
	for _, version := range []string{"29.0.0", "29.3.0", "30.0.0", "29.1.3-0ubuntu1~24.04.1"} {
		t.Run(version, func(t *testing.T) {
			n, err := NewNative(context.Background(), root, id, version, v1.Platform{})
			if err != nil {
				t.Fatal(err)
			}
			if err := n.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNativeRejectsCorruptionAndEscape(t *testing.T) {
	for _, change := range []string{"payload", "header", "escape", "diff", "config", "version", "cancel"} {
		t.Run(change, func(t *testing.T) {
			root, id, payload, metadata, _ := nativeFixture(t)
			version := "29.1.3"
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch change {
			case "version":
				version = "28.5.2"
			case "cancel":
				cancel()
			case "payload":
				f, _ := os.OpenFile(payload, os.O_WRONLY, 0)
				f.WriteAt([]byte("X"), 0)
				f.Close()
			case "escape":
				os.Remove(payload)
				os.Symlink("/etc/passwd", payload)
			case "diff":
				os.WriteFile(filepath.Join(filepath.Dir(metadata), "diff"), []byte(digest.FromString("wrong")), 0600)
			case "config":
				os.WriteFile(filepath.Join(root, "image/overlay2/imagedb/content/sha256", digest.Digest(id).Encoded()), []byte("{}"), 0600)
			case "header":
				f, _ := os.Open(metadata)
				gz, _ := gzip.NewReader(f)
				up := storage.NewJSONUnpacker(gz)
				var b bytes.Buffer
				zw := gzip.NewWriter(&b)
				pack := storage.NewJSONPacker(zw)
				changed := false
				for {
					e, err := up.Next()
					if err == io.EOF {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
					if !changed && e.Type == storage.SegmentType {
						e.Payload[0] ^= 1
						changed = true
					}
					if _, err = pack.AddEntry(*e); err != nil {
						t.Fatal(err)
					}
				}
				gz.Close()
				f.Close()
				zw.Close()
				os.WriteFile(metadata, b.Bytes(), 0600)
			}
			n, err := NewNative(ctx, root, id, version, v1.Platform{})
			if change == "diff" || change == "config" || change == "version" || change == "cancel" {
				if err == nil {
					n.Close()
					t.Fatal("invalid native metadata accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer n.Close()
			c, err := transfer.NewSource(ctx, n, 1<<20, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			r, err := c.Open(ctx, n.Image.Descriptors[1])
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			size, err := io.Copy(io.Discard, r)
			if err == nil {
				t.Fatal("corrupted layer accepted")
			}
			if size >= n.Image.Descriptors[1].Size {
				t.Fatal("unverified tail released")
			}
			if c.Metrics().VerifiedBlobs != 0 {
				t.Fatal("corrupt blob counted as verified")
			}
		})
	}
}

func TestNativeValidatesParentChain(t *testing.T) {
	root, id, _, metadata, _ := nativeFixture(t)
	configPath := filepath.Join(root, "image/overlay2/imagedb/content/sha256", digest.Digest(id).Encoded())
	config, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	var cfg v1.Image
	if err = json.Unmarshal(config, &cfg); err != nil {
		t.Fatal(err)
	}
	diff := cfg.RootFS.DiffIDs[0]
	cfg.RootFS.DiffIDs = append(cfg.RootFS.DiffIDs, diff)
	config, _ = json.Marshal(cfg)
	id = string(digest.FromBytes(config))
	if err = os.WriteFile(filepath.Join(filepath.Dir(configPath), digest.Digest(id).Encoded()), config, 0600); err != nil {
		t.Fatal(err)
	}
	chain := digest.FromString(string(diff) + " " + string(diff))
	second := filepath.Join(filepath.Dir(filepath.Dir(metadata)), chain.Encoded())
	if err = os.Mkdir(second, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"diff", "cache-id", "tar-split.json.gz"} {
		data, err := os.ReadFile(filepath.Join(filepath.Dir(metadata), name))
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(second, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	parent := filepath.Join(second, "parent")
	if err = os.WriteFile(parent, []byte(diff), 0600); err != nil {
		t.Fatal(err)
	}
	n, err := NewNative(context.Background(), root, id, "29.2.1", v1.Platform{})
	if err != nil {
		t.Fatal(err)
	}
	n.Close()
	if err = os.WriteFile(parent, []byte(digest.FromString("wrong parent")), 0600); err != nil {
		t.Fatal(err)
	}
	n, err = NewNative(context.Background(), root, id, "29.2.1", v1.Platform{})
	if err == nil {
		n.Close()
		t.Fatal("wrong parent accepted")
	}
}
