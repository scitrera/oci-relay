// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package peer

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/moby/moby/client"
	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/scitrera/oci-relay/internal/engine"
	"github.com/scitrera/oci-relay/internal/image"
	"github.com/scitrera/oci-relay/internal/testutil"
)

func rawRepresentation(t *testing.T, im *image.Image) *image.Image {
	t.Helper()
	var cfg v1.Image
	_ = json.Unmarshal(im.Config, &cfg)
	var m v1.Manifest
	_ = json.Unmarshal(im.Manifest, &m)
	for i := range m.Layers {
		m.Layers[i].Digest = cfg.RootFS.DiffIDs[i]
		m.Layers[i].MediaType = v1.MediaTypeImageLayer
		m.Layers[i].Size = 12345
	}
	b, _ := json.Marshal(m)
	raw, err := image.Parse(b, im.Config, "", im.Platform)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestRepresentationNegotiationPreservesConfigAndMissingLayers(t *testing.T) {
	_, compressed := testutil.Layout(t, 4096)
	raw := rawRepresentation(t, compressed)
	for _, pair := range [][2]*image.Image{{raw, compressed}, {compressed, raw}} {
		view, err := reuseView(pair[0], pair[1], 1)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(view.Config, pair[0].Config) || view.Descriptors[1].Digest != pair[1].Descriptors[1].Digest {
			t.Fatal("wrong receiver representation")
		}
		a, err := agree(pair[0], negotiation{SourceManifest: pair[0].Digest, Manifest: view.Manifest, Store: "containerd", CachedLayers: 1})
		if err != nil || a.MissingBytes != 0 || a.MissingBlobs != 0 {
			t.Fatalf("%+v %v", a, err)
		}
		if _, err = agree(pair[0], negotiation{SourceManifest: pair[0].Digest, Manifest: view.Manifest, Store: "overlay2", CachedLayers: 0}); err == nil {
			t.Fatal("changed a missing layer")
		}
	}
	_, unrelated := testutil.Layout(t, 4096)
	if _, err := reuseView(compressed, unrelated, 1); err == nil {
		t.Fatal("accepted unrelated parent chain")
	}
	for _, n := range []negotiation{{SourceManifest: raw.Digest, Manifest: raw.Manifest, Store: "overlay2", CachedLayers: -1}, {SourceManifest: raw.Digest, Manifest: raw.Manifest, Store: "overlay2", CachedLayers: 2}, {SourceManifest: digest.FromString("wrong"), Manifest: raw.Manifest, Store: "overlay2"}, {SourceManifest: raw.Digest, Manifest: unrelated.Manifest, Store: "overlay2", CachedLayers: 1}} {
		if _, err := agree(raw, n); err == nil {
			t.Fatal("accepted invalid negotiation")
		}
	}
}

func TestInventoryAgreementAuthenticatedAndBoundToCompletion(t *testing.T) {
	ctx, s, c := fixtureServer(t)
	if err := c.Negotiate(ctx, s.Image, s.Image, "overlay2", 0); err != nil {
		t.Fatal(err)
	}
	result := Result{Version: ProtocolVersion, Transfer: c.Credentials.Transfer, Peer: c.Credentials.Peer, State: "COMPLETE", Manifest: string(s.Image.Digest), ImageID: string(s.Image.Descriptors[0].Digest), Tag: "fixture:target", InstalledManifest: string(digest.FromString("wrong"))}
	if err := c.Report(ctx, result); err == nil {
		t.Fatal("accepted unnegotiated import")
	}
	result.InstalledManifest = string(s.Image.Digest)
	if err := c.Report(ctx, result); err != nil {
		t.Fatal(err)
	}
	if err := c.Negotiate(ctx, s.Image, s.Image, "overlay2", 0); err == nil {
		t.Fatal("allowed negotiation after completion")
	}
}

func TestBlobNegotiationDoesNotRequireUnpackedParentPrefix(t *testing.T) {
	_, compressed := testutil.Layout(t, 4096)
	raw := rawRepresentation(t, compressed)
	n := negotiation{SourceManifest: compressed.Digest, Manifest: raw.Manifest, Store: "overlay2", CacheVersion: 2, Availability: []string{layerBlob}}
	a, err := agree(compressed, n)
	if err != nil || a.MissingBlobs != 0 || a.MissingBytes != 0 || a.CacheVersion != 2 {
		t.Fatalf("%+v %v", a, err)
	}
	for _, change := range []string{"missing", "unknown", "unpacked", "count", "version"} {
		t.Run(change, func(t *testing.T) {
			altered := n
			altered.Availability = append([]string{}, n.Availability...)
			switch change {
			case "missing":
				altered.Availability[0] = layerMissing
			case "unknown":
				altered.Availability[0] = "guess"
			case "unpacked":
				altered.Availability[0] = layerUnpacked
			case "count":
				altered.Availability = nil
			case "version":
				altered.CacheVersion = 0
			}
			if _, err := agree(compressed, altered); err == nil {
				t.Fatal("accepted invalid layer availability")
			}
		})
	}
}

func TestCacheRetentionCleanupOnIndeterminateCreateAndMetadataFailure(t *testing.T) {
	for _, createFails := range []bool{true, false} {
		t.Run(map[bool]string{true: "create", false: "metadata"}[createFails], func(t *testing.T) {
			ctx, s, c := fixtureServer(t)
			name, removed := "", false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/containers/create") {
					name = r.URL.Query().Get("name")
					var body struct {
						Entrypoint []string
						HostConfig struct {
							ReadonlyRootfs bool
							NetworkMode    string
						}
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					if !strings.HasPrefix(name, "oci-relay-cache-") || len(body.Entrypoint) != 1 || body.Entrypoint[0] != "/oci-relay-cache-pin" || !body.HostConfig.ReadonlyRootfs || body.HostConfig.NetworkMode != "none" {
						t.Errorf("unsafe retention create: %+v", body)
					}
					if createFails {
						http.Error(w, `{"message":"injected create failure after side effect"}`, 500)
					} else {
						_, _ = w.Write([]byte(`{"Id":"owned-container"}`))
					}
					return
				}
				if r.Method == http.MethodDelete && name != "" && strings.HasSuffix(r.URL.Path, "/containers/"+name) {
					removed = true
					w.WriteHeader(204)
					return
				}
				t.Errorf("unexpected Docker operation %s %s", r.Method, r.URL.Path)
				http.NotFound(w, r)
			}))
			defer server.Close()
			cli, err := client.New(client.WithHost(server.URL), client.WithAPIVersion("1.52"))
			if err != nil {
				t.Fatal(err)
			}
			defer cli.Close()
			inv := engine.Inventory{InventoryRequest: inventoryFor(s.Image), Candidates: []string{string(digest.FromString("candidate"))}}
			result := Result{}
			_, _, cleanup, err := receiverView(ctx, &engine.Engine{Client: cli}, c, s.Image, PullOptions{NativeStore: "overlay2", NativeRoot: t.TempDir(), EngineVersion: "29.2.1", Inventory: &inv}, &result)
			if err == nil {
				t.Fatal("hidden retention/metadata failure")
			}
			cleanup()
			if !removed || result.CleanupError != "" {
				t.Fatalf("retention leaked: removed=%v cleanup=%s", removed, result.CleanupError)
			}
		})
	}
}
