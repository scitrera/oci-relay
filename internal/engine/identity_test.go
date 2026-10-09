// SPDX-FileCopyrightText: 2026 Spark Arena
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"encoding/json"
	"testing"

	"github.com/moby/moby/client"
	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/spark-arena/oci-relay/internal/testutil"
)

func TestBackendIdentityUsesConfigOrManifestAndExactRootFS(t *testing.T) {
	_, im := testutil.Layout(t, 100)
	var cfg v1.Image
	_ = json.Unmarshal(im.Config, &cfg)
	info := client.ImageInspectResult{}
	info.ID = string(im.Descriptors[0].Digest)
	info.Os = im.Platform.OS
	info.Architecture = im.Platform.Architecture
	for _, d := range cfg.RootFS.DiffIDs {
		info.RootFS.Layers = append(info.RootFS.Layers, string(d))
	}
	if !MatchesImage(info, im) {
		t.Fatal("classic config identity rejected")
	}
	info.ID = string(im.Digest)
	info.Descriptor = &v1.Descriptor{Digest: im.Digest}
	if !MatchesImage(info, im) {
		t.Fatal("containerd manifest identity rejected")
	}
	info.Descriptor.Digest = digest.FromString("index-or-other-manifest")
	if MatchesImage(info, im) {
		t.Fatal("unresolved index accepted as platform manifest")
	}
	info.Descriptor.Digest = im.Digest
	info.RootFS.Layers[0] = string(digest.FromString("other"))
	if MatchesImage(info, im) {
		t.Fatal("wrong rootfs accepted")
	}
}
