// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only
// Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.
package image_test

import (
	"bytes"
	"encoding/json"
	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/scitrera/oci-relay/internal/image"
	"github.com/scitrera/oci-relay/internal/testutil"
	"testing"
)

func TestRawManifestIdentityAndRejection(t *testing.T) {
	_, im := testutil.Layout(t, 1024)
	formatted := append([]byte(" \n"), im.Manifest...)
	parsed, err := image.Parse(formatted, im.Config, "", im.Platform)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(parsed.Manifest, formatted) || parsed.Digest == im.Digest {
		t.Fatal("manifest bytes normalized")
	}
	if _, err = image.Parse(formatted, im.Config, im.Digest, im.Platform); err == nil {
		t.Fatal("accepted changed raw digest")
	}
	if _, err = image.Parse(im.Manifest, im.Config, "", v1.Platform{OS: "windows"}); err == nil {
		t.Fatal("accepted wrong platform")
	}
	if _, err = image.Parse(im.Manifest, append(im.Config, ' '), "", im.Platform); err == nil {
		t.Fatal("accepted changed config")
	}
	var m v1.Manifest
	if err = json.Unmarshal(im.Manifest, &m); err != nil {
		t.Fatal(err)
	}
	m.Layers[0].URLs = []string{"https://example.invalid/external"}
	raw, _ := json.Marshal(m)
	if _, err = image.Parse(raw, im.Config, "", im.Platform); err == nil {
		t.Fatal("accepted external descriptor")
	}
	m.Layers[0].URLs = nil
	m.Layers[0].Digest = digest.Digest("sha256:../../secret")
	raw, _ = json.Marshal(m)
	if _, err = image.Parse(raw, im.Config, "", im.Platform); err == nil {
		t.Fatal("accepted invalid digest")
	}
}

func TestUncompressedDescriptorMustMatchDiffID(t *testing.T) {
	_, im := testutil.Layout(t, 1024)
	var m v1.Manifest
	if err := json.Unmarshal(im.Manifest, &m); err != nil {
		t.Fatal(err)
	}
	// A compressed digest cannot be relabeled as an uncompressed DiffID.
	m.Layers[0].MediaType = v1.MediaTypeImageLayer
	raw, _ := json.Marshal(m)
	if _, err := image.Parse(raw, im.Config, "", im.Platform); err == nil {
		t.Fatal("uncompressed digest/DiffID mismatch accepted")
	}
	var cfg v1.Image
	if err := json.Unmarshal(im.Config, &cfg); err != nil {
		t.Fatal(err)
	}
	m.Layers[0].Digest = cfg.RootFS.DiffIDs[0]
	raw, _ = json.Marshal(m)
	if _, err := image.Parse(raw, im.Config, "", im.Platform); err != nil {
		t.Fatal(err)
	}
}
