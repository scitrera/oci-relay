// SPDX-FileCopyrightText: 2026 Spark Arena
// SPDX-License-Identifier: Apache-2.0

package image

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

const MaxMetadata = 4 << 20
const MaxDescriptors = 4096
const DockerManifest = "application/vnd.docker.distribution.manifest.v2+json"
const DockerIndex = "application/vnd.docker.distribution.manifest.list.v2+json"
const DockerConfig = "application/vnd.docker.container.image.v1+json"

type Image struct {
	Manifest    []byte          `json:"manifest"`
	Config      []byte          `json:"config"`
	Digest      digest.Digest   `json:"digest"`
	MediaType   string          `json:"media_type"`
	Platform    v1.Platform     `json:"platform"`
	Descriptors []v1.Descriptor `json:"descriptors"`
}

func ReadBounded(r io.Reader, limit int64) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, fmt.Errorf("input exceeds %d bytes", limit)
	}
	return b, nil
}
func ReadFile(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return ReadBounded(f, limit)
}
func ValidateDescriptor(d v1.Descriptor) error {
	if d.Digest.Algorithm() != digest.SHA256 || d.Digest.Validate() != nil {
		return fmt.Errorf("invalid or unsupported digest %q", d.Digest)
	}
	if d.Size < 0 || d.Size > 1<<60 {
		return fmt.Errorf("invalid descriptor size %d", d.Size)
	}
	if len(d.URLs) > 0 || len(d.Data) > 0 {
		return errors.New("external URLs and embedded descriptors are unsupported")
	}
	return nil
}
func Verify(b []byte, d v1.Descriptor) error {
	if err := ValidateDescriptor(d); err != nil {
		return err
	}
	if int64(len(b)) != d.Size || digest.FromBytes(b) != d.Digest {
		return fmt.Errorf("content does not match %s (expected %d bytes)", d.Digest, d.Size)
	}
	return nil
}
func Platform(s string) (v1.Platform, error) {
	if s == "" {
		return v1.Platform{}, nil
	}
	parts := strings.Split(s, "/")
	if len(parts) < 2 || len(parts) > 3 || parts[0] == "" || parts[1] == "" {
		return v1.Platform{}, fmt.Errorf("platform must be os/architecture[/variant]")
	}
	p := v1.Platform{OS: parts[0], Architecture: parts[1]}
	if len(parts) == 3 {
		p.Variant = parts[2]
	}
	return p, nil
}
func Matches(want, actual v1.Platform) bool {
	return (want.OS == "" || want.OS == actual.OS) && (want.Architecture == "" || want.Architecture == actual.Architecture) && (want.Variant == "" || want.Variant == actual.Variant)
}
func Parse(raw, config []byte, expected digest.Digest, platform v1.Platform) (*Image, error) {
	if len(raw) > MaxMetadata || len(config) > MaxMetadata {
		return nil, errors.New("metadata exceeds 4 MiB limit")
	}
	var m v1.Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}
	if m.SchemaVersion != 2 || (m.MediaType != v1.MediaTypeImageManifest && m.MediaType != DockerManifest) || m.Subject != nil || m.ArtifactType != "" {
		return nil, errors.New("expected an OCI image or Docker schema-2 platform manifest")
	}
	actual := digest.FromBytes(raw)
	if expected != "" && actual != expected {
		return nil, fmt.Errorf("manifest digest mismatch: got %s", actual)
	}
	if len(m.Layers) > MaxDescriptors-1 {
		return nil, errors.New("too many descriptors")
	}
	if m.Config.MediaType != v1.MediaTypeImageConfig && m.Config.MediaType != DockerConfig {
		return nil, errors.New("unsupported image config media type")
	}
	if m.Config.Size > MaxMetadata {
		return nil, errors.New("config exceeds metadata limit")
	}
	descs := append([]v1.Descriptor{m.Config}, m.Layers...)
	seen := map[digest.Digest]int64{}
	for i, d := range descs {
		if err := ValidateDescriptor(d); err != nil {
			return nil, err
		}
		if old, ok := seen[d.Digest]; ok && old != d.Size {
			return nil, errors.New("conflicting descriptor sizes")
		}
		seen[d.Digest] = d.Size
		if i > 0 {
			switch d.MediaType {
			case v1.MediaTypeImageLayer, v1.MediaTypeImageLayerGzip, v1.MediaTypeImageLayerZstd, "application/vnd.docker.image.rootfs.diff.tar.gzip":
			default:
				return nil, fmt.Errorf("unsupported layer media type %q", d.MediaType)
			}
		}
	}
	if err := Verify(config, m.Config); err != nil {
		return nil, err
	}
	var cfg v1.Image
	if err := json.Unmarshal(config, &cfg); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	if cfg.OS == "" || cfg.Architecture == "" || cfg.RootFS.Type != "layers" || len(cfg.RootFS.DiffIDs) != len(m.Layers) {
		return nil, errors.New("invalid config platform or rootfs layer count")
	}
	for i, d := range cfg.RootFS.DiffIDs {
		if d.Algorithm() != digest.SHA256 || d.Validate() != nil {
			return nil, errors.New("invalid rootfs diffID")
		}
		if m.Layers[i].MediaType == v1.MediaTypeImageLayer && m.Layers[i].Digest != d {
			return nil, errors.New("uncompressed layer digest must equal config diffID")
		}
	}
	if !Matches(platform, cfg.Platform) {
		return nil, fmt.Errorf("config platform %s/%s does not match requested platform", cfg.OS, cfg.Architecture)
	}
	return &Image{Manifest: bytes.Clone(raw), Config: bytes.Clone(config), Digest: actual, MediaType: m.MediaType, Platform: cfg.Platform, Descriptors: descs}, nil
}
func (im *Image) Descriptor(d digest.Digest) (v1.Descriptor, bool) {
	for _, desc := range im.Descriptors {
		if desc.Digest == d {
			return desc, true
		}
	}
	return v1.Descriptor{}, false
}
func (im *Image) Validate() error {
	parsed, err := Parse(im.Manifest, im.Config, im.Digest, im.Platform)
	if err != nil {
		return err
	}
	if parsed.MediaType != im.MediaType || len(parsed.Descriptors) != len(im.Descriptors) {
		return errors.New("metadata bundle inconsistent with manifest")
	}
	for i, d := range parsed.Descriptors {
		if d.Digest != im.Descriptors[i].Digest || d.Size != im.Descriptors[i].Size || d.MediaType != im.Descriptors[i].MediaType {
			return errors.New("metadata bundle descriptor mismatch")
		}
	}
	return nil
}
func BlobPath(root string, d digest.Digest) (string, error) {
	if d.Algorithm() != digest.SHA256 || d.Validate() != nil {
		return "", errors.New("invalid blob digest")
	}
	return filepath.Join(root, "blobs", "sha256", d.Encoded()), nil
}
func LoadLayout(root, ref string, platform v1.Platform) (*Image, error) {
	return LoadLayoutReader(func(name string, limit int64) ([]byte, error) {
		return ReadFile(filepath.Join(root, name), limit)
	}, ref, platform)
}

// LoadLayoutReader also supports an indexed OCI tar without extracting blobs.
func LoadLayoutReader(read func(string, int64) ([]byte, error), ref string, platform v1.Platform) (*Image, error) {
	layout, err := read("oci-layout", 1024)
	if err != nil {
		return nil, err
	}
	var version v1.ImageLayout
	if json.Unmarshal(layout, &version) != nil || version.Version != "1.0.0" {
		return nil, errors.New("unsupported OCI layout version")
	}
	raw, err := read("index.json", MaxMetadata)
	if err != nil {
		return nil, err
	}
	var candidates [][]byte
	seen := map[digest.Digest]bool{}
	var visit func([]byte, int, bool) error
	visit = func(b []byte, depth int, selected bool) error {
		if depth > 8 {
			return errors.New("index nesting exceeds limit")
		}
		var header struct {
			MediaType string `json:"mediaType"`
		}
		if err := json.Unmarshal(b, &header); err != nil {
			return err
		}
		if header.MediaType == v1.MediaTypeImageManifest || header.MediaType == DockerManifest {
			candidates = append(candidates, b)
			return nil
		}
		if header.MediaType != v1.MediaTypeImageIndex && header.MediaType != DockerIndex {
			return errors.New("unsupported layout document")
		}
		var idx v1.Index
		if err := json.Unmarshal(b, &idx); err != nil {
			return err
		}
		if len(idx.Manifests) > MaxDescriptors {
			return errors.New("too many index descriptors")
		}
		for _, d := range idx.Manifests {
			if !selected && ref != "" && d.Annotations[v1.AnnotationRefName] != ref && string(d.Digest) != ref {
				continue
			}
			if d.Platform != nil && !Matches(platform, *d.Platform) {
				continue
			}
			if err := ValidateDescriptor(d); err != nil {
				return err
			}
			if d.Size > MaxMetadata {
				return errors.New("indexed metadata exceeds limit")
			}
			if seen[d.Digest] {
				return errors.New("duplicate or cyclic index descriptor")
			}
			seen[d.Digest] = true
			if len(seen) > MaxDescriptors {
				return errors.New("index traversal exceeds limit")
			}
			p, _ := BlobPath("", d.Digest)
			child, err := read(p, MaxMetadata)
			if err != nil {
				return err
			}
			if err = Verify(child, d); err != nil {
				return err
			}
			if err = visit(child, depth+1, true); err != nil {
				return err
			}
		}
		return nil
	}
	if err = visit(raw, 0, false); err != nil {
		return nil, err
	}
	var images []*Image
	for _, b := range candidates {
		var m v1.Manifest
		if json.Unmarshal(b, &m) != nil {
			return nil, errors.New("invalid manifest")
		}
		p, err := BlobPath("", m.Config.Digest)
		if err != nil {
			return nil, err
		}
		cfg, err := read(p, MaxMetadata)
		if err != nil {
			return nil, err
		}
		im, err := Parse(b, cfg, "", v1.Platform{})
		if err != nil {
			return nil, err
		}
		if Matches(platform, im.Platform) {
			images = append(images, im)
		}
	}
	if len(images) != 1 {
		return nil, fmt.Errorf("layout selection resolved %d images; select a reference and explicit platform", len(images))
	}
	return images[0], nil
}
func HexSHA256(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }
