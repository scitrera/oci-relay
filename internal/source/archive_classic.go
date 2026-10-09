// SPDX-FileCopyrightText: 2026 Spark Arena
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"encoding/json"
	"errors"
	digest "github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/spark-arena/oci-relay/internal/image"
	"path/filepath"
)

// Legacy Docker exports contain uncompressed layer tars and a pinned config.
// Turn their config's DiffIDs into descriptors without another layer SHA pass.
// The source cache verifies every descriptor before serving any blob bytes.
func (a *Archive) classic(platform v1.Platform, id string) (*image.Image, error) {
	raw, err := a.read("manifest.json", image.MaxMetadata)
	if err != nil {
		return nil, err
	}
	var manifests []struct {
		Config string
		Layers []string
	}
	if err = json.Unmarshal(raw, &manifests); err != nil || len(manifests) != 1 {
		return nil, errors.New("Docker archive requires exactly one image")
	}
	entry := manifests[0]
	config, err := a.read(entry.Config, image.MaxMetadata)
	if err != nil {
		return nil, err
	}
	if string(digest.FromBytes(config)) != id {
		return nil, errors.New("Docker archive config differs from pinned image ID")
	}
	var cfg v1.Image
	if err = json.Unmarshal(config, &cfg); err != nil {
		return nil, err
	}
	if len(cfg.RootFS.DiffIDs) != len(entry.Layers) || len(entry.Layers) >= image.MaxDescriptors {
		return nil, errors.New("Docker archive layer count differs from config")
	}
	m := v1.Manifest{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: v1.MediaTypeImageManifest, Config: v1.Descriptor{MediaType: v1.MediaTypeImageConfig, Digest: digest.FromBytes(config), Size: int64(len(config))}}
	aliases := make(map[string]archiveSection)
	alias := func(d digest.Digest, s archiveSection) error {
		name, err := image.BlobPath("", d)
		name = filepath.ToSlash(name)
		if err != nil {
			return err
		}
		if old, ok := a.entries[name]; ok && old != s {
			return errors.New("conflicting archive blob alias")
		}
		if old, ok := aliases[name]; ok && old.size != s.size {
			return errors.New("conflicting repeated layer size")
		}
		aliases[name] = s
		return nil
	}
	if err = alias(m.Config.Digest, a.entries[entry.Config]); err != nil {
		return nil, err
	}
	for i, name := range entry.Layers {
		section, ok := a.entries[name]
		if !ok {
			return nil, errors.New("missing Docker archive layer")
		}
		d := v1.Descriptor{MediaType: v1.MediaTypeImageLayer, Digest: cfg.RootFS.DiffIDs[i], Size: section.size}
		if err = alias(d.Digest, section); err != nil {
			return nil, err
		}
		m.Layers = append(m.Layers, d)
	}
	manifest, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	im, err := image.Parse(manifest, config, "", platform)
	if err != nil {
		return nil, err
	}
	for name, s := range aliases {
		a.entries[name] = s
	}
	return im, nil
}
