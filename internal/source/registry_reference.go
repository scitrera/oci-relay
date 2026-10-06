// SPDX-FileCopyrightText: 2026 The Dragonfly Authors
// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0
//
// Adapted from Dragonfly pkg/oci/image.go and reference.go at
// 41b312389c90e4f3c44adde8bbd8aa11888076d4. The standalone parser avoids linking
// that package's legacy Docker daemon/registry dependency. The upstream license
// is reproduced in THIRD_PARTY_LICENSES.txt; see THIRD_PARTY_NOTICES.md.

package source

import (
	"fmt"

	"github.com/distribution/reference"
)

type registryReference struct{ Scheme, Registry, Repository, Reference string }

func parseRegistryReference(value string, plainHTTP bool) (*registryReference, error) {
	named, err := reference.ParseNormalizedNamed(value)
	if err != nil {
		return nil, err
	}
	named = reference.TagNameOnly(named)
	var tag string
	switch ref := named.(type) {
	case reference.Digested:
		tag = ref.Digest().String()
	case reference.Tagged:
		tag = ref.Tag()
	default:
		return nil, fmt.Errorf("invalid image reference")
	}
	host := reference.Domain(named)
	if host == "docker.io" {
		host = "registry-1.docker.io"
	}
	scheme := "https"
	if plainHTTP {
		scheme = "http"
	}
	return &registryReference{Scheme: scheme, Registry: host, Repository: reference.Path(named), Reference: tag}, nil
}
func (r *registryReference) ManifestURL() string {
	return fmt.Sprintf("%s://%s/v2/%s/manifests/%s", r.Scheme, r.Registry, r.Repository, r.Reference)
}
func (r *registryReference) BlobURL(digest string) string {
	return fmt.Sprintf("%s://%s/v2/%s/blobs/%s", r.Scheme, r.Registry, r.Repository, digest)
}
