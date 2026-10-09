// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"regexp"
	"strconv"
)

var storeVersionPattern = regexp.MustCompile(`^v?([0-9]+)\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z][0-9A-Za-z.+~_-]*)?$`)

// SupportsStoreVersion checks the Docker 29+ baseline, not storage compatibility.
// Callers must still validate the backend, native layout and content integrity.
// Accept vendor/build suffixes without maintaining a patch-release allowlist.
func SupportsStoreVersion(version string) bool {
	match := storeVersionPattern.FindStringSubmatch(version)
	if match == nil {
		return false
	}
	major, err := strconv.Atoi(match[1])
	return err == nil && major >= 29
}
