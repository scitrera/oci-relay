// SPDX-FileCopyrightText: 2026 Spark Arena
// SPDX-License-Identifier: Apache-2.0

package engine

import "testing"

func TestSupportsStoreVersion(t *testing.T) {
	for _, version := range []string{"29.0.0", "29.1.3", "29.2.1", "29.3.0", "30.0.0", "99.0.0", "v29.1.3", "29.0.0-rc.1", "29.1.3+vendor.1", "29.1.3-0ubuntu1~24.04.1"} {
		t.Run(version, func(t *testing.T) {
			if !SupportsStoreVersion(version) {
				t.Fatal("rejected Docker 29+ version")
			}
		})
	}
	for _, version := range []string{"", "unknown", "28.99.99", "v28.1.0", "29", "29.2", "29.x.1", "29.1.3garbage", "29.1.3\n", "29.1.3-", "+29.1.3"} {
		t.Run(version, func(t *testing.T) {
			if SupportsStoreVersion(version) {
				t.Fatal("accepted older or malformed version")
			}
		})
	}
}
