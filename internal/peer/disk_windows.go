// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package peer

import "github.com/scitrera/oci-relay/internal/privatefs"

func availableDisk(path string) (int64, error) { return privatefs.Available(path) }
