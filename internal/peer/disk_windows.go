// SPDX-FileCopyrightText: 2026 Spark Arena
// SPDX-License-Identifier: Apache-2.0

package peer

import "github.com/spark-arena/oci-relay/internal/privatefs"

func availableDisk(path string) (int64, error) { return privatefs.Available(path) }
