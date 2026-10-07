//go:build !linux && !darwin

// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only
// Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.

package peer

import "errors"

func availableDisk(string) (int64, error) {
	return 0, errors.New("decoder disk accounting unsupported on this platform")
}
