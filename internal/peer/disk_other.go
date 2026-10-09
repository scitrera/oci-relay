//go:build !linux && !darwin

// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package peer

import "errors"

func availableDisk(string) (int64, error) {
	return 0, errors.New("decoder disk accounting unsupported on this platform")
}
