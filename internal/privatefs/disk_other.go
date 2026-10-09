//go:build !linux && !darwin && !windows

// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package privatefs

import "errors"

func Available(string) (int64, error) {
	return 0, errors.New("disk accounting unsupported on this platform")
}
