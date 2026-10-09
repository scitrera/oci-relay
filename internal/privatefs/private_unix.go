//go:build !windows

// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package privatefs

import (
	"errors"
	"os"
)

func Ensure(path string) error {
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return errors.New("session directory must be private (0700)")
	}
	return nil
}
