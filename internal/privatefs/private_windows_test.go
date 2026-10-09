// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package privatefs

import (
	"golang.org/x/sys/windows"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWindowsPrivateDirectoryAndDisk(t *testing.T) {
	root := filepath.Join(t.TempDir(), "credentials")
	if err := Ensure(root); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(root, "secret.json")
	if err := os.WriteFile(file, []byte("private"), 0600); err != nil {
		t.Fatal(err)
	}
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{root, file} {
		sd, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
		if err != nil {
			t.Fatal(err)
		}
		sddl := sd.String()
		if !strings.Contains(sddl, user.User.Sid.String()) || strings.Contains(sddl, ";;;WD)") || strings.Contains(sddl, ";;;BU)") || strings.Contains(sddl, ";;;AU)") {
			t.Fatalf("unexpected DACL: %s", sddl)
		}
	}
	available, err := Available(root)
	if err != nil || available <= 0 {
		t.Fatalf("disk space: %d %v", available, err)
	}
	if err := Ensure(file); err == nil {
		t.Fatal("accepted file as private directory")
	}
}
