// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package privatefs

import (
	"golang.org/x/sys/windows"
	"os"
	"path/filepath"
	"testing"
	"unsafe"
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
		acl, _, err := sd.DACL()
		if err != nil {
			t.Fatal(err)
		}
		if acl == nil || acl.AceCount != 2 {
			t.Fatal("private directory must grant exactly user and SYSTEM")
		}
		system, err := windows.StringToSid("S-1-5-18")
		if err != nil {
			t.Fatal(err)
		}
		seenUser, seenSystem := false, false
		for i := uint32(0); i < uint32(acl.AceCount); i++ {
			var ace *windows.ACCESS_ALLOWED_ACE
			if err = windows.GetAce(acl, i, &ace); err != nil {
				t.Fatal(err)
			}
			if ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE {
				t.Fatal("unexpected ACE type")
			}
			sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
			switch {
			case sid.Equals(user.User.Sid):
				seenUser = true
			case sid.Equals(system):
				seenSystem = true
			default:
				t.Fatalf("unexpected principal %s", sid.String())
			}
		}
		if !seenUser || !seenSystem {
			t.Fatal("missing user or SYSTEM access")
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
