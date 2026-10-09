// SPDX-FileCopyrightText: 2026 Spark Arena
// SPDX-License-Identifier: Apache-2.0

package privatefs

import (
	"errors"
	"golang.org/x/sys/windows"
	"os"
)

// Ensure protects operation files with an inheritable, protected DACL. Unix
// permission bits do not enforce privacy on Windows. Never take ownership of
// another user's directory or follow a leaf junction/symlink.
func Ensure(path string) error {
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	attrs, err := windows.GetFileAttributes(name)
	if err != nil {
		return err
	}
	if attrs&windows.FILE_ATTRIBUTE_DIRECTORY == 0 || attrs&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return errors.New("private directory cannot be a reparse point or file")
	}
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return err
	}
	existing, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.OWNER_SECURITY_INFORMATION)
	if err != nil {
		return err
	}
	owner, _, err := existing.Owner()
	if err != nil {
		return err
	}
	owned := owner.Equals(user.User.Sid)
	if !owned {
		// Elevated processes can create directories owned by their enabled
		// Administrators group rather than their individual user SID.
		groups, err := windows.GetCurrentProcessToken().GetTokenGroups()
		if err != nil {
			return err
		}
		for _, group := range groups.AllGroups() {
			if group.Attributes&(windows.SE_GROUP_ENABLED|windows.SE_GROUP_OWNER) == windows.SE_GROUP_ENABLED|windows.SE_GROUP_OWNER && group.Attributes&windows.SE_GROUP_USE_FOR_DENY_ONLY == 0 && owner.Equals(group.Sid) {
				owned = true
			}
		}
	}
	if !owned {
		return errors.New("private directory must belong to the current user")
	}
	sd, err := windows.SecurityDescriptorFromString("D:P(A;OICI;FA;;;" + user.User.Sid.String() + ")(A;OICI;FA;;;SY)")
	if err != nil {
		return err
	}
	acl, _, err := sd.DACL()
	if err != nil {
		return err
	}
	return windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION, nil, nil, acl, nil)
}
