//go:build windows

package storage

import (
	"os"

	"golang.org/x/sys/windows"
)

func lockFile(f *os.File) error {
	// LOCKFILE_FAIL_IMMEDIATELY turns a held lock into an error instead of a
	// wait, which is what makes a second start fail loudly rather than hang.
	return windows.LockFileEx(
		windows.Handle(f.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0, 1, 0, new(windows.Overlapped),
	)
}

func unlockFile(f *os.File) error {
	return windows.UnlockFileEx(windows.Handle(f.Fd()), 0, 1, 0, new(windows.Overlapped))
}
