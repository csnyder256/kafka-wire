package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// DirLock is an exclusive lock on a data directory.
//
// Two brokers sharing one data directory both append to the same segment
// files, and neither notices. The log ends up interleaved, offsets collide,
// and the corruption is discovered later by a consumer, by which time there is
// nothing to roll back to. It is an easy mistake to make: start the broker
// twice, restart a container before the old one has exited, or point a second
// Compose service at the same volume.
//
// The lock is held by an open file handle for the life of the process, so the
// operating system releases it even if the broker is killed, which means a
// crash never leaves a stale lock a user has to clear by hand.
type DirLock struct {
	f *os.File
}

// ErrDirLocked reports that another process holds the data directory. The
// caller turns it into an explanation; keeping the guidance out of the error
// value itself lets callers match on it with errors.Is.
var ErrDirLocked = errors.New("another kafka-wire is already using the data directory")

// LockDir takes the lock, or reports that someone else holds it.
func LockDir(dir string) (*DirLock, error) {
	path := filepath.Join(dir, ".lock")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("storage: opening the lock file %s: %w", path, err)
	}
	if err := lockFile(f); err != nil {
		f.Close()
		return nil, fmt.Errorf("%w: %s", ErrDirLocked, dir)
	}
	// Record who holds it. This is informational only: the lock itself is the
	// file handle, so a stale pid here can never wrongly deny a start.
	_ = f.Truncate(0)
	_, _ = f.WriteAt([]byte(fmt.Sprintf("%d\n", os.Getpid())), 0)
	return &DirLock{f: f}, nil
}

// Release drops the lock.
func (l *DirLock) Release() error {
	if l == nil || l.f == nil {
		return nil
	}
	name := l.f.Name()
	err := unlockFile(l.f)
	if cerr := l.f.Close(); err == nil {
		err = cerr
	}
	_ = os.Remove(name)
	l.f = nil
	return err
}
