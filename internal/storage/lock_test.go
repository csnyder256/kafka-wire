package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// These tests pin the data-directory lock's actual guarantee: while one broker
// holds it, no second broker can take it. The lock exists because two brokers
// sharing a data directory both append to the same segment files and neither
// notices -- the log interleaves, offsets collide, and a consumer discovers the
// corruption later with nothing left to roll back to. So a hole in the lock is
// not a lock nuisance, it is the failure the lock was written to prevent.

// lockPath is where LockDir keeps the lock file.
func lockPath(dir string) string { return filepath.Join(dir, ".lock") }

// TestLockSurvivesAReleaseThatUnlinkedTheFile is the regression for the
// unlink-on-release race.
//
// LockDir is open-then-flock, and the flock is what decides who may run. A
// broker that starts while another one is shutting down opens the lock file
// before the holder releases and calls flock a moment later. If Release()
// removes the file, that broker ends up locking the orphaned inode while the
// next broker to start creates a brand-new lock file and locks that: two
// processes, both holding "the lock", both writing the same log.
//
// The test reproduces exactly that interleaving. B opens the lock file while A
// holds it, A releases, B takes the lock on the inode it opened, and then C
// calls LockDir the way a starting broker does. C must be refused.
func TestLockSurvivesAReleaseThatUnlinkedTheFile(t *testing.T) {
	dir := t.TempDir()

	// A is a running broker holding the data directory.
	a, err := LockDir(dir)
	if err != nil {
		t.Fatalf("A taking the lock: %v", err)
	}

	// B is a starting broker: it opens the same lock file, as LockDir does,
	// before A has released. Its flock has not been attempted yet.
	bFile, err := os.OpenFile(lockPath(dir), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("B opening the lock file: %v", err)
	}
	defer bFile.Close()

	// A shuts down cleanly.
	if err := a.Release(); err != nil {
		t.Fatalf("A releasing the lock: %v", err)
	}

	// B's flock lands after A's release, so it succeeds. B is now a broker
	// that holds the data directory.
	if err := lockFile(bFile); err != nil {
		t.Fatalf("B taking the lock after A released: %v", err)
	}
	defer func() { _ = unlockFile(bFile) }()

	// C starts up and calls LockDir. B holds the directory, so C must be
	// refused. If C is admitted, two brokers are running against one data
	// directory.
	c, err := LockDir(dir)
	if err == nil {
		_ = c.Release()
		t.Fatal("LockDir admitted a second broker while B held the lock: a released lock that unlinks its file leaves the inode B locked and lets the next broker lock a fresh file")
	}
	if !errors.Is(err, ErrDirLocked) {
		t.Fatalf("C was refused with %v, want ErrDirLocked", err)
	}
}

// TestLockFileStaysInPlaceAfterRelease pins the fix's mechanism directly: the
// lock file is the rendezvous every broker must open, so it must survive a
// release. Removing it is what created the second, lockable file in the test
// above.
//
// Leaving the file behind costs one empty file in the data directory and
// cannot strand a lock: the lock itself is the flock on the open handle, which
// the operating system drops when the process exits, however it exits. A stale
// file is re-opened and re-locked by the next broker, not obeyed.
func TestLockFileStaysInPlaceAfterRelease(t *testing.T) {
	dir := t.TempDir()

	l, err := LockDir(dir)
	if err != nil {
		t.Fatalf("taking the lock: %v", err)
	}
	if err := l.Release(); err != nil {
		t.Fatalf("releasing the lock: %v", err)
	}

	if _, err := os.Stat(lockPath(dir)); err != nil {
		t.Fatalf("the lock file is gone after a release: %v; every broker must open this exact file, so removing it lets a later broker lock a different one", err)
	}

	// A clean release must still let the next broker start.
	next, err := LockDir(dir)
	if err != nil {
		t.Fatalf("taking the lock again after a release: %v", err)
	}
	if err := next.Release(); err != nil {
		t.Fatalf("releasing the second lock: %v", err)
	}
}

// TestSecondBrokerIsRefusedWhileTheLockIsHeld is the ordinary case, and it must
// keep working: a second LockDir on a held directory is refused with
// ErrDirLocked, and a fresh one is admitted only after the holder releases.
func TestSecondBrokerIsRefusedWhileTheLockIsHeld(t *testing.T) {
	dir := t.TempDir()

	held, err := LockDir(dir)
	if err != nil {
		t.Fatalf("first LockDir: %v", err)
	}

	second, err := LockDir(dir)
	if err == nil {
		_ = second.Release()
		t.Fatal("a second LockDir was admitted while the first still held the directory")
	}
	if !errors.Is(err, ErrDirLocked) {
		t.Fatalf("second LockDir returned %v, want ErrDirLocked", err)
	}

	if err := held.Release(); err != nil {
		t.Fatalf("releasing the first lock: %v", err)
	}

	after, err := LockDir(dir)
	if err != nil {
		t.Fatalf("LockDir after the holder released: %v", err)
	}
	if err := after.Release(); err != nil {
		t.Fatalf("releasing the final lock: %v", err)
	}
}
