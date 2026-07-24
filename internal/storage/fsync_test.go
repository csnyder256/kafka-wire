package storage

import (
	"testing"
	"time"
)

// These tests exist because storage.fsyncmode was once accepted by the
// configuration layer, validated against its three legal values, documented in
// detail, and then never read by anything. A durability setting that does
// nothing is worse than no setting at all: it is a promise the broker does not
// keep. What follows locks the wiring in place.

func TestFsyncDefaultsAreApplied(t *testing.T) {
	s, err := Open(Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if got := s.Cfg().FsyncMode; got != FsyncInterval {
		t.Errorf("default fsyncmode = %q, want %q", got, FsyncInterval)
	}
	if got := s.Cfg().FsyncInterval; got != 5*time.Second {
		t.Errorf("default fsyncinterval = %v, want 5s", got)
	}
}

// The mode has to survive the trip from configuration into the Log that
// actually performs the append. This is the exact link that was missing.
func TestFsyncModeReachesTheLog(t *testing.T) {
	for _, mode := range []string{FsyncNone, FsyncInterval, FsyncAlways} {
		s, err := Open(Config{DataDir: t.TempDir(), FsyncMode: mode})
		if err != nil {
			t.Fatal(err)
		}
		l, err := s.OpenLog("durable.topic", 0)
		if err != nil {
			t.Fatal(err)
		}
		if l.cfg.FsyncMode != mode {
			t.Errorf("log for mode %q received %q", mode, l.cfg.FsyncMode)
		}
		l.Close()
		s.Close()
	}
}

// Appending must work, and produce readable data, under every mode. The
// "always" case additionally exercises the fsync-before-return path, so a
// Sync that returned an error would surface here rather than in production.
func TestAppendUnderEveryFsyncMode(t *testing.T) {
	for _, mode := range []string{FsyncNone, FsyncInterval, FsyncAlways} {
		t.Run(mode, func(t *testing.T) {
			s, err := Open(Config{DataDir: t.TempDir(), FsyncMode: mode})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			l, err := s.OpenLog("durable.topic", 0)
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()

			batch := makeBatch(t, 0, 1, 0, time.Now().UnixMilli())
			off, err := l.Append([][]byte{batch})
			if err != nil {
				t.Fatalf("append in %q mode: %v", mode, err)
			}
			if off != 0 {
				t.Fatalf("first append returned offset %d, want 0", off)
			}
			if got := l.HighWatermark(); got != 1 {
				t.Fatalf("high watermark = %d, want 1", got)
			}
		})
	}
}

// RunSyncer is what makes the default mode mean anything. A sweep over a live
// log must succeed rather than, say, panicking on a partition with no data.
func TestSyncerSweepsWithoutError(t *testing.T) {
	s, err := Open(Config{DataDir: t.TempDir(), FsyncMode: FsyncInterval})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	empty, err := s.OpenLog("empty.topic", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer empty.Close()

	written, err := s.OpenLog("written.topic", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer written.Close()
	if _, err := written.Append([][]byte{makeBatch(t, 0, 1, 0, time.Now().UnixMilli())}); err != nil {
		t.Fatal(err)
	}

	for _, l := range []*Log{empty, written} {
		if err := l.FlushAndSync(); err != nil {
			t.Errorf("FlushAndSync on %s: %v", l.Topic(), err)
		}
	}
}
