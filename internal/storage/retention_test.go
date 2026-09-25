package storage

import (
	"testing"
	"time"
)

type logList []*Log

func (p logList) AllLogs() []*Log { return p }

// logWithSealed returns a log holding n sealed segments plus the active one.
// The segment size limit is one byte, so every append after the first rolls.
func logWithSealed(t *testing.T, n int) *Log {
	t.Helper()
	s, err := Open(Config{DataDir: t.TempDir(), SegmentBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	l, err := s.OpenLog("retained", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	for i := 0; i <= n; i++ {
		if _, err := l.Append([][]byte{makeBatch(t, 0, 1, 0, time.Now().UnixMilli())}); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(l.SealedSegments()); got != n {
		t.Fatalf("setup: %d sealed segments, want %d", got, n)
	}
	return l
}

func archivedBelow(offset int64) func(string, int32, int64) bool {
	return func(_ string, _ int32, base int64) bool { return base < offset }
}

var later = time.Now().Add(2 * time.Hour)

func TestAgeRetentionWithoutAnArchive(t *testing.T) {
	l := logWithSealed(t, 3)
	sweepOnce(logList{l}, RetentionConfig{RetentionMS: time.Hour.Milliseconds()}, later)
	if got := len(l.SealedSegments()); got != 0 {
		t.Fatalf("%d sealed segments left, want 0", got)
	}
	if got := l.LogStartOffset(); got != 3 {
		t.Fatalf("log start = %d, want 3", got)
	}
}

// With cold storage on, retention used to delete segments whether or not
// they had been uploaded: a size cap under load, or an archive outage longer
// than retentionage, lost records the archive never received.
func TestRetentionNeverDeletesAheadOfTheArchive(t *testing.T) {
	l := logWithSealed(t, 3)
	cfg := RetentionConfig{
		RetentionMS:    time.Hour.Milliseconds(),
		RetentionBytes: 1,
		Archived:       archivedBelow(1), // only the oldest segment is archived
	}
	sweepOnce(logList{l}, cfg, later)
	if got := l.LogStartOffset(); got != 1 {
		t.Fatalf("log start = %d, want 1: only the archived segment may go", got)
	}
	if got := len(l.SealedSegments()); got != 2 {
		t.Fatalf("%d sealed segments left, want the 2 unarchived ones", got)
	}
}

// archive.localretention was documented, validated and passed to the
// uploader, which never read it, so archived segments stayed on local disk
// for the full storage.retentionage.
func TestLocalRetentionTrimsArchivedCopies(t *testing.T) {
	l := logWithSealed(t, 3)
	cfg := RetentionConfig{
		RetentionMS:      (7 * 24 * time.Hour).Milliseconds(),
		LocalRetentionMS: time.Hour.Milliseconds(),
		Archived:         archivedBelow(2),
	}
	sweepOnce(logList{l}, cfg, time.Now().Add(30*time.Minute))
	if got := len(l.SealedSegments()); got != 3 {
		t.Fatalf("%d sealed segments left, want 3: none is older than localretention yet", got)
	}
	sweepOnce(logList{l}, cfg, later)
	if got := l.LogStartOffset(); got != 2 {
		t.Fatalf("log start = %d, want 2: both archived copies go, the unarchived one stays", got)
	}
}

func TestLocalRetentionNeedsAnArchive(t *testing.T) {
	l := logWithSealed(t, 2)
	sweepOnce(logList{l}, RetentionConfig{LocalRetentionMS: time.Hour.Milliseconds()}, later)
	if got := len(l.SealedSegments()); got != 2 {
		t.Fatalf("%d sealed segments left, want 2: nothing is archived", got)
	}
}

// The size rule used to overwrite the age rule's cutoff, so a size cap that
// needed one segment gone kept segments the age rule had already expired.
func TestAgeAndSizeRulesBothApply(t *testing.T) {
	l := logWithSealed(t, 3)
	var total int64
	for _, s := range l.AllSegments() {
		total += s.Size()
	}
	cfg := RetentionConfig{RetentionMS: time.Hour.Milliseconds(), RetentionBytes: total - 1}
	sweepOnce(logList{l}, cfg, later)
	if got := len(l.SealedSegments()); got != 0 {
		t.Fatalf("%d sealed segments left; the age rule expired all of them", got)
	}
}

func TestSweepIntervalFollowsTheShortestWindow(t *testing.T) {
	cases := []struct {
		cfg  RetentionConfig
		want time.Duration
	}{
		{RetentionConfig{}, 60 * time.Second},
		{RetentionConfig{Tick: 60 * time.Second, RetentionMS: (168 * time.Hour).Milliseconds()}, 60 * time.Second},
		{RetentionConfig{Tick: 60 * time.Second, LocalRetentionMS: 2000}, time.Second},
		{RetentionConfig{Tick: 60 * time.Second, LocalRetentionMS: 30_000}, 15 * time.Second},
		{RetentionConfig{Tick: 60 * time.Second, RetentionMS: 500}, time.Second},
	}
	for _, c := range cases {
		if got := sweepInterval(c.cfg); got != c.want {
			t.Errorf("sweepInterval(%+v) = %v, want %v", c.cfg, got, c.want)
		}
	}
}
