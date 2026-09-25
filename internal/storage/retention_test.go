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

func archivedBelow(offset int64) func(string, int32, *Segment) bool {
	return func(_ string, _ int32, seg *Segment) bool { return seg.BaseOffset() < offset }
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

func TestNothingArchivedNothingDeleted(t *testing.T) {
	l := logWithSealed(t, 2)
	cfg := RetentionConfig{RetentionMS: time.Hour.Milliseconds(), RetentionBytes: 1, Archived: archivedBelow(0)}
	sweepOnce(logList{l}, cfg, later)
	if got := len(l.SealedSegments()); got != 2 {
		t.Fatalf("%d sealed segments left, want 2: none is archived", got)
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

func TestSweepIntervalFollowsShortWindows(t *testing.T) {
	cases := []struct {
		cfg  RetentionConfig
		want time.Duration
	}{
		{RetentionConfig{}, 60 * time.Second},
		{RetentionConfig{Tick: 60 * time.Second, RetentionMS: (168 * time.Hour).Milliseconds()}, 60 * time.Second},
		{RetentionConfig{Tick: 60 * time.Second, RetentionMS: 30_000}, 15 * time.Second},
		{RetentionConfig{Tick: 60 * time.Second, RetentionMS: 500}, time.Second},
	}
	for _, c := range cases {
		if got := sweepInterval(c.cfg); got != c.want {
			t.Errorf("sweepInterval(%+v) = %v, want %v", c.cfg, got, c.want)
		}
	}
}

// An idle partition's empty active segment used to roll once it passed
// segmentage, opening its successor at the same base offset on the same
// file. The sealed "phantom" never matched its archived copy, so with cold
// storage on, retention stopped for the partition.
func TestIdleSegmentDoesNotRollIntoADuplicate(t *testing.T) {
	s, err := Open(Config{DataDir: t.TempDir(), SegmentMS: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	l, err := s.OpenLog("idle", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	time.Sleep(10 * time.Millisecond) // the empty active segment is past segmentage

	if _, err := l.Append([][]byte{makeBatch(t, 0, 1, 0, time.Now().UnixMilli())}); err != nil {
		t.Fatal(err)
	}
	if got := len(l.AllSegments()); got != 1 {
		t.Fatalf("%d segments after the first append, want 1: an empty segment must not roll", got)
	}
	if _, err := l.Append([][]byte{makeBatch(t, 0, 1, 0, time.Now().UnixMilli())}); err != nil {
		t.Fatal(err)
	}
	segs := l.AllSegments()
	if len(segs) != 2 || segs[0].BaseOffset() == segs[1].BaseOffset() {
		t.Fatalf("want two segments with distinct base offsets, got %d", len(segs))
	}
}
