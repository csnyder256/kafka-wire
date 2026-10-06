package storage

import "testing"

// These tests pin ListOffsets-by-timestamp to its documented answer: the
// first batch, in offset order, whose MaxTimestamp is at or after the target.
//
// The timestamp index is sparse (one sample per IndexInterval bytes), so the
// nearest sample at or before the target can sit a whole interval short of the
// real answer. Reporting that sample directly -- which the lookup used to do --
// returned a batch *after* the requested time and skipped every record in
// between, so a consumer seeking by time, and the admin reset-offset
// strategy=timestamp, both lost records whose timestamps were at or after the
// target. Each batch makeBatch builds has MaxTimestamp == ts+10.

// TestTimestampLookupFindsTheFirstBatchAtOrAfterTheTarget is the sparse
// within-a-segment case. With a 200-byte interval and 65-byte batches, the
// index samples offsets 0, 4 and 8 only; a query between samples must still
// answer with the first batch that actually reaches the target, not the next
// sample.
func TestTimestampLookupFindsTheFirstBatchAtOrAfterTheTarget(t *testing.T) {
	s, err := Open(Config{DataDir: t.TempDir(), SegmentBytes: 1 << 30, IndexInterval: 200})
	if err != nil {
		t.Fatalf("opening storage: %v", err)
	}
	defer s.Close()
	l, err := s.OpenLog("sparse", 0)
	if err != nil {
		t.Fatalf("opening log: %v", err)
	}
	defer l.Close()

	// Batch i carries MaxTimestamp 1010 + i*100.
	for i := 0; i < 12; i++ {
		ts := int64(1000 + i*100)
		if _, err := l.Append([][]byte{makeBatch(t, int64(i), 1, 0, ts)}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	// Guard the premise: the index really is sparse, so these cases exercise
	// the gap between samples rather than a dense index that hides the defect.
	if got := len(l.AllSegments()[0].tix.entries); got >= 12 {
		t.Fatalf("time index sampled %d of 12 batches; the test needs a sparse index", got)
	}

	cases := []struct {
		query   int64
		wantOff int64
	}{
		{1000, 0}, // before the first batch's MaxTimestamp
		{1050, 1}, // between samples 0 (1010) and 4 (1410)
		{1150, 2},
		{1200, 2},
		{1410, 4}, // exactly a sampled batch's MaxTimestamp
		{1700, 7},
		{1810, 8}, // exactly the last sample
	}
	for _, c := range cases {
		got, ok := l.LookupOffsetByTimestamp(c.query)
		if !ok || got != c.wantOff {
			t.Errorf("LookupOffsetByTimestamp(%d) = (%d,%v), want (%d,true): a seek must not skip the first batch at or after the target",
				c.query, got, ok, c.wantOff)
		}
	}
}

// TestTimestampLookupSpansSegmentsWithoutSkippingTheTail is the cross-segment
// case. A segment's last batches are usually unsampled, so a target that falls
// in that tail must be answered from this segment, not from the next segment's
// first sample -- which is what returning the sample at or after the target did.
func TestTimestampLookupSpansSegmentsWithoutSkippingTheTail(t *testing.T) {
	// SegmentBytes 130 with 65-byte batches seals a segment every two
	// batches; the 200-byte index interval is larger than a segment, so each
	// segment samples only its first batch and its tail is unsampled.
	s, err := Open(Config{DataDir: t.TempDir(), SegmentBytes: 130, IndexInterval: 200})
	if err != nil {
		t.Fatalf("opening storage: %v", err)
	}
	defer s.Close()
	l, err := s.OpenLog("segmented", 0)
	if err != nil {
		t.Fatalf("opening log: %v", err)
	}
	defer l.Close()

	// MaxTimestamp: offset 0 -> 1010, 1 -> 1210, 2 -> 1410, 3 -> 1610.
	for i, ts := range []int64{1000, 1200, 1400, 1600} {
		if _, err := l.Append([][]byte{makeBatch(t, int64(i), 1, 0, ts)}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if got := len(l.AllSegments()); got < 2 {
		t.Fatalf("setup produced %d segment(s), want at least 2", got)
	}

	// 1150 falls in segment 0's unsampled tail: offset 1 has MaxTimestamp
	// 1210. The next sample is offset 2 (1410) in segment 1, so answering
	// with a sample would return 2 and drop offset 1.
	if got, ok := l.LookupOffsetByTimestamp(1150); !ok || got != 1 {
		t.Fatalf("LookupOffsetByTimestamp(1150) = (%d,%v), want (1,true): the unsampled tail of the earlier segment must win", got, ok)
	}
}

// A target past every batch has no answer, and a target before every batch
// resolves to the first offset. These bound the scan.
func TestTimestampLookupBounds(t *testing.T) {
	s, err := Open(Config{DataDir: t.TempDir(), SegmentBytes: 1 << 30, IndexInterval: 200})
	if err != nil {
		t.Fatalf("opening storage: %v", err)
	}
	defer s.Close()
	l, err := s.OpenLog("bounds", 0)
	if err != nil {
		t.Fatalf("opening log: %v", err)
	}
	defer l.Close()

	for i := 0; i < 6; i++ {
		if _, err := l.Append([][]byte{makeBatch(t, int64(i), 1, 0, int64(1000+i*100))}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	if got, ok := l.LookupOffsetByTimestamp(1); !ok || got != 0 {
		t.Errorf("a target before every batch = (%d,%v), want (0,true)", got, ok)
	}
	if got, ok := l.LookupOffsetByTimestamp(9_999_999); ok {
		t.Errorf("a target past every batch = (%d,true), want no match", got)
	}
}

// An empty partition has nothing to find and must not panic or invent an
// offset.
func TestTimestampLookupOnEmptyLog(t *testing.T) {
	s, err := Open(Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("opening storage: %v", err)
	}
	defer s.Close()
	l, err := s.OpenLog("empty", 0)
	if err != nil {
		t.Fatalf("opening log: %v", err)
	}
	defer l.Close()

	if got, ok := l.LookupOffsetByTimestamp(1000); ok {
		t.Fatalf("empty log = (%d,true), want no match", got)
	}
}
