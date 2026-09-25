package broker

import (
	"testing"

	"github.com/csnyder256/kafka-wire/internal/tiering"
)

func archived(partition int32, base, next int64) tiering.SegmentEntry {
	return tiering.SegmentEntry{Topic: "t", Partition: partition, BaseOffset: base, NextOffset: next}
}

func TestArchivedSegmentFromSkipsHoles(t *testing.T) {
	// Partition 0 holds [0,100) and [200,300); [100,200) is a hole left by a
	// segment an older version deleted before it was archived. Partition 1
	// must never be picked for partition 0.
	entries := []tiering.SegmentEntry{archived(0, 200, 300), archived(1, 0, 1000), archived(0, 0, 100)}
	cases := []struct {
		offset   int64
		wantBase int64
		wantHit  bool
	}{
		{0, 0, true},
		{99, 0, true},
		{100, 200, true}, // in the hole: resume at the next archived segment
		{150, 200, true},
		{299, 200, true},
		{300, 0, false}, // past everything archived
	}
	for _, c := range cases {
		got, ok := archivedSegmentFrom(entries, 0, c.offset)
		if ok != c.wantHit || (ok && got.BaseOffset != c.wantBase) {
			t.Errorf("offset %d: got base %d hit %v, want base %d hit %v", c.offset, got.BaseOffset, ok, c.wantBase, c.wantHit)
		}
	}
}

func TestArchiveStartPerPartition(t *testing.T) {
	entries := []tiering.SegmentEntry{archived(0, 200, 300), archived(1, 50, 60), archived(0, 100, 200)}
	if got, ok := archiveStart(entries, 0); !ok || got != 100 {
		t.Errorf("partition 0: got %d, %v; want 100, true", got, ok)
	}
	if got, ok := archiveStart(entries, 1); !ok || got != 50 {
		t.Errorf("partition 1: got %d, %v; want 50, true", got, ok)
	}
	if _, ok := archiveStart(entries, 2); ok {
		t.Error("partition 2 has nothing archived")
	}
}
