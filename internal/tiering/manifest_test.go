package tiering

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeManifestFile(t *testing.T, dir string, entries []SegmentEntry) string {
	t.Helper()
	wrapper := struct {
		Format   int            `json:"format_version"`
		Segments []SegmentEntry `json:"segments"`
	}{Format: 1, Segments: entries}
	raw, err := json.MarshalIndent(wrapper, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	meta := filepath.Join(dir, "metadata")
	if err := os.MkdirAll(meta, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(meta, "archive.json")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func sampleEntries(n int) []SegmentEntry {
	out := make([]SegmentEntry, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, SegmentEntry{
			Topic:      "orders.events",
			Partition:  0,
			BaseOffset: int64(i * 1000),
			NextOffset: int64((i + 1) * 1000),
			SizeBytes:  4096,
			SHA256:     "deadbeef",
			S3Key:      SegmentKey("archive", "orders.events", 0, int64(i*1000)),
			UploadedAt: time.Unix(1700000000+int64(i), 0).UTC(),
		})
	}
	return out
}

func TestOpenManifest_CleanFile(t *testing.T) {
	dir := t.TempDir()
	writeManifestFile(t, dir, sampleEntries(3))
	m, err := OpenManifest(dir)
	if err != nil {
		t.Fatalf("OpenManifest: %v", err)
	}
	if got := len(m.All()); got != 3 {
		t.Fatalf("expected 3 entries, got %d", got)
	}
}

// A truncated archive.json (full disk, bad volume) must not error the
// boot path: every complete entry is salvaged, the corrupt bytes are
// quarantined, and a clean manifest is rewritten.
func TestOpenManifest_TruncatedFileSalvages(t *testing.T) {
	dir := t.TempDir()
	path := writeManifestFile(t, dir, sampleEntries(5))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Chop the file mid-way through the last entry.
	if err := os.WriteFile(path, raw[:len(raw)-80], 0o644); err != nil {
		t.Fatal(err)
	}

	m, err := OpenManifest(dir)
	if err != nil {
		t.Fatalf("OpenManifest on truncated file: %v", err)
	}
	got := len(m.All())
	if got < 3 || got > 4 {
		t.Fatalf("expected 3-4 salvaged entries from a 5-entry truncated file, got %d", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "metadata", "archive.json.corrupt")); err != nil {
		t.Fatalf("expected quarantine file: %v", err)
	}
	// The rewritten manifest must parse cleanly on the next boot.
	m2, err := OpenManifest(dir)
	if err != nil {
		t.Fatalf("OpenManifest on rewritten file: %v", err)
	}
	if len(m2.All()) != got {
		t.Fatalf("rewritten manifest lost entries: %d != %d", len(m2.All()), got)
	}
}

func TestOpenManifest_GarbageFileBoots(t *testing.T) {
	dir := t.TempDir()
	meta := filepath.Join(dir, "metadata")
	if err := os.MkdirAll(meta, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(meta, "archive.json"), []byte("not json at all"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(meta, "archive_pending.json"), []byte("{{{{"), 0o644); err != nil {
		t.Fatal(err)
	}
	m, err := OpenManifest(dir)
	if err != nil {
		t.Fatalf("OpenManifest on garbage: %v", err)
	}
	if len(m.All()) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(m.All()))
	}
	if len(m.PendingAll()) != 0 {
		t.Fatalf("expected 0 pending, got %d", len(m.PendingAll()))
	}
}

func TestHoldsNeedsAnExactMatch(t *testing.T) {
	m, err := OpenManifest(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := m.AddCompleted(SegmentEntry{Topic: "t", Partition: 0, BaseOffset: 100, NextOffset: 200, SizeBytes: 4096, S3Key: "k"}); err != nil {
		t.Fatal(err)
	}
	if !m.Holds("t", 0, 100, 200, 4096) {
		t.Error("the archived segment itself must count as held")
	}
	// Same base offset, different segment: a reused topic name, for example.
	if m.Holds("t", 0, 100, 180, 4096) || m.Holds("t", 0, 100, 200, 4000) {
		t.Error("an entry whose end offset or size differs must not count as held")
	}
	if m.Holds("t", 1, 100, 200, 4096) || m.Holds("u", 0, 100, 200, 4096) {
		t.Error("another partition or topic must not count as held")
	}
}

// A deleted topic's entries used to outlive it. A topic recreated under the
// same name then inherited them: the uploader skipped its segments as already
// archived, and fetches below its local log returned the old topic's records.
func TestForgetTopicDropsItsEntriesDurably(t *testing.T) {
	dir := t.TempDir()
	m, err := OpenManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i, topic := range []string{"gone", "kept", "gone"} {
		base := int64(i * 100)
		if err := m.AddCompleted(SegmentEntry{Topic: topic, BaseOffset: base, NextOffset: base + 100, S3Key: topic + "-done-" + string(rune('a'+i))}); err != nil {
			t.Fatal(err)
		}
	}
	for _, topic := range []string{"gone", "kept"} {
		if err := m.SetPending(&PendingUpload{Topic: topic, S3Key: topic + "-pending", UploadID: "u"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.ForgetTopic("gone"); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, mm := range []*Manifest{m, reopened} {
		if got := len(mm.AllForTopic("gone")); got != 0 {
			t.Errorf("%d entries of the deleted topic remain", got)
		}
		if got := len(mm.AllForTopic("kept")); got != 1 {
			t.Errorf("other topics must keep their entries, got %d", got)
		}
		if mm.GetPending("gone-pending") != nil {
			t.Error("the deleted topic's pending upload must be dropped")
		}
		if mm.GetPending("kept-pending") == nil {
			t.Error("another topic's pending upload must be kept")
		}
	}
}

func TestAddCompletedReplacesTheSamePosition(t *testing.T) {
	m, err := OpenManifest(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	stale := SegmentEntry{Topic: "t", BaseOffset: 100, NextOffset: 200, SizeBytes: 10, S3Key: "k"}
	fresh := SegmentEntry{Topic: "t", BaseOffset: 100, NextOffset: 180, SizeBytes: 20, S3Key: "k"}
	other := SegmentEntry{Topic: "t", BaseOffset: 200, NextOffset: 300, SizeBytes: 30, S3Key: "k2"}
	for _, e := range []SegmentEntry{stale, other, fresh} {
		if err := m.AddCompleted(e); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(m.AllForTopic("t")); got != 2 {
		t.Fatalf("%d entries, want 2: the second upload at offset 100 replaces the first", got)
	}
	if !m.Holds("t", 0, 100, 180, 20) || m.Holds("t", 0, 100, 200, 10) {
		t.Fatal("the manifest must describe the latest upload at offset 100")
	}
}

func TestAddCompletedEvictsOverlappingEntries(t *testing.T) {
	m, err := OpenManifest(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// Stale entries from a deleted topic: [0,100) and [100,250).
	for _, e := range []SegmentEntry{
		{Topic: "t", BaseOffset: 0, NextOffset: 100, S3Key: "a"},
		{Topic: "t", BaseOffset: 100, NextOffset: 250, S3Key: "b"},
		{Topic: "t", Partition: 1, BaseOffset: 0, NextOffset: 500, S3Key: "other"},
	} {
		if err := m.AddCompleted(e); err != nil {
			t.Fatal(err)
		}
	}
	// The recreated topic's first segment, [0,120), overlaps both.
	if err := m.AddCompleted(SegmentEntry{Topic: "t", BaseOffset: 0, NextOffset: 120, S3Key: "a"}); err != nil {
		t.Fatal(err)
	}
	var p0 []SegmentEntry
	for _, e := range m.AllForTopic("t") {
		if e.Partition == 0 {
			p0 = append(p0, e)
		}
	}
	if len(p0) != 1 || p0[0].NextOffset != 120 {
		t.Fatalf("partition 0 entries = %+v, want only the new [0,120)", p0)
	}
	if _, ok := m.Lookup("t", 1, 0); !ok {
		t.Fatal("another partition's entry must be kept")
	}
}
