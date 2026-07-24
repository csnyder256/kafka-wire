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
