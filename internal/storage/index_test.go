package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestIndex_AppendAndLookup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	idx, err := OpenIndex(path, 1024)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Sparse: every 1024 bytes.
	if err := idx.MaybeAppend(0, 0); err != nil {
		t.Fatal(err)
	}
	if err := idx.MaybeAppend(50, 500); err != nil { // < interval, no-op
		t.Fatal(err)
	}
	if err := idx.MaybeAppend(100, 1024); err != nil { // exactly interval, append
		t.Fatal(err)
	}
	if err := idx.MaybeAppend(200, 2048); err != nil {
		t.Fatal(err)
	}

	if got := len(idx.entries); got != 3 {
		t.Fatalf("entries = %d, want 3", got)
	}

	// Lookups: largest entry with RelOffset <= target.
	cases := []struct {
		query   int64
		wantPos int64
	}{
		{0, 0},
		{50, 0},
		{99, 0},
		{100, 1024},
		{150, 1024},
		{200, 2048},
		{500, 2048},
	}
	for _, tc := range cases {
		got, ok := idx.LookupPosition(tc.query)
		if !ok || got != tc.wantPos {
			t.Errorf("LookupPosition(%d) = (%d,%v), want (%d,true)", tc.query, got, ok, tc.wantPos)
		}
	}

	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen and verify entries persisted.
	idx2, err := OpenIndex(path, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer idx2.Close()
	if got := len(idx2.entries); got != 3 {
		t.Fatalf("after reopen entries = %d, want 3", got)
	}
}

func TestIndex_EmptyLookup(t *testing.T) {
	dir := t.TempDir()
	idx, err := OpenIndex(filepath.Join(dir, "empty.index"), 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	if _, ok := idx.LookupPosition(42); ok {
		t.Fatal("empty index should return ok=false")
	}
}

func TestIndex_PartialFileRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "partial.index")

	idx, _ := OpenIndex(path, 100)
	_ = idx.MaybeAppend(0, 0)
	_ = idx.MaybeAppend(10, 100)
	idx.Close()

	// Append 4 bytes of garbage to the file (partial entry).
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF}); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	idx2, err := OpenIndex(path, 100)
	if err != nil {
		t.Fatalf("open after corruption: %v", err)
	}
	defer idx2.Close()
	// Should have truncated the partial entry.
	if got := len(idx2.entries); got != 2 {
		t.Fatalf("entries after partial-file recovery = %d, want 2", got)
	}
}

// helper for the truncation test.
func openForAppend(path string) (interface{ Write([]byte) (int, error); Close() error }, error) {
	return nil, nil
}
