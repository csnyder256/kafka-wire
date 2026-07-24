package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
)

// Sparse offset index. One entry per `IndexInterval` bytes of log
// data. Entry format:
//
//   [relative_offset uint32][file_position uint32]
//
// `relative_offset` = absolute offset - segment.BaseOffset
// `file_position` = byte offset into the .log file
//
// 8 bytes per entry. With a 16 KB index interval and an average batch
// size of ~2 KB, we get one entry every 8 batches, index file is
// ~0.05% of log file size. Negligible.

const indexEntrySize = 8

// Index reads/writes a sparse offset index. Append-only on the write
// side; in-memory + binary search on the read side. Closed when the
// segment seals.
type Index struct {
	path           string
	file           *os.File
	entries        []indexEntry // sorted by RelOffset
	lastIndexedPos int64
	interval       int64
}

type indexEntry struct {
	RelOffset uint32
	Position  uint32
}

// OpenIndex opens or creates an index file. Loads existing entries
// into memory.
func OpenIndex(path string, interval int64) (*Index, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open index %s: %w", path, err)
	}
	idx := &Index{path: path, file: f, interval: interval}
	if err := idx.load(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return idx, nil
}

func (i *Index) load() error {
	stat, err := i.file.Stat()
	if err != nil {
		return err
	}
	size := stat.Size()
	if size%indexEntrySize != 0 {
		// Truncate any partial trailing entry from a crash. We'll
		// rebuild it on the next batch flush.
		if err := i.file.Truncate(size - (size % indexEntrySize)); err != nil {
			return fmt.Errorf("truncate partial index: %w", err)
		}
		size -= size % indexEntrySize
	}
	if size == 0 {
		return nil
	}
	buf := make([]byte, size)
	if _, err := i.file.ReadAt(buf, 0); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("read index: %w", err)
	}
	count := size / indexEntrySize
	i.entries = make([]indexEntry, count)
	for k := int64(0); k < count; k++ {
		off := k * indexEntrySize
		i.entries[k] = indexEntry{
			RelOffset: binary.BigEndian.Uint32(buf[off : off+4]),
			Position:  binary.BigEndian.Uint32(buf[off+4 : off+8]),
		}
	}
	if n := len(i.entries); n > 0 {
		i.lastIndexedPos = int64(i.entries[n-1].Position)
	}
	return nil
}

// MaybeAppend writes a new index entry IF position has advanced by at
// least `interval` bytes since the last entry. Called after each
// batch append.
func (i *Index) MaybeAppend(relOffset int64, position int64) error {
	if position-i.lastIndexedPos < i.interval && len(i.entries) > 0 {
		return nil
	}
	if relOffset < 0 || relOffset > 0xFFFFFFFF {
		return fmt.Errorf("relative offset %d out of range", relOffset)
	}
	if position < 0 || position > 0xFFFFFFFF {
		// 4 GB segment cap: we'd have rolled by now anyway.
		return fmt.Errorf("position %d out of range", position)
	}
	e := indexEntry{RelOffset: uint32(relOffset), Position: uint32(position)}
	var buf [indexEntrySize]byte
	binary.BigEndian.PutUint32(buf[0:4], e.RelOffset)
	binary.BigEndian.PutUint32(buf[4:8], e.Position)
	if _, err := i.file.Write(buf[:]); err != nil {
		return fmt.Errorf("write index entry: %w", err)
	}
	i.entries = append(i.entries, e)
	i.lastIndexedPos = position
	return nil
}

// LookupPosition returns the largest position <= the position for the
// requested relative offset. Caller must scan forward in the .log
// from this position to find the exact batch.
//
// Returns (0, false) if the index is empty.
func (i *Index) LookupPosition(relOffset int64) (int64, bool) {
	if len(i.entries) == 0 {
		return 0, false
	}
	target := uint32(relOffset)
	// Find the largest entry with RelOffset <= target. sort.Search
	// returns the first index where the predicate is true; we want
	// the first FALSE-to-TRUE boundary, then back up one.
	idx := sort.Search(len(i.entries), func(k int) bool {
		return i.entries[k].RelOffset > target
	})
	if idx == 0 {
		// Even the first entry is past the target; start from byte 0.
		return 0, true
	}
	return int64(i.entries[idx-1].Position), true
}

// Close flushes and closes the index file.
func (i *Index) Close() error {
	if i.file == nil {
		return nil
	}
	err := i.file.Close()
	i.file = nil
	return err
}

// Sync fsyncs the index. Called from the segment's group-commit
// timer; we don't sync per-append (would dominate latency).
func (i *Index) Sync() error {
	if i.file == nil {
		return nil
	}
	return i.file.Sync()
}

// Reset truncates the index to zero entries. Called by recovery when
// a segment's tail bytes are corrupt: we throw away the index and
// rebuild it from a full segment scan.
func (i *Index) Reset() error {
	i.entries = nil
	i.lastIndexedPos = 0
	if i.file == nil {
		return nil
	}
	if err := i.file.Truncate(0); err != nil {
		return err
	}
	if _, err := i.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return nil
}
