package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
)

// Timestamp index. Entry format:
//
//   [timestamp int64][relative_offset uint32][_pad uint32]
//
// 16 bytes per entry. Used by ListOffsets API to answer
// "smallest offset whose first record timestamp >= X".
//
// Like the offset index, timeindex is sparse, one entry per
// `IndexInterval` bytes of log data. Entries are appended in
// monotonic timestamp order (we use max(prev, batch.MaxTimestamp) so
// out-of-order timestamps don't corrupt the index ordering).

const timeIndexEntrySize = 16

// TimeIndex tracks the segment's max-timestamp -> earliest offset
// containing that timestamp.
type TimeIndex struct {
	path           string
	file           *os.File
	entries        []timeEntry // sorted by Timestamp
	lastIndexedPos int64
	lastTS         int64
	interval       int64
}

type timeEntry struct {
	Timestamp int64
	RelOffset uint32
}

// OpenTimeIndex opens/creates the timestamp index.
func OpenTimeIndex(path string, interval int64) (*TimeIndex, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open timeindex %s: %w", path, err)
	}
	ti := &TimeIndex{path: path, file: f, interval: interval}
	if err := ti.load(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return ti, nil
}

func (t *TimeIndex) load() error {
	stat, err := t.file.Stat()
	if err != nil {
		return err
	}
	size := stat.Size()
	if size%timeIndexEntrySize != 0 {
		if err := t.file.Truncate(size - (size % timeIndexEntrySize)); err != nil {
			return fmt.Errorf("truncate partial timeindex: %w", err)
		}
		size -= size % timeIndexEntrySize
	}
	if size == 0 {
		return nil
	}
	buf := make([]byte, size)
	if _, err := t.file.ReadAt(buf, 0); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("read timeindex: %w", err)
	}
	count := size / timeIndexEntrySize
	t.entries = make([]timeEntry, count)
	for k := int64(0); k < count; k++ {
		off := k * timeIndexEntrySize
		t.entries[k] = timeEntry{
			Timestamp: int64(binary.BigEndian.Uint64(buf[off : off+8])),
			RelOffset: binary.BigEndian.Uint32(buf[off+8 : off+12]),
			// Last 4 bytes are padding for alignment; ignored.
		}
	}
	if n := len(t.entries); n > 0 {
		t.lastTS = t.entries[n-1].Timestamp
	}
	return nil
}

// MaybeAppend records a (timestamp, offset) sample, gated by interval.
func (t *TimeIndex) MaybeAppend(ts int64, relOffset int64, position int64) error {
	if position-t.lastIndexedPos < t.interval && len(t.entries) > 0 {
		return nil
	}
	// Enforce monotonicity. If a batch has a clock-skewed timestamp
	// older than the last sample, clamp to lastTS+1.
	if ts <= t.lastTS && len(t.entries) > 0 {
		ts = t.lastTS + 1
	}
	if relOffset < 0 || relOffset > 0xFFFFFFFF {
		return fmt.Errorf("relative offset %d out of range", relOffset)
	}
	e := timeEntry{Timestamp: ts, RelOffset: uint32(relOffset)}
	var buf [timeIndexEntrySize]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(e.Timestamp))
	binary.BigEndian.PutUint32(buf[8:12], e.RelOffset)
	// buf[12:16] left as zero (reserved).
	if _, err := t.file.Write(buf[:]); err != nil {
		return fmt.Errorf("write timeindex entry: %w", err)
	}
	t.entries = append(t.entries, e)
	t.lastIndexedPos = position
	t.lastTS = ts
	return nil
}

// LookupOffset returns the smallest relative offset whose containing
// batch had Timestamp >= ts. Returns (0, false) if no entry meets the
// criterion (caller should fall back to LATEST).
func (t *TimeIndex) LookupOffset(ts int64) (int64, bool) {
	if len(t.entries) == 0 {
		return 0, false
	}
	idx := sort.Search(len(t.entries), func(k int) bool {
		return t.entries[k].Timestamp >= ts
	})
	if idx == len(t.entries) {
		return 0, false
	}
	return int64(t.entries[idx].RelOffset), true
}

// Close flushes and closes.
func (t *TimeIndex) Close() error {
	if t.file == nil {
		return nil
	}
	err := t.file.Close()
	t.file = nil
	return err
}

// Sync fsyncs the timeindex.
func (t *TimeIndex) Sync() error {
	if t.file == nil {
		return nil
	}
	return t.file.Sync()
}

// Reset clears the timeindex (used during recovery).
func (t *TimeIndex) Reset() error {
	t.entries = nil
	t.lastIndexedPos = 0
	t.lastTS = 0
	if t.file == nil {
		return nil
	}
	if err := t.file.Truncate(0); err != nil {
		return err
	}
	if _, err := t.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return nil
}

// MaxTimestamp returns the last sampled timestamp (0 if empty).
func (t *TimeIndex) MaxTimestamp() int64 {
	return t.lastTS
}
