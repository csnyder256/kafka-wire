package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Log is one partition's storage: an ordered list of Segment files,
// the last of which is active (writable). Provides the Append/Read
// API the broker uses; the wire protocol layer never touches segments
// directly.
//
// All append operations are serialized via `mu` (sync.Mutex). Reads
// are concurrent with each other and with appends, segment files are
// either active (being appended) or sealed (immutable). Readers
// holding a stale file position past the active segment's tail get
// "no more data" (no read past s.logSize), which is the correct
// behavior.
type Log struct {
	dir       string
	topic     string
	partition int32

	cfg       Config
	mu        sync.Mutex
	segments  []*Segment // sorted by BaseOffset, last = active
	logStart  int64      // smallest offset still on disk (after retention deletes)
	diskGuard *DiskGuard
}

// Config tunes the storage layer.
type Config struct {
	DataDir       string
	SegmentBytes  int64
	SegmentMS     int64
	IndexInterval int64
}

// Storage is the top-level wrapper main.go uses to bring everything
// up. Holds the data directory and provides factory methods for
// per-partition logs.
type Storage struct {
	cfg   Config
	guard *DiskGuard
}

// AttachDiskGuard wires a DiskGuard for the storage layer to consult
// before each Append. Optional; storage works without one.
func (s *Storage) AttachDiskGuard(g *DiskGuard) {
	s.guard = g
}

// Paused reports whether writes are currently paused due to low disk.
// Tee through the guard if attached.
func (s *Storage) Paused() bool {
	if s.guard == nil {
		return false
	}
	return s.guard.Paused()
}

// Open initializes the data directory (creates it if missing) and
// returns the Storage handle. Per-partition logs are opened lazily
// via OpenLog or recovered en-masse via the broker's LoadState.
func Open(cfg Config) (*Storage, error) {
	if cfg.DataDir == "" {
		return nil, errors.New("storage: DataDir required")
	}
	if cfg.SegmentBytes <= 0 {
		cfg.SegmentBytes = 1 << 30
	}
	if cfg.SegmentMS <= 0 {
		cfg.SegmentMS = 7 * 24 * 3600 * 1000
	}
	if cfg.IndexInterval <= 0 {
		cfg.IndexInterval = 16 * 1024
	}
	for _, sub := range []string{"topics", "groups", "metadata", "cache"} {
		if err := os.MkdirAll(filepath.Join(cfg.DataDir, sub), 0o755); err != nil {
			return nil, fmt.Errorf("storage: mkdir %s: %w", sub, err)
		}
	}
	return &Storage{cfg: cfg}, nil
}

// Close is a no-op today (per-Log Close is what flushes data); kept
// here for API parity with future S3-cache cleanup.
func (s *Storage) Close() error { return nil }

// Cfg exposes the storage config for the broker.
func (s *Storage) Cfg() Config { return s.cfg }

// TopicsDir returns the absolute path to the topics directory.
func (s *Storage) TopicsDir() string {
	return filepath.Join(s.cfg.DataDir, "topics")
}

// PartitionDir returns the directory for (topic, partition).
func (s *Storage) PartitionDir(topic string, partition int32) string {
	return filepath.Join(s.cfg.DataDir, "topics", topic, fmt.Sprintf("%d", partition))
}

// OpenLog opens (or creates) the log for one partition.
func (s *Storage) OpenLog(topic string, partition int32) (*Log, error) {
	dir := s.PartitionDir(topic, partition)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("storage: mkdir partition %s/%d: %w", topic, partition, err)
	}
	l := &Log{
		dir:       dir,
		topic:     topic,
		partition: partition,
		cfg:       s.cfg,
		diskGuard: s.guard,
	}
	if err := l.recover(); err != nil {
		return nil, err
	}
	if len(l.segments) == 0 {
		// Fresh partition: create segment 0.
		seg, err := OpenSegment(SegmentOpts{
			Dir:           dir,
			BaseOffset:    0,
			IndexInterval: s.cfg.IndexInterval,
			MaxBytes:      s.cfg.SegmentBytes,
			MaxAge:        time.Duration(s.cfg.SegmentMS) * time.Millisecond,
		})
		if err != nil {
			return nil, err
		}
		l.segments = append(l.segments, seg)
	}
	return l, nil
}

var segmentFilenameRe = regexp.MustCompile(`^(\d{20})\.log$`)

func (l *Log) recover() error {
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return fmt.Errorf("read partition dir: %w", err)
	}
	var bases []int64
	for _, e := range entries {
		m := segmentFilenameRe.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		base, err := strconv.ParseInt(m[1], 10, 64)
		if err != nil {
			continue
		}
		bases = append(bases, base)
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	for i, base := range bases {
		seg, err := OpenSegment(SegmentOpts{
			Dir:           l.dir,
			BaseOffset:    base,
			IndexInterval: l.cfg.IndexInterval,
			MaxBytes:      l.cfg.SegmentBytes,
			MaxAge:        time.Duration(l.cfg.SegmentMS) * time.Millisecond,
		})
		if err != nil {
			return err
		}
		// All but the last are sealed.
		if i < len(bases)-1 {
			seg.sealed = true
		}
		l.segments = append(l.segments, seg)
	}
	if len(l.segments) > 0 {
		l.logStart = l.segments[0].BaseOffset()
	}
	return nil
}

// Topic returns the topic name.
func (l *Log) Topic() string { return l.topic }

// Partition returns the partition index.
func (l *Log) Partition() int32 { return l.partition }

// HighWatermark returns the offset of the next record to be assigned
// (i.e., one past the last persisted record).
func (l *Log) HighWatermark() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.segments) == 0 {
		return 0
	}
	return l.segments[len(l.segments)-1].NextOffset()
}

// LogStartOffset returns the smallest offset still on local disk.
// (S3-archived segments may exist below this; the restorer transparently
// fetches them when needed.)
func (l *Log) LogStartOffset() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.logStart
}

// Append appends one or more record batches that arrived together
// (same Produce request) to the active segment, rewriting their
// BaseOffsets to the partition's actual offsets. Rolls a new
// segment if the active segment exceeds size/age.
//
// `batches` is a slice of complete record-batch byte buffers. Each
// must already have the correct fixed format (we validate the CRC
// on read, so the producer's CRC is preserved verbatim).
//
// Returns the offset of the FIRST record in the FIRST batch. Returns
// ErrDiskFull if the storage's DiskGuard has tripped, producers
// receive this as a NotEnoughReplicas/CorruptMessage and back off.
func (l *Log) Append(batches [][]byte) (firstOffset int64, err error) {
	if l.diskGuard != nil && l.diskGuard.Paused() {
		return 0, ErrDiskFull
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if len(batches) == 0 {
		return 0, errors.New("append: empty batches")
	}
	active := l.activeSegment()
	if active == nil {
		return 0, errors.New("append: no active segment")
	}

	// Roll if needed before this batch.
	if active.ShouldRoll() {
		newActive, rerr := l.rollLocked()
		if rerr != nil {
			return 0, fmt.Errorf("roll segment: %w", rerr)
		}
		active = newActive
	}

	startOffset := active.NextOffset()
	for i, batch := range batches {
		if len(batch) < MinBatchSize {
			return 0, fmt.Errorf("batch %d too small (%d bytes)", i, len(batch))
		}
		if len(batch) > MaxBatchSize {
			return 0, fmt.Errorf("batch %d too large (%d bytes)", i, len(batch))
		}
		// Rewrite BaseOffset to the active partition offset.
		// CRC is over Attributes-onward, so this rewrite does NOT
		// invalidate it (Kafka design: see record.go).
		RewriteBaseOffset(batch, active.NextOffset())
		SetPartitionLeaderEpoch(batch, 0)

		_, _, _, aerr := active.Append(batch)
		if aerr != nil {
			return 0, fmt.Errorf("append batch %d: %w", i, aerr)
		}

		// If this batch pushed us over the size threshold, roll
		// before the next batch (prevents a single huge produce
		// from blowing the segment cap by 4MB).
		if i < len(batches)-1 && active.ShouldRoll() {
			newActive, rerr := l.rollLocked()
			if rerr != nil {
				return 0, fmt.Errorf("roll mid-produce: %w", rerr)
			}
			active = newActive
		}
	}

	return startOffset, nil
}

// rollLocked seals the current active segment and creates a new one.
// Caller must hold l.mu.
func (l *Log) rollLocked() (*Segment, error) {
	old := l.segments[len(l.segments)-1]
	if err := old.Seal(); err != nil {
		return nil, err
	}
	newBase := old.NextOffset()
	seg, err := OpenSegment(SegmentOpts{
		Dir:           l.dir,
		BaseOffset:    newBase,
		IndexInterval: l.cfg.IndexInterval,
		MaxBytes:      l.cfg.SegmentBytes,
		MaxAge:        time.Duration(l.cfg.SegmentMS) * time.Millisecond,
	})
	if err != nil {
		return nil, err
	}
	l.segments = append(l.segments, seg)
	return seg, nil
}

func (l *Log) activeSegment() *Segment {
	if len(l.segments) == 0 {
		return nil
	}
	return l.segments[len(l.segments)-1]
}

// FetchAt returns up to maxBytes of contiguous batch bytes starting
// AT or AFTER fetchOffset. The caller is responsible for not
// half-decoding: we always return on a batch boundary.
//
// Returns (bytes, firstOffset, error). firstOffset is the BaseOffset
// of the first batch in the returned slice, or fetchOffset if no
// data is available.
func (l *Log) FetchAt(fetchOffset int64, maxBytes int) ([]byte, int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.segments) == 0 {
		return nil, fetchOffset, nil
	}
	seg := l.findSegmentLocked(fetchOffset)
	if seg == nil {
		// Offset out of range. The wire layer translates this to
		// either OFFSET_OUT_OF_RANGE (below logStart) or "no data
		// yet" (above HWM).
		hwm := l.segments[len(l.segments)-1].NextOffset()
		if fetchOffset >= hwm {
			return nil, fetchOffset, nil
		}
		return nil, fetchOffset, ErrOffsetOutOfRange
	}

	startPos, err := seg.FindBatchAt(fetchOffset)
	if err != nil {
		return nil, fetchOffset, fmt.Errorf("find batch: %w", err)
	}
	buf, err := seg.ReadAt(startPos, maxBytes)
	if err != nil {
		return nil, fetchOffset, fmt.Errorf("read at %d: %w", startPos, err)
	}
	if len(buf) == 0 {
		return nil, fetchOffset, nil
	}
	// Trim to the last complete batch boundary.
	trimmed := trimToBatchBoundary(buf)
	if len(trimmed) == 0 {
		// Caller asked for less than one batch's worth; honor by
		// returning the first batch in full. This matches Kafka's
		// "even if max_bytes is too small for one batch, return one
		// batch" behavior.
		hdr, perr := ParseBatchHeader(buf)
		if perr != nil {
			return nil, fetchOffset, perr
		}
		need := hdr.TotalSize()
		full, ferr := seg.ReadAt(startPos, need)
		if ferr != nil {
			return nil, fetchOffset, ferr
		}
		if len(full) < need {
			return nil, fetchOffset, nil
		}
		hdr2, _ := ParseBatchHeader(full)
		return full, hdr2.BaseOffset, nil
	}
	hdr, _ := ParseBatchHeader(trimmed)
	return trimmed, hdr.BaseOffset, nil
}

// trimToBatchBoundary trims `buf` to end on a batch boundary. Returns
// nil if `buf` doesn't even contain one full batch.
func trimToBatchBoundary(buf []byte) []byte {
	pos := 0
	for pos < len(buf) {
		if pos+v2HeaderSize > len(buf) {
			break
		}
		h, err := ParseBatchHeader(buf[pos : pos+v2HeaderSize])
		if err != nil {
			break
		}
		end := pos + h.TotalSize()
		if end > len(buf) {
			break
		}
		pos = end
	}
	return buf[:pos]
}

// findSegmentLocked returns the segment containing fetchOffset, or
// nil if out of range. Caller must hold l.mu.
func (l *Log) findSegmentLocked(fetchOffset int64) *Segment {
	if len(l.segments) == 0 {
		return nil
	}
	if fetchOffset < l.segments[0].BaseOffset() {
		return nil
	}
	hwm := l.segments[len(l.segments)-1].NextOffset()
	if fetchOffset >= hwm {
		return nil
	}
	// Binary search for the largest segment whose BaseOffset <= fetchOffset.
	idx := sort.Search(len(l.segments), func(k int) bool {
		return l.segments[k].BaseOffset() > fetchOffset
	})
	if idx == 0 {
		return l.segments[0]
	}
	return l.segments[idx-1]
}

// EarliestOffset returns the smallest offset still on disk.
func (l *Log) EarliestOffset() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.segments) == 0 {
		return 0
	}
	return l.segments[0].BaseOffset()
}

// LatestOffset returns the high watermark (next offset).
func (l *Log) LatestOffset() int64 {
	return l.HighWatermark()
}

// LookupOffsetByTimestamp returns the smallest offset whose batch
// MaxTimestamp >= ts. Used by ListOffsets API.
func (l *Log) LookupOffsetByTimestamp(ts int64) (int64, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, seg := range l.segments {
		if off, ok := seg.LookupOffsetByTimestamp(ts); ok {
			return off, true
		}
	}
	return 0, false
}

// SealedSegments returns a snapshot of all sealed segments. Used by
// the S3 uploader and retention reaper.
func (l *Log) SealedSegments() []*Segment {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]*Segment, 0, len(l.segments)-1)
	for i := 0; i < len(l.segments)-1; i++ {
		out = append(out, l.segments[i])
	}
	return out
}

// AllSegments returns all segments (active + sealed). Used by admin
// API to list partition state.
func (l *Log) AllSegments() []*Segment {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]*Segment, len(l.segments))
	copy(out, l.segments)
	return out
}

// DeleteSegmentsBefore removes all sealed segments whose BaseOffset
// is < cutoff. Updates logStart. Caller is responsible for ensuring
// archival has completed before calling.
func (l *Log) DeleteSegmentsBefore(cutoff int64) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	deleted := 0
	for i := 0; i < len(l.segments); {
		seg := l.segments[i]
		if i == len(l.segments)-1 {
			break // never delete the active segment
		}
		// Delete a segment only if the NEXT segment's base <= cutoff.
		// That ensures we don't lose offsets that are still inside the
		// retention window.
		nextBase := l.segments[i+1].BaseOffset()
		if nextBase > cutoff {
			break
		}
		if err := seg.Delete(); err != nil {
			return deleted, err
		}
		l.segments = append(l.segments[:i], l.segments[i+1:]...)
		deleted++
	}
	if len(l.segments) > 0 {
		l.logStart = l.segments[0].BaseOffset()
	}
	return deleted, nil
}

// FlushAndSync fsyncs the active segment + indexes. Called by the
// group-commit timer.
func (l *Log) FlushAndSync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.segments) == 0 {
		return nil
	}
	return l.segments[len(l.segments)-1].Sync()
}

// Close closes all segments. Returns the first error encountered;
// best-effort closes the rest regardless.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var firstErr error
	for _, seg := range l.segments {
		if err := seg.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	l.segments = nil
	return firstErr
}

// ErrOffsetOutOfRange signals the caller that the requested offset
// has been retention-deleted or never existed. The wire layer maps
// this to OFFSET_OUT_OF_RANGE.
var ErrOffsetOutOfRange = errors.New("offset out of range")
