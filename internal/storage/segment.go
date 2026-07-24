package storage

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

// Segment is a single append-only log file plus its companion offset
// index and timestamp index. Active segments accept appends; sealed
// segments are immutable and may be archived to S3.
//
// Naming: the .log filename is the zero-padded base offset, e.g.
//   00000000000000000000.log   (segment starting at offset 0)
//   00000000000000123456.log   (segment starting at offset 123456)
// The matching .index and .timeindex share the prefix. This is the
// same scheme Kafka uses, so file dumps are familiar to operators and
// importable by Kafka tooling.

const (
	logSuffix       = ".log"
	indexSuffix     = ".index"
	timeIndexSuffix = ".timeindex"
)

// Segment is the partition's storage primitive.
type Segment struct {
	dir        string
	baseOffset int64
	logFile    *os.File
	logSize    int64
	createdAt  time.Time

	idx *Index
	tix *TimeIndex

	// Cached upper bound (last batch's last offset). Updated on each
	// append. Equal to baseOffset-1 for an empty segment.
	nextOffset int64

	maxBytes int64
	maxAge   time.Duration

	// Set true when sealed (next segment created); blocks further
	// appends.
	sealed bool
}

// SegmentOpts configures Open/Create.
type SegmentOpts struct {
	Dir           string
	BaseOffset    int64
	IndexInterval int64
	MaxBytes      int64
	MaxAge        time.Duration
}

func segmentFilename(base int64, suffix string) string {
	return fmt.Sprintf("%020d%s", base, suffix)
}

// OpenSegment opens an existing segment for read+append. Recovers
// from a partial-write tail by truncating to the last valid batch.
func OpenSegment(opts SegmentOpts) (*Segment, error) {
	logPath := filepath.Join(opts.Dir, segmentFilename(opts.BaseOffset, logSuffix))
	idxPath := filepath.Join(opts.Dir, segmentFilename(opts.BaseOffset, indexSuffix))
	tixPath := filepath.Join(opts.Dir, segmentFilename(opts.BaseOffset, timeIndexSuffix))

	logFile, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open segment log %s: %w", logPath, err)
	}

	idx, err := OpenIndex(idxPath, opts.IndexInterval)
	if err != nil {
		_ = logFile.Close()
		return nil, err
	}
	tix, err := OpenTimeIndex(tixPath, opts.IndexInterval)
	if err != nil {
		_ = logFile.Close()
		_ = idx.Close()
		return nil, err
	}

	stat, err := logFile.Stat()
	if err != nil {
		return nil, err
	}

	s := &Segment{
		dir:        opts.Dir,
		baseOffset: opts.BaseOffset,
		logFile:    logFile,
		logSize:    stat.Size(),
		createdAt:  stat.ModTime(),
		idx:        idx,
		tix:        tix,
		nextOffset: opts.BaseOffset,
		maxBytes:   opts.MaxBytes,
		maxAge:     opts.MaxAge,
	}

	if err := s.recover(); err != nil {
		s.Close()
		return nil, err
	}

	return s, nil
}

// recover scans the log from byte 0 (or from the last index entry,
// whichever is closer to the end) validating CRCs of each batch.
// Truncates at the first invalid batch. Rebuilds index/timeindex if
// they fall behind the log's recovered tail.
func (s *Segment) recover() error {
	if s.logSize == 0 {
		s.nextOffset = s.baseOffset
		return nil
	}

	// Start from the log start; we always have enough metadata at the
	// start of each batch to skip forward. We could optimize by
	// jumping to the last index entry, but the index might be stale
	// or missing on an unclean shutdown, start clean.
	pos := int64(0)
	header := make([]byte, v2HeaderSize)
	indexNeedsRebuild := false

	// If the timeindex's lastIndexedPos > the log's last good byte,
	// we must reset it. Same for offset index. Easier: reset both,
	// rebuild from scratch. They're tiny.
	if err := s.idx.Reset(); err != nil {
		return fmt.Errorf("reset index: %w", err)
	}
	if err := s.tix.Reset(); err != nil {
		return fmt.Errorf("reset timeindex: %w", err)
	}
	indexNeedsRebuild = true
	_ = indexNeedsRebuild // silence linter; future-use placeholder

	for pos < s.logSize {
		// Read the header (61 bytes).
		n, err := s.logFile.ReadAt(header, pos)
		if err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("read segment header at %d: %w", pos, err)
		}
		if n < v2HeaderSize {
			// Partial header at tail: truncate.
			break
		}
		h, err := ParseBatchHeader(header)
		if err != nil {
			break
		}
		batchSize := int64(h.TotalSize())
		if pos+batchSize > s.logSize {
			break
		}
		// Read the full batch and validate CRC. This catches torn
		// writes where the header is intact but the body is corrupt.
		body := make([]byte, batchSize)
		if _, err := s.logFile.ReadAt(body, pos); err != nil {
			break
		}
		if err := ValidateCRC(body); err != nil {
			break
		}
		// Good batch. Update indexes.
		relOff := h.BaseOffset - s.baseOffset
		_ = s.idx.MaybeAppend(relOff, pos)
		_ = s.tix.MaybeAppend(h.MaxTimestamp, relOff, pos)
		s.nextOffset = h.LastOffset() + 1
		pos += batchSize
	}

	if pos < s.logSize {
		// Tail truncate. This DISCARDS acknowledged-but-partially-written
		// data (the group-commit fsync window on an unclean shutdown), so
		// it must never be silent: operators need the exact offset range
		// and byte count to correlate consumer gaps with the crash.
		truncatedBytes := s.logSize - pos
		slog.Warn("storage.segment.recovery_truncated_tail",
			"log", s.logFile.Name(),
			"base_offset", s.baseOffset,
			"first_lost_offset", s.nextOffset,
			"truncated_bytes", truncatedBytes,
			"good_bytes", pos)
		if err := s.logFile.Truncate(pos); err != nil {
			return fmt.Errorf("truncate corrupt tail: %w", err)
		}
		s.logSize = pos
	}

	// Seek to end so future writes go to the right place.
	if _, err := s.logFile.Seek(s.logSize, io.SeekStart); err != nil {
		return err
	}
	return nil
}

// BaseOffset returns the offset of the first record in this segment.
func (s *Segment) BaseOffset() int64 { return s.baseOffset }

// NextOffset returns the offset that will be assigned to the next
// appended batch's first record.
func (s *Segment) NextOffset() int64 { return s.nextOffset }

// Size returns current .log file size in bytes.
func (s *Segment) Size() int64 { return s.logSize }

// Sealed returns true if this segment is no longer accepting writes.
func (s *Segment) Sealed() bool { return s.sealed }

// CreatedAt returns the segment file's mtime (used for retention).
func (s *Segment) CreatedAt() time.Time { return s.createdAt }

// ShouldRoll returns true if this segment should be sealed because it
// exceeds size or age thresholds.
func (s *Segment) ShouldRoll() bool {
	if s.maxBytes > 0 && s.logSize >= s.maxBytes {
		return true
	}
	if s.maxAge > 0 && time.Since(s.createdAt) >= s.maxAge {
		return true
	}
	return false
}

// Append writes a batch to the active segment. The caller has
// already rewritten BaseOffset to be absolute; we trust the bytes.
//
// Returns:
//   firstOffset:  offset of first record in this batch
//   lastOffset:   offset of last record in this batch
//   position:     byte position of this batch in the log file
//   err:          any error (write, parse, CRC)
func (s *Segment) Append(batch []byte) (firstOffset, lastOffset, position int64, err error) {
	if s.sealed {
		return 0, 0, 0, errors.New("segment sealed")
	}
	h, err := ParseBatchHeader(batch)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("parse batch: %w", err)
	}
	if err := ValidateCRC(batch); err != nil {
		return 0, 0, 0, fmt.Errorf("validate CRC: %w", err)
	}
	if err := ValidateCompressionCodec(h.Attributes); err != nil {
		return 0, 0, 0, fmt.Errorf("validate codec: %w", err)
	}
	pos := s.logSize
	if _, err := s.logFile.Write(batch); err != nil {
		return 0, 0, 0, fmt.Errorf("write batch: %w", err)
	}
	s.logSize += int64(len(batch))
	s.nextOffset = h.LastOffset() + 1
	relOff := h.BaseOffset - s.baseOffset
	_ = s.idx.MaybeAppend(relOff, pos)
	_ = s.tix.MaybeAppend(h.MaxTimestamp, relOff, pos)
	return h.BaseOffset, h.LastOffset(), pos, nil
}

// ReadAt copies up to maxBytes from `position` into a freshly
// allocated buffer. Returns the slice and the actual byte count.
//
// Caller is responsible for parsing batch headers within the slice;
// we always return whole-batch boundaries (see ReadFrom).
func (s *Segment) ReadAt(position int64, maxBytes int) ([]byte, error) {
	if position >= s.logSize {
		return nil, nil
	}
	want := int64(maxBytes)
	if position+want > s.logSize {
		want = s.logSize - position
	}
	buf := make([]byte, want)
	n, err := s.logFile.ReadAt(buf, position)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return buf[:n], nil
}

// FindBatchAt returns the byte position of the batch containing
// targetOffset, scanning forward from the index lookup.
func (s *Segment) FindBatchAt(targetOffset int64) (int64, error) {
	relOff := targetOffset - s.baseOffset
	startPos, ok := s.idx.LookupPosition(relOff)
	if !ok {
		startPos = 0
	}
	pos := startPos
	header := make([]byte, v2HeaderSize)
	for pos < s.logSize {
		_, err := s.logFile.ReadAt(header, pos)
		if err != nil && !errors.Is(err, io.EOF) {
			return 0, err
		}
		h, err := ParseBatchHeader(header)
		if err != nil {
			return 0, fmt.Errorf("scan batch at %d: %w", pos, err)
		}
		if targetOffset >= h.BaseOffset && targetOffset <= h.LastOffset() {
			return pos, nil
		}
		if h.BaseOffset > targetOffset {
			// Walked past: should not happen if data is consistent.
			return pos, nil
		}
		pos += int64(h.TotalSize())
	}
	return s.logSize, nil
}

// LookupOffsetByTimestamp returns the smallest relative offset whose
// containing batch had MaxTimestamp >= ts.
func (s *Segment) LookupOffsetByTimestamp(ts int64) (int64, bool) {
	rel, ok := s.tix.LookupOffset(ts)
	if !ok {
		return 0, false
	}
	return s.baseOffset + rel, true
}

// Sync fsyncs the log + index + timeindex.
func (s *Segment) Sync() error {
	if err := s.logFile.Sync(); err != nil {
		return err
	}
	if err := s.idx.Sync(); err != nil {
		return err
	}
	return s.tix.Sync()
}

// Seal marks the segment as immutable. fsyncs everything one last
// time. Future Append calls will error.
func (s *Segment) Seal() error {
	if err := s.Sync(); err != nil {
		return err
	}
	s.sealed = true
	return nil
}

// Close flushes and closes all files. Safe to call multiple times.
func (s *Segment) Close() error {
	var firstErr error
	if s.logFile != nil {
		if err := s.logFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.logFile = nil
	}
	if s.idx != nil {
		if err := s.idx.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.idx = nil
	}
	if s.tix != nil {
		if err := s.tix.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.tix = nil
	}
	return firstErr
}

// Delete removes the segment's files from disk. Caller must have
// confirmed the segment is sealed AND archived (if applicable) before
// calling.
func (s *Segment) Delete() error {
	if err := s.Close(); err != nil {
		return err
	}
	for _, suffix := range []string{logSuffix, indexSuffix, timeIndexSuffix} {
		path := filepath.Join(s.dir, segmentFilename(s.baseOffset, suffix))
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove %s: %w", path, err)
		}
	}
	return nil
}

// LogPath returns the absolute path to the .log file (used by the S3
// uploader and zero-copy fetch).
func (s *Segment) LogPath() string {
	return filepath.Join(s.dir, segmentFilename(s.baseOffset, logSuffix))
}

// LogFD returns the underlying file descriptor for sendfile(2). The
// file MUST stay open while the caller holds the FD.
func (s *Segment) LogFD() *os.File {
	return s.logFile
}
