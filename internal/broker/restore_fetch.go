package broker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/csnyder256/kafka-wire/internal/storage"
	"github.com/csnyder256/kafka-wire/internal/tiering"
)

// fetchFromArchive looks up the requested offset in the S3 manifest,
// restores the segment to the local cache if needed, and reads bytes
// containing the target offset.
//
// Returns the same shape as storage.Log.FetchAt: (bytes, firstOffset).
// Caller (Broker.Fetch) wraps with hwm + logStart for the wire response.
//
// MULTI-TENANT INVARIANT: if requesterTenant != "" (the caller is a
// tenant principal), the resolved manifest entry's TenantID MUST
// match requesterTenant. A mismatch here would mean a tenant fetched
// an offset whose archived segment belongs to a different tenant,
// the chaos engine treats this as a P0 isolation breach.
func (b *Broker) fetchFromArchive(ctx context.Context, topic string, partition int32, fetchOffset int64, maxBytes int, requesterTenant string) ([]byte, int64, error) {
	if b.restorer == nil || b.manifest == nil || b.cache == nil {
		return nil, fetchOffset, storage.ErrOffsetOutOfRange
	}

	// Find the archived segment whose [BaseOffset, NextOffset) contains
	// fetchOffset or, when fetchOffset falls in a hole (segments that older
	// versions deleted before they were archived), the first one after it.
	// Like a gap in a compacted topic, the fetch continues with the next
	// records instead of failing, which would send a consumer reset to
	// earliest straight back into the hole.
	entry, hit := archivedSegmentFrom(b.manifest.AllForTopic(topic), partition, fetchOffset)
	if !hit {
		return nil, fetchOffset, storage.ErrOffsetOutOfRange
	}
	baseOffset, entryTenant := entry.BaseOffset, entry.TenantID
	if baseOffset > fetchOffset {
		slog.Warn("archive.fetch.gap_skipped", "topic", topic, "partition", partition,
			"fetch_offset", fetchOffset, "resumed_at", baseOffset)
	}

	// Cross-tenant access prevention: requested tenant must match
	// the segment's tenant. Platform principals (empty tenant) bypass.
	if requesterTenant != "" && entryTenant != requesterTenant {
		return nil, fetchOffset, ErrUnauthorizedTopic
	}

	// Restore (no-op if already cached). Bound the restore by a
	// reasonable timeout: fetches shouldn't block on a slow S3
	// download for too long; better to time out and let the consumer
	// retry.
	restoreCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := b.restorer.Restore(restoreCtx, topic, partition, baseOffset); err != nil {
		return nil, fetchOffset, fmt.Errorf("restore: %w", err)
	}

	// Open the cached segment file (tenant-aware path) and scan to
	// find the batch containing fetchOffset.
	f, err := b.cache.OpenTenant(entryTenant, topic, partition, baseOffset)
	if err != nil {
		return nil, fetchOffset, fmt.Errorf("open cached segment: %w", err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, fetchOffset, err
	}
	totalSize := stat.Size()

	// Scan forward for the first batch ending at or after fetchOffset: the
	// one bracketing it, or the segment's first batch after a hole. Linear;
	// bounded by a 1GB segment cap and
	// 4KB-16KB index intervals, so ~64K-250K batches max, sub-millisecond.
	header := make([]byte, 61)
	pos := int64(0)
	startPos := int64(-1)
	for pos < totalSize {
		if _, err := f.ReadAt(header, pos); err != nil && !errors.Is(err, io.EOF) {
			return nil, fetchOffset, fmt.Errorf("scan archived segment: %w", err)
		}
		h, err := storage.ParseBatchHeader(header)
		if err != nil {
			return nil, fetchOffset, fmt.Errorf("parse archived batch at %d: %w", pos, err)
		}
		if h.LastOffset() >= fetchOffset {
			startPos = pos
			break
		}
		pos += int64(h.TotalSize())
	}
	if startPos < 0 {
		return nil, fetchOffset, storage.ErrOffsetOutOfRange
	}

	// Read up to maxBytes from startPos, trimmed to a batch boundary.
	want := int64(maxBytes)
	if startPos+want > totalSize {
		want = totalSize - startPos
	}
	buf := make([]byte, want)
	n, err := f.ReadAt(buf, startPos)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fetchOffset, err
	}
	buf = buf[:n]

	// Trim to last full batch boundary. Same logic as storage.Log.
	trimmed := trimToBatchBoundary(buf)
	if len(trimmed) == 0 {
		// Less than one batch's worth requested, fall back to
		// returning the FIRST batch in full, matching storage.Log
		// semantics. Better an over-large response than zero data.
		hdr, perr := storage.ParseBatchHeader(buf)
		if perr != nil {
			return nil, fetchOffset, perr
		}
		need := int64(hdr.TotalSize())
		if startPos+need > totalSize {
			return nil, fetchOffset, storage.ErrOffsetOutOfRange
		}
		full := make([]byte, need)
		if _, err := f.ReadAt(full, startPos); err != nil {
			return nil, fetchOffset, err
		}
		return full, hdr.BaseOffset, nil
	}
	hdr, _ := storage.ParseBatchHeader(trimmed)
	return trimmed, hdr.BaseOffset, nil
}

// trimToBatchBoundary mirrors storage.trimToBatchBoundary; replicated
// here because it's an unexported helper. Trims `buf` to end on a
// batch boundary; returns nil if `buf` doesn't even hold one full
// batch.
func trimToBatchBoundary(buf []byte) []byte {
	pos := 0
	for pos < len(buf) {
		if pos+61 > len(buf) {
			break
		}
		h, err := storage.ParseBatchHeader(buf[pos : pos+61])
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

// Used to silence the unused-import linter for `os` if compilers
// complain. Stat is used above; this is a no-op.
var _ = os.Stat

// archivedSegmentFrom returns the archived segment of partition holding
// offset, or else the first archived segment after it.
func archivedSegmentFrom(entries []tiering.SegmentEntry, partition int32, offset int64) (tiering.SegmentEntry, bool) {
	var best tiering.SegmentEntry
	found := false
	for _, e := range entries {
		if e.Partition != partition || e.NextOffset <= offset {
			continue
		}
		if !found || e.BaseOffset < best.BaseOffset {
			best, found = e, true
		}
	}
	return best, found
}

// archiveStart returns the first offset of partition held in cold storage.
func archiveStart(entries []tiering.SegmentEntry, partition int32) (int64, bool) {
	var start int64
	found := false
	for _, e := range entries {
		if e.Partition == partition && (!found || e.BaseOffset < start) {
			start, found = e.BaseOffset, true
		}
	}
	return start, found
}

// archivedStart is archiveStart for a topic's partition, when cold storage
// is attached.
func (b *Broker) archivedStart(topic string, partition int32) (int64, bool) {
	if b.manifest == nil {
		return 0, false
	}
	return archiveStart(b.manifest.AllForTopic(topic), partition)
}
