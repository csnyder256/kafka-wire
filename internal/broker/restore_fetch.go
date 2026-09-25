package broker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/csnyder256/kafka-wire/internal/storage"
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

	// Find the archived segment whose [BaseOffset, NextOffset)
	// contains fetchOffset.
	var hit bool
	var baseOffset int64
	var entryTenant string
	for _, e := range b.manifest.AllForTopic(topic) {
		if e.Partition != partition {
			continue
		}
		if fetchOffset >= e.BaseOffset && fetchOffset < e.NextOffset {
			baseOffset = e.BaseOffset
			entryTenant = e.TenantID
			hit = true
			break
		}
	}
	if !hit {
		return nil, fetchOffset, storage.ErrOffsetOutOfRange
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

	// Scan forward looking for the batch whose [BaseOffset, LastOffset]
	// brackets fetchOffset. Linear; bounded by a 1GB segment cap and
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
		if fetchOffset >= h.BaseOffset && fetchOffset <= h.LastOffset() {
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
