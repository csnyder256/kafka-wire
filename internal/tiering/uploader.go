package tiering

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/csnyder256/kafka-wire/internal/objstore"
)

// Config tunes the uploader.
type Config struct {
	Bucket         string
	Prefix         string        // e.g. "kafka-wire-archive/"
	ArchiveAge     time.Duration // sealed segment must be at least this old
	LocalRetention time.Duration // delete local copy after this once archived
	PartSize       int64         // multipart part size (default 5 MiB)
	Tick           time.Duration // sweep interval
	Concurrency    int           // max parallel uploads

	// HMACKey signs each segment's ownership tuple. Required for
	// tenant-scoped archives; ignored for legacy shared archives
	// (TenantResolver returning empty for every segment).
	HMACKey []byte

	// TenantResolver maps a (topic, partition) to the tenant_id that
	// owns the topic at the moment of archive. Returning an empty
	// string means the topic is shared/legacy (path falls back to
	// the non-tenant layout).
	TenantResolver func(topic string, partition int32) string
}

// Store is re-exported for callers that build an Uploader. The uploader
// knows nothing about any particular vendor: everything it needs from cold
// storage is the objstore.Store contract.
type Store = objstore.Store

// SegmentSource is what the uploader needs from each sealed segment.
// Implemented by *storage.Segment indirectly via SegmentDescriptor.
type SegmentSource interface {
	Topic() string
	Partition() int32
	BaseOffset() int64
	NextOffset() int64
	Size() int64
	LogPath() string
	CreatedAt() time.Time
}

// LogProvider yields per-partition logs to scan for archivable
// segments. broker.TopicRegistry implements this.
type LogProvider interface {
	AllSealedSegments() []SegmentSource
}

// Metrics is the minimal interface the s3 package needs. Both the
// uploader and the restorer share it; the restorer additionally
// uses IncS3Restored.
type Metrics interface {
	IncS3Uploaded()
	IncS3UploadFailed()
	IncS3Restored()
}

// Uploader walks every topic's sealed segments and uploads them to
// S3 with multipart upload + sha256 verification + checkpoint
// resumption.
type Uploader struct {
	cfg      Config
	store    objstore.Store
	manifest *Manifest
	metrics  Metrics

	stopOnce sync.Once
	stop     chan struct{}
}

// NewUploader constructs an Uploader. Defaults applied for any
// zero-valued config field.
func NewUploader(cfg Config, store objstore.Store, manifest *Manifest, mreg Metrics) *Uploader {
	if cfg.PartSize <= 0 {
		cfg.PartSize = 5 * 1024 * 1024
	}
	if cfg.Tick <= 0 {
		cfg.Tick = 30 * time.Second
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 2
	}
	if cfg.ArchiveAge <= 0 {
		cfg.ArchiveAge = time.Hour
	}
	return &Uploader{
		cfg:      cfg,
		store:    store,
		manifest: manifest,
		metrics:  mreg,
		stop:     make(chan struct{}),
	}
}

// Run drives the uploader loop. Cancel by calling Stop or ctx done.
func (u *Uploader) Run(ctx context.Context, provider LogProvider) {
	if u.cfg.Bucket == "" {
		slog.Info("s3.uploader.disabled", "reason", "no bucket configured")
		return
	}

	// Reconcile pending uploads first. A crash between
	// CompleteMultipartUpload and the manifest write leaves a COMPLETED
	// object in S3 with its pending checkpoint still on disk. Blindly
	// aborting (the old behavior) re-uploads the segment at best; if
	// retention already deleted the local file, the archived object is
	// unreachable through the manifest, which is data loss. HEAD the key
	// and adopt the completed object into the manifest when it exists.
	for _, p := range u.manifest.PendingAll() {
		select {
		case <-ctx.Done():
			return
		default:
		}
		u.reconcileOrAbort(ctx, p)
	}

	tick := time.NewTicker(u.cfg.Tick)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-u.stop:
			return
		case <-tick.C:
			u.sweep(ctx, provider)
		}
	}
}

// Stop signals the uploader to exit.
func (u *Uploader) Stop() {
	u.stopOnce.Do(func() { close(u.stop) })
}

func (u *Uploader) sweep(ctx context.Context, provider LogProvider) {
	now := time.Now()
	sem := make(chan struct{}, u.cfg.Concurrency)
	var wg sync.WaitGroup
	for _, seg := range provider.AllSealedSegments() {
		if now.Sub(seg.CreatedAt()) < u.cfg.ArchiveAge {
			continue
		}
		// Skip if already archived.
		if _, ok := u.manifest.Lookup(seg.Topic(), seg.Partition(), seg.BaseOffset()); ok {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(s SegmentSource) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := u.uploadOne(ctx, s); err != nil {
				slog.Warn("s3.uploader.upload_failed",
					"topic", s.Topic(), "partition", s.Partition(),
					"base_offset", s.BaseOffset(), "err", err)
				u.metrics.IncS3UploadFailed()
			}
		}(seg)
	}
	wg.Wait()
}

func (u *Uploader) uploadOne(ctx context.Context, seg SegmentSource) error {
	tenantID := ""
	if u.cfg.TenantResolver != nil {
		tenantID = u.cfg.TenantResolver(seg.Topic(), seg.Partition())
	}
	key := SegmentKeyTenant(u.cfg.Prefix, tenantID, seg.Topic(), seg.Partition(), seg.BaseOffset())

	f, err := os.Open(seg.LogPath())
	if err != nil {
		return fmt.Errorf("open segment: %w", err)
	}
	defer f.Close()

	// Hash the whole segment first. The digest is stamped into the object
	// metadata and the manifest, and the restore path refuses any segment
	// whose bytes do not match it.
	hasher := sha256.New()
	if _, err := io.Copy(hasher, f); err != nil {
		return fmt.Errorf("hash: %w", err)
	}
	sha := hex.EncodeToString(hasher.Sum(nil))
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}

	// Resume an upload that was interrupted.
	//
	// The checkpoint written after every part is only worth writing if
	// something reads it back, so this is where it is read. If the store
	// still knows the upload id and still holds parts, skip straight to the
	// first part it is missing instead of re-sending from byte zero. On a
	// 1 GiB segment over a slow link that is the difference between a
	// restart costing seconds and costing the entire transfer.
	var (
		uploadID string
		parts    []objstore.Part
		pending  *PendingUpload
	)
	if prev := u.manifest.GetPending(key); prev != nil && prev.UploadID != "" && prev.SHA256 == sha {
		existing, lerr := u.store.ListParts(ctx, key, prev.UploadID)
		switch {
		case lerr == nil && len(existing) > 0:
			// Trust only a contiguous 1..n prefix of uniformly sized parts.
			// A gap would silently reassemble into a corrupt object, and a
			// short non-final part is rejected at completion by most stores.
			for i, ep := range existing {
				if ep.Number != i+1 || (i < len(existing)-1 && ep.Size != u.cfg.PartSize) {
					existing = existing[:i]
					break
				}
			}
			if len(existing) > 0 {
				uploadID, parts, pending = prev.UploadID, existing, prev
				pending.Parts = pending.Parts[:0]
				for _, ep := range existing {
					pending.Parts = append(pending.Parts, PendingUploadPart{
						PartNumber: int32(ep.Number), ETag: ep.ETag, SizeBytes: ep.Size,
					})
				}
				resumeAt := int64(len(existing)) * u.cfg.PartSize
				if _, err := f.Seek(resumeAt, io.SeekStart); err != nil {
					return err
				}
				slog.Info("archive.upload.resumed",
					"key", key, "parts_already_stored", len(existing), "resume_at_bytes", resumeAt)
			}
		case lerr != nil && !errors.Is(lerr, objstore.ErrNotFound):
			slog.Warn("archive.upload.list_parts_failed", "err", lerr, "key", key,
				"action", "starting a fresh upload")
		}
	}

	if uploadID == "" {
		uploadID, err = u.store.CreateMultipart(ctx, key, map[string]string{
			"sha256":     sha,
			"topic":      seg.Topic(),
			"partition":  fmt.Sprintf("%d", seg.Partition()),
			"baseoffset": fmt.Sprintf("%d", seg.BaseOffset()),
		})
		if err != nil {
			return fmt.Errorf("create multipart upload: %w", err)
		}
		pending = &PendingUpload{
			Topic:      seg.Topic(),
			Partition:  seg.Partition(),
			BaseOffset: seg.BaseOffset(),
			NextOffset: seg.NextOffset(),
			TenantID:   tenantID,
			SHA256:     sha,
			S3Key:      key,
			UploadID:   uploadID,
			StartedAt:  time.Now(),
		}
	}
	if err := u.manifest.SetPending(pending); err != nil {
		return fmt.Errorf("checkpoint pending upload: %w", err)
	}

	// Stream the remaining parts. Every part except the last is exactly
	// PartSize bytes, which is what Cloudflare R2 requires and what makes
	// the resume seek above arithmetic rather than bookkeeping.
	partNum := len(parts) + 1
	totalSize := int64(len(parts)) * u.cfg.PartSize
	buf := make([]byte, u.cfg.PartSize)
	for {
		n, rerr := io.ReadFull(f, buf)
		if rerr != nil && !errors.Is(rerr, io.EOF) && !errors.Is(rerr, io.ErrUnexpectedEOF) {
			u.tryAbort(ctx, key, uploadID)
			return fmt.Errorf("read segment: %w", rerr)
		}
		if n == 0 {
			break
		}

		part, perr := u.store.UploadPart(ctx, key, uploadID, partNum, bytes.NewReader(buf[:n]), int64(n))
		if perr != nil {
			u.tryAbort(ctx, key, uploadID)
			return fmt.Errorf("upload part %d: %w", partNum, perr)
		}
		parts = append(parts, part)
		pending.Parts = append(pending.Parts, PendingUploadPart{
			PartNumber: int32(partNum), ETag: part.ETag, SizeBytes: int64(n),
		})
		// Checkpoint after every accepted part. This is the write whose only
		// purpose is to be read back by the resume path above.
		_ = u.manifest.SetPending(pending)

		totalSize += int64(n)
		partNum++

		if errors.Is(rerr, io.EOF) || errors.Is(rerr, io.ErrUnexpectedEOF) {
			break
		}
	}

	if err := u.store.CompleteMultipart(ctx, key, uploadID, parts); err != nil {
		u.tryAbort(ctx, key, uploadID)
		return fmt.Errorf("complete multipart upload: %w", err)
	}

	entry := SegmentEntry{
		Topic:      seg.Topic(),
		Partition:  seg.Partition(),
		BaseOffset: seg.BaseOffset(),
		NextOffset: seg.NextOffset(),
		SizeBytes:  totalSize,
		SHA256:     sha,
		S3Key:      key,
		UploadedAt: time.Now(),
		TenantID:   tenantID,
	}
	if len(u.cfg.HMACKey) > 0 {
		entry.HMACSignature = SegmentSignature(u.cfg.HMACKey, tenantID, seg.Topic(), seg.Partition(), seg.BaseOffset(), sha)
	}
	if err := u.manifest.AddCompleted(entry); err != nil {
		return fmt.Errorf("record archived segment: %w", err)
	}
	u.metrics.IncS3Uploaded()
	slog.Info("archive.upload.completed",
		"topic", seg.Topic(), "partition", seg.Partition(),
		"base_offset", seg.BaseOffset(), "size_bytes", totalSize,
		"parts", len(parts), "sha256", sha[:12], "backend", u.store.Name())
	return nil
}

func (u *Uploader) tryAbort(ctx context.Context, key, uploadID string) {
	if err := u.store.AbortMultipart(ctx, key, uploadID); err != nil {
		slog.Warn("archive.upload.abort_failed", "err", err, "key", key)
	}
	_ = u.manifest.AbortPending(key)
}

func (u *Uploader) abortStale(ctx context.Context, p *PendingUpload) error {
	// Best effort. A store that already garbage-collected the upload reports
	// it as missing, which is the outcome we wanted anyway.
	_ = u.store.AbortMultipart(ctx, p.S3Key, p.UploadID)
	return u.manifest.AbortPending(p.S3Key)
}

// reconcileOrAbort resolves one pending checkpoint at boot.
//
//   - the object exists at the key: the upload completed just before the
//     crash, so adopt it into the manifest rather than re-uploading.
//   - the object is absent but the store still holds parts: leave the
//     checkpoint alone, and the next sweep resumes it.
//   - the object is absent and no parts survive: abort, and the next sweep
//     starts over from local disk.
//   - the lookup failed for any other reason: keep the checkpoint. Deciding
//     on unreliable information is how archives get dropped.
func (u *Uploader) reconcileOrAbort(ctx context.Context, p *PendingUpload) {
	info, err := u.store.Stat(ctx, p.S3Key)
	if err != nil {
		if !errors.Is(err, objstore.ErrNotFound) {
			slog.Warn("archive.reconcile.stat_failed", "err", err, "key", p.S3Key,
				"action", "keeping the checkpoint for the next boot")
			return
		}
		if parts, lerr := u.store.ListParts(ctx, p.S3Key, p.UploadID); lerr == nil && len(parts) > 0 {
			slog.Info("archive.reconcile.resumable", "key", p.S3Key, "parts_already_stored", len(parts))
			return
		}
		if aerr := u.abortStale(ctx, p); aerr != nil {
			slog.Warn("archive.reconcile.abort_failed", "err", aerr, "key", p.S3Key)
		}
		return
	}

	sha := p.SHA256
	if sha == "" {
		sha = info.Metadata["sha256"]
	}
	entry := SegmentEntry{
		Topic:      p.Topic,
		Partition:  p.Partition,
		BaseOffset: p.BaseOffset,
		NextOffset: p.NextOffset,
		SizeBytes:  info.Size,
		SHA256:     sha,
		S3Key:      p.S3Key,
		UploadedAt: p.StartedAt,
		TenantID:   p.TenantID,
	}
	if len(u.cfg.HMACKey) > 0 && sha != "" {
		entry.HMACSignature = SegmentSignature(u.cfg.HMACKey, p.TenantID, p.Topic, p.Partition, p.BaseOffset, sha)
	}
	if err := u.manifest.AddCompleted(entry); err != nil {
		slog.Warn("archive.reconcile.adopt_failed", "err", err, "key", p.S3Key)
		return
	}
	_ = u.manifest.AbortPending(p.S3Key)
	slog.Info("archive.reconcile.adopted_completed_upload",
		"topic", p.Topic, "partition", p.Partition, "base_offset", p.BaseOffset, "key", p.S3Key)
}
