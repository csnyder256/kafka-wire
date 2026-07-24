package tiering

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/csnyder256/kafka-wire/internal/objstore"
)

// Restorer fetches an archived segment from S3 into the local cache,
// suspending duplicate downloads of the same key (singleflight).
//
// Used by the Fetch path when the requested offset is below the local
// retention horizon and only present in S3.
//
// CRITICAL invariants enforced before any byte hits the cache:
//
//  1. SHA-256 of downloaded bytes matches manifest.SHA256.
//  2. HMAC signature matches our HMACKey (if configured).
//  3. (caller) The requesting tenant matches manifest.TenantID
//     before serving from cache.
//
// Any failure aborts the restore + drops the bytes (never written to
// cache); the chaos engine treats it as a tenant-isolation event.
type Restorer struct {
	bucket   string
	cache    *Cache
	manifest *Manifest
	store    objstore.Store
	metrics  Metrics
	hmacKey  []byte

	mu       sync.Mutex
	inFlight map[string]*restoreCall
}

type restoreCall struct {
	done chan struct{}
	err  error
}

// NewRestorer constructs a Restorer.
func NewRestorer(bucket string, cache *Cache, manifest *Manifest, store objstore.Store, mreg Metrics) *Restorer {
	return NewRestorerWithKey(bucket, cache, manifest, store, mreg, nil)
}

// NewRestorerWithKey is the multi-tenant variant. hmacKey enables
// signature verification on every restore.
func NewRestorerWithKey(bucket string, cache *Cache, manifest *Manifest, store objstore.Store, mreg Metrics, hmacKey []byte) *Restorer {
	return &Restorer{
		bucket:   bucket,
		cache:    cache,
		manifest: manifest,
		store:    store,
		metrics:  mreg,
		hmacKey:  hmacKey,
		inFlight: make(map[string]*restoreCall),
	}
}

// EntryFor returns the manifest entry for (topic, partition, baseOffset)
// : exposed so callers can verify tenant ownership BEFORE serving
// bytes from cache.
func (r *Restorer) EntryFor(topic string, partition int32, baseOffset int64) (SegmentEntry, bool) {
	return r.manifest.Lookup(topic, partition, baseOffset)
}

// Restore ensures the segment for (topic, partition, baseOffset) is
// available in the local cache. Blocks until the file is on disk or
// returns an error. Concurrent calls for the same segment dedupe.
func (r *Restorer) Restore(ctx context.Context, topic string, partition int32, baseOffset int64) error {
	if r.cache.Has(topic, partition, baseOffset) {
		return nil
	}
	entry, ok := r.manifest.Lookup(topic, partition, baseOffset)
	if !ok {
		return ErrNotArchived
	}

	r.mu.Lock()
	if rc, exists := r.inFlight[entry.S3Key]; exists {
		r.mu.Unlock()
		select {
		case <-rc.done:
			return rc.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	rc := &restoreCall{done: make(chan struct{})}
	r.inFlight[entry.S3Key] = rc
	r.mu.Unlock()

	err := r.doRestore(ctx, entry)
	rc.err = err
	close(rc.done)

	r.mu.Lock()
	delete(r.inFlight, entry.S3Key)
	r.mu.Unlock()

	return err
}

func (r *Restorer) doRestore(ctx context.Context, entry SegmentEntry) error {
	// Verify HMAC ownership signature BEFORE any S3 download. If the
	// manifest itself has been tampered with (storage corruption,
	// adversarial overwrite, anything), refuse to restore. This is
	// the chaos engine's strongest cross-tenant test surface.
	if !entry.VerifySignature(r.hmacKey) {
		return fmt.Errorf("manifest entry HMAC mismatch for %s, refusing restore", entry.S3Key)
	}

	// Bounded retry with exponential backoff: a transient S3 blip used
	// to fail the consumer's Fetch outright even though the very next
	// attempt would have succeeded. Integrity failures (sha mismatch)
	// also retry: they mean the DOWNLOAD was corrupt, not the object.
	backoff := 250 * time.Millisecond
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		if attempt > 1 {
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
			backoff *= 4
			slog.Info("archive.restore.retry",
				"key", entry.S3Key, "attempt", attempt, "prev_err", lastErr)
		}
		lastErr = r.downloadOnce(ctx, entry)
		if lastErr == nil {
			r.metrics.IncS3Restored()
			return nil
		}
		if ctx.Err() != nil {
			return lastErr
		}
	}
	return lastErr
}

func (r *Restorer) downloadOnce(ctx context.Context, entry SegmentEntry) error {
	body, err := r.store.Get(ctx, entry.S3Key)
	if err != nil {
		return fmt.Errorf("fetch archived segment %s: %w", entry.S3Key, err)
	}
	defer body.Close()

	// Stream straight to the cache file, hashing on the way (SHA-256 is
	// the second integrity layer beyond HMAC). Never buffered in RAM:
	// concurrent restores of large segments OOM'd the old io.ReadAll
	// path. Tenant ID is part of the cache path so cross-tenant cache
	// bleed is impossible by construction.
	n, err := r.cache.PutTenantStream(entry.TenantID, entry.Topic, entry.Partition, entry.BaseOffset, body, entry.SHA256)
	if err != nil {
		return fmt.Errorf("cache put: %w", err)
	}

	slog.Info("archive.restore.completed",
		"topic", entry.Topic, "partition", entry.Partition,
		"base_offset", entry.BaseOffset, "size_bytes", n,
		"sha256", entry.SHA256[:12], "tenant", entry.TenantID)
	return nil
}
