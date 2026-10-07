package tiering

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"testing"

	"github.com/csnyder256/kafka-wire/internal/objstore"
)

// countingGetStore counts Get calls so a test can prove a cached segment is
// served from the cache instead of re-downloaded from cold storage.
type countingGetStore struct {
	objstore.Store
	gets int
}

func (c *countingGetStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	c.gets++
	return c.Store.Get(ctx, key)
}

// archiveObject writes payload to the backend under key as one part.
func archiveObject(t *testing.T, st objstore.Store, key string, payload []byte) {
	t.Helper()
	ctx := context.Background()
	id, err := st.CreateMultipart(ctx, key, nil)
	if err != nil {
		t.Fatal(err)
	}
	p, err := st.UploadPart(ctx, key, id, 1, bytes.NewReader(payload), int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CompleteMultipart(ctx, key, id, []objstore.Part{p}); err != nil {
		t.Fatal(err)
	}
}

// A tenant-scoped segment is cached under its tenant path. The restore path
// used to ask the cache for the *shared* path, which never exists for a
// tenant topic, so every fetch re-downloaded the whole segment from cold
// storage even though it was already on local disk.
func TestRestoreServesTenantSegmentsFromTheCache(t *testing.T) {
	ctx := context.Background()
	payload := []byte("an archived segment body")
	sum := sha256.Sum256(payload)

	backend, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	key := SegmentKeyTenant("archive/", "t1", "orders.events", 0, 5000)
	archiveObject(t, backend, key, payload)

	m, err := OpenManifest(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := m.AddCompleted(SegmentEntry{
		Topic: "orders.events", Partition: 0, BaseOffset: 5000, NextOffset: 6000,
		SizeBytes: int64(len(payload)), SHA256: hex.EncodeToString(sum[:]),
		S3Key: key, TenantID: "t1",
	}); err != nil {
		t.Fatal(err)
	}

	cache, err := NewCache(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	counting := &countingGetStore{Store: backend}
	r := NewRestorer("", cache, m, counting, nullMetrics{})

	if err := r.Restore(ctx, "orders.events", 0, 5000); err != nil {
		t.Fatalf("first restore: %v", err)
	}
	if err := r.Restore(ctx, "orders.events", 0, 5000); err != nil {
		t.Fatalf("second restore: %v", err)
	}
	if counting.gets != 1 {
		t.Fatalf("cold storage was read %d times; a cached tenant segment must be served from the cache after the first restore", counting.gets)
	}
	if !cache.HasTenant("t1", "orders.events", 0, 5000) {
		t.Fatal("the restored segment is not at its tenant-scoped cache path")
	}
}

// The legacy shared path must keep working: a segment with no tenant is
// cached under the shared path and served from there after the first restore.
func TestRestoreServesSharedSegmentsFromTheCache(t *testing.T) {
	ctx := context.Background()
	payload := []byte("a legacy shared segment")
	sum := sha256.Sum256(payload)

	backend, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	key := SegmentKey("archive/", "orders.events", 0, 5000)
	archiveObject(t, backend, key, payload)

	m, err := OpenManifest(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := m.AddCompleted(SegmentEntry{
		Topic: "orders.events", Partition: 0, BaseOffset: 5000, NextOffset: 6000,
		SizeBytes: int64(len(payload)), SHA256: hex.EncodeToString(sum[:]),
		S3Key: key,
	}); err != nil {
		t.Fatal(err)
	}

	cache, err := NewCache(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	counting := &countingGetStore{Store: backend}
	r := NewRestorer("", cache, m, counting, nullMetrics{})

	if err := r.Restore(ctx, "orders.events", 0, 5000); err != nil {
		t.Fatalf("first restore: %v", err)
	}
	if err := r.Restore(ctx, "orders.events", 0, 5000); err != nil {
		t.Fatalf("second restore: %v", err)
	}
	if counting.gets != 1 {
		t.Fatalf("cold storage was read %d times; a cached shared segment must be served from the cache after the first restore", counting.gets)
	}
}
