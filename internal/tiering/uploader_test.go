package tiering

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/csnyder256/kafka-wire/internal/objstore"
)

// scriptedStore wraps a real backend so a test can script the two calls the
// boot-time reconciler makes, without stubbing the whole contract. Wrapping
// rather than faking means the parts of the path that are not being scripted
// still run for real.
type scriptedStore struct {
	objstore.Store
	statErr    error
	statInfo   *objstore.ObjectInfo
	listParts  []objstore.Part
	listErr    error
	aborts     int
	statHits   int
	scriptList bool
}

func (s *scriptedStore) Stat(ctx context.Context, key string) (objstore.ObjectInfo, error) {
	s.statHits++
	if s.statErr != nil {
		return objstore.ObjectInfo{}, s.statErr
	}
	if s.statInfo != nil {
		return *s.statInfo, nil
	}
	return s.Store.Stat(ctx, key)
}

func (s *scriptedStore) ListParts(ctx context.Context, key, uploadID string) ([]objstore.Part, error) {
	if s.scriptList {
		return s.listParts, s.listErr
	}
	return s.Store.ListParts(ctx, key, uploadID)
}

func (s *scriptedStore) AbortMultipart(ctx context.Context, key, uploadID string) error {
	s.aborts++
	return s.Store.AbortMultipart(ctx, key, uploadID)
}

type nullMetrics struct{}

func (nullMetrics) IncS3Uploaded()     {}
func (nullMetrics) IncS3UploadFailed() {}
func (nullMetrics) IncS3Restored()     {}

func newScripted(t *testing.T) *scriptedStore {
	t.Helper()
	fs, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return &scriptedStore{Store: fs}
}

func pendingFixture(t *testing.T, dir string) (*Manifest, *PendingUpload) {
	t.Helper()
	m, err := OpenManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	p := &PendingUpload{
		Topic:      "orders.events",
		Partition:  0,
		BaseOffset: 5000,
		NextOffset: 6000,
		TenantID:   "t1",
		SHA256:     "cafebabe",
		S3Key:      "archive/orders.events/0/00000000000000005000.log",
		UploadID:   "upload-1",
		StartedAt:  time.Unix(1700000000, 0).UTC(),
	}
	if err := m.SetPending(p); err != nil {
		t.Fatal(err)
	}
	return m, p
}

// Crash landed AFTER the upload completed: the object exists, so the
// checkpoint must be adopted into the manifest and never aborted. Aborting,
// combined with a local file that retention has already deleted, would make
// the data unreachable.
func TestReconcileAdoptsCompletedUpload(t *testing.T) {
	m, p := pendingFixture(t, t.TempDir())
	st := newScripted(t)
	st.statInfo = &objstore.ObjectInfo{Key: p.S3Key, Size: 4096, Metadata: map[string]string{}}
	u := NewUploader(Config{HMACKey: []byte("k")}, st, m, nullMetrics{})

	u.reconcileOrAbort(context.Background(), p)

	if st.aborts != 0 {
		t.Fatalf("expected no abort, got %d", st.aborts)
	}
	entry, ok := m.Lookup(p.Topic, p.Partition, p.BaseOffset)
	if !ok {
		t.Fatal("expected the completed upload to be adopted into the manifest")
	}
	if entry.NextOffset != 6000 || entry.TenantID != "t1" || entry.SHA256 != "cafebabe" {
		t.Fatalf("adopted entry lost checkpoint fields: %+v", entry)
	}
	if entry.HMACSignature == "" {
		t.Fatal("expected an HMAC signature on the adopted entry")
	}
	if len(m.PendingAll()) != 0 {
		t.Fatal("the pending checkpoint should be cleared after adoption")
	}
}

// Crash landed before completion and nothing survives in the store: abort the
// stale upload and drop the checkpoint so the next sweep starts over.
func TestReconcileAbortsWhenNothingSurvives(t *testing.T) {
	m, p := pendingFixture(t, t.TempDir())
	st := newScripted(t)
	st.statErr = objstore.ErrNotFound
	st.scriptList, st.listErr = true, objstore.ErrNotFound
	u := NewUploader(Config{}, st, m, nullMetrics{})

	u.reconcileOrAbort(context.Background(), p)

	if st.aborts != 1 {
		t.Fatalf("expected exactly 1 abort, got %d", st.aborts)
	}
	if _, ok := m.Lookup(p.Topic, p.Partition, p.BaseOffset); ok {
		t.Fatal("a missing object must never be adopted")
	}
	if len(m.PendingAll()) != 0 {
		t.Fatal("the stale checkpoint should be dropped")
	}
}

// Crash landed before completion but parts survive in the store: keep the
// checkpoint so the next sweep resumes instead of re-uploading. This is the
// case the original implementation could not express at all.
func TestReconcileKeepsResumableUpload(t *testing.T) {
	m, p := pendingFixture(t, t.TempDir())
	st := newScripted(t)
	st.statErr = objstore.ErrNotFound
	st.scriptList = true
	st.listParts = []objstore.Part{{Number: 1, ETag: "e1", Size: objstore.MinPartSize}}
	u := NewUploader(Config{}, st, m, nullMetrics{})

	u.reconcileOrAbort(context.Background(), p)

	if st.aborts != 0 {
		t.Fatalf("an upload with surviving parts must not be aborted, got %d aborts", st.aborts)
	}
	if len(m.PendingAll()) != 1 {
		t.Fatal("the checkpoint must survive so the next sweep can resume it")
	}
}

// A lookup that failed for an unknown reason must decide nothing. Acting on
// unreliable information is how archives get dropped.
func TestReconcileKeepsPendingOnTransientError(t *testing.T) {
	m, p := pendingFixture(t, t.TempDir())
	st := newScripted(t)
	st.statErr = errors.New("connection reset by peer")
	u := NewUploader(Config{}, st, m, nullMetrics{})

	u.reconcileOrAbort(context.Background(), p)

	if st.aborts != 0 {
		t.Fatalf("expected no abort on a transient error, got %d", st.aborts)
	}
	if len(m.PendingAll()) != 1 {
		t.Fatal("the checkpoint must survive a transient lookup failure")
	}
}

// ---------------------------------------------------------------------------
// End to end, against a real backend
// ---------------------------------------------------------------------------

type fakeSegment struct {
	topic     string
	partition int32
	base      int64
	next      int64
	path      string
	size      int64
	created   time.Time
}

func (f fakeSegment) Topic() string         { return f.topic }
func (f fakeSegment) Partition() int32      { return f.partition }
func (f fakeSegment) BaseOffset() int64     { return f.base }
func (f fakeSegment) NextOffset() int64     { return f.next }
func (f fakeSegment) Size() int64           { return f.size }
func (f fakeSegment) LogPath() string       { return f.path }
func (f fakeSegment) CreatedAt() time.Time  { return f.created }

func writeSegment(t *testing.T, dir string, n int) (fakeSegment, []byte) {
	t.Helper()
	payload := make([]byte, n)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "00000000000000000000.log")
	if err := os.WriteFile(path, payload, 0o640); err != nil {
		t.Fatal(err)
	}
	return fakeSegment{
		topic: "orders.events", partition: 0, base: 0, next: 100,
		path: path, size: int64(n), created: time.Now().Add(-2 * time.Hour),
	}, payload
}

func TestUploadOneRoundTripsThroughRealBackend(t *testing.T) {
	dir := t.TempDir()
	seg, payload := writeSegment(t, dir, objstore.MinPartSize*2+777)

	store, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m, err := OpenManifest(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	u := NewUploader(Config{Prefix: "archive/", PartSize: objstore.MinPartSize}, store, m, nullMetrics{})

	if err := u.uploadOne(context.Background(), seg); err != nil {
		t.Fatalf("uploadOne: %v", err)
	}

	entry, ok := m.Lookup(seg.topic, seg.partition, seg.base)
	if !ok {
		t.Fatal("no manifest entry after a successful upload")
	}
	if entry.SizeBytes != int64(len(payload)) {
		t.Fatalf("manifest size %d, want %d", entry.SizeBytes, len(payload))
	}
	sum := sha256.Sum256(payload)
	if entry.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatal("manifest digest does not match the segment bytes")
	}
	if len(m.PendingAll()) != 0 {
		t.Fatal("the pending checkpoint should be cleared once the upload completes")
	}

	rc, err := store.Get(context.Background(), entry.S3Key)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, payload) {
		t.Fatal("the archived object does not match the segment byte for byte")
	}
}

// The behavior the private implementation claimed but never had: an upload
// interrupted after some parts were accepted resumes from the first missing
// part instead of re-sending the whole segment.
func TestUploadResumesFromCheckpoint(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	seg, payload := writeSegment(t, dir, objstore.MinPartSize*3)

	store, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m, err := OpenManifest(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Prefix: "archive/", PartSize: objstore.MinPartSize}
	key := SegmentKeyTenant(cfg.Prefix, "", seg.topic, seg.partition, seg.base)

	// Simulate a process that uploaded two of three parts and then died,
	// leaving a checkpoint behind.
	sum := sha256.Sum256(payload)
	uploadID, err := store.CreateMultipart(ctx, key, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 2; i++ {
		chunk := payload[(i-1)*objstore.MinPartSize : i*objstore.MinPartSize]
		if _, err := store.UploadPart(ctx, key, uploadID, i, bytes.NewReader(chunk), int64(len(chunk))); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.SetPending(&PendingUpload{
		Topic: seg.topic, Partition: seg.partition, BaseOffset: seg.base, NextOffset: seg.next,
		SHA256: hex.EncodeToString(sum[:]), S3Key: key, UploadID: uploadID, StartedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	// A counting wrapper proves only the missing part is sent.
	counter := &countingStore{Store: store}
	u := NewUploader(cfg, counter, m, nullMetrics{})
	if err := u.uploadOne(ctx, seg); err != nil {
		t.Fatalf("uploadOne after interruption: %v", err)
	}

	if counter.creates != 0 {
		t.Errorf("resume started a new multipart upload (%d creates); it should reuse the checkpointed one", counter.creates)
	}
	if counter.partsSent != 1 {
		t.Errorf("resume sent %d parts, want exactly 1 (the only one missing)", counter.partsSent)
	}

	entry, ok := m.Lookup(seg.topic, seg.partition, seg.base)
	if !ok {
		t.Fatal("no manifest entry after the resumed upload")
	}
	if entry.SizeBytes != int64(len(payload)) {
		t.Fatalf("resumed upload recorded %d bytes, want %d", entry.SizeBytes, len(payload))
	}
	rc, err := store.Get(ctx, entry.S3Key)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, payload) {
		t.Fatal("the resumed upload did not reassemble to the original segment")
	}
}

type countingStore struct {
	objstore.Store
	creates   int
	partsSent int
}

func (c *countingStore) CreateMultipart(ctx context.Context, key string, meta map[string]string) (string, error) {
	c.creates++
	return c.Store.CreateMultipart(ctx, key, meta)
}

func (c *countingStore) UploadPart(ctx context.Context, key, uploadID string, n int, r io.Reader, size int64) (objstore.Part, error) {
	c.partsSent++
	return c.Store.UploadPart(ctx, key, uploadID, n, r, size)
}

// A checkpoint whose digest no longer matches the segment on disk must be
// ignored. Resuming onto changed bytes would produce a corrupt archive that
// still passed its own manifest check.
func TestResumeRefusedWhenSegmentChanged(t *testing.T) {
	ctx := context.Background()
	seg, _ := writeSegment(t, t.TempDir(), objstore.MinPartSize*2)

	store, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m, err := OpenManifest(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Prefix: "archive/", PartSize: objstore.MinPartSize}
	key := SegmentKeyTenant(cfg.Prefix, "", seg.topic, seg.partition, seg.base)
	uploadID, _ := store.CreateMultipart(ctx, key, nil)
	if err := m.SetPending(&PendingUpload{
		Topic: seg.topic, Partition: seg.partition, BaseOffset: seg.base,
		SHA256: "0000deadbeef", S3Key: key, UploadID: uploadID, StartedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	counter := &countingStore{Store: store}
	u := NewUploader(cfg, counter, m, nullMetrics{})
	if err := u.uploadOne(ctx, seg); err != nil {
		t.Fatalf("uploadOne: %v", err)
	}
	if counter.creates != 1 {
		t.Errorf("a stale checkpoint must be discarded and a fresh upload started; creates=%d", counter.creates)
	}
	if counter.partsSent != 2 {
		t.Errorf("the whole segment should be re-sent; parts=%d want 2", counter.partsSent)
	}
}
