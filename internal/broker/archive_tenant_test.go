package broker

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/csnyder256/kafka-wire/internal/metrics"
	"github.com/csnyder256/kafka-wire/internal/objstore"
	"github.com/csnyder256/kafka-wire/internal/storage"
	"github.com/csnyder256/kafka-wire/internal/tiering"
)

// The cold-storage tier archives a sealed segment under a key that carries the
// tenant owning the topic. The restore path refuses any manifest entry whose
// TenantID does not match the requesting principal's, so a segment archived
// without its tenant can never be served back to the tenant that owns it: the
// fetch fails with TOPIC_AUTHORIZATION_FAILED instead of returning the records.
//
// That is what the broker did. The uploader had a TenantResolver field and the
// broker had a TenantOfTopic lookup, and nothing joined them, so every segment
// was uploaded with an empty TenantID to the shared key layout. The tests below
// drive the wiring the server uses (Broker.AttachArchive) and assert both
// halves of the contract: the archive records the owner tenant, and the tenant
// can read its own archived segment while another tenant still cannot.

// fakeSegment is a tiering.SegmentSource over a real segment file on disk. The
// uploader only needs the file's path and its offset range, so a sealed-segment
// source can be described directly instead of rolling a real one.
type fakeSegment struct {
	topic     string
	partition int32
	base      int64
	next      int64
	path      string
	created   time.Time
}

func (f fakeSegment) Topic() string        { return f.topic }
func (f fakeSegment) Partition() int32     { return f.partition }
func (f fakeSegment) BaseOffset() int64    { return f.base }
func (f fakeSegment) NextOffset() int64    { return f.next }
func (f fakeSegment) CreatedAt() time.Time { return f.created }
func (f fakeSegment) LogPath() string      { return f.path }

func (f fakeSegment) Size() int64 {
	fi, err := os.Stat(f.path)
	if err != nil {
		return 0
	}
	return fi.Size()
}

type fakeProvider struct{ segs []tiering.SegmentSource }

func (p fakeProvider) AllSealedSegments() []tiering.SegmentSource { return p.segs }

// tenantBatch builds one valid v2 record batch holding a single record. The
// append path validates the magic byte and the CRC, so the batch has to be
// genuinely valid; only the header is ever parsed.
func tenantBatch() []byte {
	tail := []byte{0x01, 0x02, 0x03, 0x04}
	afterBatchLength := 61 + len(tail) - 12
	buf := make([]byte, 12+afterBatchLength)
	binary.BigEndian.PutUint64(buf[0:8], 0)                         // BaseOffset
	binary.BigEndian.PutUint32(buf[8:12], uint32(afterBatchLength)) // BatchLength
	buf[16] = 2                                                     // Magic
	binary.BigEndian.PutUint16(buf[21:23], 0)                       // Attributes
	binary.BigEndian.PutUint32(buf[23:27], 0)                       // LastOffsetDelta (1 record)
	binary.BigEndian.PutUint64(buf[27:35], 1714234567000)           // FirstTimestamp
	binary.BigEndian.PutUint64(buf[35:43], 1714234567010)           // MaxTimestamp
	binary.BigEndian.PutUint64(buf[43:51], ^uint64(0))              // ProducerID: unset
	binary.BigEndian.PutUint16(buf[51:53], 0xFFFF)                  // ProducerEpoch: unset
	binary.BigEndian.PutUint32(buf[53:57], 0xFFFFFFFF)              // BaseSequence: unset
	binary.BigEndian.PutUint32(buf[57:61], 1)                       // RecordCount
	copy(buf[61:], tail)
	binary.BigEndian.PutUint32(buf[17:21], storage.CRC32C(buf[21:]))
	return buf
}

// archiveFixture brings up a broker with one topic holding one appended batch,
// plus an fs cold-storage tier wired through the same Broker.AttachArchive the
// server calls. ownerTenant is the tenant the topic belongs to; empty means a
// shared/platform topic.
type archiveFixture struct {
	topic    string
	brk      *Broker
	manifest *tiering.Manifest
	segPath  string
	batch    []byte
	uploader *tiering.Uploader
}

func newArchiveFixture(t *testing.T, topic, ownerTenant string) *archiveFixture {
	t.Helper()
	dir := t.TempDir()

	store, err := storage.Open(storage.Config{DataDir: dir, FsyncMode: storage.FsyncNone})
	if err != nil {
		t.Fatalf("opening storage: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	brk := New(Config{
		BrokerID:       1,
		ClusterID:      "test-cluster",
		AdvertisedHost: "broker.example",
		AdvertisedPort: 9092,
		DataDir:        dir,
		Storage:        store,
		Metrics:        metrics.New(),
	})
	if err := brk.LoadState(); err != nil {
		t.Fatalf("loading broker state: %v", err)
	}
	t.Cleanup(func() { _ = brk.Drain(context.Background()) })

	// A tenant principal's first Produce auto-creates the topic with the tenant
	// as its owner; a shared topic is created the ordinary way. Either way the
	// broker records a binding the uploader is supposed to archive.
	var tp *Topic
	if ownerTenant != "" {
		tp, err = brk.EnsureTopicForTenant(topic, 1, ownerTenant)
	} else {
		tp, err = brk.CreateTopic(topic, 1, 1)
	}
	if err != nil {
		t.Fatalf("creating topic %q: %v", topic, err)
	}

	batch := tenantBatch()
	if _, err := brk.AppendBatchesAuthorized(topic, 0, [][]byte{batch}, "", ownerTenant); err != nil {
		t.Fatalf("appending to %q: %v", topic, err)
	}

	log, ok := tp.Partition(0)
	if !ok {
		t.Fatal("partition 0 is missing")
	}
	segs := log.AllSegments()
	if len(segs) != 1 {
		t.Fatalf("expected exactly one segment, got %d", len(segs))
	}

	backend, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatalf("opening the fs archive: %v", err)
	}
	manifest, err := tiering.OpenManifest(dir)
	if err != nil {
		t.Fatalf("opening the manifest: %v", err)
	}
	cache, err := tiering.NewCache(filepath.Join(dir, "cache"), 1<<20)
	if err != nil {
		t.Fatalf("opening the restore cache: %v", err)
	}

	uploader := brk.AttachArchive(backend, manifest, cache, tiering.Config{
		Prefix:      "kafka-wire/",
		ArchiveAge:  time.Nanosecond,
		PartSize:    1 << 20,
		Tick:        10 * time.Millisecond,
		Concurrency: 1,
	}, metrics.New())

	return &archiveFixture{
		topic:    topic,
		brk:      brk,
		manifest: manifest,
		segPath:  segs[0].LogPath(),
		batch:    batch,
		uploader: uploader,
	}
}

// archive runs the uploader until the manifest records the segment.
func (f *archiveFixture) archive(t *testing.T) tiering.SegmentEntry {
	t.Helper()
	const partition, base = 0, 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.uploader.Run(ctx, fakeProvider{segs: []tiering.SegmentSource{
		fakeSegment{
			topic:     f.topic,
			partition: partition,
			base:      base,
			next:      base + 1,
			path:      f.segPath,
			created:   time.Now().Add(-time.Hour),
		},
	}})

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if e, ok := f.manifest.Lookup(f.topic, partition, base); ok {
			return e
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("the uploader never archived %s/%d at base offset %d", f.topic, partition, base)
	return tiering.SegmentEntry{}
}

func TestArchivedSegmentsCarryTheTopicOwnerTenant(t *testing.T) {
	f := newArchiveFixture(t, "orders", "acme")
	entry := f.archive(t)

	if entry.TenantID != "acme" {
		t.Errorf("archived entry tenant = %q, want %q; a segment archived without its tenant can never be served back to the tenant that owns it",
			entry.TenantID, "acme")
	}
	if !strings.Contains(entry.S3Key, "/tenants/acme/") {
		t.Errorf("archive key %q does not place the segment under the owning tenant's subtree", entry.S3Key)
	}
}

// The user-visible half: a tenant principal reads back the segment archived for
// its topic, and a different tenant is still refused.
func TestTenantPrincipalCanReadItsOwnArchivedSegment(t *testing.T) {
	f := newArchiveFixture(t, "orders", "acme")
	entry := f.archive(t)

	got, firstOffset, err := f.brk.fetchFromArchive(context.Background(), f.topic, 0, 0, 1<<20, "acme")
	if err != nil {
		t.Fatalf("the tenant that owns %q could not read its own archived segment (%s): %v", f.topic, entry.S3Key, err)
	}
	if firstOffset != 0 {
		t.Errorf("archived read started at offset %d, want 0", firstOffset)
	}
	if len(got) != len(f.batch) {
		t.Errorf("archived read returned %d bytes, want the %d-byte batch that was appended", len(got), len(f.batch))
	}

	if _, _, err := f.brk.fetchFromArchive(context.Background(), f.topic, 0, 0, 1<<20, "globex"); !errors.Is(err, ErrUnauthorizedTopic) {
		t.Errorf("a different tenant read another tenant's archived segment: err = %v, want %v", err, ErrUnauthorizedTopic)
	}
}

// A topic with no owner tenant stays on the shared key layout: the resolver is
// the broker's own ownership lookup, so it must not invent a tenant.
func TestAttachArchiveKeepsSharedTopicsOnTheSharedLayout(t *testing.T) {
	f := newArchiveFixture(t, "shared", "")
	entry := f.archive(t)

	if entry.TenantID != "" {
		t.Errorf("shared topic archived with tenant %q, want no tenant", entry.TenantID)
	}
	if strings.Contains(entry.S3Key, "/tenants/") {
		t.Errorf("shared topic was filed under a tenant subtree: %s", entry.S3Key)
	}
	if want := "kafka-wire/shared/0/00000000000000000000.log"; entry.S3Key != want {
		t.Errorf("shared archive key = %q, want %q", entry.S3Key, want)
	}
}
