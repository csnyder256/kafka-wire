package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func produceRecords(t *testing.T, addr, topic string, n int, prefix string) {
	t.Helper()
	// Uncompressed, so the archived bytes show which topic wrote them.
	p := newClient(t, addr, kgo.ProducerBatchCompression(kgo.NoCompression()))
	value := []byte(prefix + strings.Repeat("a", 400-len(prefix)))
	for i := 0; i < n; i++ {
		if err := p.ProduceSync(context.Background(), &kgo.Record{
			Topic: topic, Key: []byte(fmt.Sprintf("k%d", i)), Value: value,
		}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
}

func localSegmentCount(dataDir, topic string) int {
	entries, _ := os.ReadDir(filepath.Join(dataDir, "topics", topic, "0"))
	n := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".log") {
			n++
		}
	}
	return n
}

func archivedObjectCount(archiveDir string) int {
	n := 0
	_ = filepath.Walk(archiveDir, func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.HasSuffix(p, ".log") {
			n++
		}
		return nil
	})
	return n
}

func consumeValues(t *testing.T, addr, topic string, n int) [][]byte {
	t.Helper()
	c := newClient(t, addr, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	var values [][]byte
	deadline := time.Now().Add(60 * time.Second)
	for len(values) < n && time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		f := c.PollFetches(ctx)
		cancel()
		if errs := f.Errors(); len(errs) > 0 && !errors.Is(errs[0].Err, context.DeadlineExceeded) {
			t.Fatalf("fetch: %v", errs[0].Err)
		}
		f.EachRecord(func(r *kgo.Record) { values = append(values, r.Value) })
	}
	return values
}

// With cold storage on, retention used to delete segments whether or not they
// had been uploaded. Here every upload fails and both retention rules are far
// past due, and still nothing may be deleted.
func TestRetentionWaitsForTheArchive(t *testing.T) {
	archiveDir, dataDir := t.TempDir(), t.TempDir()
	b := startBroker(t,
		"KAFKA_WIRE_STORAGE_DATADIR="+dataDir,
		"KAFKA_WIRE_ARCHIVE_BACKEND=fs",
		"KAFKA_WIRE_ARCHIVE_FS_PATH="+archiveDir,
		"KAFKA_WIRE_STORAGE_SEGMENTBYTES=8KiB",
		"KAFKA_WIRE_ARCHIVE_AGE=1s",
		"KAFKA_WIRE_STORAGE_RETENTIONAGE=3s",
		"KAFKA_WIRE_STORAGE_RETENTIONSIZE=16KiB",
	)
	// The fs backend stages uploads in <archive>/uploads, created at startup.
	// A file in its place makes every upload fail.
	uploads := filepath.Join(archiveDir, "uploads")
	if err := os.RemoveAll(uploads); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(uploads, []byte("not a directory"), 0o644); err != nil {
		t.Fatal(err)
	}
	const topic = "archive.failing"
	createTopic(t, newClient(t, b.addr), topic, 1)
	produceRecords(t, b.addr, topic, 400, "F")
	before := localSegmentCount(dataDir, topic)

	// Retention sweeps every 1.5s at this age; uploads run on a 30s timer.
	time.Sleep(40 * time.Second)
	if after := localSegmentCount(dataDir, topic); after < before {
		t.Fatalf("%d of %d local segments were deleted while none had been archived", before-after, before)
	}
	// Nothing reached the archive, so nothing was eligible. (The broker's
	// log is not read here: the process is still writing it.)
	if n := archivedObjectCount(archiveDir); n != 0 {
		t.Fatalf("expected every upload to fail, but %d objects reached the archive", n)
	}
	if got := len(consumeValues(t, b.addr, topic, 400)); got != 400 {
		t.Fatalf("read back %d of 400 records", got)
	}
}

// A deleted topic's archive entries used to outlive it. Recreated under the
// same name, the new topic's segments were skipped by the uploader as already
// archived, and retention took them for archived ones.
func TestRecreatedTopicGetsItsOwnArchive(t *testing.T) {
	archiveDir, dataDir := t.TempDir(), t.TempDir()
	b := startBroker(t,
		"KAFKA_WIRE_STORAGE_DATADIR="+dataDir,
		"KAFKA_WIRE_ARCHIVE_BACKEND=fs",
		"KAFKA_WIRE_ARCHIVE_FS_PATH="+archiveDir,
		"KAFKA_WIRE_STORAGE_SEGMENTBYTES=8KiB",
		"KAFKA_WIRE_ARCHIVE_AGE=1s",
	)
	admin := newClient(t, b.addr)
	adm := kadm.NewClient(admin)
	const topic = "archive.reused"

	archivedWith := func(marker string) int {
		n := 0
		_ = filepath.Walk(archiveDir, func(p string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(p, ".log") {
				return nil
			}
			if raw, rerr := os.ReadFile(p); rerr == nil && bytes.Contains(raw, []byte(marker)) {
				n++
			}
			return nil
		})
		return n
	}
	waitFor := func(what string, ok func() bool) {
		t.Helper()
		deadline := time.Now().Add(90 * time.Second)
		for !ok() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(2 * time.Second)
		}
	}

	// Every segment but the active one.
	sealed := func() int { return localSegmentCount(dataDir, topic) - 1 }

	createTopic(t, admin, topic, 1)
	produceRecords(t, b.addr, topic, 400, "OLD")
	oldSealed := sealed()
	if oldSealed < 3 {
		t.Fatalf("expected several sealed segments, got %d", oldSealed)
	}
	waitFor("every sealed segment of the first topic to be archived", func() bool { return archivedWith("OLD") >= oldSealed })

	resp, err := adm.DeleteTopics(context.Background(), topic)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range resp {
		if r.Err != nil {
			t.Fatalf("deleting %s: %v", topic, r.Err)
		}
	}
	createTopic(t, admin, topic, 1)
	produceRecords(t, b.addr, topic, 400, "NEW")
	newSealed := sealed()

	// Same records, same sizes: the new segments start at the offsets of the
	// archived ones, which is exactly where a stale manifest entry would stop
	// the uploader.
	waitFor("every sealed segment of the recreated topic to be archived", func() bool { return archivedWith("NEW") >= newSealed })

	values := consumeValues(t, b.addr, topic, 400)
	if len(values) != 400 {
		t.Fatalf("read back %d of 400 records", len(values))
	}
	for i, v := range values {
		if !bytes.HasPrefix(v, []byte("NEW")) {
			t.Fatalf("record %d came from the deleted topic", i)
		}
	}
}

// The guard must not stop retention for good: once the archive holds a
// segment, the ordinary rules delete the local copy as before.
func TestRetentionDeletesArchivedSegments(t *testing.T) {
	archiveDir, dataDir := t.TempDir(), t.TempDir()
	b := startBroker(t,
		"KAFKA_WIRE_STORAGE_DATADIR="+dataDir,
		"KAFKA_WIRE_ARCHIVE_BACKEND=fs",
		"KAFKA_WIRE_ARCHIVE_FS_PATH="+archiveDir,
		"KAFKA_WIRE_STORAGE_SEGMENTBYTES=8KiB",
		"KAFKA_WIRE_ARCHIVE_AGE=1s",
		"KAFKA_WIRE_STORAGE_RETENTIONAGE=3s",
	)
	const topic = "archive.retained"
	createTopic(t, newClient(t, b.addr), topic, 1)
	produceRecords(t, b.addr, topic, 400, "R")
	before := localSegmentCount(dataDir, topic)
	if before < 3 {
		t.Fatalf("expected several segments, got %d", before)
	}
	// Uploads run on a 30s timer; retention sweeps every 1.5s at this age.
	deadline := time.Now().Add(90 * time.Second)
	for localSegmentCount(dataDir, topic) > 1 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Second)
	}
	if after := localSegmentCount(dataDir, topic); after > 1 {
		t.Fatalf("%d of %d segments are still on local disk although they were archived and are past retentionage", after, before)
	}
}

func deleteTopic(t *testing.T, admin *kgo.Client, topic string) {
	t.Helper()
	resp, err := kadm.NewClient(admin).DeleteTopics(context.Background(), topic)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range resp {
		if r.Err != nil {
			t.Fatalf("deleting %s: %v", topic, r.Err)
		}
	}
}

// consumeFromOffset reads up to n records of partition 0 starting at an exact
// offset, so reads below the local log go through the archive.
func consumeFromOffset(t *testing.T, addr, topic string, from int64, n int) [][]byte {
	t.Helper()
	c := newClient(t, addr, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().At(from)}}))
	var values [][]byte
	deadline := time.Now().Add(60 * time.Second)
	for len(values) < n && time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		f := c.PollFetches(ctx)
		cancel()
		f.EachRecord(func(r *kgo.Record) { values = append(values, r.Value) })
	}
	return values
}

// Restored segments are cached by topic, partition and offset. A deleted
// topic's cache used to survive it, and the recreated topic was served the
// old topic's records from it.
func TestRecreatedTopicNeverReadsTheDeletedOnesCache(t *testing.T) {
	archiveDir, dataDir := t.TempDir(), t.TempDir()
	b := startBroker(t,
		"KAFKA_WIRE_STORAGE_DATADIR="+dataDir,
		"KAFKA_WIRE_ARCHIVE_BACKEND=fs",
		"KAFKA_WIRE_ARCHIVE_FS_PATH="+archiveDir,
		"KAFKA_WIRE_STORAGE_SEGMENTBYTES=8KiB",
		"KAFKA_WIRE_ARCHIVE_AGE=1s",
		"KAFKA_WIRE_STORAGE_RETENTIONAGE=3s",
	)
	const topic = "archive.cached"
	admin := newClient(t, b.addr)
	onlyActiveSegmentLeft := func() {
		t.Helper()
		deadline := time.Now().Add(150 * time.Second)
		for localSegmentCount(dataDir, topic) > 1 {
			if time.Now().After(deadline) {
				t.Fatal("the archived segments were never removed locally")
			}
			time.Sleep(2 * time.Second)
		}
	}

	createTopic(t, admin, topic, 1)
	produceRecords(t, b.addr, topic, 200, "OLD")
	onlyActiveSegmentLeft()
	// Offset 0 is no longer local: it is restored from the archive, into the cache.
	if old := consumeFromOffset(t, b.addr, topic, 0, 50); len(old) == 0 || !bytes.HasPrefix(old[0], []byte("OLD")) {
		t.Fatalf("setup: could not read the first topic back from the archive (%d records)", len(old))
	}

	deleteTopic(t, admin, topic)
	createTopic(t, admin, topic, 1)
	produceRecords(t, b.addr, topic, 200, "NEW")
	onlyActiveSegmentLeft()

	values := consumeFromOffset(t, b.addr, topic, 0, 50)
	if len(values) == 0 {
		t.Fatal("read nothing back from the recreated topic's archive")
	}
	for i, v := range values {
		if !bytes.HasPrefix(v, []byte("NEW")) {
			t.Fatalf("record %d came from the deleted topic", i)
		}
	}
}
