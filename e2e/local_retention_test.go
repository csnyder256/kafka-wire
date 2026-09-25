package e2e

import (
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

// TestLocalRetentionKeepsTrimmedDataReadable covers archive.localretention end
// to end. The setting was documented, validated and handed to the uploader,
// which never read it, so archived segments sat on local disk for the whole
// storage.retentionage. Now the local copies go once they are old enough, and
// nothing may be lost by it: the earliest offset a new consumer is given is
// still the first one ever written, and every record reads back, the trimmed
// ones from the archive.
func TestLocalRetentionKeepsTrimmedDataReadable(t *testing.T) {
	archiveDir := t.TempDir()
	dataDir := t.TempDir()

	b := startBroker(t,
		// Listed after startBroker's own data directory, so this one wins.
		"KAFKA_WIRE_STORAGE_DATADIR="+dataDir,
		"KAFKA_WIRE_ARCHIVE_BACKEND=fs",
		"KAFKA_WIRE_ARCHIVE_FS_PATH="+archiveDir,
		"KAFKA_WIRE_STORAGE_SEGMENTBYTES=8KiB",
		"KAFKA_WIRE_ARCHIVE_AGE=1s",
		"KAFKA_WIRE_ARCHIVE_LOCALRETENTION=2s",
	)

	const topic = "archive.trim"
	const n = 400
	admin := newClient(t, b.addr)
	createTopic(t, admin, topic, 1)

	ctx := context.Background()
	p := newClient(t, b.addr)
	payload := []byte(strings.Repeat("a", 400))
	for i := 0; i < n; i++ {
		if err := p.ProduceSync(ctx, &kgo.Record{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("k%d", i)),
			Value: payload,
		}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	partitionDir := filepath.Join(dataDir, "topics", topic, "0")
	localSegments := func() int {
		entries, _ := os.ReadDir(partitionDir)
		count := 0
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".log") {
				count++
			}
		}
		return count
	}
	before := localSegments()
	if before < 5 {
		t.Fatalf("expected the produce to roll several segments, found %d", before)
	}

	// Uploads run on a 30s timer; the trim follows within a second or two.
	deadline := time.Now().Add(150 * time.Second)
	for localSegments() > 1 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Second)
	}
	after := localSegments()
	if after > 1 {
		t.Fatalf("%d of %d segments are still on local disk after 150s; archive.localretention=2s should have trimmed every archived one.\n%s",
			after, before, b.out.String())
	}
	t.Logf("local segments: %d before, %d after archiving and trimming", before, after)

	starts, err := kadm.NewClient(admin).ListStartOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if o, ok := starts.Lookup(topic, 0); !ok || o.Err != nil || o.Offset != 0 {
		t.Fatalf("earliest offset = %+v, want 0: trimmed segments are still readable from the archive", o)
	}

	c := newClient(t, b.addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	var keys []string
	readDeadline := time.Now().Add(90 * time.Second)
	for len(keys) < n && time.Now().Before(readDeadline) {
		pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		f := c.PollFetches(pctx)
		cancel()
		if errs := f.Errors(); len(errs) > 0 && !errors.Is(errs[0].Err, context.DeadlineExceeded) {
			t.Fatalf("fetch: %v", errs[0].Err)
		}
		f.EachRecord(func(r *kgo.Record) { keys = append(keys, string(r.Key)) })
	}
	if len(keys) != n {
		t.Fatalf("read back %d of %d records", len(keys), n)
	}
	for i, k := range keys {
		if want := fmt.Sprintf("k%d", i); k != want {
			t.Fatalf("record %d has key %q, want %q", i, k, want)
		}
	}
}
