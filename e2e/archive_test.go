package e2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestColdStorageActuallyArchives exists because the cold-storage tier, the
// headline feature of this project, once shipped completely inert.
//
// The uploader gated on a bucket name that only the S3 driver ever had. Once
// the backend interface replaced it, that field was never set, so the sweeper
// returned immediately for every backend. The startup banner still said cold
// storage was on, the log still said the archive was enabled, and nothing was
// ever uploaded. Every test passed, because no test ran the uploader.
//
// This one drives a real broker until segments seal, then asserts that objects
// appear in the archive directory. It is deliberately end to end: the defect
// lived in the wiring between configuration and the sweeper, which is exactly
// the seam a unit test does not cross.
func TestColdStorageActuallyArchives(t *testing.T) {
	archiveDir := t.TempDir()

	b := startBroker(t,
		"KAFKA_WIRE_ARCHIVE_BACKEND=fs",
		"KAFKA_WIRE_ARCHIVE_FS_PATH="+archiveDir,
		// Seal segments quickly and make them eligible immediately, so the
		// test finishes in seconds rather than in the default hour.
		"KAFKA_WIRE_STORAGE_SEGMENTBYTES=8KiB",
		"KAFKA_WIRE_ARCHIVE_AGE=1s",
		"KAFKA_WIRE_ARCHIVE_LOCALRETENTION=1h",
	)

	const topic = "archive.demo"
	admin := newClient(t, b.addr)
	createTopic(t, admin, topic, 1)

	// Enough data to roll several segments; only sealed ones are archived.
	ctx := context.Background()
	p := newClient(t, b.addr)
	payload := []byte(strings.Repeat("a", 400))
	for i := 0; i < 400; i++ {
		if err := p.ProduceSync(ctx, &kgo.Record{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("k%d", i)),
			Value: payload,
		}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	// The sweep runs on a timer, so poll rather than sleeping a fixed amount.
	deadline := time.Now().Add(90 * time.Second)
	var found []string
	for time.Now().Before(deadline) {
		found = found[:0]
		_ = filepath.Walk(archiveDir, func(path string, info os.FileInfo, err error) error {
			if err == nil && info != nil && !info.IsDir() && strings.HasSuffix(path, ".log") {
				found = append(found, path)
			}
			return nil
		})
		if len(found) > 0 {
			break
		}
		time.Sleep(2 * time.Second)
	}

	if len(found) == 0 {
		t.Fatalf("no segments reached the archive at %s after 90s.\n"+
			"Cold storage reported itself enabled but uploaded nothing, which is\n"+
			"the exact failure this test exists to catch.", archiveDir)
	}

	// An archived object must have real content, not a zero-length placeholder.
	info, err := os.Stat(found[0])
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Fatalf("archived object %s is empty", found[0])
	}
	t.Logf("archived %d segment(s), first is %d bytes", len(found), info.Size())
}
