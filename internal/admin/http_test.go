package admin

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/csnyder256/kafka-wire/internal/broker"
	"github.com/csnyder256/kafka-wire/internal/metrics"
	"github.com/csnyder256/kafka-wire/internal/storage"
)

// The lag column on the groups endpoint is read straight out of the committed
// offsets, which consumers are writing at the same time on their own
// connection goroutines. Before the endpoint took a copy under the store's
// lock, this read the live map, and a map iteration that overlaps a write is a
// fatal runtime error ("concurrent map iteration and map write") rather than a
// stale answer -- the whole broker process exits, taking every other group's
// connections with it. Under -race it is reported as a data race on the map
// header.
func TestGroupLagDoesNotRaceConcurrentCommits(t *testing.T) {
	dir := t.TempDir()
	st, err := storage.Open(storage.Config{DataDir: dir})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}

	brk := broker.New(broker.Config{
		BrokerID:       1,
		ClusterID:      "test",
		AdvertisedHost: "127.0.0.1",
		AdvertisedPort: 9092,
		DataDir:        dir,
		Storage:        st,
		Metrics:        metrics.New(),
	})
	if err := brk.LoadState(); err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	// Close the partition files and release the data directory before the test
	// returns. t.TempDir removes the directory on the way out, and Windows
	// refuses to unlink a file another handle still has open, so a broker left
	// running here fails the test in cleanup rather than in an assertion.
	defer func() {
		_ = brk.Drain(context.Background())
		_ = st.Close()
	}()
	if _, err := brk.CreateTopic("orders", 2, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	h := &handlers{brk: brk}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_ = brk.Offsets().CommitOffset("workers", "orders", int32(i%2), int64(i), 0, "")
			time.Sleep(100 * time.Microsecond)
		}
	}()

	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		groupLag(h, "workers")
	}
	close(stop)
	wg.Wait()

	// The lag rows are the answer, not just a race-free read: two partitions
	// with a committed offset and a high watermark each.
	rows := groupLag(h, "workers")
	if len(rows) != 2 {
		t.Fatalf("groupLag returned %d rows, want 2: %v", len(rows), rows)
	}
}
