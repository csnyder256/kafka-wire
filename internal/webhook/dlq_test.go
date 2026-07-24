package webhook

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func testRecord(offset int64) Record {
	return Record{
		Topic:     "orders.events",
		Partition: 0,
		Offset:    offset,
		Key:       []byte("k"),
		Value:     []byte(`{"event_type":"document.uploaded","n":` + string(rune('0'+offset%10)) + `}`),
		Timestamp: time.Unix(1700000000, 0).UTC(),
	}
}

func writeSubscription(t *testing.T, m *Manager, url string) {
	t.Helper()
	if err := os.MkdirAll(m.dataDir+"/metadata", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := m.SaveConfig(&Config{Subscriptions: []Subscription{{
		Topic: "orders.events", URL: url,
	}}}); err != nil {
		t.Fatal(err)
	}
}

func TestDLQ_AppendAndCount(t *testing.T) {
	m := NewManager(t.TempDir())
	key := "orders.events::http://example.invalid/hook"

	if got := m.dlqCount(key); got != 0 {
		t.Fatalf("empty DLQ should count 0, got %d", got)
	}
	for i := int64(0); i < 3; i++ {
		if err := m.appendDLQ(key, testRecord(i), context.DeadlineExceeded); err != nil {
			t.Fatalf("appendDLQ: %v", err)
		}
	}
	if got := m.dlqCount(key); got != 3 {
		t.Fatalf("expected 3 parked records, got %d", got)
	}
}

// Downstream healthy again: every parked record replays, the file is
// removed, and the count drops to zero.
func TestRedeliverDLQ_AllSucceed(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	m := NewManager(t.TempDir())
	writeSubscription(t, m, srv.URL)
	key := "orders.events::" + srv.URL
	for i := int64(0); i < 4; i++ {
		if err := m.appendDLQ(key, testRecord(i), context.DeadlineExceeded); err != nil {
			t.Fatal(err)
		}
	}

	redelivered, remaining, err := m.RedeliverDLQ(context.Background(), "orders.events", srv.URL)
	if err != nil {
		t.Fatalf("RedeliverDLQ: %v", err)
	}
	if redelivered != 4 || remaining != 0 {
		t.Fatalf("expected 4 redelivered / 0 remaining, got %d / %d", redelivered, remaining)
	}
	if hits.Load() != 4 {
		t.Fatalf("expected 4 HTTP deliveries, got %d", hits.Load())
	}
	if got := m.dlqCount(key); got != 0 {
		t.Fatalf("DLQ should be empty after full redelivery, got %d", got)
	}
	if _, err := os.Stat(m.dlqPath(key)); !os.IsNotExist(err) {
		t.Fatal("DLQ file should be removed once drained")
	}
}

// Downstream still broken: records stay parked with refreshed errors;
// nothing is lost.
func TestRedeliverDLQ_StillFailingKeepsRecords(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()

	m := NewManager(t.TempDir())
	writeSubscription(t, m, srv.URL)
	key := "orders.events::" + srv.URL
	for i := int64(0); i < 2; i++ {
		if err := m.appendDLQ(key, testRecord(i), context.DeadlineExceeded); err != nil {
			t.Fatal(err)
		}
	}

	// Redelivery is single-attempt per record (operator-triggered), so
	// this returns promptly even against a dead endpoint.
	redelivered, remaining, err := m.RedeliverDLQ(context.Background(), "orders.events", srv.URL)
	if err != nil {
		t.Fatalf("RedeliverDLQ: %v", err)
	}
	if redelivered != 0 {
		t.Fatalf("expected 0 redelivered against a 500 endpoint, got %d", redelivered)
	}
	if remaining != 2 {
		t.Fatalf("both failing records must remain parked, got %d", remaining)
	}
	if got := m.dlqCount(key); got != remaining {
		t.Fatalf("count (%d) disagrees with remaining (%d)", got, remaining)
	}
}

func TestRedeliverDLQ_UnknownSubscription(t *testing.T) {
	m := NewManager(t.TempDir())
	if _, _, err := m.RedeliverDLQ(context.Background(), "nope", "http://x"); err == nil {
		t.Fatal("expected error for unknown subscription")
	}
}

// A record dead-lettered WHILE a redelivery's network phase is running
// must survive the redelivery's cleanup. The old read-process-rewrite
// shape removed/rewrote the whole file at the end, silently dropping
// anything appendDLQ parked in between; the rename-claim protocol keeps
// the canonical file free for fresh appends.
func TestRedeliverDLQ_ConcurrentAppendSurvives(t *testing.T) {
	m := NewManager(t.TempDir())
	var key string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Runs inside RedeliverDLQ's unlocked network phase by
		// construction: park a fresh record like a live delivery would.
		if err := m.appendDLQ(key, testRecord(99), context.DeadlineExceeded); err != nil {
			t.Errorf("concurrent appendDLQ: %v", err)
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()
	writeSubscription(t, m, srv.URL)
	key = "orders.events::" + srv.URL

	if err := m.appendDLQ(key, testRecord(1), context.DeadlineExceeded); err != nil {
		t.Fatal(err)
	}
	redelivered, remaining, err := m.RedeliverDLQ(context.Background(), "orders.events", srv.URL)
	if err != nil {
		t.Fatalf("RedeliverDLQ: %v", err)
	}
	if redelivered != 1 || remaining != 0 {
		t.Fatalf("expected 1 redelivered / 0 remaining from the claimed batch, got %d / %d", redelivered, remaining)
	}
	if got := m.dlqCount(key); got != 1 {
		t.Fatalf("record appended during redelivery must survive; count = %d, want 1", got)
	}
}

// A .processing file left behind by a crashed redelivery is folded back
// in on the next run: nothing is lost, everything replays.
func TestRedeliverDLQ_SalvagesCrashedClaim(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	m := NewManager(t.TempDir())
	writeSubscription(t, m, srv.URL)
	key := "orders.events::" + srv.URL

	// Simulate a crash mid-redelivery: one record stranded in the claim
	// file, one parked normally afterwards.
	if err := m.appendDLQ(key, testRecord(1), context.DeadlineExceeded); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(m.dlqPath(key), m.dlqPath(key)+".processing"); err != nil {
		t.Fatal(err)
	}
	if err := m.appendDLQ(key, testRecord(2), context.DeadlineExceeded); err != nil {
		t.Fatal(err)
	}
	if got := m.dlqCount(key); got != 2 {
		t.Fatalf("count should include the stranded claim, got %d", got)
	}

	redelivered, remaining, err := m.RedeliverDLQ(context.Background(), "orders.events", srv.URL)
	if err != nil {
		t.Fatalf("RedeliverDLQ: %v", err)
	}
	if redelivered != 2 || remaining != 0 {
		t.Fatalf("expected both records (stranded + live) to replay, got %d / %d", redelivered, remaining)
	}
	if hits.Load() != 2 {
		t.Fatalf("expected 2 HTTP deliveries, got %d", hits.Load())
	}
	if got := m.dlqCount(key); got != 0 {
		t.Fatalf("DLQ should be empty after salvage + full redelivery, got %d", got)
	}
	if _, err := os.Stat(m.dlqPath(key) + ".processing"); !os.IsNotExist(err) {
		t.Fatal("claim file should be removed once drained")
	}
}

// Cancelling the context mid-replay must never drop records: every
// record is either delivered or still parked afterwards (conservation).
// The pre-fix loop broke out on ctx.Err() without keeping the
// unprocessed lines, then removed the claim file, losing them.
func TestRedeliverDLQ_CancelKeepsUnprocessed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cancel() // aborts the replay while later records are unprocessed
		w.WriteHeader(200)
	}))
	defer srv.Close()

	m := NewManager(t.TempDir())
	writeSubscription(t, m, srv.URL)
	key := "orders.events::" + srv.URL
	for i := int64(0); i < 3; i++ {
		if err := m.appendDLQ(key, testRecord(i), context.DeadlineExceeded); err != nil {
			t.Fatal(err)
		}
	}

	redelivered, remaining, err := m.RedeliverDLQ(ctx, "orders.events", srv.URL)
	if err != nil {
		t.Fatalf("RedeliverDLQ: %v", err)
	}
	// The first record's own delivery may land as success or as a
	// canceled-request failure depending on timing; conservation must
	// hold either way, and records 2..3 were never attempted.
	if redelivered+remaining != 3 {
		t.Fatalf("records lost on cancel: redelivered=%d remaining=%d (want sum 3)", redelivered, remaining)
	}
	if remaining < 2 {
		t.Fatalf("at least the 2 unattempted records must remain parked, got %d", remaining)
	}
	if got := m.dlqCount(key); got != remaining {
		t.Fatalf("count (%d) disagrees with remaining (%d)", got, remaining)
	}
}

// Two concurrent redeliveries for the same subscription would race for
// the claim file; the second is refused outright.
func TestRedeliverDLQ_RefusesConcurrentRun(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer srv.Close()

	m := NewManager(t.TempDir())
	writeSubscription(t, m, srv.URL)
	key := "orders.events::" + srv.URL
	if err := m.appendDLQ(key, testRecord(1), context.DeadlineExceeded); err != nil {
		t.Fatal(err)
	}

	m.dlqMu.Lock()
	m.dlqBusy[key] = true
	m.dlqMu.Unlock()
	if _, _, err := m.RedeliverDLQ(context.Background(), "orders.events", srv.URL); err == nil {
		t.Fatal("expected in-progress redelivery to be refused")
	}
	m.dlqMu.Lock()
	delete(m.dlqBusy, key)
	m.dlqMu.Unlock()

	// And once released, the real run proceeds normally.
	redelivered, remaining, err := m.RedeliverDLQ(context.Background(), "orders.events", srv.URL)
	if err != nil || redelivered != 1 || remaining != 0 {
		t.Fatalf("post-release redelivery failed: %d / %d / %v", redelivered, remaining, err)
	}
}
