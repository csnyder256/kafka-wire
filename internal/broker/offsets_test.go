package broker

import (
	"sync"
	"testing"
	"time"
)

// A committed offset is written to disk as one JSON document per group, and
// two consumers in one group commit at the same time as a matter of course:
// one per partition, on separate connections, each on its own goroutine. These
// tests pin the two things that made that unsafe before the store's lock
// covered the write.
//
// Before the fix, TestConcurrentOffsetCommitsAreSerialized failed on the first
// run with a rename error:
//
//	CommitOffset: rename .../groups/workers.json.tmp .../groups/workers.json:
//	no such file or directory
//
// because both writers used the same temp path and the second rename found it
// already consumed. The offset had been recorded in memory, so the wire layer
// reported that rename error as CORRUPT_MESSAGE on a commit that had actually
// succeeded -- a producer of retries, and on restart an offset that was never
// persisted. The same run under -race also reported a write/write race on
// GroupState.UpdatedAt and a read/write race between json.MarshalIndent walking
// the offsets map and the other goroutine's CommitOffset writing into it, which
// Go raises as a fatal runtime error ("concurrent map read and map write")
// rather than a recoverable panic.
func TestConcurrentOffsetCommitsAreSerialized(t *testing.T) {
	dir := t.TempDir()
	store, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("NewOffsetStore: %v", err)
	}

	const committers = 4
	const commitsPerPartition = 200

	var wg sync.WaitGroup
	commitErr := make(chan error, committers)
	for c := 0; c < committers; c++ {
		wg.Add(1)
		go func(partition int32) {
			defer wg.Done()
			for i := 1; i <= commitsPerPartition; i++ {
				if err := store.CommitOffset("workers", "orders", partition, int64(i), 0, ""); err != nil {
					commitErr <- err
					return
				}
			}
		}(int32(c))
	}
	wg.Wait()
	close(commitErr)
	for err := range commitErr {
		t.Errorf("a concurrent commit failed: %v", err)
	}

	for partition := int32(0); partition < committers; partition++ {
		co := store.FetchOffset("workers", "orders", partition)
		if co.Offset != commitsPerPartition {
			t.Errorf("partition %d committed offset is %d, want %d", partition, co.Offset, commitsPerPartition)
		}
	}

	// The document on disk has to be one a restart can read. A shared temp
	// path could also leave the loser's bytes in it, which is a group file
	// NewOffsetStore refuses to parse -- a broker that will not start until
	// somebody repairs the file by hand.
	reopened, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("the group file written by concurrent commits cannot be read back: %v", err)
	}
	for partition := int32(0); partition < committers; partition++ {
		co := reopened.FetchOffset("workers", "orders", partition)
		if co.Offset != commitsPerPartition {
			t.Errorf("after reopening, partition %d committed offset is %d, want %d", partition, co.Offset, commitsPerPartition)
		}
	}
}

// The coordinator rewrites the same GroupState from a different goroutine:
// every JoinGroup settles a generation, and the persist that follows serializes
// the members map. That is the same unsynchronized write the commit path was
// doing, so it is pinned against a real coordinator rather than against a
// hand-written goroutine.
//
// Before the fix this failed under -race with a read of the members map (or of
// GroupState.Generation) inside json.MarshalIndent reported against the
// coordinator's write to the same field.
func TestCoordinatorAndCommitsShareOneStateLock(t *testing.T) {
	dir := t.TempDir()
	store, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("NewOffsetStore: %v", err)
	}
	coord := NewGroupCoordinator(store)

	// Keep a commit in flight for the whole test: the coordinator's first
	// round waits out initialRebalanceDelay, and the overlap with a commit
	// marshal is the thing under test.
	stop := make(chan struct{})
	commitErr := make(chan error, 1)
	var wg sync.WaitGroup
	var stopOnce sync.Once
	stopCommits := func() {
		stopOnce.Do(func() {
			close(stop)
			wg.Wait()
		})
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := store.CommitOffset("workers", "orders", int32(i%4), int64(i), 0, ""); err != nil {
				commitErr <- err
				return
			}
			time.Sleep(200 * time.Microsecond)
		}
	}()
	defer stopCommits()

	join := func(memberID string) *JoinGroupResult {
		t.Helper()
		res, err := coord.JoinGroup(JoinGroupArgs{
			GroupID:            "workers",
			MemberID:           memberID,
			ClientID:           "consumer-1",
			ClientHost:         "127.0.0.1:1234",
			SessionTimeoutMS:   30000,
			RebalanceTimeoutMS: 60000,
			ProtocolType:       "consumer",
			Protocols:          []GroupProtocol{{Name: "range", Metadata: []byte{0, 0, 0, 0}}},
			// v3 registers the member immediately; v4 would answer
			// MEMBER_ID_REQUIRED and need a second round trip.
			APIVersion: 3,
		})
		if err != nil {
			t.Fatalf("JoinGroup: %v", err)
		}
		if res.ErrorCode != 0 {
			t.Fatalf("JoinGroup answered error code %d", res.ErrorCode)
		}
		return res
	}

	res := join("")
	for i := 0; i < 25; i++ {
		res = join(res.MemberID)
		sync, err := coord.SyncGroup(SyncGroupArgs{
			GroupID:     "workers",
			Generation:  res.Generation,
			MemberID:    res.MemberID,
			Assignments: map[string][]byte{res.MemberID: {0, 0, 0, 0}},
		})
		if err != nil {
			t.Fatalf("SyncGroup: %v", err)
		}
		if sync.ErrorCode != 0 {
			t.Fatalf("SyncGroup answered error code %d", sync.ErrorCode)
		}
	}

	stopCommits()
	select {
	case err := <-commitErr:
		t.Fatalf("a commit failed while the group rebalanced: %v", err)
	default:
	}
}
