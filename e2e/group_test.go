package e2e

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestTwoMemberGroupDividesWork is the test that separates a consumer group
// that works from one that only looks like it works.
//
// Two members subscribe to a two-partition topic. Kafka's contract is that
// each partition belongs to exactly one member of a generation, so the group
// as a whole must see every record exactly once, and each member must see
// some of them.
//
// A coordinator that settles the generation on the first JoinGroup rather
// than holding a join window fails this in a way that never looks like an
// error: each member becomes the sole member of its own generation, assigns
// itself every partition, and the group delivers every record twice. The
// assertion below is on the exact total for that reason. Asserting "at least
// 20" would pass against the broken behavior.
func TestTwoMemberGroupDividesWork(t *testing.T) {
	b := startBroker(t)
	const (
		topic   = "mm.topic"
		group   = "mm.group"
		records = 20
	)

	admin := newClient(t, b.addr)
	createTopic(t, admin, topic, 2)

	ctx := context.Background()

	var (
		mu      sync.Mutex
		seen    = map[string]int{} // partition:offset -> times delivered
		perMemb = map[int]int{}
		wg      sync.WaitGroup
		ready   sync.WaitGroup
	)

	// Both members join and let the group settle BEFORE anything is
	// produced. Without that, a member that happens to start first can
	// legitimately drain the whole topic before the second one arrives, and
	// the split would be a race rather than a property.
	ready.Add(2)
	for m := 0; m < 2; m++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c, err := kgo.NewClient(
				kgo.SeedBrokers(b.addr),
				kgo.DisableIdempotentWrite(),
				kgo.ConsumeTopics(topic),
				kgo.ConsumerGroup(group),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			)
			if err != nil {
				t.Error(err)
				ready.Done()
				return
			}
			defer c.Close()

			// One empty poll drives the join and sync exchange.
			settleCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
			c.PollFetches(settleCtx)
			cancel()
			ready.Done()

			deadline := time.Now().Add(30 * time.Second)
			for time.Now().Before(deadline) {
				pctx, pcancel := context.WithTimeout(ctx, 3*time.Second)
				f := c.PollFetches(pctx)
				pcancel()
				f.EachRecord(func(r *kgo.Record) {
					mu.Lock()
					seen[key(r)]++
					perMemb[id]++
					mu.Unlock()
				})
				mu.Lock()
				done := len(seen) >= records
				mu.Unlock()
				if done {
					return
				}
			}
		}(m)
	}

	ready.Wait()
	p := newClient(t, b.addr)
	for i := 0; i < records; i++ {
		if err := p.ProduceSync(ctx, &kgo.Record{
			Topic: topic,
			Key:   []byte{byte(i)}, // spread across both partitions
			Value: []byte{byte(i)},
		}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()

	total := 0
	dupes := 0
	for _, n := range seen {
		total += n
		if n > 1 {
			dupes++
		}
	}

	if len(seen) != records {
		t.Fatalf("the group saw %d distinct records, want %d", len(seen), records)
	}
	if dupes != 0 {
		t.Errorf("%d record(s) were delivered to more than one member; partitions are not exclusively owned", dupes)
	}
	if total != records {
		t.Errorf("group delivered %d records in total, want exactly %d", total, records)
	}
	// Both members must do some of the work, or the "group" is one consumer
	// with a spectator.
	if perMemb[0] == 0 || perMemb[1] == 0 {
		t.Errorf("work was not divided: member 0 got %d records, member 1 got %d", perMemb[0], perMemb[1])
	}
	t.Logf("two members split %d records exactly once: %d / %d", total, perMemb[0], perMemb[1])
}

func key(r *kgo.Record) string {
	return string(rune(r.Partition)) + ":" + string(rune(r.Offset))
}
