package e2e

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// BenchmarkThroughput measures produce and end-to-end consume rate against a
// real broker process on whatever machine runs it.
//
// It exists so that any number quoted in the README can be reproduced by the
// person reading it, on their own hardware, with one command:
//
//	go test ./e2e/ -run XXX -bench Throughput -benchtime 10x
//
// Numbers from a laptop are not a capacity plan. They bound the shape of the
// thing: whether this is a hundreds-per-second or a hundreds-of-thousands-per
// -second system.
func BenchmarkThroughput(b *testing.B) {
	for _, size := range []int{256, 4096} {
		b.Run(fmt.Sprintf("payload_%dB", size), func(b *testing.B) {
			t := &testing.T{}
			bp := startBroker(t)
			topic := fmt.Sprintf("bench.%d", size)

			admin, err := kgo.NewClient(kgo.SeedBrokers(bp.addr), kgo.DisableIdempotentWrite())
			if err != nil {
				b.Fatal(err)
			}
			defer admin.Close()
			createTopic(t, admin, topic, 1)

			// Random, not zero-filled. A zero-filled payload compresses to
			// almost nothing, and a benchmark on compressible data measures
			// the compressor rather than the broker.
			payload := make([]byte, size)
			if _, err := rand.Read(payload); err != nil {
				b.Fatal(err)
			}
			ctx := context.Background()

			const (
				producers = 4
				perProd   = 50000
			)
			cls := make([]*kgo.Client, producers)
			for i := range cls {
				c, err := kgo.NewClient(
					kgo.SeedBrokers(bp.addr),
					kgo.DisableIdempotentWrite(),
					kgo.DefaultProduceTopic(topic),
					// Explicit, so the number below is a broker measurement
					// rather than a measurement of whatever codec the client
					// happened to default to.
					kgo.ProducerBatchCompression(kgo.NoCompression()),
				)
				if err != nil {
					b.Fatal(err)
				}
				cls[i] = c
				defer c.Close()
			}

			b.ResetTimer()
			start := time.Now()
			var wg sync.WaitGroup
			var mu sync.Mutex
			var failures int
			for _, c := range cls {
				wg.Add(1)
				go func(cl *kgo.Client) {
					defer wg.Done()
					for i := 0; i < perProd; i++ {
						cl.Produce(ctx, &kgo.Record{Topic: topic, Value: payload}, func(_ *kgo.Record, err error) {
							if err != nil {
								mu.Lock()
								failures++
								mu.Unlock()
							}
						})
					}
					if err := cl.Flush(ctx); err != nil {
						mu.Lock()
						failures++
						mu.Unlock()
					}
				}(c)
			}
			wg.Wait()
			elapsed := time.Since(start)
			b.StopTimer()

			total := producers * perProd
			if failures > 0 {
				b.Fatalf("%d produce failures", failures)
			}
			b.ReportMetric(float64(total)/elapsed.Seconds(), "records/s")
			b.ReportMetric(float64(total*size)/elapsed.Seconds()/(1<<20), "MiB/s")
		})
	}
}
