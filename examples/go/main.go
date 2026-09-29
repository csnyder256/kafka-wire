// Produce and consume against kafka-wire with franz-go.
//
//	From repository root: go run ./examples/go
//	go run .
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	nonce := make([]byte, 12)
	_, err := rand.Read(nonce)
	check(err)
	topic := "demo.go." + hex.EncodeToString(nonce)
	brokers := strings.Split(envOr("KAFKA_WIRE_BROKERS", "127.0.0.1:9092"), ",")

	everyByte := make([]byte, 256)
	for i := range everyByte {
		everyByte[i] = byte(i)
	}
	messages := [][]byte{
		[]byte("a plain line"),
		[]byte(`{"id":1,"note":"json is just bytes here"}`),
		everyByte,
		{},
		[]byte("こんにちは · Kafka"),
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		// kafka-wire has no transaction coordinator, so it does not offer
		// InitProducerId. franz-go only needs this when idempotence is on.
		kgo.DisableIdempotentWrite(),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
	)
	check(err)
	defer cl.Close()

	ctx, cancelAll := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancelAll()
	created, err := kadm.NewClient(cl).CreateTopics(ctx, 1, 1, nil, topic)
	check(err)
	for _, result := range created {
		check(result.Err)
	}

	for _, m := range messages {
		check(cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte("k"), Value: m}).FirstErr())
	}
	fmt.Printf("produced %d records to %s\n", len(messages), topic)

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DisableIdempotentWrite(),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	check(err)
	defer consumer.Close()

	var got []*kgo.Record
	deadline := time.Now().Add(15 * time.Second)
	for len(got) < len(messages) && time.Now().Before(deadline) {
		pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		f := consumer.PollFetches(pctx)
		cancel()
		f.EachRecord(func(r *kgo.Record) { got = append(got, r) })
	}

	if len(got) != len(messages) {
		fmt.Fprintf(os.Stderr, "MISMATCH: sent %d records, got %d back\n", len(messages), len(got))
		os.Exit(1)
	}
	for i := range got {
		if !bytes.Equal(got[i].Value, messages[i]) || !bytes.Equal(got[i].Key, []byte("k")) || got[i].Partition != 0 || got[i].Offset != int64(i) {
			fmt.Fprintf(os.Stderr, "MISMATCH at record %d\n", i)
			os.Exit(1)
		}
	}
	fmt.Printf("consumed %d records, byte-identical to what was sent\n", len(got))
	clientVersion := "unknown"
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			if dep.Path == "github.com/twmb/franz-go" {
				clientVersion = dep.Version
			}
		}
	}
	check(json.NewEncoder(os.Stdout).Encode(map[string]any{"schema": "kafka-wire.client-check", "version": 1, "client": "franz-go", "client_version": clientVersion, "language": "go", "runtime": runtime.Version(), "status": "passed", "records": len(got), "checks": []string{"create-topic", "produce-acks", "byte-fidelity", "key-fidelity", "partition-order"}, "settings": map[string]any{"idempotence": false, "compression": "none", "partitions": 1, "security": "PLAINTEXT", "group": "none"}}))
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
