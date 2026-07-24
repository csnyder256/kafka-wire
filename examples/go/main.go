// Produce and consume against kafka-wire with franz-go.
//
//	go mod init example && go get github.com/twmb/franz-go/pkg/kgo
//	go run .
package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const topic = "demo.go"

func main() {
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
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		// kafka-wire has no transaction coordinator, so it does not offer
		// InitProducerId. franz-go only needs this when idempotence is on.
		kgo.DisableIdempotentWrite(),
	)
	check(err)
	defer cl.Close()

	ctx := context.Background()
	if _, err := kadm.NewClient(cl).CreateTopics(ctx, 1, 1, nil, topic); err != nil {
		fmt.Fprintln(os.Stderr, "create topic:", err)
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

	var got [][]byte
	deadline := time.Now().Add(15 * time.Second)
	for len(got) < len(messages) && time.Now().Before(deadline) {
		pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		f := consumer.PollFetches(pctx)
		cancel()
		f.EachRecord(func(r *kgo.Record) { got = append(got, r.Value) })
	}

	if len(got) != len(messages) {
		fmt.Fprintf(os.Stderr, "MISMATCH: sent %d records, got %d back\n", len(messages), len(got))
		os.Exit(1)
	}
	for i := range got {
		if !bytes.Equal(got[i], messages[i]) {
			fmt.Fprintf(os.Stderr, "MISMATCH at record %d\n", i)
			os.Exit(1)
		}
	}
	fmt.Printf("consumed %d records, byte-identical to what was sent\n", len(got))
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
