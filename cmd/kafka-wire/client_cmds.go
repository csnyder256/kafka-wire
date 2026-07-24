package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// These subcommands exist so a new user can prove the broker works without
// installing a client library in some other language first. They are built on
// franz-go, an independent Kafka client that knows nothing about this broker,
// which makes them a compatibility check rather than a self-consistent loop.

func clientFlags(fs *flag.FlagSet) (*string, *string, *string) {
	return fs.String("brokers", envOr("KAFKA_WIRE_BROKERS", "127.0.0.1:9092"), "comma-separated bootstrap servers"),
		fs.String("user", os.Getenv("KAFKA_WIRE_USER"), "SASL username (enables SASL/SCRAM-SHA-256 when set)"),
		fs.String("password", os.Getenv("KAFKA_WIRE_PASSWORD"), "SASL password")
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func newClient(brokers, user, pass string, extra ...kgo.Opt) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		// Idempotent producing needs InitProducerId, which a single-node
		// broker with no transaction coordinator does not implement. Turning
		// it off here is what lets these commands work out of the box.
		kgo.DisableIdempotentWrite(),
	}
	if user != "" {
		opts = append(opts, scramOpt(user, pass))
	}
	opts = append(opts, extra...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return cl, nil
}

func cmdProduce(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("produce", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, `kafka-wire produce - send records to a topic

  Reads one record per line from stdin, or sends the arguments after the topic.

  echo hello | kafka-wire produce demo
  kafka-wire produce demo "first" "second"
  cat events.jsonl | kafka-wire produce events --key-from-json id

FLAGS
`)
		fs.PrintDefaults()
	}
	brokers, user, pass := clientFlags(fs)
	key := fs.String("key", "", "record key to use for every record")
	if err := fs.Parse(permute(fs, args)); err != nil {
		return err
	}
	if fs.NArg() == 0 {
		return errors.New("kafka-wire produce: name a topic\n  kafka-wire produce demo")
	}
	topic := fs.Arg(0)

	cl, err := newClient(*brokers, *user, *pass, kgo.DefaultProduceTopic(topic))
	if err != nil {
		return err
	}
	defer cl.Close()

	send := func(value string) error {
		r := &kgo.Record{Topic: topic, Value: []byte(value)}
		if *key != "" {
			r.Key = []byte(*key)
		}
		return cl.ProduceSync(ctx, r).FirstErr()
	}

	n := 0
	if fs.NArg() > 1 {
		for _, v := range fs.Args()[1:] {
			if err := send(v); err != nil {
				return produceError(*brokers, err)
			}
			n++
		}
	} else {
		sc := bufio.NewScanner(os.Stdin)
		sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
		for sc.Scan() {
			if err := send(sc.Text()); err != nil {
				return produceError(*brokers, err)
			}
			n++
		}
		if err := sc.Err(); err != nil {
			return err
		}
	}
	fmt.Fprintf(os.Stderr, "produced %d record(s) to %s\n", n, topic)
	return nil
}

func produceError(brokers string, err error) error {
	return fmt.Errorf("producing to %s failed: %w\n"+
		"  Is the broker running?  kafka-wire serve\n"+
		"  Right address?          --brokers host:port  (or KAFKA_WIRE_BROKERS)\n"+
		"  Behind NAT or Docker?   set listeners.advertisedhost on the broker",
		brokers, err)
}

func cmdConsume(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("consume", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, `kafka-wire consume - read records from a topic

  kafka-wire consume demo --from-beginning
  kafka-wire consume demo --group workers
  kafka-wire consume demo -n 10 --format json

FLAGS
`)
		fs.PrintDefaults()
	}
	brokers, user, pass := clientFlags(fs)
	group := fs.String("group", "", "consumer group to join (omit to read without a group)")
	fromStart := fs.Bool("from-beginning", false, "start at the oldest available record")
	max := fs.Int("n", 0, "stop after this many records (0 means keep reading)")
	format := fs.String("format", "value", "value, json, or hex")
	timeout := fs.Duration("timeout", 0, "stop after this long with no records (0 means wait forever)")
	if err := fs.Parse(permute(fs, args)); err != nil {
		return err
	}
	if fs.NArg() == 0 {
		return errors.New("kafka-wire consume: name a topic\n  kafka-wire consume demo --from-beginning")
	}
	topic := fs.Arg(0)

	opts := []kgo.Opt{kgo.ConsumeTopics(topic)}
	if *group != "" {
		opts = append(opts, kgo.ConsumerGroup(*group))
	}
	if *fromStart {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	}
	cl, err := newClient(*brokers, *user, *pass, opts...)
	if err != nil {
		return err
	}
	defer cl.Close()

	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()

	seen := 0
	for {
		pollCtx := ctx
		var cancel context.CancelFunc
		if *timeout > 0 {
			pollCtx, cancel = context.WithTimeout(ctx, *timeout)
		}
		fetches := cl.PollFetches(pollCtx)
		if cancel != nil {
			cancel()
		}
		if fetches.IsClientClosed() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			if *timeout > 0 && errors.Is(errs[0].Err, context.DeadlineExceeded) {
				out.Flush()
				return nil
			}
			if errors.Is(errs[0].Err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("consuming from %s: %w", *brokers, errs[0].Err)
		}
		var stop bool
		fetches.EachRecord(func(r *kgo.Record) {
			if stop {
				return
			}
			writeRecord(out, r, *format)
			seen++
			if *max > 0 && seen >= *max {
				stop = true
			}
		})
		out.Flush()
		if stop {
			return nil
		}
	}
}

// writeRecord prints a record without assuming its value is text. A broker
// that carries Avro, Protobuf, or an image must not have its own CLI mangle
// the bytes on the way out.
func writeRecord(w io.Writer, r *kgo.Record, format string) {
	switch format {
	case "json":
		fmt.Fprintf(w, `{"topic":%q,"partition":%d,"offset":%d,"timestamp":%q,"key":%q,"value":%q}`+"\n",
			r.Topic, r.Partition, r.Offset, r.Timestamp.UTC().Format(time.RFC3339Nano),
			string(r.Key), previewValue(r.Value))
	case "hex":
		fmt.Fprintf(w, "%s\n", hex.EncodeToString(r.Value))
	default:
		if utf8.Valid(r.Value) {
			fmt.Fprintf(w, "%s\n", r.Value)
			return
		}
		fmt.Fprintf(w, "<%d binary bytes: %s...>\n", len(r.Value), hex.EncodeToString(firstN(r.Value, 16)))
	}
}

func previewValue(b []byte) string {
	if utf8.Valid(b) {
		return string(b)
	}
	return "0x" + hex.EncodeToString(b)
}

func firstN(b []byte, n int) []byte {
	if len(b) < n {
		return b
	}
	return b[:n]
}

func cmdTopic(ctx context.Context, args []string) error {
	const topicUsage = `kafka-wire topic - manage topics

  topic list
  topic create NAME [-partitions N]
  topic describe NAME
  topic delete NAME
`
	if len(args) == 0 {
		fmt.Print(topicUsage)
		return nil
	}
	sub := args[0]
	fs := flag.NewFlagSet("topic "+sub, flag.ContinueOnError)
	brokers, user, pass := clientFlags(fs)
	partitions := fs.Int("partitions", 1, "partition count for create")
	if err := fs.Parse(permute(fs, args[1:])); err != nil {
		return err
	}
	cl, err := newClient(*brokers, *user, *pass)
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	switch sub {
	case "list", "ls":
		td, err := adm.ListTopics(ctx)
		if err != nil {
			return fmt.Errorf("listing topics on %s: %w", *brokers, err)
		}
		if len(td) == 0 {
			fmt.Println("no topics yet. Create one:  kafka-wire topic create demo")
			return nil
		}
		fmt.Printf("%-40s %s\n", "TOPIC", "PARTITIONS")
		for _, t := range td.Sorted() {
			fmt.Printf("%-40s %d\n", t.Topic, len(t.Partitions))
		}
	case "create":
		if fs.NArg() == 0 {
			return errors.New("kafka-wire topic create: name a topic")
		}
		resp, err := adm.CreateTopics(ctx, int32(*partitions), 1, nil, fs.Args()...)
		if err != nil {
			return fmt.Errorf("creating topics on %s: %w", *brokers, err)
		}
		for _, r := range resp.Sorted() {
			if r.Err != nil {
				return fmt.Errorf("creating %s: %w", r.Topic, r.Err)
			}
			fmt.Printf("created %s with %d partition(s)\n", r.Topic, *partitions)
		}
	case "describe":
		if fs.NArg() == 0 {
			return errors.New("kafka-wire topic describe: name a topic")
		}
		td, err := adm.ListTopics(ctx, fs.Args()...)
		if err != nil {
			return err
		}
		starts, _ := adm.ListStartOffsets(ctx, fs.Args()...)
		ends, _ := adm.ListEndOffsets(ctx, fs.Args()...)
		for _, t := range td.Sorted() {
			fmt.Printf("%s\n", t.Topic)
			fmt.Printf("  %-10s %-14s %-14s %s\n", "PARTITION", "START", "END", "RECORDS")
			for _, p := range t.Partitions.Sorted() {
				s, _ := starts.Lookup(t.Topic, p.Partition)
				e, _ := ends.Lookup(t.Topic, p.Partition)
				fmt.Printf("  %-10d %-14d %-14d %d\n", p.Partition, s.Offset, e.Offset, e.Offset-s.Offset)
			}
		}
	case "delete", "rm":
		if fs.NArg() == 0 {
			return errors.New("kafka-wire topic delete: name a topic")
		}
		resp, err := adm.DeleteTopics(ctx, fs.Args()...)
		if err != nil {
			return err
		}
		for _, r := range resp.Sorted() {
			if r.Err != nil {
				return fmt.Errorf("deleting %s: %w", r.Topic, r.Err)
			}
			fmt.Printf("deleted %s\n", r.Topic)
		}
	default:
		return fmt.Errorf("kafka-wire topic: unknown subcommand %q\n\n%s", sub, topicUsage)
	}
	return nil
}
