// Package e2e drives a real kafka-wire process with a real, independent Kafka
// client and asserts what a user actually cares about: that the bytes they put
// in are the bytes they get out, whatever those bytes are.
//
// The broker is started as a subprocess of the compiled binary, not wired up
// in-process, so the configuration layer, the listener, the banner and the
// shutdown path are all exercised the way a user would exercise them.
//
// Run with:  go test ./e2e/
package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	buildOnce sync.Once
	binPath   string
	buildErr  error
)

// buildBinary compiles the real command once per test run.
func buildBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "kafka-wire-e2e-bin")
		if err != nil {
			buildErr = err
			return
		}
		name := "kafka-wire"
		if runtime.GOOS == "windows" {
			name += ".exe"
		}
		binPath = filepath.Join(dir, name)
		cmd := exec.Command("go", "build", "-o", binPath, "./cmd/kafka-wire")
		cmd.Dir = ".."
		out, err := cmd.CombinedOutput()
		if err != nil {
			buildErr = fmt.Errorf("building the broker: %v\n%s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return binPath
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

type brokerProc struct {
	addr string
	cmd  *exec.Cmd
	out  *bytes.Buffer
}

// startBroker launches the binary and waits until the Kafka port answers.
func startBroker(t *testing.T, extraEnv ...string) *brokerProc {
	t.Helper()
	bin := buildBinary(t)
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	dataDir := t.TempDir()

	cmd := exec.Command(bin, "serve")
	cmd.Env = append(os.Environ(),
		"KAFKA_WIRE_LISTENERS_KAFKA="+addr,
		fmt.Sprintf("KAFKA_WIRE_LISTENERS_ADMIN=127.0.0.1:%d", freePort(t)),
		"KAFKA_WIRE_STORAGE_DATADIR="+dataDir,
		"KAFKA_WIRE_LOG_LEVEL=warn",
	)
	cmd.Env = append(cmd.Env, extraEnv...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting the broker: %v", err)
	}

	bp := &brokerProc{addr: addr, cmd: cmd, out: &out}
	t.Cleanup(func() { bp.stop(t) })

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 300*time.Millisecond)
		if err == nil {
			c.Close()
			return bp
		}
		if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
			t.Fatalf("the broker exited before it was ready:\n%s", out.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("the broker did not open %s within 30s:\n%s", addr, out.String())
	return nil
}

func (b *brokerProc) stop(t *testing.T) {
	if b.cmd.Process != nil {
		_ = b.cmd.Process.Kill()
		_, _ = b.cmd.Process.Wait()
	}
}

func newClient(t *testing.T, addr string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	base := []kgo.Opt{
		kgo.SeedBrokers(addr),
		// The broker implements no transaction coordinator, so it advertises
		// no InitProducerId. Modern clients default idempotent writes on, and
		// this is the one line every user needs. It is documented in the
		// README compatibility section for exactly that reason.
		kgo.DisableIdempotentWrite(),
		kgo.RetryTimeout(20 * time.Second),
	}
	cl, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func createTopic(t *testing.T, cl *kgo.Client, topic string, partitions int32) {
	t.Helper()
	adm := kadm.NewClient(cl)
	resp, err := adm.CreateTopics(context.Background(), partitions, 1, nil, topic)
	if err != nil {
		t.Fatalf("creating %s: %v", topic, err)
	}
	for _, r := range resp.Sorted() {
		if r.Err != nil && !strings.Contains(r.Err.Error(), "already exists") {
			t.Fatalf("creating %s: %v", topic, r.Err)
		}
	}
}

// ---------------------------------------------------------------------------
// The payload matrix
// ---------------------------------------------------------------------------

// payloadCase is one shape of data a user might put on a topic. The broker is
// supposed to be indifferent to all of them: keys, values and headers are
// opaque bytes, and anything else would couple it to a serialization format.
type payloadCase struct {
	name  string
	key   []byte
	value []byte
	// headers exercise the metadata channel, which carries arbitrary bytes
	// too and is where CloudEvents and schema-registry ids actually live.
	headers []kgo.RecordHeader
}

func payloadCases(t *testing.T) []payloadCase {
	t.Helper()
	big := make([]byte, 900*1024)
	if _, err := rand.Read(big); err != nil {
		t.Fatal(err)
	}
	allBytes := make([]byte, 256)
	for i := range allBytes {
		allBytes[i] = byte(i)
	}

	return []payloadCase{
		{name: "utf8_text", key: []byte("k1"), value: []byte("a plain line of text")},
		{name: "json", key: []byte("order-1"), value: []byte(`{"id":1,"total":19.99,"tags":["a","b"]}`)},
		{name: "json_lines", value: []byte("{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n")},
		// Confluent's Avro framing: a zero magic byte then a 4-byte big-endian
		// schema id, then the Avro body. The leading NUL is the interesting
		// part: anything that treats values as C strings truncates here.
		{name: "avro_confluent_framing", key: []byte("k"), value: append([]byte{0x00, 0x00, 0x00, 0x00, 0x2A}, 0x02, 0x06, 'a', 'b', 'c')},
		// Protobuf message-index prefix, same trap.
		{name: "protobuf_with_index", value: append([]byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x00}, 0x08, 0x96, 0x01)},
		{name: "msgpack", value: []byte{0x82, 0xa1, 0x61, 0x01, 0xa1, 0x62, 0x02}},
		{name: "cbor", value: []byte{0xa2, 0x61, 0x61, 0x01, 0x61, 0x62, 0x02}},
		// A PNG header: binary with a NUL and high bytes, and the shape of
		// "someone is shipping images through this".
		{name: "png_header", value: []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D}},
		{name: "gzip_blob", value: []byte{0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xff}},
		{name: "every_byte_0_to_255", key: allBytes, value: allBytes},
		{name: "embedded_nulls", value: []byte("before\x00middle\x00after")},
		{name: "invalid_utf8", value: []byte{0xff, 0xfe, 0xfd, 0xc0, 0x80}},
		{name: "unicode_astral", value: []byte("emoji and CJK: \xf0\x9f\x9a\x80 \xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e")},
		{name: "empty_value", key: []byte("k"), value: []byte{}},
		// A null value is a tombstone in a compacted topic. It must stay
		// distinguishable from an empty value.
		{name: "null_value", key: []byte("tombstone"), value: nil},
		{name: "null_key", key: nil, value: []byte("no key at all")},
		{name: "large_900kb", value: big},
		{name: "headers_binary", key: []byte("h"), value: []byte("body"), headers: []kgo.RecordHeader{
			{Key: "content-type", Value: []byte("application/octet-stream")},
			{Key: "trace-id", Value: []byte{0x00, 0x01, 0x02, 0xff}},
			{Key: "empty", Value: []byte{}},
		}},
	}
}

// TestPayloadNeutrality is the proof that the broker does not care what is on
// the wire. Every shape goes in, and every shape must come back byte for byte,
// with its key, headers and null-versus-empty distinction intact.
func TestPayloadNeutrality(t *testing.T) {
	b := startBroker(t, "KAFKA_WIRE_LIMITS_MAXREQUESTBYTES=8MiB")
	cl := newClient(t, b.addr)
	const topic = "payload.matrix"
	createTopic(t, cl, topic, 1)

	cases := payloadCases(t)
	ctx := context.Background()

	for _, c := range cases {
		r := &kgo.Record{Topic: topic, Key: c.key, Value: c.value, Headers: c.headers}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatalf("producing %s: %v", c.name, err)
		}
	}

	consumer := newClient(t, b.addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)

	got := make([]*kgo.Record, 0, len(cases))
	deadline := time.Now().Add(60 * time.Second)
	for len(got) < len(cases) && time.Now().Before(deadline) {
		pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		f := consumer.PollFetches(pollCtx)
		cancel()
		if errs := f.Errors(); len(errs) > 0 && !errors.Is(errs[0].Err, context.DeadlineExceeded) {
			t.Fatalf("fetch: %v", errs[0].Err)
		}
		f.EachRecord(func(r *kgo.Record) { got = append(got, r) })
	}
	if len(got) != len(cases) {
		t.Fatalf("produced %d records but consumed %d", len(cases), len(got))
	}

	for i, c := range cases {
		r := got[i]
		t.Run(c.name, func(t *testing.T) {
			if !bytes.Equal(r.Key, c.key) {
				t.Errorf("key mismatch: sent %d bytes, got %d", len(c.key), len(r.Key))
			}
			if !bytes.Equal(r.Value, c.value) {
				t.Errorf("value mismatch: sent %d bytes, got %d", len(c.value), len(r.Value))
			}
			// nil and empty are different things: one is a tombstone.
			if (c.value == nil) != (r.Value == nil) {
				t.Errorf("null-versus-empty value distinction lost: sent nil=%v, got nil=%v", c.value == nil, r.Value == nil)
			}
			if (c.key == nil) != (r.Key == nil) {
				t.Errorf("null-versus-empty key distinction lost: sent nil=%v, got nil=%v", c.key == nil, r.Key == nil)
			}
			if len(r.Headers) != len(c.headers) {
				t.Fatalf("header count: sent %d, got %d", len(c.headers), len(r.Headers))
			}
			for j, h := range c.headers {
				if r.Headers[j].Key != h.Key || !bytes.Equal(r.Headers[j].Value, h.Value) {
					t.Errorf("header %d mismatch: sent %s=%x, got %s=%x", j, h.Key, h.Value, r.Headers[j].Key, r.Headers[j].Value)
				}
			}
		})
	}
}

// TestCompressionCodecs covers the four codecs Kafka clients negotiate. The
// broker stores record batches verbatim and never decompresses them, so the
// real assertion is that a codec it does not understand still round-trips.
func TestCompressionCodecs(t *testing.T) {
	b := startBroker(t)
	codecs := map[string]kgo.CompressionCodec{
		"none":   kgo.NoCompression(),
		"gzip":   kgo.GzipCompression(),
		"snappy": kgo.SnappyCompression(),
		"lz4":    kgo.Lz4Compression(),
		"zstd":   kgo.ZstdCompression(),
	}
	for name, codec := range codecs {
		t.Run(name, func(t *testing.T) {
			topic := "codec." + name
			admin := newClient(t, b.addr)
			createTopic(t, admin, topic, 1)

			payload := bytes.Repeat([]byte("compress me, i am extremely repetitive. "), 500)
			p := newClient(t, b.addr, kgo.ProducerBatchCompression(codec))
			if err := p.ProduceSync(context.Background(), &kgo.Record{
				Topic: topic, Key: []byte("k"), Value: payload,
			}).FirstErr(); err != nil {
				t.Fatalf("producing with %s: %v", name, err)
			}

			c := newClient(t, b.addr, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			f := c.PollFetches(ctx)
			if errs := f.Errors(); len(errs) > 0 {
				t.Fatalf("fetch %s: %v", name, errs[0].Err)
			}
			recs := f.Records()
			if len(recs) != 1 {
				t.Fatalf("%s: got %d records, want 1", name, len(recs))
			}
			if !bytes.Equal(recs[0].Value, payload) {
				t.Errorf("%s: value did not survive the round trip", name)
			}
		})
	}
}

// TestConsumerGroupRoundTrip covers the join/sync/heartbeat/commit lifecycle
// and, more importantly, that a restarted consumer resumes where it left off
// rather than replaying or skipping.
func TestConsumerGroupRoundTrip(t *testing.T) {
	b := startBroker(t)
	const topic, group = "group.demo", "workers"
	admin := newClient(t, b.addr)
	createTopic(t, admin, topic, 1)

	ctx := context.Background()
	p := newClient(t, b.addr)
	for i := 0; i < 10; i++ {
		if err := p.ProduceSync(ctx, &kgo.Record{
			Topic: topic, Value: []byte(fmt.Sprintf("record-%d", i)),
		}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	// First consumer reads five and commits.
	c1, err := kgo.NewClient(
		kgo.SeedBrokers(b.addr), kgo.DisableIdempotentWrite(),
		kgo.ConsumeTopics(topic), kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		t.Fatal(err)
	}
	var first []string
	var toCommit []*kgo.Record
	for len(first) < 5 {
		pctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		f := c1.PollFetches(pctx)
		cancel()
		if errs := f.Errors(); len(errs) > 0 && !errors.Is(errs[0].Err, context.DeadlineExceeded) {
			t.Fatal(errs[0].Err)
		}
		f.EachRecord(func(r *kgo.Record) {
			if len(first) < 5 {
				first = append(first, string(r.Value))
				toCommit = append(toCommit, r)
			}
		})
	}
	// CommitRecords, not MarkCommitRecords: marking only feeds franz-go's
	// AutoCommitMarks mode, and this client has autocommit disabled, so marks
	// would be collected and never sent.
	if err := c1.CommitRecords(ctx, toCommit...); err != nil {
		t.Fatalf("committing offsets: %v", err)
	}
	c1.Close()

	// A second member of the same group must pick up at record-5.
	c2 := newClient(t, b.addr,
		kgo.ConsumeTopics(topic), kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	var second []string
	deadline := time.Now().Add(30 * time.Second)
	for len(second) < 5 && time.Now().Before(deadline) {
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		f := c2.PollFetches(pctx)
		cancel()
		f.EachRecord(func(r *kgo.Record) { second = append(second, string(r.Value)) })
	}
	if len(second) != 5 {
		t.Fatalf("after committing 5 of 10, the group resumed with %d records: %v", len(second), second)
	}
	if second[0] != "record-5" {
		t.Errorf("group resumed at %q, want record-5; committed offsets were not honored", second[0])
	}
}

// TestRecordsSurviveRestart is the durability claim a user cares about most:
// stop the broker, start it again, and the data is still there at the same
// offsets.
func TestRecordsSurviveRestart(t *testing.T) {
	bin := buildBinary(t)
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	dataDir := t.TempDir()
	env := append(os.Environ(),
		"KAFKA_WIRE_LISTENERS_KAFKA="+addr,
		fmt.Sprintf("KAFKA_WIRE_LISTENERS_ADMIN=127.0.0.1:%d", freePort(t)),
		"KAFKA_WIRE_STORAGE_DATADIR="+dataDir,
		"KAFKA_WIRE_LOG_LEVEL=warn",
	)

	run := func() *exec.Cmd {
		cmd := exec.Command(bin, "serve")
		cmd.Env = env
		var out bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &out
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			if c, err := net.DialTimeout("tcp", addr, 300*time.Millisecond); err == nil {
				c.Close()
				return cmd
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("broker never came up:\n%s", out.String())
		return nil
	}

	const topic = "durable.topic"
	proc := run()
	cl := newClient(t, addr)
	createTopic(t, cl, topic, 1)
	ctx := context.Background()
	want := []string{"alpha", "beta", "gamma"}
	for _, v := range want {
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte(v)}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	cl.Close()
	_ = proc.Process.Kill()
	_, _ = proc.Process.Wait()

	proc2 := run()
	t.Cleanup(func() { _ = proc2.Process.Kill(); _, _ = proc2.Process.Wait() })

	c := newClient(t, addr, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	var got []string
	deadline := time.Now().Add(30 * time.Second)
	for len(got) < len(want) && time.Now().Before(deadline) {
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		f := c.PollFetches(pctx)
		cancel()
		f.EachRecord(func(r *kgo.Record) { got = append(got, string(r.Value)) })
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("after restart got %v, want %v", got, want)
	}
}

// TestOversizedRecordIsRejectedClearly checks the failure path a user hits
// when they raise their producer's batch size past the broker's limit. The
// requirement is that it fails, rather than silently truncating.
func TestOversizedRecordIsRejectedClearly(t *testing.T) {
	b := startBroker(t, "KAFKA_WIRE_LIMITS_MAXREQUESTBYTES=1MiB")
	const topic = "too.big"
	admin := newClient(t, b.addr)
	createTopic(t, admin, topic, 1)

	huge := make([]byte, 4*1024*1024)
	if _, err := rand.Read(huge); err != nil {
		t.Fatal(err)
	}
	// The broker refuses the frame before it knows which API it belongs to,
	// and closes the connection, which is what Apache Kafka does when a
	// request exceeds socket.request.max.bytes. The client then retries until
	// it gives up, so this asserts refusal rather than a specific error, and
	// keeps the window short because the retry loop is the slow part.
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	p := newClient(t, b.addr, kgo.ProducerBatchMaxBytes(8*1024*1024), kgo.RetryTimeout(5*time.Second))
	err := p.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: huge}).FirstErr()
	if err == nil {
		t.Fatal("a record above limits.maxrequestbytes was accepted; it must be refused, not truncated")
	}
	t.Logf("oversized record correctly refused: %v", err)
}
