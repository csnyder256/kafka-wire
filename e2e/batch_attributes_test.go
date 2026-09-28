package e2e

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// kafka-wire has no transaction coordinator, and the README says so in three
// places. The consequence people miss is what the broker must do when a client
// sends a batch that *claims* a transaction anyway: the attribute bits are the
// client's side of a bargain only a coordinator can keep.
//
// This file drives those batches over the wire and asserts the broker refuses
// them, because the alternative is worse than an error -- see the tests below.

const (

	// Batch attribute bits. Bit 4 says "this batch belongs to a transaction";
	// bit 5 says "this batch is a COMMIT/ABORT marker, not data".
	attrTransactional = 0x10
	attrControlBatch  = 0x20

	errCodeCorruptMessage int16 = 2
)

// TestTransactionalBatchIsRefused is the regression this file exists for.
//
// A batch with bit 4 set is uncommitted from the producer's point of view: it
// becomes visible only when the producer commits, and never visible if it
// aborts. kafka-wire used to store such a batch as ordinary data, count it in
// the high watermark, and answer a read_committed fetch with
// LastStableOffset == high watermark and an empty aborted-transaction list --
// that is, it told a read_committed consumer the records were committed and
// stable. Aborting the transaction afterwards could not take them back: the
// consumer already had them. Refusing the batch is the only honest option for a
// broker that cannot implement commit or abort.
func TestTransactionalBatchIsRefused(t *testing.T) {
	b := startBroker(t)
	topic := "txn.refused"
	createTopicRaw(t, b.addr, topic)

	res := produceBatch(t, b.addr, topic, attrTransactional, []byte("uncommittable"))
	if res.err != nil {
		t.Fatalf("produce request failed at the transport level: %v", res.err)
	}
	if res.errorCode == 0 {
		t.Fatalf("a batch with the transactional attribute bit was accepted (base offset %d); "+
			"this broker cannot commit or abort it, so a read_committed consumer will be handed records "+
			"the producer may still abort", res.baseOffset)
	}
	if res.errorCode != errCodeCorruptMessage {
		t.Errorf("transactional batch refused with error code %d, want %d (CORRUPT_MESSAGE)", res.errorCode, errCodeCorruptMessage)
	}
	if res.errMessage == "" {
		t.Error("the refusal carried no message; the operator is left to guess which bit was the problem")
	}
	t.Logf("transactional batch refused: code=%d message=%q", res.errorCode, res.errMessage)
}

// TestControlBatchIsRefused covers the other unreachable bit. A control batch
// is a transaction marker, not client data; storing one would index it, count it
// in offsets, and hand it to consumers as a record.
func TestControlBatchIsRefused(t *testing.T) {
	b := startBroker(t)
	topic := "control.refused"
	createTopicRaw(t, b.addr, topic)

	res := produceBatch(t, b.addr, topic, attrControlBatch, []byte("marker"))
	if res.err != nil {
		t.Fatalf("produce request failed at the transport level: %v", res.err)
	}
	if res.errorCode == 0 {
		t.Fatalf("a transaction control batch was accepted at offset %d; it is a marker, not data", res.baseOffset)
	}
	t.Logf("control batch refused: code=%d message=%q", res.errorCode, res.errMessage)
}

// TestRefusedBatchLeavesThePartitionUntouched is the part that makes the error
// code trustworthy. A Produce response carries one error code per partition, so
// a client that reads a failure there concludes nothing from that request
// landed and is entitled to retry the whole batch. If an earlier batch of the
// same request had been written first, that retry would duplicate it.
func TestRefusedBatchLeavesThePartitionUntouched(t *testing.T) {
	b := startBroker(t)
	topic := "atomic.refusal"
	createTopicRaw(t, b.addr, topic)

	// One legal batch followed by a transactional one, in a single request.
	good := buildTaggedBatch(0, []byte("legitimate"))
	bad := buildTaggedBatch(attrTransactional, []byte("poison"))
	res := produceTwoBatches(t, b.addr, topic, good, bad)
	if res.err != nil {
		t.Fatalf("produce request failed at the transport level: %v", res.err)
	}
	if res.errorCode == 0 {
		t.Fatal("the request was accepted even though it contained a transactional batch")
	}

	// Nothing may be readable: the partition must look empty.
	got := readAll(t, b.addr, topic)
	if len(got) != 0 {
		t.Fatalf("a refused request left %v readable; a client retrying the batch would duplicate it", got)
	}

	// And a clean batch still works, at offset 0.
	ok := produceBatch(t, b.addr, topic, 0, []byte("after"))
	if ok.err != nil || ok.errorCode != 0 {
		t.Fatalf("the partition stopped accepting valid writes after a refusal: err=%v code=%d", ok.err, ok.errorCode)
	}
	if ok.baseOffset != 0 {
		t.Fatalf("the first accepted batch landed at offset %d, want 0 -- the refused request consumed offsets", ok.baseOffset)
	}
}

// TestReadCommittedIsNotPromisedStability guards the reporting side. With no
// coordinator, the broker may only claim stability for records that really are
// ordinary data -- which is exactly what it can say once transactional batches
// are refused. This pins that the fields a read_committed consumer reads are
// still reported (LastStableOffset present and equal to the high watermark),
// so the refusal above did not break the read path while fixing the lie.
func TestReadCommittedReportsStableOrdinaryData(t *testing.T) {
	b := startBroker(t)
	topic := "read.committed"
	createTopicRaw(t, b.addr, topic)

	if res := produceBatch(t, b.addr, topic, 0, []byte("plain")); res.errorCode != 0 {
		t.Fatalf("plain batch refused: code=%d %s", res.errorCode, res.errMessage)
	}

	cl := newClient(t, b.addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	t.Cleanup(cl.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var got []string
	deadline := time.Now().Add(20 * time.Second)
	for len(got) == 0 && time.Now().Before(deadline) {
		pctx, pcancel := context.WithTimeout(ctx, 3*time.Second)
		f := cl.PollFetches(pctx)
		pcancel()
		f.EachRecord(func(r *kgo.Record) { got = append(got, string(r.Value)) })
	}
	if len(got) != 1 || got[0] != "plain" {
		t.Fatalf("read_committed fetch returned %v, want [plain]", got)
	}
}

// ---------------------------------------------------------------------------
// Wire helpers. These build requests by hand so the attribute bits are exactly
// what the test set, with nothing between the test and the bytes.

type produceResult struct {
	errorCode  int16
	baseOffset int64
	errMessage string
	err        error
}

func produceBatch(t *testing.T, addr, topic string, attrs int16, value []byte) produceResult {
	t.Helper()
	return produceTwoBatches(t, addr, topic, buildTaggedBatch(attrs, value))
}

// produceTwoBatches sends one Produce v9 request carrying the given batches for
// partition 0, and reports the partition's error code.
func produceTwoBatches(t *testing.T, addr, topic string, batches ...[]byte) produceResult {
	t.Helper()
	var records []byte
	for _, b := range batches {
		records = append(records, b...)
	}

	// Compact array lengths are stored as (count + 1), not count.
	var p []byte
	p = append(p, 0) // null transactional id (compact nullable string)
	p = appendInt16(p, 1)
	p = appendInt32(p, 5000)
	p = appendUVarint(p, 2) // topics (1 topic)
	p = appendCompactString(p, topic)
	p = appendUVarint(p, 2) // partitions (1 partition)
	p = appendInt32(p, 0)
	p = appendUVarint(p, uint32(len(records))+1)
	p = append(p, records...)
	p = append(p, 0) // partition tagged fields
	p = append(p, 0) // topic tagged fields
	p = append(p, 0) // request tagged fields

	body, err := rawRequest(addr, apiKeyProduce, 9, p)
	if err != nil {
		return produceResult{err: err}
	}
	return decodeProduceV9(t, body)
}

func decodeProduceV9(t *testing.T, body []byte) produceResult {
	t.Helper()
	if len(body) < 2 {
		return produceResult{err: fmt.Errorf("produce response too short: %d bytes", len(body))}
	}
	// ProduceResponse v8+ body:
	//   [compact responses][topic: name, compact partitions]
	//   [partition: index(int32), error_code(int16), base_offset(int64),
	//               log_append_time(int64), log_start_offset(int64),
	//               compact record_errors, error_message(compact nullable)]
	//   [throttle_ms][tags]
	// Index precedes ErrorCode -- reading them the other way around makes a
	// healthy response look like a partition-2 failure.
	pos := 1
	if pos >= len(body) {
		return produceResult{err: fmt.Errorf("produce response truncated at the responses array")}
	}
	nameLen, w := uvarintAt(body, pos)
	pos += w + int(nameLen) - 1
	if pos >= len(body) {
		return produceResult{err: fmt.Errorf("produce response truncated after the topic name")}
	}
	pos++ // compact partitions array length
	if pos+6 > len(body) {
		return produceResult{err: fmt.Errorf("produce response truncated before the partition entry")}
	}
	pos += 4 // partition index
	res := produceResult{errorCode: int16(binary.BigEndian.Uint16(body[pos : pos+2]))}
	pos += 2
	if pos+8 <= len(body) {
		res.baseOffset = int64(binary.BigEndian.Uint64(body[pos : pos+8]))
	}
	pos += 8 + 8 // base_offset, log_append_time_ms
	if pos+8 <= len(body) {
		pos += 8 // log_start_offset
	}
	if pos < len(body) {
		n, w2 := uvarintAt(body, pos)
		pos += w2 + int(n) - 1 // compact record_errors
	}
	if pos < len(body) {
		l, w3 := uvarintAt(body, pos)
		pos += w3
		if l > 1 && pos+int(l)-2 <= len(body) {
			res.errMessage = string(body[pos : pos+int(l)-2])
		}
	}
	return res
}

// readAll reads partition 0 from offset 0 and returns the record values it can
// see. It uses a real client so a stored batch is reported the way a consumer
// would actually receive it.
func readAll(t *testing.T, addr, topic string) []string {
	t.Helper()
	cl := newClient(t, addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	t.Cleanup(cl.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	var got []string
	pctx, pcancel := context.WithTimeout(ctx, 3*time.Second)
	f := cl.PollFetches(pctx)
	pcancel()
	f.EachRecord(func(r *kgo.Record) { got = append(got, string(r.Value)) })
	return got
}

// createTopicRaw makes a topic with a raw v6 CreateTopics request, so this file
// needs no client library for setup.
func createTopicRaw(t *testing.T, addr, topic string) {
	t.Helper()
	var b []byte
	b = appendUVarint(b, 2) // compact array of 1 topic
	b = appendCompactString(b, topic)
	b = appendInt32(b, 1)    // num_partitions
	b = appendInt16(b, 1)    // replication_factor
	b = appendUVarint(b, 1)  // assignments (compact array, empty)
	b = appendUVarint(b, 1)  // configs (compact array, empty)
	b = appendInt32(b, 5000) // timeout_ms
	b = append(b, 0)         // validate_only (v1+)
	b = appendUVarint(b, 1)  // topic-level tagged fields (empty)
	b = append(b, 0)         // request-level tagged fields (empty)

	body, err := rawRequest(addr, apiKeyCreateTopics, 6, b)
	if err != nil {
		t.Fatalf("CreateTopics(%s): %v", topic, err)
	}
	if len(body) < 2 {
		t.Fatalf("CreateTopics response too short: %d bytes", len(body))
	}
	// The response leads with [throttle_ms][topics]; the topic error code is
	// inside the first entry, not at the front of the body.
	if code, err := firstCreateTopicsError(body); err == nil && code != 0 {
		t.Fatalf("CreateTopics(%s) returned error code %d", topic, code)
	}
}

// firstCreateTopicsError walks [throttle_ms][compact topics][tags] far enough to
// read the first topic's error code.
func firstCreateTopicsError(body []byte) (int16, error) {
	if len(body) < 5 {
		return 0, fmt.Errorf("too short")
	}
	pos := 4 // throttle_ms
	count := int(body[pos]) - 1
	pos++
	if count < 1 {
		return 0, fmt.Errorf("no topics in the response")
	}
	nameLen, w := uvarintAt(body, pos)
	pos += w + int(nameLen) - 1
	if pos+2 > len(body) {
		return 0, fmt.Errorf("truncated before the error code")
	}
	return int16(binary.BigEndian.Uint16(body[pos : pos+2])), nil
}

// buildTaggedBatch assembles one legal magic-2 batch with a single record and
// the requested attribute bits.
func buildTaggedBatch(attrs int16, value []byte) []byte {
	inner := []byte{}
	inner = append(inner, 0)                       // record attributes
	inner = appendVarint(inner, 0)                 // timestampDelta
	inner = appendVarint(inner, 0)                 // offsetDelta
	inner = appendVarint(inner, -1)                // keyLength (null)
	inner = appendVarint(inner, int64(len(value))) // valueLength
	inner = append(inner, value...)
	inner = appendVarint(inner, 0) // headersCount

	rec := appendVarint(nil, int64(len(inner)))
	rec = append(rec, inner...)

	body := []byte{}
	body = appendInt16(body, attrs)
	body = appendInt32(body, 0)  // lastOffsetDelta
	body = appendInt64(body, 0)  // firstTimestamp
	body = appendInt64(body, 0)  // maxTimestamp
	body = appendInt64(body, -1) // producerId
	body = appendInt16(body, -1) // producerEpoch
	body = appendInt32(body, -1) // baseSequence
	body = appendInt32(body, 1)  // recordCount
	body = append(body, rec...)

	header := []byte{}
	header = appendInt64(header, 0)                  // baseOffset
	header = appendInt32(header, 49+int32(len(rec))) // batchLength
	header = appendInt32(header, -1)                 // partitionLeaderEpoch
	header = append(header, 2)                       // magic
	header = binary.BigEndian.AppendUint32(header, crc32.Checksum(body, crc32.MakeTable(crc32.Castagnoli)))
	return append(header, body...)
}
