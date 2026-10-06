package wire

import (
	"encoding/binary"
	"testing"

	"github.com/csnyder256/kafka-wire/internal/storage"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// These tests drive the REAL handleListOffsets over a real connection and
// assert on the bytes it actually wrote, decoded by kmsg at the negotiated
// version. ListOffsets had no coverage at all, and its handler built every
// response partition with a bare struct literal instead of kmsg's
// constructor, which is exactly the defect class the OffsetFetch tests below
// pin: the protocol's "-1 means unknown" defaults were never applied, and v0's
// alternate response field was never filled.

// listOffsetsRequest builds the single-topic, single-partition request shape
// every client sends.
func listOffsetsRequest(topic string, partition int32, ts int64) *kmsg.ListOffsetsRequest {
	req := kmsg.NewPtrListOffsetsRequest()
	req.Topics = []kmsg.ListOffsetsRequestTopic{{
		Topic: topic,
		Partitions: []kmsg.ListOffsetsRequestTopicPartition{{
			Partition: partition,
			Timestamp: ts,
		}},
	}}
	return req
}

// listOffsetsPartition decodes a response and returns its one partition entry.
func listOffsetsPartition(t *testing.T, resp kmsg.Response) kmsg.ListOffsetsResponseTopicPartition {
	t.Helper()
	got, ok := resp.(*kmsg.ListOffsetsResponse)
	if !ok {
		t.Fatalf("handler returned %T, want *kmsg.ListOffsetsResponse", resp)
	}
	if len(got.Topics) != 1 || len(got.Topics[0].Partitions) != 1 {
		t.Fatalf("shape lost: %+v", got.Topics)
	}
	return got.Topics[0].Partitions[0]
}

// listOffsetsBatch builds a one-record v2 record batch with a real CRC, the
// bytes a producer actually sends. ListOffsets reads only batch headers, but
// the append path validates magic and CRC, so the batch has to be genuinely
// valid or it is refused before it reaches the log.
func listOffsetsBatch(value []byte) []byte {
	inner := []byte{0} // record attributes
	inner = binary.AppendVarint(inner, 0)
	inner = binary.AppendVarint(inner, 0)
	inner = binary.AppendVarint(inner, -1) // null key
	inner = binary.AppendVarint(inner, int64(len(value)))
	inner = append(inner, value...)
	inner = binary.AppendVarint(inner, 0) // headers count

	rec := binary.AppendVarint(nil, int64(len(inner)))
	rec = append(rec, inner...)

	body := make([]byte, 0, 61)
	body = binary.BigEndian.AppendUint16(body, 0)          // attributes
	body = binary.BigEndian.AppendUint32(body, 0)          // last offset delta
	body = binary.BigEndian.AppendUint64(body, 0)          // first timestamp
	body = binary.BigEndian.AppendUint64(body, 0)          // max timestamp
	body = binary.BigEndian.AppendUint64(body, ^uint64(0)) // producer id -1
	body = binary.BigEndian.AppendUint16(body, 0xFFFF)     // producer epoch -1
	body = binary.BigEndian.AppendUint32(body, 0xFFFFFFFF) // base sequence -1
	body = binary.BigEndian.AppendUint32(body, 1)          // record count
	body = append(body, rec...)

	checksum := storage.CRC32C(body)
	batchLength := int32(4 + 1 + 4 + len(body))

	out := make([]byte, 0, 12+int(batchLength))
	out = binary.BigEndian.AppendUint64(out, 0) // base offset
	out = binary.BigEndian.AppendUint32(out, uint32(batchLength))
	out = binary.BigEndian.AppendUint32(out, 0xFFFFFFFF) // partition leader epoch -1
	out = append(out, 2)                                 // magic
	out = binary.BigEndian.AppendUint32(out, checksum)
	return append(out, body...)
}

// TestListOffsetsV0ReportsTheOffsetInOldStyleOffsets is the v0 regression.
//
// ListOffsets v0 answers with an ARRAY of offsets; the scalar Offset field
// only exists from v1. The handler built its partition entries with a bare
// struct literal and never filled OldStyleOffsets, so kmsg encoded a
// zero-length array and a v0 client read "no offset available" for an
// ordinary latest-offset query -- even though the broker advertises ListOffsets
// v0 and serves it. Filling the array from the same value the v1+ path uses
// fixes it; the encoder writes exactly one of the two fields per version.
func TestListOffsetsV0ReportsTheOffsetInOldStyleOffsets(t *testing.T) {
	d := testDispatcher(t)
	if _, err := d.brk.CreateTopic("orders.events", 1, 1); err != nil {
		t.Fatalf("creating topic: %v", err)
	}
	if _, err := d.brk.AppendBatches("orders.events", 0, [][]byte{listOffsetsBatch([]byte("hello"))}); err != nil {
		t.Fatalf("appending a record: %v", err)
	}

	latest, _ := exchange(t, d, int16(kmsg.ListOffsets), 0, listOffsetsRequest("orders.events", 0, -1))
	p := listOffsetsPartition(t, latest)
	if p.ErrorCode != errCodeNone {
		t.Fatalf("v0 latest error code = %d", p.ErrorCode)
	}
	if len(p.OldStyleOffsets) != 1 || p.OldStyleOffsets[0] != 1 {
		t.Fatalf("v0 latest OldStyleOffsets = %v, want [1]; an empty array is what a v0 client reads as \"no offset\"", p.OldStyleOffsets)
	}

	earliest, _ := exchange(t, d, int16(kmsg.ListOffsets), 0, listOffsetsRequest("orders.events", 0, -2))
	e := listOffsetsPartition(t, earliest)
	if len(e.OldStyleOffsets) != 1 || e.OldStyleOffsets[0] != 0 {
		t.Fatalf("v0 earliest OldStyleOffsets = %v, want [0]", e.OldStyleOffsets)
	}
}

// TestListOffsetsModernShapeUsesScalarOffsetAndUnknownEpoch pins the v1+ shape.
//
// v1-v3 carry the scalar Offset and must NOT grow an OldStyleOffsets array;
// v4+ additionally carry LeaderEpoch, which this broker has no per-record
// value for, so the honest answer is the protocol's -1 "unknown" marker
// rather than a fabricated 0. A consumer cannot tell a real epoch 0 from an
// invented one.
func TestListOffsetsModernShapeUsesScalarOffsetAndUnknownEpoch(t *testing.T) {
	d := testDispatcher(t)
	if _, err := d.brk.CreateTopic("orders.events", 1, 1); err != nil {
		t.Fatalf("creating topic: %v", err)
	}
	if _, err := d.brk.AppendBatches("orders.events", 0, [][]byte{listOffsetsBatch([]byte("hello"))}); err != nil {
		t.Fatalf("appending a record: %v", err)
	}

	for _, version := range []int16{1, 3, 4, 5} {
		resp, _ := exchange(t, d, int16(kmsg.ListOffsets), version, listOffsetsRequest("orders.events", 0, -1))
		p := listOffsetsPartition(t, resp)
		if p.ErrorCode != errCodeNone {
			t.Errorf("v%d error code = %d", version, p.ErrorCode)
		}
		if p.Offset != 1 {
			t.Errorf("v%d Offset = %d, want 1 (the latest offset after one record)", version, p.Offset)
		}
		if len(p.OldStyleOffsets) != 0 {
			t.Errorf("v%d carried %d OldStyleOffsets; that array does not exist from v1", version, len(p.OldStyleOffsets))
		}
		if version >= 4 && p.LeaderEpoch != -1 {
			t.Errorf("v%d LeaderEpoch = %d, want -1 (the protocol's unknown marker, not a fabricated epoch)", version, p.LeaderEpoch)
		}
	}
}

// TestListOffsetsErrorReportsUnknownMarkers covers the error path. Kafka
// reports offset -1 / timestamp -1 for a partition it could not answer for; a
// bare literal left them at 0, which reads as "offset 0 exists", so a consumer
// following the returned tuple would fetch from the very start of a topic it
// was just told did not exist.
func TestListOffsetsErrorReportsUnknownMarkers(t *testing.T) {
	d := testDispatcher(t)

	resp, _ := exchange(t, d, int16(kmsg.ListOffsets), 5, listOffsetsRequest("missing.topic", 0, -1))
	p := listOffsetsPartition(t, resp)
	if p.ErrorCode != errCodeUnknownTopicOrPart {
		t.Fatalf("error code = %d, want %d", p.ErrorCode, errCodeUnknownTopicOrPart)
	}
	if p.Offset != -1 || p.Timestamp != -1 || p.LeaderEpoch != -1 {
		t.Fatalf("error partition = offset %d timestamp %d epoch %d, want -1/-1/-1", p.Offset, p.Timestamp, p.LeaderEpoch)
	}
}
