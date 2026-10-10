package wire

import (
	"encoding/binary"
	"testing"

	"github.com/csnyder256/kafka-wire/internal/storage"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// These tests drive the REAL Fetch handler over a real connection and decode
// the bytes it actually writes, the same way protocol_shape_test.go does for
// the coordinator APIs. The point is the wire value, not a hand-built struct:
// a struct the test fills in proves what kmsg does with a correctly-shaped
// value, not what handleFetch sends.

// buildTestBatch hand-builds one valid v2 record batch covering recordCount
// records. The broker parses only the batch header (the records themselves are
// opaque), so the tail bytes are arbitrary as long as the CRC over
// Attributes-onward matches.
func buildTestBatch(t *testing.T, recordCount int, ts int64) []byte {
	t.Helper()
	tail := []byte{0x01, 0x02, 0x03, 0x04}
	batchLen := storage.MinBatchSize - 12 + len(tail)
	buf := make([]byte, 12+batchLen)
	binary.BigEndian.PutUint64(buf[0:8], 0)                       // BaseOffset (rewritten on append)
	binary.BigEndian.PutUint32(buf[8:12], uint32(batchLen))       // BatchLength
	binary.BigEndian.PutUint32(buf[12:16], 0)                     // PartitionLeaderEpoch
	buf[16] = 2                                                   // Magic
	binary.BigEndian.PutUint16(buf[21:23], 0)                     // Attributes: no codec, no transaction
	binary.BigEndian.PutUint32(buf[23:27], uint32(recordCount-1)) // LastOffsetDelta
	binary.BigEndian.PutUint64(buf[27:35], uint64(ts))            // FirstTimestamp
	binary.BigEndian.PutUint64(buf[35:43], uint64(ts+10))         // MaxTimestamp
	binary.BigEndian.PutUint64(buf[43:51], ^uint64(0))            // ProducerID unset
	binary.BigEndian.PutUint16(buf[51:53], 0xFFFF)                // ProducerEpoch unset
	binary.BigEndian.PutUint32(buf[53:57], 0xFFFFFFFF)            // BaseSequence unset
	binary.BigEndian.PutUint32(buf[57:61], uint32(recordCount))   // RecordCount
	copy(buf[61:], tail)
	crc := storage.CRC32C(buf[21:]) // CRC covers Attributes onward
	binary.BigEndian.PutUint32(buf[17:21], crc)
	return buf
}

// fetchRequest builds a single-topic, single-partition Fetch request that
// returns immediately (MinBytes 0 means "do not long-poll").
func fetchRequest(topic string, partition int32, offset int64) *kmsg.FetchRequest {
	req := kmsg.NewPtrFetchRequest()
	req.MinBytes = 0
	req.MaxWaitMillis = 0
	req.Topics = []kmsg.FetchRequestTopic{{
		Topic: topic,
		Partitions: []kmsg.FetchRequestTopicPartition{{
			Partition:         partition,
			FetchOffset:       offset,
			PartitionMaxBytes: 1 << 20,
		}},
	}}
	return req
}

// TestFetchErrorResponseKeepsProtocolSentinels is the Fetch error-path
// regression.
//
// FetchResponseTopicPartition.Default() seeds PreferredReadReplica,
// LastStableOffset and LogStartOffset to -1, the protocol's "none / unknown"
// markers. The handler built its partitions with a bare struct literal and
// never applied Default(), and the error branch sets only the error code and
// the watermark fields. So a failed fetch -- an unknown topic, an offset past
// the retention horizon, a denied ACL -- answered a Fetch v11 client with
// PreferredReadReplica 0, which is broker id 0, not "no preference". A client
// that reads it stops surfacing the error and redirects the partition to a
// broker that does not exist in a single-node cluster whose broker id is 1, so
// the consumer retries forever while the broker log shows every fetch
// answered. The success path set the field by hand and was already correct;
// this pins the error path so the two cannot drift apart again.
func TestFetchErrorResponseKeepsProtocolSentinels(t *testing.T) {
	d := testDispatcher(t)

	req := fetchRequest("no.such.topic", 0, 0)

	// v11 is the top of the advertised Fetch range and the first version that
	// carries PreferredReadReplica at all.
	resp, _ := exchange(t, d, int16(kmsg.Fetch), 11, req)
	got, ok := resp.(*kmsg.FetchResponse)
	if !ok {
		t.Fatalf("handler returned %T, want *kmsg.FetchResponse", resp)
	}
	if len(got.Topics) != 1 || len(got.Topics[0].Partitions) != 1 {
		t.Fatalf("shape lost: topics=%+v", got.Topics)
	}
	p := got.Topics[0].Partitions[0]
	if p.ErrorCode != errCodeUnknownTopicOrPart {
		t.Fatalf("error code = %d, want %d (UNKNOWN_TOPIC_OR_PARTITION)", p.ErrorCode, errCodeUnknownTopicOrPart)
	}
	if p.PreferredReadReplica != -1 {
		t.Errorf("error response PreferredReadReplica = %d, want -1 (\"no preference\"); 0 is broker 0, so a v11 client redirects to a nonexistent broker instead of surfacing the error", p.PreferredReadReplica)
	}
	if p.LastStableOffset != -1 {
		t.Errorf("error response LastStableOffset = %d, want -1 (the protocol's unknown marker)", p.LastStableOffset)
	}
}

// The success path already reported -1 by hand. Now that the field comes from
// the constructor instead, this keeps the two halves tied together: a good
// fetch must still report the real high watermark as LastStableOffset (not the
// constructor's -1), and must still refuse to nominate a preferred replica.
func TestFetchSuccessResponseReportsHighWatermarkAndNoPreferredReplica(t *testing.T) {
	d := testDispatcher(t)

	if _, err := d.brk.EnsureTopic("orders.events", 1); err != nil {
		t.Fatalf("creating a topic: %v", err)
	}
	batch := buildTestBatch(t, 3, 1_700_000_000_000)
	if _, err := d.brk.AppendBatches("orders.events", 0, [][]byte{batch}); err != nil {
		t.Fatalf("appending a batch: %v", err)
	}

	resp, _ := exchange(t, d, int16(kmsg.Fetch), 11, fetchRequest("orders.events", 0, 0))
	got, ok := resp.(*kmsg.FetchResponse)
	if !ok {
		t.Fatalf("handler returned %T, want *kmsg.FetchResponse", resp)
	}
	if len(got.Topics) != 1 || len(got.Topics[0].Partitions) != 1 {
		t.Fatalf("shape lost: topics=%+v", got.Topics)
	}
	p := got.Topics[0].Partitions[0]
	if p.ErrorCode != errCodeNone {
		t.Fatalf("error code = %d, want 0", p.ErrorCode)
	}
	if p.HighWatermark != 3 {
		t.Errorf("high watermark = %d, want 3", p.HighWatermark)
	}
	if p.LastStableOffset != p.HighWatermark {
		t.Errorf("LastStableOffset = %d, want the high watermark %d; a read_committed consumer must see committed data as stable", p.LastStableOffset, p.HighWatermark)
	}
	if p.PreferredReadReplica != -1 {
		t.Errorf("PreferredReadReplica = %d, want -1 (\"no preference\")", p.PreferredReadReplica)
	}
}
