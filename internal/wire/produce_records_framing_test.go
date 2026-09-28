package wire

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestKmsgProduceV9RoundTripDocumentsTheRecordsFraming(t *testing.T) {
	batch := make([]byte, 61)
	batch[16] = 2

	req := kmsg.NewPtrProduceRequest()
	req.SetVersion(9)
	req.Acks = 1
	req.TimeoutMillis = 1000
	tp := kmsg.ProduceRequestTopic{Topic: "t"}
	tp.Partitions = append(tp.Partitions, kmsg.ProduceRequestTopicPartition{
		Partition: 0,
		Records:   batch,
	})
	req.Topics = append(req.Topics, tp)

	enc := req.AppendTo(nil)
	t.Logf("encoded v9 request: %d bytes", len(enc))
	t.Logf("last 70 bytes: % x", enc[len(enc)-70:])

	// Find the records length prefix: it is the byte immediately before the
	// batch, which is the final 61 bytes of the frame.
	prefix := enc[len(enc)-62]
	t.Logf("byte before the batch = %d (len+1 would be %d)", prefix, len(batch)+1)

	back := kmsg.NewPtrProduceRequest()
	back.SetVersion(9)
	if err := back.ReadFrom(enc); err != nil {
		t.Fatalf("kmsg could not read back its own v9 encoding: %v", err)
	}
	got := back.Topics[0].Partitions[0].Records
	t.Logf("records round-tripped as %d bytes, want %d", len(got), len(batch))
	if len(got) != len(batch) {
		t.Fatalf("records round-tripped as %d bytes, want %d", len(got), len(batch))
	}
}
