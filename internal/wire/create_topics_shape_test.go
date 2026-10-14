package wire

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// These tests drive the REAL handleCreateTopics over a real connection and
// assert on the response bytes kmsg decodes, the same way the OffsetFetch and
// ListOffsets shape tests do.
//
// CreateTopics had no coverage, and its handler rewrote a partition or
// replication count it could not honor into 1 instead of answering the error
// Kafka answers for it. The topic that resulted existed, so the request did
// something; it just was not the thing the client asked for, and nothing said
// so. `kafka-wire topic create demo -partitions 0` printed "created demo with
// 0 partition(s)" over a one-partition topic.

// createTopicsRequest builds the single-topic request shape a client sends.
func createTopicsRequest(topic string, partitions int32, replication int16) *kmsg.CreateTopicsRequest {
	req := kmsg.NewPtrCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = topic
	rt.NumPartitions = partitions
	rt.ReplicationFactor = replication
	req.Topics = append(req.Topics, rt)
	return req
}

// createTopicsTopic runs the handler at v6 (the highest advertised, so the
// response carries NumPartitions/ReplicationFactor) and returns the one topic
// entry.
func createTopicsTopic(t *testing.T, d *Dispatcher, req *kmsg.CreateTopicsRequest) kmsg.CreateTopicsResponseTopic {
	t.Helper()
	resp, _ := exchange(t, d, int16(kmsg.CreateTopics), 6, req)
	got, ok := resp.(*kmsg.CreateTopicsResponse)
	if !ok {
		t.Fatalf("handler returned %T, want *kmsg.CreateTopicsResponse", resp)
	}
	if len(got.Topics) != 1 {
		t.Fatalf("response carried %d topic(s), want 1: %+v", len(got.Topics), got.Topics)
	}
	return got.Topics[0]
}

// A partition count of 0 is not "the broker default": -1 is (KIP-464), and
// Kafka answers INVALID_PARTITIONS for any other non-positive value. The
// handler clamped 0 to 1, so the client was told the topic it asked for was
// created when a different topic was.
func TestCreateTopicsRejectsZeroPartitions(t *testing.T) {
	d := testDispatcher(t)

	got := createTopicsTopic(t, d, createTopicsRequest("orders.events", 0, 1))
	if got.ErrorCode != errCodeInvalidPartitions {
		t.Errorf("numPartitions 0 answered error code %d, want %d (INVALID_PARTITIONS); "+
			"the broker must not substitute a shape the client did not ask for",
			got.ErrorCode, errCodeInvalidPartitions)
	}
	if tp := d.brk.Topics().Get("orders.events"); tp != nil {
		t.Errorf("a request for 0 partitions created %q with %d partition(s)",
			tp.Name(), tp.NumPartitions())
	}
}

// Same rule for the replication factor: -1 means the broker default, anything
// else non-positive is INVALID_REPLICATION_FACTOR.
func TestCreateTopicsRejectsZeroReplicationFactor(t *testing.T) {
	d := testDispatcher(t)

	got := createTopicsTopic(t, d, createTopicsRequest("orders.events", 1, 0))
	if got.ErrorCode != errCodeInvalidReplicationFactor {
		t.Errorf("replicationFactor 0 answered error code %d, want %d (INVALID_REPLICATION_FACTOR)",
			got.ErrorCode, errCodeInvalidReplicationFactor)
	}
	if tp := d.brk.Topics().Get("orders.events"); tp != nil {
		t.Errorf("a request for replication factor 0 created %q anyway", tp.Name())
	}
}

// -1 is the protocol's "use the broker default", and it has to keep working:
// that is what a client following KIP-464 sends.
func TestCreateTopicsUsesBrokerDefaultsForMinusOne(t *testing.T) {
	d := testDispatcher(t)

	got := createTopicsTopic(t, d, createTopicsRequest("orders.events", -1, -1))
	if got.ErrorCode != errCodeNone {
		t.Fatalf("numPartitions -1 answered error code %d, want 0: -1 is the protocol's broker default", got.ErrorCode)
	}
	tp := d.brk.Topics().Get("orders.events")
	if tp == nil {
		t.Fatal("-1 did not create the topic")
	}
	if tp.NumPartitions() != 1 {
		t.Errorf("broker default produced %d partition(s), want 1", tp.NumPartitions())
	}
	if got.NumPartitions != 1 || got.ReplicationFactor != 1 {
		t.Errorf("response reported %d partition(s) and replication factor %d, want 1 and 1",
			got.NumPartitions, got.ReplicationFactor)
	}
}

// A validate-only request is a dry run of the real one, so it must answer what
// the real one would. The handler answered success unconditionally, which told
// a client pre-flighting a create that a name the broker rejects was fine.
func TestCreateTopicsValidateOnlyPredictsTheRealCreate(t *testing.T) {
	d := testDispatcher(t)

	// A name the create path refuses (it contains a path separator).
	bad := createTopicsRequest("orders/events", 1, 1)
	bad.ValidateOnly = true
	got := createTopicsTopic(t, d, bad)
	if got.ErrorCode != errCodeInvalidTopic {
		t.Errorf("validate-only for a name the broker rejects answered %d, want %d (INVALID_TOPIC): "+
			"a dry run that says yes to a create that will fail is worse than no dry run",
			got.ErrorCode, errCodeInvalidTopic)
	}
	if d.brk.Topics().Get("orders/events") != nil {
		t.Error("a validate-only request created the topic")
	}

	// A valid request must still validate cleanly, report the counts it would
	// use, and create nothing.
	ok := createTopicsRequest("orders.events", 3, 1)
	ok.ValidateOnly = true
	got = createTopicsTopic(t, d, ok)
	if got.ErrorCode != errCodeNone {
		t.Fatalf("validate-only for a valid request answered error code %d, want 0", got.ErrorCode)
	}
	if got.NumPartitions != 3 || got.ReplicationFactor != 1 {
		t.Errorf("validate-only reported %d partition(s) / replication factor %d, want 3 / 1",
			got.NumPartitions, got.ReplicationFactor)
	}
	if d.brk.Topics().Get("orders.events") != nil {
		t.Error("a validate-only request created the topic")
	}
}

// A response with no real count must not carry a fabricated 0: -1 is the
// protocol's "not reported" marker for these fields, and 0 reads as a real
// count of zero.
func TestCreateTopicsErrorPathsDoNotFabricateCounts(t *testing.T) {
	d := testDispatcher(t)

	if _, err := d.brk.CreateTopic("orders.events", 4, 1); err != nil {
		t.Fatalf("creating the topic up front: %v", err)
	}

	got := createTopicsTopic(t, d, createTopicsRequest("orders.events", 4, 1))
	if got.ErrorCode != errCodeTopicAlreadyExists {
		t.Fatalf("creating an existing topic answered error code %d, want %d",
			got.ErrorCode, errCodeTopicAlreadyExists)
	}
	if got.NumPartitions != -1 || got.ReplicationFactor != -1 {
		t.Errorf("error response reported %d partition(s) / replication factor %d, want -1 / -1 "+
			"(the protocol's not-reported marker, not a real-looking zero)",
			got.NumPartitions, got.ReplicationFactor)
	}
}
