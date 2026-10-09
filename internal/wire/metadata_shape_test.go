package wire

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// These tests pin the three shapes of the Metadata topic array, because the
// handler used to collapse two of them into one. The array became nullable in
// v1: a null array means "every topic", and an empty (non-null) array means
// "no topics at all". The handler tested `len(req.Topics) == 0`, which is true
// for both, so a client that deliberately asked for no topics got the entire
// topic namespace back.
//
// That is not hypothetical. franz-go's broker-only metadata refresh
// (Client.fetchBrokerMetadata, behind Ping and the periodic broker reload)
// sends exactly `Topics: []kmsg.MetadataRequestTopic{}` with the comment
// "brokers only, no opinion on topics", and its storeCachedMeta has a matching
// guard for `req.Topics != nil && len(req.Topics) == 0`. Apache Kafka spells
// the rule out in MetadataRequest.isAllTopics:
//
//	topics == null || (topics.isEmpty() && version == 0)
//
// v0 is the one exception: its array is not nullable, so an empty array there
// still means "all topics". The handler must not fix the v1+ case by breaking
// v0.
//
// The tests drive the real handler over a real connection through exchange()
// and assert on the decoded bytes, matching the shape tests in
// protocol_shape_test.go.

// createTopic registers a topic on the broker the dispatcher drives, so a
// metadata response has something real to include or omit.
func createTopic(t *testing.T, d *Dispatcher, name string) {
	t.Helper()
	if _, err := d.brk.CreateTopic(name, 1, 1); err != nil {
		t.Fatalf("creating topic %s: %v", name, err)
	}
}

// topicNames flattens a Metadata response's topic list to names.
func topicNames(resp *kmsg.MetadataResponse) []string {
	out := make([]string, 0, len(resp.Topics))
	for _, tp := range resp.Topics {
		if tp.Topic != nil {
			out = append(out, *tp.Topic)
		}
	}
	return out
}

// An empty (non-null) topic array is a request for no topics. Returning the
// whole namespace makes a broker-only refresh carry every topic and partition
// on the broker.
func TestMetadataEmptyTopicListReturnsNoTopics(t *testing.T) {
	d := testDispatcher(t)
	createTopic(t, d, "orders.events")
	createTopic(t, d, "billing.events")

	req := kmsg.NewPtrMetadataRequest()
	req.Topics = []kmsg.MetadataRequestTopic{} // non-null, empty: "no topics"

	resp, _ := exchange(t, d, int16(kmsg.Metadata), 9, req)
	got := resp.(*kmsg.MetadataResponse)

	if names := topicNames(got); len(names) != 0 {
		t.Fatalf("an empty (non-null) topic list returned %d topic(s) %v; the array is nullable from v1, so empty means no topics, not all of them",
			len(names), names)
	}
	// The broker list must survive: this is the shape franz-go's broker-only
	// refresh sends, and it still needs the brokers.
	if len(got.Brokers) == 0 {
		t.Fatalf("the response carried no brokers; a broker-only metadata request still needs them")
	}
}

// A null topic array is the "all topics" form every list-topics client sends
// (kcat -L, kadm.ListTopics, AdminClient.listTopics). It must keep returning
// everything.
func TestMetadataNullTopicListReturnsEveryTopic(t *testing.T) {
	d := testDispatcher(t)
	createTopic(t, d, "orders.events")
	createTopic(t, d, "billing.events")

	req := kmsg.NewPtrMetadataRequest()
	req.Topics = nil // null: "all topics"

	resp, _ := exchange(t, d, int16(kmsg.Metadata), 9, req)
	got := resp.(*kmsg.MetadataResponse)

	names := topicNames(got)
	if len(names) != 2 {
		t.Fatalf("a null topic list returned %d topic(s) %v, want both", len(names), names)
	}
	seen := map[string]bool{}
	for _, n := range names {
		seen[n] = true
	}
	if !seen["orders.events"] || !seen["billing.events"] {
		t.Fatalf("a null topic list returned %v, want orders.events and billing.events", names)
	}
}

// v0 has no null marker on the topic array, so an empty array there still means
// "all topics". Fixing the v1+ case must not regress this.
func TestMetadataVersionZeroEmptyListStillMeansAllTopics(t *testing.T) {
	d := testDispatcher(t)
	createTopic(t, d, "orders.events")

	req := kmsg.NewPtrMetadataRequest()
	req.Topics = []kmsg.MetadataRequestTopic{} // v0: empty means all

	resp, _ := exchange(t, d, int16(kmsg.Metadata), 0, req)
	got := resp.(*kmsg.MetadataResponse)

	if names := topicNames(got); len(names) != 1 || names[0] != "orders.events" {
		t.Fatalf("v0 with an empty topic list returned %v, want all topics (orders.events): v0's array is not nullable, so empty there means all topics",
			names)
	}
}

// The explicit-names path is the other half of the array's three shapes and
// must keep returning only what was asked for.
func TestMetadataNamedTopicReturnsOnlyThatTopic(t *testing.T) {
	d := testDispatcher(t)
	createTopic(t, d, "orders.events")
	createTopic(t, d, "billing.events")

	req := kmsg.NewPtrMetadataRequest()
	name := "billing.events"
	req.Topics = []kmsg.MetadataRequestTopic{{Topic: &name}}

	resp, _ := exchange(t, d, int16(kmsg.Metadata), 9, req)
	got := resp.(*kmsg.MetadataResponse)

	names := topicNames(got)
	if len(names) != 1 || names[0] != "billing.events" {
		t.Fatalf("a named request returned %v, want just billing.events", names)
	}
}
