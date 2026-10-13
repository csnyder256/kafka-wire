package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestDescribeGroupsReportsMemberAssignment drives a real consumer through
// join, sync and fetch and then asks a real admin client the question
// DescribeGroups exists to answer: which member owns which partition.
//
// The broker used to answer that question with member rows whose assignment
// and subscription were empty, so the group looked like one whose members own
// nothing. The check here is deliberately on the decoded assignment, not on
// the member list: a member list with no assignment is exactly the broken
// output, and it is silently wrong rather than an error.
func TestDescribeGroupsReportsMemberAssignment(t *testing.T) {
	b := startBroker(t)
	const (
		topic = "dg.topic"
		group = "dg.group"
	)
	admin := newClient(t, b.addr)
	createTopic(t, admin, topic, 1)

	ctx := context.Background()

	// One record before the consumer starts, so consuming it proves the
	// member has finished join, sync and fetch rather than merely connected.
	p := newClient(t, b.addr)
	if err := p.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("hello")}).FirstErr(); err != nil {
		t.Fatalf("producing: %v", err)
	}

	c := newClient(t, b.addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)

	got := 0
	deadline := time.Now().Add(30 * time.Second)
	for got < 1 && time.Now().Before(deadline) {
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		f := c.PollFetches(pctx)
		cancel()
		got += len(f.Records())
	}
	if got != 1 {
		t.Fatalf("the consumer read %d records, want 1; it never completed join/sync", got)
	}

	adm := kadm.NewClient(newClient(t, b.addr))
	described, err := adm.DescribeGroups(ctx, group)
	if err != nil {
		t.Fatalf("describing %s: %v", group, err)
	}
	dg, ok := described[group]
	if !ok {
		t.Fatalf("group %s missing from the DescribeGroups response: %+v", group, described)
	}
	if dg.Err != nil {
		t.Fatalf("group %s: %v", group, dg.Err)
	}
	if dg.State != "Stable" {
		t.Fatalf("group state = %q, want Stable", dg.State)
	}
	if len(dg.Members) != 1 {
		t.Fatalf("group reported %d member(s), want 1: %+v", len(dg.Members), dg.Members)
	}

	m := dg.Members[0]
	assignment, ok := m.Assigned.AsConsumer()
	if !ok {
		t.Fatalf("the member's assignment did not decode as a consumer assignment: %#v", m.Assigned)
	}
	if !assignmentHas(assignment, topic, 0) {
		t.Fatalf("DescribeGroups reported no assignment of %s/0 to the only member; "+
			"\"which member owns which partition\" is unanswerable: %+v", topic, assignment.Topics)
	}

	// The subscription is the other half of the member row: it is what the
	// member asked for, and a client showing member detail reads it too.
	sub, ok := m.Join.AsConsumer()
	if !ok {
		t.Fatalf("the member's subscription did not decode as a consumer subscription: %#v", m.Join)
	}
	if !subscriptionHas(sub, topic) {
		t.Errorf("the member's subscription does not name %s: %+v", topic, sub.Topics)
	}
}

func assignmentHas(a *kmsg.ConsumerMemberAssignment, topic string, partition int32) bool {
	for _, at := range a.Topics {
		if at.Topic != topic {
			continue
		}
		for _, p := range at.Partitions {
			if p == partition {
				return true
			}
		}
	}
	return false
}

func subscriptionHas(s *kmsg.ConsumerMemberMetadata, topic string) bool {
	for _, st := range s.Topics {
		if st == topic {
			return true
		}
	}
	return false
}
