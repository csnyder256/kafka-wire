package wire

import (
	"testing"

	"github.com/csnyder256/kafka-wire/internal/broker"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// OffsetFetch had no authorization check at all: any authenticated principal
// could read any group's committed offsets. That is not a small leak -- a
// group's offsets name the topics it reads, how many partitions each has, and
// how far the group has consumed them. docs/security.md states plainly that
// authorization is enforced on OffsetFetch, and the sibling write path
// (handleOffsetCommit) has enforced the group and topic grants all along, so
// the read path was the one that silently did not.
//
// These tests drive the real handler over a real connection with a real,
// configured ACL store and assert on the bytes it actually sends.

// aclDispatcher builds the standard in-memory broker and installs ACL
// principals. Any entry switches the store to "configured", after which a
// principal with no matching grant is denied.
func aclDispatcher(t *testing.T, principals ...broker.Principal) *Dispatcher {
	t.Helper()
	d := testDispatcher(t)
	acl := d.brk.ACL()
	if acl == nil {
		t.Fatal("broker has no ACL store")
	}
	for _, p := range principals {
		if err := acl.Upsert(p); err != nil {
			t.Fatalf("upserting principal %q: %v", p.Name, err)
		}
	}
	if !acl.Configured() {
		t.Fatal("ACL store is not configured after installing principals")
	}
	return d
}

// tenantPrincipal grants read+write on a topic prefix and read on a matching
// group prefix, the shape the chaos topology provisions.
func tenantPrincipal(name, prefix string) broker.Principal {
	return broker.Principal{
		Name:          name,
		TopicPrefixes: []broker.TopicPrefixACL{{Prefix: prefix, Ops: []string{broker.OpRead, broker.OpWrite}}},
		Groups:        []broker.GroupPrefixACL{{IDPrefix: prefix, Ops: []string{broker.OpRead}}},
	}
}

// assertNoCommittedOffsetLeak fails if any partition in either response shape
// carries the committed offset. The denial has to be real: an error code that
// rides along with the data still leaks the data.
func assertNoCommittedOffsetLeak(t *testing.T, resp *kmsg.OffsetFetchResponse, committed int64) {
	t.Helper()
	for _, topic := range resp.Topics {
		for _, p := range topic.Partitions {
			if p.Offset == committed {
				t.Fatalf("response leaked committed offset %d for %s/%d despite the denial",
					committed, topic.Topic, p.Partition)
			}
		}
	}
	for _, g := range resp.Groups {
		for _, topic := range g.Topics {
			for _, p := range topic.Partitions {
				if p.Offset == committed {
					t.Fatalf("response leaked committed offset %d for group %s %s/%d despite the denial",
						committed, g.Group, topic.Topic, p.Partition)
				}
			}
		}
	}
}

// The legacy (v0-v7) shape. A principal with no grant on the group must be
// told GROUP_AUTHORIZATION_FAILED and must not receive the offsets.
func TestOffsetFetchDeniesAPrincipalWithoutTheGroupGrant(t *testing.T) {
	d := aclDispatcher(t,
		tenantPrincipal("tenant-a", "tenant.a."),
		broker.Principal{Name: "outsider"},
	)
	if err := d.brk.Offsets().CommitOffset("tenant.a.workers", "tenant.a.orders", 0, 42, 0, ""); err != nil {
		t.Fatalf("committing an offset: %v", err)
	}

	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = "tenant.a.workers"
	req.Topics = []kmsg.OffsetFetchRequestTopic{{
		Topic:      "tenant.a.orders",
		Partitions: []int32{0},
	}}

	// v4 is non-flexible and carries the top-level error code (v2+).
	resp, _ := exchangeAs(t, d, "outsider", int16(kmsg.OffsetFetch), 4, req)
	got := resp.(*kmsg.OffsetFetchResponse)
	if got.ErrorCode != errCodeGroupAuthorizationFailed {
		t.Fatalf("outsider top-level error code = %d, want %d (GROUP_AUTHORIZATION_FAILED)",
			got.ErrorCode, errCodeGroupAuthorizationFailed)
	}
	assertNoCommittedOffsetLeak(t, got, 42)
}

// The null/absent topic list is the form kafka-consumer-groups.sh --describe
// and lag dashboards send. It must be denied just as the explicit form is, or
// the same principal reads the offsets through the other door.
func TestOffsetFetchDeniesTheAllTopicsFormWithoutTheGroupGrant(t *testing.T) {
	d := aclDispatcher(t,
		tenantPrincipal("tenant-a", "tenant.a."),
		broker.Principal{Name: "outsider"},
	)
	if err := d.brk.Offsets().CommitOffset("tenant.a.workers", "tenant.a.orders", 0, 42, 0, ""); err != nil {
		t.Fatalf("committing an offset: %v", err)
	}

	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = "tenant.a.workers" // no Topics: "everything this group committed"

	resp, _ := exchangeAs(t, d, "outsider", int16(kmsg.OffsetFetch), 4, req)
	got := resp.(*kmsg.OffsetFetchResponse)
	if got.ErrorCode != errCodeGroupAuthorizationFailed {
		t.Fatalf("outsider all-topics error code = %d, want %d (GROUP_AUTHORIZATION_FAILED)",
			got.ErrorCode, errCodeGroupAuthorizationFailed)
	}
	if len(got.Topics) != 0 {
		t.Fatalf("outsider all-topics response carried %d topic(s); a denied principal must learn nothing", len(got.Topics))
	}
	assertNoCommittedOffsetLeak(t, got, 42)
}

// A principal that may read the group but names a topic it has no grant for
// gets a per-partition TOPIC_AUTHORIZATION_FAILED with an unknown offset, not
// another tenant's committed position.
func TestOffsetFetchDeniesAnUnauthorizedTopic(t *testing.T) {
	d := aclDispatcher(t,
		tenantPrincipal("tenant-a", "tenant.a."),
	)
	if err := d.brk.Offsets().CommitOffset("tenant.a.workers", "tenant.b.secret", 0, 42, 0, ""); err != nil {
		t.Fatalf("committing an offset: %v", err)
	}

	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = "tenant.a.workers"
	req.Topics = []kmsg.OffsetFetchRequestTopic{{
		Topic:      "tenant.b.secret",
		Partitions: []int32{0},
	}}

	resp, _ := exchangeAs(t, d, "tenant-a", int16(kmsg.OffsetFetch), 4, req)
	got := resp.(*kmsg.OffsetFetchResponse)
	if len(got.Topics) != 1 || len(got.Topics[0].Partitions) != 1 {
		t.Fatalf("shape lost: %+v", got.Topics)
	}
	p := got.Topics[0].Partitions[0]
	if p.ErrorCode != errCodeTopicAuthorizationFailed {
		t.Errorf("partition error code = %d, want %d (TOPIC_AUTHORIZATION_FAILED)", p.ErrorCode, errCodeTopicAuthorizationFailed)
	}
	if p.Offset != -1 {
		t.Errorf("partition offset = %d, want -1 for a denied topic", p.Offset)
	}
	assertNoCommittedOffsetLeak(t, got, 42)
}

// The v8+ Groups shape carries the error per group instead of at the top
// level, so it needs its own check.
func TestOffsetFetchGroupShapeDeniesAnUnauthorizedGroup(t *testing.T) {
	d := aclDispatcher(t,
		tenantPrincipal("tenant-a", "tenant.a."),
		broker.Principal{Name: "outsider"},
	)
	if err := d.brk.Offsets().CommitOffset("tenant.a.workers", "tenant.a.orders", 0, 42, 0, ""); err != nil {
		t.Fatalf("committing an offset: %v", err)
	}

	req := kmsg.NewPtrOffsetFetchRequest()
	req.Groups = []kmsg.OffsetFetchRequestGroup{{
		Group: "tenant.a.workers",
		Topics: []kmsg.OffsetFetchRequestGroupTopic{{
			Topic:      "tenant.a.orders",
			Partitions: []int32{0},
		}},
	}}

	resp, _ := exchangeAs(t, d, "outsider", int16(kmsg.OffsetFetch), 9, req)
	got := resp.(*kmsg.OffsetFetchResponse)
	if len(got.Groups) != 1 {
		t.Fatalf("shape lost: %+v", got.Groups)
	}
	if got.Groups[0].ErrorCode != errCodeGroupAuthorizationFailed {
		t.Errorf("group error code = %d, want %d (GROUP_AUTHORIZATION_FAILED)",
			got.Groups[0].ErrorCode, errCodeGroupAuthorizationFailed)
	}
	assertNoCommittedOffsetLeak(t, got, 42)
}

// The positive control: the group's own principal still reads its committed
// offsets with no error. The fix must deny the outsider, not everybody.
func TestOffsetFetchServesTheGroupItsOwnOffsets(t *testing.T) {
	d := aclDispatcher(t, tenantPrincipal("tenant-a", "tenant.a."))
	if err := d.brk.Offsets().CommitOffset("tenant.a.workers", "tenant.a.orders", 0, 42, 0, ""); err != nil {
		t.Fatalf("committing an offset: %v", err)
	}

	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = "tenant.a.workers"
	req.Topics = []kmsg.OffsetFetchRequestTopic{{
		Topic:      "tenant.a.orders",
		Partitions: []int32{0},
	}}

	resp, _ := exchangeAs(t, d, "tenant-a", int16(kmsg.OffsetFetch), 4, req)
	got := resp.(*kmsg.OffsetFetchResponse)
	if got.ErrorCode != errCodeNone {
		t.Fatalf("authorized principal got top-level error code %d, want 0", got.ErrorCode)
	}
	if len(got.Topics) != 1 || len(got.Topics[0].Partitions) != 1 {
		t.Fatalf("shape lost: %+v", got.Topics)
	}
	p := got.Topics[0].Partitions[0]
	if p.ErrorCode != errCodeNone {
		t.Errorf("authorized partition error code = %d, want 0", p.ErrorCode)
	}
	if p.Offset != 42 {
		t.Errorf("authorized partition offset = %d, want 42", p.Offset)
	}
}
