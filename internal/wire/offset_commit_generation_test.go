package wire

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// Committed offsets belong to the group's CURRENT generation.
//
// OffsetCommit carries the Generation and MemberID of the member issuing it
// (v1+, and kmsg defaults Generation to -1, which the protocol defines as
// "store offsets only, no generation validation"). A broker that accepts the
// commit without checking them lets a member of an OLD generation overwrite
// what the current owner has committed. That member is not a hypothetical: it
// is a consumer that was evicted by the fast liveness sweep or by a rebalance
// it has not noticed yet, and it is still holding the offsets it read before
// the partitions moved. When it commits them, the group's stored offsets go
// backwards, and the next restart or reassignment resumes from the rewound
// position and reprocesses everything in between. Kafka refuses that commit
// with ILLEGAL_GENERATION; a consumer-group-compatible broker has to as well.
//
// These tests drive the REAL handlers over a real connection, like the rest of
// this package: the error code asserted is the one that actually leaves the
// broker, not one a test built by hand.

const commitTestTopic = "orders.events"

// joinGroupOnce runs one real JoinGroup and returns the decoded response. It
// deliberately does not assert on the error code: the first join of a member
// with no id answers MEMBER_ID_REQUIRED by design (KIP-394), and the caller is
// the one that knows which of the two shapes it expects.
func joinGroupOnce(t *testing.T, d *Dispatcher, group, memberID string) *kmsg.JoinGroupResponse {
	t.Helper()
	req := kmsg.NewPtrJoinGroupRequest()
	req.Group = group
	req.MemberID = memberID
	req.SessionTimeoutMillis = 10_000
	req.RebalanceTimeoutMillis = 10_000
	req.ProtocolType = "consumer"
	req.Protocols = []kmsg.JoinGroupRequestProtocol{{Name: "range", Metadata: []byte("subscription")}}
	resp, _ := exchange(t, d, int16(kmsg.JoinGroup), 4, req)
	got, ok := resp.(*kmsg.JoinGroupResponse)
	if !ok {
		t.Fatalf("JoinGroup answered %T, want *kmsg.JoinGroupResponse", resp)
	}
	return got
}

// syncGroupAs drives a real SyncGroup for the group's leader and returns the
// error code, so a test can assert on the coordinator's answer rather than
// assume the sync succeeded.
func syncGroupAs(t *testing.T, d *Dispatcher, group, memberID string, generation int32) int16 {
	t.Helper()
	req := kmsg.NewPtrSyncGroupRequest()
	req.Group = group
	req.Generation = generation
	req.MemberID = memberID
	req.GroupAssignment = []kmsg.SyncGroupRequestGroupAssignment{
		{MemberID: memberID, MemberAssignment: []byte("assignment")},
	}
	resp, _ := exchange(t, d, int16(kmsg.SyncGroup), 4, req)
	got, ok := resp.(*kmsg.SyncGroupResponse)
	if !ok {
		t.Fatalf("SyncGroup answered %T, want *kmsg.SyncGroupResponse", resp)
	}
	return got.ErrorCode
}

// commitOffsetsAs issues one real OffsetCommit and returns the error code the
// client would read for partition 0.
func commitOffsetsAs(t *testing.T, d *Dispatcher, group, memberID string, generation int32, offset int64) int16 {
	t.Helper()
	req := kmsg.NewPtrOffsetCommitRequest()
	req.Group = group
	req.MemberID = memberID
	req.Generation = generation
	req.Topics = []kmsg.OffsetCommitRequestTopic{{
		Topic: commitTestTopic,
		Partitions: []kmsg.OffsetCommitRequestTopicPartition{
			{Partition: 0, Offset: offset, Metadata: stringPtr("")},
		},
	}}
	resp, _ := exchange(t, d, int16(kmsg.OffsetCommit), 6, req)
	got, ok := resp.(*kmsg.OffsetCommitResponse)
	if !ok {
		t.Fatalf("OffsetCommit answered %T, want *kmsg.OffsetCommitResponse", resp)
	}
	if len(got.Topics) != 1 || len(got.Topics[0].Partitions) != 1 {
		t.Fatalf("OffsetCommit response lost the requested partition: %+v", got.Topics)
	}
	return got.Topics[0].Partitions[0].ErrorCode
}

// TestOffsetCommitRejectsAStaleGeneration is the regression. It walks a real
// single-member group through two generations, commits at each, and then
// commits at the OLD generation — the zombie member — and asserts both that
// the answer is ILLEGAL_GENERATION and that the stored offset did not move.
func TestOffsetCommitRejectsAStaleGeneration(t *testing.T) {
	d := testDispatcher(t)
	const group = "workers"

	// A member with no id is issued one, then rejoins with it.
	issued := joinGroupOnce(t, d, group, "")
	if issued.ErrorCode != errCodeMemberIDRequired {
		t.Fatalf("first JoinGroup error = %d, want MEMBER_ID_REQUIRED (%d)", issued.ErrorCode, errCodeMemberIDRequired)
	}
	member := issued.MemberID
	if member == "" {
		t.Fatal("the coordinator issued an empty member id")
	}

	gen1 := joinGroupOnce(t, d, group, member)
	if gen1.ErrorCode != errCodeNone {
		t.Fatalf("second JoinGroup error = %d, want 0", gen1.ErrorCode)
	}
	if gen1.Generation != 1 {
		t.Fatalf("first settled generation = %d, want 1", gen1.Generation)
	}
	if code := syncGroupAs(t, d, group, member, gen1.Generation); code != errCodeNone {
		t.Fatalf("SyncGroup error = %d, want 0", code)
	}

	// The current owner's commit must be accepted: the fix may only reject
	// commits that are actually stale.
	if code := commitOffsetsAs(t, d, group, member, gen1.Generation, 500); code != errCodeNone {
		t.Fatalf("the current member's commit was refused with error %d", code)
	}

	// The same member rejoins, which settles a new generation and puts the
	// group into CompletingRebalance until the leader syncs.
	gen2 := joinGroupOnce(t, d, group, member)
	if gen2.ErrorCode != errCodeNone {
		t.Fatalf("rejoin error = %d, want 0", gen2.ErrorCode)
	}
	if gen2.Generation != 2 {
		t.Fatalf("second settled generation = %d, want 2", gen2.Generation)
	}

	// Mid-rebalance: the assignment for this generation does not exist yet,
	// so a commit for it cannot be honored.
	if code := commitOffsetsAs(t, d, group, member, gen2.Generation, 700); code != errCodeRebalanceInProgress {
		t.Errorf("commit during CompletingRebalance = %d, want REBALANCE_IN_PROGRESS (%d)", code, errCodeRebalanceInProgress)
	}

	// The zombie: still holding generation 1, committing the offsets it read
	// before the rebalance. Accepting this is what rewinds the group.
	if code := commitOffsetsAs(t, d, group, member, gen1.Generation, 100); code != errCodeIllegalGeneration {
		t.Errorf("stale-generation commit = %d, want ILLEGAL_GENERATION (%d)", code, errCodeIllegalGeneration)
	}

	// The leader syncs generation 2, so the current generation may commit.
	if code := syncGroupAs(t, d, group, member, gen2.Generation); code != errCodeNone {
		t.Fatalf("SyncGroup for generation 2 error = %d, want 0", code)
	}
	if code := commitOffsetsAs(t, d, group, member, gen2.Generation, 900); code != errCodeNone {
		t.Fatalf("the current generation's commit was refused with error %d", code)
	}

	// A member id the group does not know cannot commit for it either. This is
	// checked with the group stable on purpose: mid-rebalance Kafka answers
	// REBALANCE_IN_PROGRESS before it ever looks at the member id, so asking
	// this question during CompletingRebalance would pin the wrong rule.
	if code := commitOffsetsAs(t, d, group, "worker-not-in-this-group", gen2.Generation, 100); code != errCodeUnknownMember {
		t.Errorf("unknown-member commit = %d, want UNKNOWN_MEMBER (%d)", code, errCodeUnknownMember)
	}

	// Nothing refused above may have touched the stored offset: 900 is the
	// only commit that was accepted, and the rewinding ones were not.
	stored := d.brk.Offsets().FetchOffset(group, commitTestTopic, 0)
	if stored.Offset != 900 {
		t.Fatalf("stored offset = %d, want 900; a refused commit rewound the group", stored.Offset)
	}
}

// TestOffsetCommitOffsetsOnlyFormNeedsAnEmptyGroup pins the other half of the
// protocol rule. kmsg documents Generation -1 with an empty member id as "the
// group is being used to store offsets only": admin tools and offset-store
// clients send exactly that, and it must keep working. Against a group that
// has live members it must not, because there is no member whose generation it
// could be checked against — Kafka answers UNKNOWN_MEMBER, and so does this
// broker.
func TestOffsetCommitOffsetsOnlyFormNeedsAnEmptyGroup(t *testing.T) {
	d := testDispatcher(t)

	// No members yet: an offsets-only commit is the group's only owner and
	// must be stored.
	if code := commitOffsetsAs(t, d, "offsets.only", "", -1, 7); code != errCodeNone {
		t.Fatalf("offsets-only commit against an empty group = %d, want 0", code)
	}
	if got := d.brk.Offsets().FetchOffset("offsets.only", commitTestTopic, 0).Offset; got != 7 {
		t.Fatalf("offsets-only commit stored %d, want 7", got)
	}

	// The same form against a group that has a live member: refused, because
	// it would overwrite that member's offsets with no generation to check.
	// The member has to complete the join to be registered; a first JoinGroup
	// that answers MEMBER_ID_REQUIRED does not add it to the group.
	const group = "with.members"
	issued := joinGroupOnce(t, d, group, "")
	if issued.MemberID == "" {
		t.Fatal("the coordinator issued an empty member id")
	}
	if joined := joinGroupOnce(t, d, group, issued.MemberID); joined.ErrorCode != errCodeNone {
		t.Fatalf("JoinGroup error = %d, want 0", joined.ErrorCode)
	}
	if code := commitOffsetsAs(t, d, group, "", -1, 3); code != errCodeUnknownMember {
		t.Errorf("offsets-only commit against a live group = %d, want UNKNOWN_MEMBER (%d)", code, errCodeUnknownMember)
	}
}
