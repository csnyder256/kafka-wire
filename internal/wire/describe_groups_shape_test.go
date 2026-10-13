package wire

import (
	"bytes"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// These tests drive the REAL JoinGroup, SyncGroup and DescribeGroups handlers
// over a real connection, in the same order a client drives them, and assert
// on the bytes DescribeGroups actually writes.
//
// DescribeGroups exists to answer "who is in this group and what does each
// member own". The handler reported MemberID/ClientID/ClientHost and left
// ProtocolMetadata and MemberAssignment empty, so the member rows named the
// members but attributed no partitions to any of them. That is the exact
// output `kafka-consumer-groups.sh --describe --members --verbose`,
// AdminClient.describeConsumerGroups and consumer-lag dashboards read, and it
// is silently wrong rather than an error: a group whose members own nothing
// looks like a group whose members own nothing.

// joinAndSync drives one member through JoinGroup and SyncGroup and returns
// the member id the coordinator assigned. The subscription and assignment are
// deliberately non-empty, distinct, and not valid protocol payloads: the
// broker never decodes either blob, so a test that only ever sees bytes it
// recognizes could not tell a faithful echo from a re-encode.
func joinAndSync(t *testing.T, d *Dispatcher, group string, subscription, assignment []byte) string {
	t.Helper()

	// v3: below the KIP-394 member-id handshake (v4+), so the coordinator
	// registers the member on the first join instead of answering
	// MEMBER_ID_REQUIRED and making the test rejoin.
	jreq := kmsg.NewPtrJoinGroupRequest()
	jreq.Group = group
	jreq.SessionTimeoutMillis = 10000
	// A short rebalance window: the join barrier waits it out, and a test
	// does not need the 3s production default to be correct.
	jreq.RebalanceTimeoutMillis = 100
	jreq.ProtocolType = "consumer"
	jreq.Protocols = []kmsg.JoinGroupRequestProtocol{
		{Name: "range", Metadata: subscription},
	}

	jresp, _ := exchange(t, d, int16(kmsg.JoinGroup), 3, jreq)
	jg, ok := jresp.(*kmsg.JoinGroupResponse)
	if !ok {
		t.Fatalf("JoinGroup returned %T, want *kmsg.JoinGroupResponse", jresp)
	}
	if jg.ErrorCode != errCodeNone {
		t.Fatalf("JoinGroup error code = %d, want 0", jg.ErrorCode)
	}
	if jg.MemberID == "" {
		t.Fatal("JoinGroup returned an empty member id")
	}
	if jg.LeaderID != jg.MemberID {
		t.Fatalf("the only member (%s) was not made leader (%s); no member could distribute assignments",
			jg.MemberID, jg.LeaderID)
	}

	// The leader distributes the assignment; every other member would
	// block here until it arrives.
	sreq := kmsg.NewPtrSyncGroupRequest()
	sreq.Group = group
	sreq.Generation = jg.Generation
	sreq.MemberID = jg.MemberID
	sreq.GroupAssignment = []kmsg.SyncGroupRequestGroupAssignment{
		{MemberID: jg.MemberID, MemberAssignment: assignment},
	}

	sresp, _ := exchange(t, d, int16(kmsg.SyncGroup), 3, sreq)
	sg, ok := sresp.(*kmsg.SyncGroupResponse)
	if !ok {
		t.Fatalf("SyncGroup returned %T, want *kmsg.SyncGroupResponse", sresp)
	}
	if sg.ErrorCode != errCodeNone {
		t.Fatalf("SyncGroup error code = %d, want 0", sg.ErrorCode)
	}
	if !bytes.Equal(sg.MemberAssignment, assignment) {
		t.Fatalf("SyncGroup handed the member %x, want %x; the coordinator never stored the assignment",
			sg.MemberAssignment, assignment)
	}
	return jg.MemberID
}

// TestDescribeGroupsReportsMemberSubscriptionAndAssignment is the regression:
// after a real join and sync, DescribeGroups must report the subscription the
// member joined with and the assignment its leader handed it.
func TestDescribeGroupsReportsMemberSubscriptionAndAssignment(t *testing.T) {
	d := testDispatcher(t)

	const group = "dg.members"
	subscription := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05}
	assignment := []byte{0x10, 0x11, 0x12, 0x13}

	memberID := joinAndSync(t, d, group, subscription, assignment)

	req := kmsg.NewPtrDescribeGroupsRequest()
	req.Groups = []string{group}

	resp, _ := exchange(t, d, int16(kmsg.DescribeGroups), 3, req)
	got, ok := resp.(*kmsg.DescribeGroupsResponse)
	if !ok {
		t.Fatalf("DescribeGroups returned %T, want *kmsg.DescribeGroupsResponse", resp)
	}
	if len(got.Groups) != 1 {
		t.Fatalf("DescribeGroups answered %d group(s), want 1", len(got.Groups))
	}
	g := got.Groups[0]
	if g.ErrorCode != errCodeNone {
		t.Fatalf("group error code = %d, want 0", g.ErrorCode)
	}
	if g.Group != group {
		t.Errorf("group = %q, want %q", g.Group, group)
	}
	if g.State != "Stable" {
		t.Errorf("state = %q, want Stable after a completed sync", g.State)
	}
	if g.ProtocolType != "consumer" {
		t.Errorf("protocol type = %q, want consumer", g.ProtocolType)
	}
	if g.Protocol != "range" {
		t.Errorf("protocol = %q, want the protocol the member joined with (range)", g.Protocol)
	}
	if len(g.Members) != 1 {
		t.Fatalf("group reported %d member(s), want 1", len(g.Members))
	}

	m := g.Members[0]
	if m.MemberID != memberID {
		t.Errorf("member id = %q, want %q", m.MemberID, memberID)
	}
	if !bytes.Equal(m.ProtocolMetadata, subscription) {
		t.Errorf("member protocol metadata = %x, want %x (the subscription it joined with)",
			m.ProtocolMetadata, subscription)
	}
	if !bytes.Equal(m.MemberAssignment, assignment) {
		t.Errorf("member assignment = %x, want %x (the assignment its leader handed it); "+
			"empty means every member appears to own nothing",
			m.MemberAssignment, assignment)
	}
}

// A member that has joined but not yet synced owns nothing yet. DescribeGroups
// must say exactly that: an empty assignment, not a fabricated one and not a
// copy of the subscription.
func TestDescribeGroupsMemberWithoutAssignmentReportsEmptyNotSubscription(t *testing.T) {
	d := testDispatcher(t)

	const group = "dg.unsynced"
	subscription := []byte{0xaa, 0xbb, 0xcc}

	// Join only; no SyncGroup, so the leader has not distributed anything.
	req := kmsg.NewPtrJoinGroupRequest()
	req.Group = group
	req.SessionTimeoutMillis = 10000
	req.RebalanceTimeoutMillis = 100
	req.ProtocolType = "consumer"
	req.Protocols = []kmsg.JoinGroupRequestProtocol{{Name: "range", Metadata: subscription}}
	jresp, _ := exchange(t, d, int16(kmsg.JoinGroup), 3, req)
	jg := jresp.(*kmsg.JoinGroupResponse)
	if jg.ErrorCode != errCodeNone {
		t.Fatalf("JoinGroup error code = %d, want 0", jg.ErrorCode)
	}

	dreq := kmsg.NewPtrDescribeGroupsRequest()
	dreq.Groups = []string{group}
	dresp, _ := exchange(t, d, int16(kmsg.DescribeGroups), 3, dreq)
	got := dresp.(*kmsg.DescribeGroupsResponse)
	if len(got.Groups) != 1 || len(got.Groups[0].Members) != 1 {
		t.Fatalf("shape lost: groups=%+v", got.Groups)
	}

	m := got.Groups[0].Members[0]
	if !bytes.Equal(m.ProtocolMetadata, subscription) {
		t.Errorf("member protocol metadata = %x, want the subscription %x",
			m.ProtocolMetadata, subscription)
	}
	if len(m.MemberAssignment) != 0 {
		t.Errorf("member assignment = %x, want empty before the leader has distributed anything",
			m.MemberAssignment)
	}
}

// The version that every modern client negotiates (v5 is flexible) must carry
// the same two blobs. A fix that only reached the non-flexible encoding would
// leave the clients that matter most with the old empty rows.
func TestDescribeGroupsFlexibleVersionCarriesAssignment(t *testing.T) {
	d := testDispatcher(t)

	const group = "dg.flex"
	subscription := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}
	assignment := []byte{0x20, 0x21}

	memberID := joinAndSync(t, d, group, subscription, assignment)

	req := kmsg.NewPtrDescribeGroupsRequest()
	req.Groups = []string{group}

	resp, _ := exchange(t, d, int16(kmsg.DescribeGroups), 5, req)
	got, ok := resp.(*kmsg.DescribeGroupsResponse)
	if !ok {
		t.Fatalf("DescribeGroups returned %T, want *kmsg.DescribeGroupsResponse", resp)
	}
	if len(got.Groups) != 1 || len(got.Groups[0].Members) != 1 {
		t.Fatalf("shape lost: groups=%+v", got.Groups)
	}
	m := got.Groups[0].Members[0]
	if m.MemberID != memberID {
		t.Errorf("member id = %q, want %q", m.MemberID, memberID)
	}
	if !bytes.Equal(m.ProtocolMetadata, subscription) {
		t.Errorf("v5 member protocol metadata = %x, want %x", m.ProtocolMetadata, subscription)
	}
	if !bytes.Equal(m.MemberAssignment, assignment) {
		t.Errorf("v5 member assignment = %x, want %x", m.MemberAssignment, assignment)
	}
}
