package wire

import (
	"net"
	"testing"

	"github.com/csnyder256/kafka-wire/internal/broker"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// These tests pin the ACL enforcement on the three consumer-group membership
// APIs that did not have it.
//
// JoinGroup, OffsetCommit, OffsetFetch and DescribeGroups all check the
// caller's grant on the group. SyncGroup, Heartbeat and LeaveGroup did not, so
// once ACLs were configured a principal with no entry for a group could still
// drive that group's membership: keep a stale member "alive" with Heartbeat,
// hand it an assignment with SyncGroup, or evict a live consumer with
// LeaveGroup -- a cross-tenant denial of service on somebody else's consumer
// group. docs/security.md already promises authorization on "the consumer
// group APIs".
//
// The tests drive the REAL handlers over a real connection and assert on the
// bytes that leave the broker, so they cannot pass on a struct the test built
// itself.

// exchangeAsPrincipal is exchange() with the connection's SASL principal set,
// which is the only way to reach the authorization path: every ACL decision is
// keyed on state.saslPrincipal. The rest mirrors exchange() exactly -- the
// request is encoded with kmsg at the negotiated version and the response is
// read off the wire and decoded by kmsg at the same version.
func exchangeAsPrincipal(t *testing.T, d *Dispatcher, apiKey int16, version int16, req kmsg.Request, principal string) (kmsg.Response, []byte) {
	t.Helper()

	client, server := net.Pipe()
	state := &connState{
		conn:          server,
		remote:        "test",
		saslComplete:  true,
		saslPrincipal: principal,
		dispatcher:    d,
	}

	req.SetVersion(version)
	hdr := RequestHeader{
		APIKey:        apiKey,
		APIVersion:    version,
		CorrelationID: 77,
		ClientID:      "test-client",
	}

	done := make(chan error, 1)
	go func() { done <- d.handle(t.Context(), state, hdr, req.AppendTo(nil)) }()

	raw, err := readOneFrame(client)
	client.Close()
	server.Close()
	if err != nil {
		t.Fatalf("reading response frame: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	if len(raw) < 4 {
		t.Fatalf("handler wrote %d bytes, too few for a frame", len(raw))
	}

	respBody := raw[4:]
	if len(respBody) < 4 {
		t.Fatalf("response body too short after the size prefix: %x", raw)
	}
	respBody = respBody[4:] // correlation id
	if isFlexibleResponseForKey(apiKey, version) {
		if len(respBody) == 0 {
			t.Fatal("response is missing the flexible header's tagged-fields byte")
		}
		respBody = respBody[1:]
	}

	got := kmsg.ResponseForKey(apiKey)
	if got == nil {
		t.Fatalf("kmsg has no response type for api key %d", apiKey)
	}
	got.SetVersion(version)
	if err := got.ReadFrom(respBody); err != nil {
		t.Fatalf("decoding the response the handler sent: %v (api key %d v%d, body %x)", err, apiKey, version, respBody)
	}
	return got, raw
}

// aclDispatcher is a real dispatcher whose broker has ACLs configured: one
// principal, "allowed", with read on groups under "allowed." and read+write on
// topics under "allowed.". Anything else is denied, because the ACL store
// denies every principal with no matching entry once any entry exists.
func aclDispatcher(t *testing.T) *Dispatcher {
	t.Helper()
	d := testDispatcher(t)
	acl := d.brk.ACL()
	if acl == nil {
		t.Fatal("broker has no ACL store after LoadState")
	}
	if err := acl.Upsert(broker.Principal{
		Name: "allowed",
		TopicPrefixes: []broker.TopicPrefixACL{
			{Prefix: "allowed.", Ops: []string{broker.OpRead, broker.OpWrite}},
		},
		Groups: []broker.GroupPrefixACL{
			{IDPrefix: "allowed.", Ops: []string{broker.OpRead}},
		},
	}); err != nil {
		t.Fatalf("upserting the allowed principal: %v", err)
	}
	if !acl.Configured() {
		t.Fatal("ACL store reports not configured right after an upsert")
	}
	return d
}

// joinOneMember registers exactly one member in a fresh group and returns its
// member id and the generation it landed in. JoinGroup blocks until its join
// window closes; a 50ms RebalanceTimeoutMillis bounds that, and APIVersion 3
// keeps the KIP-394 member-id handshake out of the way so the member is
// registered on the first call rather than after a rejoin.
func joinOneMember(t *testing.T, d *Dispatcher, group string) (memberID string, generation int32) {
	t.Helper()
	res, err := d.brk.Groups().JoinGroup(broker.JoinGroupArgs{
		GroupID:            group,
		ClientID:           "member-1",
		ClientHost:         "test",
		SessionTimeoutMS:   30000,
		RebalanceTimeoutMS: 50,
		ProtocolType:       "consumer",
		Protocols:          []broker.GroupProtocol{{Name: "range"}},
		APIVersion:         3,
	})
	if err != nil {
		t.Fatalf("JoinGroup(%s): %v", group, err)
	}
	if res.MemberID == "" {
		t.Fatalf("JoinGroup(%s) returned no member id (error code %d)", group, res.ErrorCode)
	}
	if res.Generation == 0 {
		t.Fatalf("JoinGroup(%s) admitted a member into generation 0", group)
	}
	return res.MemberID, res.Generation
}

// membersIn returns how many members the group has, or -1 if the group is not
// known at all.
func membersIn(d *Dispatcher, group string) int {
	for _, g := range d.brk.Groups().AllGroups() {
		if g.GroupID == group {
			return len(g.Members)
		}
	}
	return -1
}

// Heartbeat refreshes a member's liveness. A principal with no grant on the
// group must not be able to do that for somebody else's member.
func TestHeartbeatRequiresGroupReadGrant(t *testing.T) {
	d := aclDispatcher(t)
	member, gen := joinOneMember(t, d, "denied.workers")

	req := kmsg.NewPtrHeartbeatRequest()
	req.Group = "denied.workers"
	req.MemberID = member
	req.Generation = gen

	resp, _ := exchangeAsPrincipal(t, d, int16(kmsg.Heartbeat), 4, req, "intruder")
	got := resp.(*kmsg.HeartbeatResponse)
	if got.ErrorCode != errCodeGroupAuthorizationFailed {
		t.Fatalf("Heartbeat on a group the principal has no grant on returned error code %d, want %d (GROUP_AUTHORIZATION_FAILED)",
			got.ErrorCode, errCodeGroupAuthorizationFailed)
	}
}

// SyncGroup is where the leader hands out assignments. An unauthorized
// principal must not be able to drive it.
func TestSyncGroupRequiresGroupReadGrant(t *testing.T) {
	d := aclDispatcher(t)
	member, gen := joinOneMember(t, d, "denied.workers")

	req := kmsg.NewPtrSyncGroupRequest()
	req.Group = "denied.workers"
	req.MemberID = member
	req.Generation = gen

	resp, _ := exchangeAsPrincipal(t, d, int16(kmsg.SyncGroup), 5, req, "intruder")
	got := resp.(*kmsg.SyncGroupResponse)
	if got.ErrorCode != errCodeGroupAuthorizationFailed {
		t.Fatalf("SyncGroup on a group the principal has no grant on returned error code %d, want %d (GROUP_AUTHORIZATION_FAILED)",
			got.ErrorCode, errCodeGroupAuthorizationFailed)
	}
}

// LeaveGroup is the destructive one: answering it removes the member and
// forces the rest of the group into a rebalance. The refusal must land AND the
// member must still be there afterwards, which is the part an error code alone
// would not prove.
func TestLeaveGroupRequiresGroupReadGrantAndKeepsTheMember(t *testing.T) {
	d := aclDispatcher(t)
	const group = "denied.workers"
	member, _ := joinOneMember(t, d, group)

	// v0-v2 shape: the member id is a top-level field and the error code is
	// top-level too.
	req := kmsg.NewPtrLeaveGroupRequest()
	req.Group = group
	req.MemberID = member

	resp, _ := exchangeAsPrincipal(t, d, int16(kmsg.LeaveGroup), 2, req, "intruder")
	got := resp.(*kmsg.LeaveGroupResponse)
	if got.ErrorCode != errCodeGroupAuthorizationFailed {
		t.Fatalf("LeaveGroup (v2) on a group the principal has no grant on returned error code %d, want %d (GROUP_AUTHORIZATION_FAILED)",
			got.ErrorCode, errCodeGroupAuthorizationFailed)
	}
	if n := membersIn(d, group); n != 1 {
		t.Fatalf("after a refused LeaveGroup the group has %d member(s), want 1: the unauthorized caller evicted somebody else's consumer", n)
	}

	// v3+ shape: the members arrive in a list and the error code is per-member,
	// because a single request may carry several.
	req3 := kmsg.NewPtrLeaveGroupRequest()
	req3.Group = group
	req3.Members = []kmsg.LeaveGroupRequestMember{{MemberID: member}}

	resp3, _ := exchangeAsPrincipal(t, d, int16(kmsg.LeaveGroup), 3, req3, "intruder")
	got3 := resp3.(*kmsg.LeaveGroupResponse)
	if len(got3.Members) != 1 {
		t.Fatalf("LeaveGroup (v3) answered %d member(s), want 1", len(got3.Members))
	}
	if got3.Members[0].ErrorCode != errCodeGroupAuthorizationFailed {
		t.Fatalf("LeaveGroup (v3) per-member error code = %d, want %d (GROUP_AUTHORIZATION_FAILED)",
			got3.Members[0].ErrorCode, errCodeGroupAuthorizationFailed)
	}
	if n := membersIn(d, group); n != 1 {
		t.Fatalf("after a refused LeaveGroup (v3) the group has %d member(s), want 1", n)
	}
}

// The control: a principal that DOES hold the grant is unaffected, on all
// three APIs, so the check is a grant check and not a blanket refusal.
func TestGroupMembershipAllowsAPrincipalWithTheGrant(t *testing.T) {
	d := aclDispatcher(t)
	const group = "allowed.workers"
	member, gen := joinOneMember(t, d, group)

	hb := kmsg.NewPtrHeartbeatRequest()
	hb.Group = group
	hb.MemberID = member
	hb.Generation = gen
	hbResp, _ := exchangeAsPrincipal(t, d, int16(kmsg.Heartbeat), 4, hb, "allowed")
	if code := hbResp.(*kmsg.HeartbeatResponse).ErrorCode; code != errCodeNone {
		t.Fatalf("Heartbeat by the granted principal returned error code %d, want 0", code)
	}

	sg := kmsg.NewPtrSyncGroupRequest()
	sg.Group = group
	sg.MemberID = member
	sg.Generation = gen
	sgResp, _ := exchangeAsPrincipal(t, d, int16(kmsg.SyncGroup), 5, sg, "allowed")
	if code := sgResp.(*kmsg.SyncGroupResponse).ErrorCode; code != errCodeNone {
		t.Fatalf("SyncGroup by the granted principal returned error code %d, want 0", code)
	}

	lv := kmsg.NewPtrLeaveGroupRequest()
	lv.Group = group
	lv.Members = []kmsg.LeaveGroupRequestMember{{MemberID: member}}
	lvResp, _ := exchangeAsPrincipal(t, d, int16(kmsg.LeaveGroup), 3, lv, "allowed")
	lvGot := lvResp.(*kmsg.LeaveGroupResponse)
	if len(lvGot.Members) != 1 {
		t.Fatalf("LeaveGroup by the granted principal answered %d member(s), want 1", len(lvGot.Members))
	}
	if lvGot.Members[0].ErrorCode != errCodeNone {
		t.Fatalf("LeaveGroup by the granted principal returned error code %d, want 0", lvGot.Members[0].ErrorCode)
	}
	if n := membersIn(d, group); n != 0 {
		t.Fatalf("a granted LeaveGroup left %d member(s), want 0: the check must not swallow an authorized leave", n)
	}
}

// The default deployment has no ACLs at all, and the new check must be inert
// there: an empty ACL store means "not configured", so every principal is
// allowed and the common case cannot regress.
func TestGroupMembershipUnaffectedWhenACLsAreOff(t *testing.T) {
	d := testDispatcher(t) // no ACL entries => Configured() is false
	const group = "plain.workers"
	member, gen := joinOneMember(t, d, group)

	hb := kmsg.NewPtrHeartbeatRequest()
	hb.Group = group
	hb.MemberID = member
	hb.Generation = gen
	hbResp, _ := exchangeAsPrincipal(t, d, int16(kmsg.Heartbeat), 4, hb, "")
	if code := hbResp.(*kmsg.HeartbeatResponse).ErrorCode; code != errCodeNone {
		t.Fatalf("Heartbeat with ACLs off returned error code %d, want 0", code)
	}

	lv := kmsg.NewPtrLeaveGroupRequest()
	lv.Group = group
	lv.MemberID = member
	lvResp, _ := exchangeAsPrincipal(t, d, int16(kmsg.LeaveGroup), 2, lv, "")
	if code := lvResp.(*kmsg.LeaveGroupResponse).ErrorCode; code != errCodeNone {
		t.Fatalf("LeaveGroup with ACLs off returned error code %d, want 0", code)
	}
	if n := membersIn(d, group); n != 0 {
		t.Fatalf("LeaveGroup with ACLs off left %d member(s), want 0", n)
	}
}
