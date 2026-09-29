package wire

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/csnyder256/kafka-wire/internal/broker"
	"github.com/csnyder256/kafka-wire/internal/metrics"
	"github.com/csnyder256/kafka-wire/internal/storage"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// These tests exercise the REAL handlers over a real connection and assert on
// the bytes that actually leave the broker. That distinction is the point:
// a struct built by hand in a test proves what kmsg does with a
// correctly-shaped struct, not what handleFindCoordinator/handleOffsetFetch
// actually send. OffsetFetch had a real defect: an uncommitted partition's
// Go zero-value leader epoch looked like a valid epoch on the wire.
// FindCoordinator already handled the version-dependent coordinator shapes
// correctly; those cases pin existing behavior rather than prove a new fix.

// testDispatcher builds a real Dispatcher over an in-memory broker. The
// broker owns an OffsetStore rooted at t.TempDir(), so tests can commit
// real offsets and read them back through the real handler.
func testDispatcher(t *testing.T) *Dispatcher {
	t.Helper()
	dir := t.TempDir()
	store, err := storage.Open(storage.Config{
		DataDir:   dir,
		FsyncMode: storage.FsyncNone,
	})
	if err != nil {
		t.Fatalf("opening storage: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	brk := broker.New(broker.Config{
		BrokerID:       1,
		ClusterID:      "test-cluster",
		AdvertisedHost: "broker.example",
		AdvertisedPort: 9092,
		DataDir:        dir,
		Storage:        store,
		Metrics:        metrics.New(),
	})
	t.Cleanup(func() {
		if err := brk.Drain(context.Background()); err != nil {
			t.Errorf("draining broker: %v", err)
		}
	})
	if err := brk.LoadState(); err != nil {
		t.Fatalf("loading broker state: %v", err)
	}
	return NewDispatcher(brk, metrics.New(), Config{})
}

// exchange runs one real handler against its request body and returns the
// decoded response plus the raw frame.
//
// It drives the handler the way dispatch does: the request is encoded with
// kmsg at the negotiated version, handed to the handler, and the response is
// read off the wire and decoded by kmsg at the same version. Nothing about
// the response is constructed by the test.
func exchange(t *testing.T, d *Dispatcher, apiKey int16, version int16, req kmsg.Request) (kmsg.Response, []byte) {
	t.Helper()

	client, server := net.Pipe()
	state := &connState{
		conn:         server,
		remote:       "test",
		saslComplete: true,
		dispatcher:   d,
	}

	req.SetVersion(version)
	hdr := RequestHeader{
		APIKey:        apiKey,
		APIVersion:    version,
		CorrelationID: 77,
		ClientID:      "test-client",
	}

	// Run the handler on the server side; the read below blocks until it has
	// written the whole response.
	done := make(chan error, 1)
	go func() { done <- d.handle(t.Context(), state, hdr, req.AppendTo(nil)) }()

	// The response is a single length-prefixed frame. Read the size, then
	// exactly that many bytes; reading to EOF would block forever because the
	// handler keeps the connection open for the next request.
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

	// Strip the response frame header. The flexible tagged-fields byte is
	// present only when the handler asked for it: the response type's own
	// IsFlexible() at this version, except ApiVersions, which always keeps the
	// v0 (non-flexible) header even at v3.
	respBody := raw[4:]
	if len(respBody) < 4 {
		t.Fatalf("response body too short after the size prefix: %x", raw)
	}
	respBody = respBody[4:] // correlation id
	flexibleHeader := isFlexibleResponseForKey(apiKey, version)
	if flexibleHeader {
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
		t.Fatalf("decoding the response the handler sent: %v (apiKey %d v%d, body %x)", err, apiKey, version, respBody)
	}
	return got, raw
}

// isFlexibleResponseForKey reports whether the response header for this API at
// this version carries the KIP-482 tagged-fields byte. kmsg's response type
// owns that table (mirroring how isFlexibleRequest asks the request type),
// with ApiVersions hardcoded to the v0 header.
func isFlexibleResponseForKey(apiKey, apiVersion int16) bool {
	if apiKey == int16(kmsg.ApiVersions) {
		return false
	}
	resp := kmsg.ResponseForKey(apiKey)
	if resp == nil {
		return false
	}
	resp.SetVersion(apiVersion)
	return resp.IsFlexible()
}

// readOneFrame reads a single Kafka response frame: a 4-byte big-endian size
// followed by exactly that many bytes. It returns the whole frame including
// the size prefix, matching what writeResponse emits.
func readOneFrame(conn net.Conn) ([]byte, error) {
	var sizeBuf [4]byte
	if _, err := io.ReadFull(conn, sizeBuf[:]); err != nil {
		return nil, err
	}
	size := int32(binary.BigEndian.Uint32(sizeBuf[:]))
	if size < 0 || size > 1<<20 {
		return nil, fmt.Errorf("refusing frame size %d", size)
	}
	body := make([]byte, size)
	if _, err := io.ReadFull(conn, body); err != nil {
		return nil, err
	}
	return append(sizeBuf[:], body...), nil
}

// TestFindCoordinatorV4AnswersEveryRequestedKey is the FindCoordinator
// regression. A v4+ client reads ONLY the Coordinators list: the
// top-level NodeID/Host/Port fields are not part of a v4+ response at all
// (kmsg guards them with `version >= 0 && version <= 3`) and the encoder
// drops them silently. So a handler that fills the top-level fields and
// leaves Coordinators empty hands a v4+ client an empty list, which it reads
// as "no coordinator exists for the group you asked about" and never reaches
// JoinGroup.
func TestFindCoordinatorV4AnswersEveryRequestedKey(t *testing.T) {
	d := testDispatcher(t)

	keys := []string{"orders.workers", "billing.workers"}
	req := kmsg.NewPtrFindCoordinatorRequest()
	req.CoordinatorKeys = keys
	req.CoordinatorType = 0

	resp, _ := exchange(t, d, int16(kmsg.FindCoordinator), 4, req)
	got, ok := resp.(*kmsg.FindCoordinatorResponse)
	if !ok {
		t.Fatalf("handler returned %T, want *kmsg.FindCoordinatorResponse", resp)
	}
	if got.ErrorCode != errCodeNone {
		t.Fatalf("v4 top-level error code = %d, want 0", got.ErrorCode)
	}
	if len(got.Coordinators) != len(keys) {
		t.Fatalf("v4 answered %d coordinator(s), want %d; a v4 client reads only this list and an empty one means \"no coordinator\"",
			len(got.Coordinators), len(keys))
	}
	for i, k := range keys {
		c := got.Coordinators[i]
		if c.Key != k {
			t.Errorf("coordinator %d key = %q, want %q", i, c.Key, k)
		}
		if c.NodeID != 1 || c.Host != "broker.example" || c.Port != 9092 {
			t.Errorf("coordinator %d = node %d %s:%d, want this broker (1, broker.example:9092)", i, c.NodeID, c.Host, c.Port)
		}
		if c.ErrorCode != errCodeNone {
			t.Errorf("coordinator %d error code = %d, want 0", i, c.ErrorCode)
		}
	}
}

// A single-key v4 request must answer with that one key, not an empty list.
// This is the shape every real client sends: CoordinatorKey does not exist on
// the v4 wire, so a v4 client asking about one group still fills the array.
func TestFindCoordinatorV4SingleKey(t *testing.T) {
	d := testDispatcher(t)

	req := kmsg.NewPtrFindCoordinatorRequest()
	req.CoordinatorKeys = []string{"only.group"}

	resp, _ := exchange(t, d, int16(kmsg.FindCoordinator), 4, req)
	got := resp.(*kmsg.FindCoordinatorResponse)
	if len(got.Coordinators) != 1 {
		t.Fatalf("single-key v4 request answered %d coordinator(s), want 1", len(got.Coordinators))
	}
	if got.Coordinators[0].Key != "only.group" {
		t.Errorf("key = %q, want only.group", got.Coordinators[0].Key)
	}
}

// The pre-v4 versions read the top-level fields, which a v4-shaped response
// omits entirely. This is the other half of the same version split and must
// keep working: v0-v3 was correct before the change and stays correct after.
func TestFindCoordinatorLegacyVersionsUseTopLevelShape(t *testing.T) {
	d := testDispatcher(t)

	for _, version := range []int16{0, 1, 2, 3} {
		req := kmsg.NewPtrFindCoordinatorRequest()
		req.CoordinatorKey = "legacy.group"

		resp, _ := exchange(t, d, int16(kmsg.FindCoordinator), version, req)
		got := resp.(*kmsg.FindCoordinatorResponse)
		if got.NodeID != 1 || got.Host != "broker.example" || got.Port != 9092 {
			t.Errorf("v%d = node %d %s:%d, want this broker from the top-level fields",
				version, got.NodeID, got.Host, got.Port)
		}
		if len(got.Coordinators) != 0 {
			t.Errorf("v%d carried %d per-key coordinators; that field does not exist before v4",
				version, len(got.Coordinators))
		}
	}
}

// TestOffsetFetchGroupShapeKeepsUnknownEpochForUncommittedPartitions is the
// OffsetFetch regression, on the v8+ Groups shape.
//
// OffsetFetch v5+ writes LeaderEpoch unconditionally, and kmsg's own Default()
// seeds it to -1 because a partition with no commit must not report epoch 0.
// The handler builds its partitions with a bare struct literal and never calls
// Default(), so the store's zero value wins: a partition with nothing
// committed comes back as offset -1 (correct) but epoch 0 (a real-looking
// epoch that was never committed). A consumer resuming from that tuple starts
// at the right offset with a fabricated leader epoch.
func TestOffsetFetchGroupShapeKeepsUnknownEpochForUncommittedPartitions(t *testing.T) {
	d := testDispatcher(t)

	// Commit one partition for real; leave the other uncommitted.
	if err := d.brk.Offsets().CommitOffset("workers", "orders.events", 1, 42, 0, ""); err != nil {
		t.Fatalf("committing an offset: %v", err)
	}

	req := kmsg.NewPtrOffsetFetchRequest()
	req.Groups = []kmsg.OffsetFetchRequestGroup{{
		Group: "workers",
		Topics: []kmsg.OffsetFetchRequestGroupTopic{{
			Topic:      "orders.events",
			Partitions: []int32{0, 1},
		}},
	}}

	resp, _ := exchange(t, d, int16(kmsg.OffsetFetch), 9, req)
	got := resp.(*kmsg.OffsetFetchResponse)
	if len(got.Groups) != 1 || len(got.Groups[0].Topics) != 1 {
		t.Fatalf("shape lost: groups=%+v", got.Groups)
	}
	parts := got.Groups[0].Topics[0].Partitions
	byPartition := map[int32]kmsg.OffsetFetchResponseGroupTopicPartition{}
	for _, p := range parts {
		byPartition[p.Partition] = p
	}
	if len(byPartition) != 2 {
		t.Fatalf("got %d partitions, want 2: %+v", len(byPartition), parts)
	}

	uncommitted := byPartition[0]
	if uncommitted.Offset != -1 {
		t.Errorf("uncommitted partition offset = %d, want -1", uncommitted.Offset)
	}
	if uncommitted.LeaderEpoch != -1 {
		t.Errorf("uncommitted partition leader epoch = %d, want -1 (the protocol's unknown marker, not a fabricated 0)",
			uncommitted.LeaderEpoch)
	}

	committed := byPartition[1]
	if committed.Offset != 42 || committed.LeaderEpoch != 0 {
		t.Errorf("committed partition = offset %d epoch %d, want 42 and 0", committed.Offset, committed.LeaderEpoch)
	}
}

// The v0-v7 shape carries LeaderEpoch too (from v5), and needs the same
// treatment. A partition the client named but the group never committed must
// report the unknown epoch marker, not the store's zero value.
func TestOffsetFetchLegacyShapeKeepsUnknownEpochForUncommittedPartitions(t *testing.T) {
	d := testDispatcher(t)

	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = "workers"
	req.Topics = []kmsg.OffsetFetchRequestTopic{{
		Topic:      "orders.events",
		Partitions: []int32{0},
	}}

	// v6 is flexible and carries LeaderEpoch on the response.
	resp, _ := exchange(t, d, int16(kmsg.OffsetFetch), 6, req)
	got := resp.(*kmsg.OffsetFetchResponse)
	if len(got.Topics) != 1 || len(got.Topics[0].Partitions) != 1 {
		t.Fatalf("shape lost: topics=%+v", got.Topics)
	}
	p := got.Topics[0].Partitions[0]
	if p.Offset != -1 {
		t.Errorf("uncommitted partition offset = %d, want -1", p.Offset)
	}
	if p.LeaderEpoch != -1 {
		t.Errorf("uncommitted partition leader epoch = %d, want -1 (the unknown marker)", p.LeaderEpoch)
	}
}

// A legitimate committed epoch must survive the change untouched: the fix may
// only touch the offsets that were never committed. This is the guard that
// keeps the correction from swallowing real data.
func TestOffsetFetchPreservesRealCommittedEpoch(t *testing.T) {
	d := testDispatcher(t)

	if err := d.brk.Offsets().CommitOffset("workers", "orders.events", 3, 500, 7, "meta"); err != nil {
		t.Fatalf("committing an offset: %v", err)
	}

	req := kmsg.NewPtrOffsetFetchRequest()
	req.Groups = []kmsg.OffsetFetchRequestGroup{{
		Group: "workers",
		Topics: []kmsg.OffsetFetchRequestGroupTopic{{
			Topic:      "orders.events",
			Partitions: []int32{3},
		}},
	}}

	resp, _ := exchange(t, d, int16(kmsg.OffsetFetch), 9, req)
	got := resp.(*kmsg.OffsetFetchResponse)
	p := got.Groups[0].Topics[0].Partitions[0]
	if p.Offset != 500 || p.LeaderEpoch != 7 {
		t.Errorf("committed tuple = offset %d epoch %d, want 500 and 7 (a real commit must not be rewritten)", p.Offset, p.LeaderEpoch)
	}
	if p.Metadata == nil || *p.Metadata != "meta" {
		t.Errorf("committed metadata = %v, want \"meta\"", p.Metadata)
	}
}

// The advertised version ranges must stay within what the handlers can
// encode, so the table itself is asserted. This is the check that makes a
// raised ceiling without a matching handler a loud failure instead of a
// silent client-side mystery.
func TestAdvertisedVersionsStayWithinWhatTheHandlersEncode(t *testing.T) {
	// key -> the highest version whose response shape this broker builds.
	// Raising a ceiling here without teaching the handler the new shape is
	// the mistake this map exists to catch.
	maxHandled := map[int16]int16{
		int16(kmsg.Produce):          9,  // LogAppendTime set; v10+ adds fields
		int16(kmsg.Fetch):            11, // PreferredReadReplica is a v11 field
		int16(kmsg.ListOffsets):      5,  // v6+ adds LeaderEpoch to the request path
		int16(kmsg.Metadata):         9,  // v10+ uses topic IDs
		int16(kmsg.OffsetCommit):     8,  // v9 adds rack on the response
		int16(kmsg.OffsetFetch):      7,  // v8 switches to the Groups shape
		int16(kmsg.FindCoordinator):  4,  // v4 switches to the Coordinators list
		int16(kmsg.JoinGroup):        7,  // v6+ adds ProtocolName/Reason
		int16(kmsg.SyncGroup):        5,
		int16(kmsg.Heartbeat):        4,
		int16(kmsg.LeaveGroup):       4,
		int16(kmsg.SASLHandshake):    1,
		int16(kmsg.SASLAuthenticate): 2,
		int16(kmsg.ApiVersions):      3,
		int16(kmsg.CreateTopics):     6,
		int16(kmsg.DeleteTopics):     5,
		int16(kmsg.DescribeConfigs):  4,
		int16(kmsg.DescribeGroups):   5,
		int16(kmsg.ListGroups):       4,
	}
	for _, a := range advertisedAPIs {
		ceiling, ok := maxHandled[a.APIKey]
		if !ok {
			t.Errorf("api key %d is advertised but no handler shape is recorded for it", a.APIKey)
			continue
		}
		if a.MaxVersion > ceiling {
			t.Errorf("api key %d advertises up to v%d but the handler only encodes through v%d",
				a.APIKey, a.MaxVersion, ceiling)
		}
	}
}

// ApiVersions keeps the v0 (non-flexible) response header even at v3, because
// a client cannot negotiate before it has parsed this one response. A framing
// bug here is invisible until a client fails to negotiate at all.
func TestApiVersionsResponseUsesTheNonFlexibleHeader(t *testing.T) {
	d := testDispatcher(t)

	req := kmsg.NewPtrApiVersionsRequest()
	_, raw := exchange(t, d, int16(kmsg.ApiVersions), 3, req)

	// A non-flexible response header is size(4) + correlation(4) + body. If
	// the flexible tagged-fields byte had been added, the body would start one
	// byte late; exchange() decoded it without the byte and would already have
	// failed in that case. Assert the frame arithmetic too.
	size := int32(binary.BigEndian.Uint32(raw[0:4]))
	if size != int32(len(raw)-4) {
		t.Errorf("frame size prefix = %d, body length is %d", size, len(raw)-4)
	}
	corr := binary.BigEndian.Uint32(raw[4:8])
	if corr != 77 {
		t.Errorf("correlation id = %d, want 77", corr)
	}
}
