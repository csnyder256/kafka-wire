package broker

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// GroupCoordinator implements the Kafka consumer-group state machine
// for a single broker. State transitions:
//
//   Empty            -> PreparingRebalance   (first JoinGroup arrives)
//   PreparingRebalance -> CompletingRebalance (rebalance timeout fires
//                                              OR all members joined)
//   CompletingRebalance -> Stable            (leader's SyncGroup
//                                              returns assignments)
//   Stable           -> PreparingRebalance   (member joins/leaves OR
//                                              session timeout)
//   * -> Dead                                (LeaveGroup from sole
//                                              member, GC sweep)
//
// Transitions are guarded by `mu`. Heartbeat liveness is checked
// inside the same lock at every JoinGroup / Heartbeat / SyncGroup
// call: no separate timer goroutine, which keeps idle CPU at zero.
type GroupCoordinator struct {
	store *OffsetStore
	mu    sync.Mutex
	live  map[string]*liveGroup // in-memory transient state
}

// liveGroup is the runtime layer over a persisted GroupState. Tracks
// heartbeat timestamps and the in-flight rebalance round.
type liveGroup struct {
	id             string
	state          GroupStatus
	persisted      *GroupState
	rebalanceUntil time.Time
	pending        map[string]*MemberState // members joined this round but not yet sync'd
	heartbeats     map[string]time.Time    // last heartbeat per member
	leader         string
	protocolType   string
	protocols      []GroupProtocol
	round          *rebalanceRound
}

// rebalanceRound is one join barrier.
//
// A consumer group only divides work correctly if every member of a
// generation is assigned partitions by ONE leader that can see ALL of them.
// That requires the coordinator to hold each JoinGroup response open until
// the membership for the generation is settled, then answer everyone at once.
//
// Completing the rebalance on the first JoinGroup instead, which is the
// obvious simplification when a group only ever has one consumer, produces a
// group that never appears broken and is silently wrong: each member becomes
// the sole member of its own generation, each assigns itself every partition,
// and every record is delivered to every member.
type rebalanceRound struct {
	done     chan struct{} // closed when the generation is decided
	sync     chan struct{} // closed when the leader has distributed assignments
	results  map[string]*JoinGroupResult
	joined   map[string]bool
	expected map[string]bool // members already known when the round opened
	deadline time.Time
	closed   bool
	synced   bool
}

// initialRebalanceDelay is how long a brand-new group waits for siblings
// before deciding the generation. Apache Kafka calls this
// group.initial.rebalance.delay.ms and defaults it to 3s for the same
// reason: without it, members that start together each land in their own
// generation. It is the cost of correct group semantics, and it applies
// only when a group has no previously known members.
const initialRebalanceDelay = 3 * time.Second

// maxRebalanceWait bounds how long a join can block, whatever the client
// asked for, so a stuck member cannot pin a request goroutine forever.
const maxRebalanceWait = 60 * time.Second

// GroupStatus is the discrete coordinator state.
type GroupStatus int

const (
	GroupEmpty GroupStatus = iota
	GroupPreparingRebalance
	GroupCompletingRebalance
	GroupStable
	GroupDead
)

func (gs GroupStatus) String() string {
	switch gs {
	case GroupEmpty:
		return "Empty"
	case GroupPreparingRebalance:
		return "PreparingRebalance"
	case GroupCompletingRebalance:
		return "CompletingRebalance"
	case GroupStable:
		return "Stable"
	case GroupDead:
		return "Dead"
	}
	return "Unknown"
}

// GroupProtocol represents one protocol the joining member supports.
type GroupProtocol struct {
	Name     string
	Metadata []byte
}

// NewGroupCoordinator wraps an OffsetStore.
func NewGroupCoordinator(store *OffsetStore) *GroupCoordinator {
	g := &GroupCoordinator{
		store: store,
		live:  make(map[string]*liveGroup),
	}
	// Adopt persisted groups so a fresh process keeps committed
	// offsets visible to OffsetFetch even before any client rejoins.
	for _, gs := range store.All() {
		g.live[gs.GroupID] = &liveGroup{
			id:         gs.GroupID,
			state:      GroupEmpty,
			persisted:  gs,
			pending:    make(map[string]*MemberState),
			heartbeats: make(map[string]time.Time),
		}
	}
	return g
}

// findOrCreate returns the live group, creating a fresh empty record
// if absent. Caller must hold g.mu.
func (g *GroupCoordinator) findOrCreate(groupID string) *liveGroup {
	lg, ok := g.live[groupID]
	if !ok {
		gs := g.store.Get(groupID)
		lg = &liveGroup{
			id:         groupID,
			state:      GroupEmpty,
			persisted:  gs,
			pending:    make(map[string]*MemberState),
			heartbeats: make(map[string]time.Time),
		}
		g.live[groupID] = lg
	}
	return lg
}

// evictExpired removes any member whose heartbeat is older than
// the eviction threshold (or who has no heartbeat record at all).
// Caller must hold g.mu. Returns true if any member was evicted
// (caller should bump rebalance state).
//
// Two eviction conditions:
//
//  1. Heartbeat is older than min(session_timeout, 3s). Short threshold
//     handles crash-loops: a consumer that respawns within session
//     timeout (30s default) was getting its OLD member_id treated as
//     still-live, causing pickLeader to pick the dead id, which made
//     the new joiner think it wasn't leader and send empty SyncGroup.
//
//  2. Member is in lg.persisted.Members but absent from lg.heartbeats.
//     This happens on broker restart: persisted state contains the
//     pre-restart members but the in-memory heartbeats map is empty.
//     Without this branch, those stale members would NEVER be evicted
//     until someone explicitly LeaveGroups them, same dead-leader
//     symptom as case 1, plus survives across deploys.
func (g *GroupCoordinator) evictExpired(lg *liveGroup, now time.Time) bool {
	if lg.persisted == nil || len(lg.persisted.Members) == 0 {
		return false
	}
	evicted := false
	const fastEvictionThreshold = 3 * time.Second
	for memberID, m := range lg.persisted.Members {
		hb, ok := lg.heartbeats[memberID]
		if !ok {
			// No heartbeat record on this broker process, member
			// predates this broker's lifetime. Evict.
			delete(lg.persisted.Members, memberID)
			if lg.leader == memberID {
				lg.leader = ""
			}
			evicted = true
			continue
		}
		// Default session timeout: 30s if member didn't specify.
		sto := time.Duration(m.SessionTimeoutMS) * time.Millisecond
		if sto <= 0 {
			sto = 30 * time.Second
		}
		// Use the shorter of session_timeout and the fast threshold
		// so crash-looping clients don't accumulate stale members.
		evictAfter := sto
		if fastEvictionThreshold < evictAfter {
			evictAfter = fastEvictionThreshold
		}
		if now.Sub(hb) > evictAfter {
			delete(lg.persisted.Members, memberID)
			delete(lg.heartbeats, memberID)
			evicted = true
			if lg.leader == memberID {
				lg.leader = ""
			}
		}
	}
	return evicted
}

// JoinGroupResult is what the wire layer needs to build a JoinGroup
// response.
type JoinGroupResult struct {
	GroupID      string
	Generation   int32
	ProtocolType string
	Protocol     string
	LeaderID     string
	MemberID     string
	IsLeader     bool
	Members      []JoinGroupMember // populated only for the leader
	ErrorCode    int16
	NeedRejoin   bool // when set, client should retry JoinGroup
}

// JoinGroupMember is one member's metadata as seen by the leader.
type JoinGroupMember struct {
	MemberID string
	Metadata []byte
}

// JoinGroupArgs is the input from the wire layer.
type JoinGroupArgs struct {
	GroupID            string
	MemberID           string
	GroupInstanceID    string
	ClientID           string
	ClientHost         string
	SessionTimeoutMS   int32
	RebalanceTimeoutMS int32
	ProtocolType       string
	Protocols          []GroupProtocol
	// APIVersion lets the coordinator apply the KIP-394 member-id handshake
	// only where the protocol defines it. Kafka restricts it to JoinGroup v4
	// and above; returning MEMBER_ID_REQUIRED to an older client that does
	// not implement the rejoin dance makes it loop forever.
	APIVersion int16
}

// JoinGroup runs the join barrier described on rebalanceRound.
//
// It blocks. Each connection is served by its own goroutine, so holding a
// JoinGroup open is exactly what the protocol expects: Kafka clients send a
// JoinGroup with a long timeout and wait for the coordinator to answer once
// the generation is settled.
func (g *GroupCoordinator) JoinGroup(args JoinGroupArgs) (*JoinGroupResult, error) {
	if args.GroupID == "" {
		return &JoinGroupResult{ErrorCode: invalidGroupID}, nil
	}

	g.mu.Lock()

	lg := g.findOrCreate(args.GroupID)
	now := time.Now()
	g.evictExpired(lg, now)

	if lg.persisted == nil {
		lg.persisted = g.store.Get(args.GroupID)
	}
	if lg.persisted.Members == nil {
		lg.persisted.Members = make(map[string]*MemberState)
	}

	memberID := args.MemberID
	if memberID == "" {
		// A member with no id is a fresh process. Evict any prior
		// incarnation carrying the same client id, whose last heartbeat is
		// too recent for the liveness sweep to have collected it yet.
		if args.ClientID != "" {
			for existingID, existing := range lg.persisted.Members {
				if existing.ClientID != args.ClientID {
					continue
				}
				delete(lg.persisted.Members, existingID)
				delete(lg.heartbeats, existingID)
				if lg.leader == existingID {
					lg.leader = ""
				}
			}
		}
		memberID = newMemberID(args.ClientID)

		// KIP-394: from JoinGroup v4 the coordinator issues the member id
		// and the client rejoins with it. Older versions have no such
		// exchange, so those clients are registered immediately instead.
		if args.APIVersion >= 4 {
			g.mu.Unlock()
			return &JoinGroupResult{
				GroupID:    args.GroupID,
				MemberID:   memberID,
				ErrorCode:  memberIDRequired,
				NeedRejoin: true,
			}, nil
		}
	}

	// Open a round if none is in flight.
	round := lg.round
	if round == nil || round.closed {
		expected := make(map[string]bool, len(lg.persisted.Members))
		for id := range lg.persisted.Members {
			expected[id] = true
		}
		wait := initialRebalanceDelay
		if args.RebalanceTimeoutMS > 0 {
			if d := time.Duration(args.RebalanceTimeoutMS) * time.Millisecond; d < wait {
				wait = d
			}
		}
		round = &rebalanceRound{
			done:     make(chan struct{}),
			sync:     make(chan struct{}),
			results:  make(map[string]*JoinGroupResult),
			joined:   make(map[string]bool),
			expected: expected,
			deadline: now.Add(wait),
		}
		lg.round = round
		lg.state = GroupPreparingRebalance
		go g.expireRound(args.GroupID, round)
	}

	subBytes := joinGroupSubscriptionBytes(args.Protocols)
	names := make([]string, 0, len(args.Protocols))
	for _, pr := range args.Protocols {
		names = append(names, pr.Name)
	}
	lg.persisted.Members[memberID] = &MemberState{
		MemberID:           memberID,
		ClientID:           args.ClientID,
		ClientHost:         args.ClientHost,
		SessionTimeoutMS:   args.SessionTimeoutMS,
		RebalanceTimeoutMS: args.RebalanceTimeoutMS,
		Subscription:       subBytes,
		Protocols:          names,
	}
	lg.heartbeats[memberID] = now
	lg.protocolType = args.ProtocolType
	lg.protocols = args.Protocols
	round.joined[memberID] = true

	// A group whose entire previous membership has rejoined has nothing left
	// to wait for. A brand-new group has no previous membership, so it waits
	// out the window and collects whoever else is starting alongside it.
	if len(round.expected) > 0 && allRejoined(round) {
		g.closeRound(lg, round)
	}

	done := round.done
	g.mu.Unlock()

	select {
	case <-done:
	case <-time.After(maxRebalanceWait):
		return &JoinGroupResult{GroupID: args.GroupID, MemberID: memberID, ErrorCode: rebalanceInProgress}, nil
	}

	g.mu.Lock()
	res, ok := round.results[memberID]
	g.mu.Unlock()
	if !ok {
		return &JoinGroupResult{GroupID: args.GroupID, MemberID: memberID, ErrorCode: rebalanceInProgress}, nil
	}
	return res, nil
}

// allRejoined reports whether every member known when the round opened has
// come back.
func allRejoined(r *rebalanceRound) bool {
	for id := range r.expected {
		if !r.joined[id] {
			return false
		}
	}
	return true
}

// expireRound decides the generation once the join window closes, for the
// members that did arrive. Members that did not are dropped, which is how a
// crashed consumer stops holding up its group.
func (g *GroupCoordinator) expireRound(groupID string, round *rebalanceRound) {
	<-time.After(time.Until(round.deadline))
	g.mu.Lock()
	defer g.mu.Unlock()
	lg, ok := g.live[groupID]
	if !ok || lg.round != round || round.closed {
		return
	}
	g.closeRound(lg, round)
}

// closeRound settles one generation. Callers must hold g.mu.
func (g *GroupCoordinator) closeRound(lg *liveGroup, round *rebalanceRound) {
	if round.closed {
		return
	}
	round.closed = true

	// Members that never joined this round are gone.
	for id := range lg.persisted.Members {
		if !round.joined[id] {
			delete(lg.persisted.Members, id)
			delete(lg.heartbeats, id)
			if lg.leader == id {
				lg.leader = ""
			}
		}
	}

	if _, present := lg.persisted.Members[lg.leader]; lg.leader == "" || !present {
		lg.leader = pickLeader(lg.persisted.Members)
	}

	lg.persisted.Generation++
	lg.persisted.Leader = lg.leader
	lg.persisted.ProtocolType = lg.protocolType
	lg.persisted.Protocol = pickCommonProtocol(lg)
	lg.state = GroupCompletingRebalance

	// The leader alone receives the membership list; that is what lets it
	// compute an assignment that covers every member exactly once.
	members := make([]JoinGroupMember, 0, len(lg.persisted.Members))
	for _, mm := range lg.persisted.Members {
		members = append(members, JoinGroupMember{MemberID: mm.MemberID, Metadata: mm.Subscription})
	}
	sort.Slice(members, func(i, j int) bool { return members[i].MemberID < members[j].MemberID })

	for id := range round.joined {
		if _, still := lg.persisted.Members[id]; !still {
			continue
		}
		res := &JoinGroupResult{
			GroupID:      lg.id,
			Generation:   lg.persisted.Generation,
			ProtocolType: lg.persisted.ProtocolType,
			Protocol:     lg.persisted.Protocol,
			LeaderID:     lg.leader,
			MemberID:     id,
			IsLeader:     id == lg.leader,
		}
		if id == lg.leader {
			res.Members = members
		}
		round.results[id] = res
	}

	if err := g.store.Persist(lg.persisted); err != nil {
		// Persisting is best effort here: the generation is already decided
		// in memory, and refusing to answer would strand every member.
		_ = err
	}
	close(round.done)
}

// pickCommonProtocol chooses an assignment protocol every member supports.
// Picking the first member's favourite instead would hand some members an
// assignment encoded in a format they cannot decode.
func pickCommonProtocol(lg *liveGroup) string {
	counts := map[string]int{}
	order := []string{}
	total := 0
	for _, m := range lg.persisted.Members {
		total++
		seen := map[string]bool{}
		for _, name := range decodeProtocolNames(m) {
			if seen[name] {
				continue
			}
			seen[name] = true
			if counts[name] == 0 {
				order = append(order, name)
			}
			counts[name]++
		}
	}
	for _, name := range order {
		if counts[name] == total {
			return name
		}
	}
	if len(lg.protocols) > 0 {
		return lg.protocols[0].Name
	}
	return ""
}

// decodeProtocolNames returns the protocols a member advertised. The
// coordinator records the most recent JoinGroup's protocol list per group;
// members of one group almost always ship the same client and therefore the
// same list, so the group-level list is used when a per-member one is absent.
func decodeProtocolNames(m *MemberState) []string {
	if len(m.Protocols) > 0 {
		return m.Protocols
	}
	return nil
}

// SyncGroupArgs is the leader's distribution of partition assignments.
type SyncGroupArgs struct {
	GroupID      string
	Generation   int32
	MemberID     string
	Assignments  map[string][]byte // member_id -> assignment bytes (only leader sets)
}

// SyncGroupResult is what the wire layer returns. The member's own
// assignment is the bytes-per-member entry indexed by the requestor.
type SyncGroupResult struct {
	Assignment []byte
	ErrorCode  int16
}

// SyncGroup distributes the leader's assignments.
//
// The leader supplies the whole map; every other member blocks here until it
// arrives. Answering a follower immediately with whatever assignment happened
// to be on file would hand it the previous generation's partitions, which is
// how the same partition ends up being consumed twice.
func (g *GroupCoordinator) SyncGroup(args SyncGroupArgs) (*SyncGroupResult, error) {
	g.mu.Lock()

	lg, ok := g.live[args.GroupID]
	if !ok {
		g.mu.Unlock()
		return &SyncGroupResult{ErrorCode: invalidGroupID}, nil
	}
	if lg.persisted == nil || lg.persisted.Generation != args.Generation {
		g.mu.Unlock()
		return &SyncGroupResult{ErrorCode: illegalGeneration}, nil
	}
	m, ok := lg.persisted.Members[args.MemberID]
	if !ok {
		g.mu.Unlock()
		return &SyncGroupResult{ErrorCode: unknownMember}, nil
	}
	lg.heartbeats[args.MemberID] = time.Now()
	round := lg.round

	if args.MemberID == lg.leader {
		for mid, a := range args.Assignments {
			if mm, ok := lg.persisted.Members[mid]; ok {
				mm.Assignment = a
			}
		}
		lg.state = GroupStable
		if err := g.store.Persist(lg.persisted); err != nil {
			g.mu.Unlock()
			return nil, err
		}
		if round != nil && !round.synced {
			round.synced = true
			close(round.sync)
		}
		assignment := m.Assignment
		g.mu.Unlock()
		return &SyncGroupResult{Assignment: assignment}, nil
	}

	// Follower: wait for the leader.
	if round == nil || round.synced {
		assignment := m.Assignment
		g.mu.Unlock()
		return &SyncGroupResult{Assignment: assignment}, nil
	}
	syncCh := round.sync
	g.mu.Unlock()

	select {
	case <-syncCh:
	case <-time.After(maxRebalanceWait):
		return &SyncGroupResult{ErrorCode: rebalanceInProgress}, nil
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	if lg.persisted == nil || lg.persisted.Generation != args.Generation {
		return &SyncGroupResult{ErrorCode: illegalGeneration}, nil
	}
	mm, ok := lg.persisted.Members[args.MemberID]
	if !ok {
		return &SyncGroupResult{ErrorCode: unknownMember}, nil
	}
	return &SyncGroupResult{Assignment: mm.Assignment}, nil
}

// HeartbeatArgs is the heartbeat input.
type HeartbeatArgs struct {
	GroupID    string
	MemberID   string
	Generation int32
}

// Heartbeat refreshes the member's last-seen timestamp and returns
// REBALANCE_IN_PROGRESS if the group has moved on without it.
func (g *GroupCoordinator) Heartbeat(args HeartbeatArgs) int16 {
	g.mu.Lock()
	defer g.mu.Unlock()
	lg, ok := g.live[args.GroupID]
	if !ok {
		return invalidGroupID
	}
	if lg.persisted == nil || lg.persisted.Generation != args.Generation {
		return illegalGeneration
	}
	if _, ok := lg.persisted.Members[args.MemberID]; !ok {
		return unknownMember
	}
	lg.heartbeats[args.MemberID] = time.Now()
	if lg.state == GroupPreparingRebalance {
		return rebalanceInProgress
	}
	return 0
}

// LeaveGroup removes a member. Triggers a fresh rebalance for the
// remaining members on next JoinGroup.
func (g *GroupCoordinator) LeaveGroup(groupID, memberID string) int16 {
	g.mu.Lock()
	defer g.mu.Unlock()
	lg, ok := g.live[groupID]
	if !ok {
		return invalidGroupID
	}
	if lg.persisted == nil {
		return invalidGroupID
	}
	if _, ok := lg.persisted.Members[memberID]; !ok {
		return unknownMember
	}
	delete(lg.persisted.Members, memberID)
	delete(lg.heartbeats, memberID)
	if lg.leader == memberID {
		lg.leader = pickLeader(lg.persisted.Members)
		lg.persisted.Leader = lg.leader
	}
	if len(lg.persisted.Members) == 0 {
		lg.state = GroupEmpty
	} else {
		lg.state = GroupPreparingRebalance
	}
	_ = g.store.Persist(lg.persisted)
	return 0
}

// Snapshot is the admin-readable state of one group.
type Snapshot struct {
	GroupID    string
	State      string
	Generation int32
	Protocol   string
	Members    []MemberSnapshot
}

// MemberSnapshot is one member's state.
type MemberSnapshot struct {
	MemberID         string
	ClientID         string
	ClientHost       string
	SessionTimeoutMS int32
	LastHeartbeat    time.Time
}

// AllGroups returns a snapshot of every known group.
func (g *GroupCoordinator) AllGroups() []Snapshot {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]Snapshot, 0, len(g.live))
	for _, lg := range g.live {
		s := Snapshot{
			GroupID:    lg.id,
			State:      lg.state.String(),
			Generation: lg.persisted.Generation,
			Protocol:   lg.persisted.Protocol,
		}
		for _, m := range lg.persisted.Members {
			s.Members = append(s.Members, MemberSnapshot{
				MemberID:         m.MemberID,
				ClientID:         m.ClientID,
				ClientHost:       m.ClientHost,
				SessionTimeoutMS: m.SessionTimeoutMS,
				LastHeartbeat:    lg.heartbeats[m.MemberID],
			})
		}
		out = append(out, s)
	}
	return out
}

// ── helpers ──────────────────────────────────────────────────────────

func newMemberID(clientID string) string {
	var rnd [8]byte
	_, _ = rand.Read(rnd[:])
	return fmt.Sprintf("%s-%s", sanitizeClientID(clientID), hex.EncodeToString(rnd[:]))
}

func sanitizeClientID(s string) string {
	if s == "" {
		return "client"
	}
	// Strip anything that's not [A-Za-z0-9._-] for filesystem safety
	// (member IDs end up in JSON state files but never as filenames;
	// belt-and-braces).
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') ||
			(c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') ||
			c == '.' || c == '_' || c == '-' {
			out = append(out, c)
		}
	}
	if len(out) == 0 {
		return "client"
	}
	return string(out)
}

func pickLeader(members map[string]*MemberState) string {
	if len(members) == 0 {
		return ""
	}
	ids := make([]string, 0, len(members))
	for id := range members {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids[0]
}

// joinGroupSubscriptionBytes flattens the protocols slice into a
// single bytes blob keyed by name. We persist the FIRST protocol's
// metadata only: this build does not support per-protocol selection.
func joinGroupSubscriptionBytes(protocols []GroupProtocol) []byte {
	if len(protocols) == 0 {
		return nil
	}
	return protocols[0].Metadata
}

// Local error-code aliases. These shadow the wire package's constants
// to avoid an import cycle (broker -> wire would loop). Same values.
const (
	invalidGroupID      int16 = 24
	memberIDRequired    int16 = 79
	illegalGeneration   int16 = 22
	unknownMember       int16 = 25
	rebalanceInProgress int16 = 27
)

// ErrInvalidGeneration is the broker-internal sentinel for old
// generation IDs. Wire layer translates to the int16 code.
var ErrInvalidGeneration = errors.New("illegal generation")
