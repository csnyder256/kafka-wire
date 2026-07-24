package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// OffsetStore persists committed consumer-group offsets per (group,
// topic, partition). State is serialized to one JSON file per group
// under $DATA_DIR/groups/<group>.json with atomic write-temp-then-rename.
//
// Format on disk:
//
//	{
//	  "group_id": "a worker",
//	  "generation": 7,
//	  "members": { ... },             // see group.go
//	  "offsets": {
//	    "orders.events": {
//	      "0": {
//	        "offset": 12345,
//	        "leader_epoch": 0,
//	        "metadata": "",
//	        "commit_timestamp": 1714234567000
//	      }
//	    }
//	  }
//	}
type OffsetStore struct {
	dir    string
	mu     sync.Mutex
	groups map[string]*GroupState
}

// GroupState is the persisted shape per group. Members + offsets
// share a file so a single atomic rename captures both.
type GroupState struct {
	GroupID      string                               `json:"group_id"`
	Generation   int32                                `json:"generation"`
	ProtocolType string                               `json:"protocol_type"`
	Protocol     string                               `json:"protocol"`
	Members      map[string]*MemberState              `json:"members"`
	Offsets      map[string]map[int32]CommittedOffset `json:"offsets"`
	Leader       string                               `json:"leader"`
	UpdatedAt    int64                                `json:"updated_at_ms"`
}

// CommittedOffset is one committed (group, topic, partition) tuple.
type CommittedOffset struct {
	Offset          int64  `json:"offset"`
	LeaderEpoch     int32  `json:"leader_epoch"`
	Metadata        string `json:"metadata"`
	CommitTimestamp int64  `json:"commit_timestamp_ms"`
}

// MemberState is one consumer-group member. Live state (heartbeat
// timestamps) lives in memory in the coordinator; this is the
// rebalance-time snapshot we persist so a restart can resume the
// previous rebalance without losing offsets.
type MemberState struct {
	MemberID           string `json:"member_id"`
	ClientID           string `json:"client_id"`
	ClientHost         string `json:"client_host"`
	SessionTimeoutMS   int32  `json:"session_timeout_ms"`
	RebalanceTimeoutMS int32  `json:"rebalance_timeout_ms"`
	Subscription       []byte `json:"subscription"` // metadata bytes from JoinGroup
	Assignment         []byte `json:"assignment"`   // assignment bytes from SyncGroup
	// Protocols is the assignment-protocol list this member advertised, in
	// its own preference order. The coordinator needs it to pick a protocol
	// every member of the group can actually decode.
	Protocols []string `json:"protocols,omitempty"`
}

// NewOffsetStore opens (creates) the groups directory and loads any
// existing state files. Errors on the first unparseable file (refuse
// to silently lose committed offsets).
func NewOffsetStore(dataDir string) (*OffsetStore, error) {
	dir := filepath.Join(dataDir, "groups")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("offsets: mkdir groups: %w", err)
	}
	s := &OffsetStore{dir: dir, groups: make(map[string]*GroupState)}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("offsets: read groups dir: %w", err)
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		path := filepath.Join(dir, name)
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("offsets: read %s: %w", path, err)
		}
		var gs GroupState
		if err := json.Unmarshal(raw, &gs); err != nil {
			return nil, fmt.Errorf("offsets: parse %s: %w", path, err)
		}
		if gs.GroupID == "" {
			// Use the filename minus extension as the group id.
			gs.GroupID = name[:len(name)-len(".json")]
		}
		if gs.Members == nil {
			gs.Members = make(map[string]*MemberState)
		}
		if gs.Offsets == nil {
			gs.Offsets = make(map[string]map[int32]CommittedOffset)
		}
		s.groups[gs.GroupID] = &gs
	}
	return s, nil
}

// Get returns the live state pointer for the group, creating an
// empty record if absent. Caller holds the broker-coordinator lock.
func (s *OffsetStore) Get(groupID string) *GroupState {
	s.mu.Lock()
	defer s.mu.Unlock()
	gs, ok := s.groups[groupID]
	if !ok {
		gs = &GroupState{
			GroupID:   groupID,
			Members:   make(map[string]*MemberState),
			Offsets:   make(map[string]map[int32]CommittedOffset),
			UpdatedAt: time.Now().UnixMilli(),
		}
		s.groups[groupID] = gs
	}
	return gs
}

// All returns a snapshot of all groups. Used by the admin API.
func (s *OffsetStore) All() []*GroupState {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*GroupState, 0, len(s.groups))
	for _, gs := range s.groups {
		out = append(out, gs)
	}
	return out
}

// Persist atomically flushes one group's state to disk. Called after
// each OffsetCommit and after each successful SyncGroup.
func (s *OffsetStore) Persist(gs *GroupState) error {
	gs.UpdatedAt = time.Now().UnixMilli()
	raw, err := json.MarshalIndent(gs, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal group state: %w", err)
	}
	final := filepath.Join(s.dir, gs.GroupID+".json")
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return fmt.Errorf("write tmp: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}

// CommitOffset records (or overwrites) one committed offset for a
// group/topic/partition.
func (s *OffsetStore) CommitOffset(groupID, topic string, partition int32, offset int64, leaderEpoch int32, metadata string) error {
	s.mu.Lock()
	gs, ok := s.groups[groupID]
	if !ok {
		gs = &GroupState{
			GroupID:   groupID,
			Members:   make(map[string]*MemberState),
			Offsets:   make(map[string]map[int32]CommittedOffset),
			UpdatedAt: time.Now().UnixMilli(),
		}
		s.groups[groupID] = gs
	}
	if gs.Offsets == nil {
		gs.Offsets = make(map[string]map[int32]CommittedOffset)
	}
	if _, ok := gs.Offsets[topic]; !ok {
		gs.Offsets[topic] = make(map[int32]CommittedOffset)
	}
	gs.Offsets[topic][partition] = CommittedOffset{
		Offset:          offset,
		LeaderEpoch:     leaderEpoch,
		Metadata:        metadata,
		CommitTimestamp: time.Now().UnixMilli(),
	}
	s.mu.Unlock()
	return s.Persist(gs)
}

// FetchOffset returns the committed offset for one (group, topic,
// partition). Returns -1 if no commit recorded yet (Kafka convention
// for "no offset").
func (s *OffsetStore) FetchOffset(groupID, topic string, partition int32) CommittedOffset {
	s.mu.Lock()
	defer s.mu.Unlock()
	gs, ok := s.groups[groupID]
	if !ok {
		return CommittedOffset{Offset: -1}
	}
	parts, ok := gs.Offsets[topic]
	if !ok {
		return CommittedOffset{Offset: -1}
	}
	co, ok := parts[partition]
	if !ok {
		return CommittedOffset{Offset: -1}
	}
	return co
}

// ErrGroupNotFound signals an admin call asked for a group that
// doesn't exist on disk.
var ErrGroupNotFound = errors.New("group not found")

// AllOffsets returns every committed offset for a group, keyed by topic then
// partition.
//
// This backs the "give me everything this group has committed" form of
// OffsetFetch, which is what an OffsetFetch request with a null topic list
// means. That form is not an edge case: it is what
// kafka-consumer-groups.sh --describe, AdminClient.listConsumerGroupOffsets,
// and every consumer-lag dashboard send.
func (s *OffsetStore) AllOffsets(groupID string) map[string]map[int32]CommittedOffset {
	s.mu.Lock()
	defer s.mu.Unlock()
	gs, ok := s.groups[groupID]
	if !ok || gs.Offsets == nil {
		return nil
	}
	out := make(map[string]map[int32]CommittedOffset, len(gs.Offsets))
	for topic, parts := range gs.Offsets {
		cp := make(map[int32]CommittedOffset, len(parts))
		for p, co := range parts {
			cp[p] = co
		}
		out[topic] = cp
	}
	return out
}
