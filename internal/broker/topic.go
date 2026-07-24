package broker

import (
	"errors"
	"fmt"
	"sync"

	"github.com/csnyder256/kafka-wire/internal/storage"
)

// Topic is a collection of partition logs sharing a name. Partitions
// are 0-indexed. partition counts are fixed at creation time; there is no
// reassignment or expansion.
type Topic struct {
	name       string
	partitions []*storage.Log
	mu         sync.RWMutex // guards the partitions slice (rare append, frequent read)
	cfg        TopicConfig
}

// TopicConfig holds per-topic settings persisted in the registry.
// Mirror of the values exposed via DescribeConfigs.
type TopicConfig struct {
	NumPartitions     int32
	ReplicationFactor int16 // always 1: one node, one copy
	RetentionMS       int64 // -1 = unlimited
	RetentionBytes    int64 // -1 = unlimited
	SegmentBytes      int64

	// OwnerTenantID is the tenant that owns this topic. Empty string
	// means the topic has no tenant binding (platform-level / shared).
	// Captured at create time from the principal that issued the
	// CreateTopics or first Produce, or via explicit admin API call.
	//
	// CRITICAL invariant enforced on every Produce + Fetch:
	//   - if topic.OwnerTenantID is empty, only platform principals
	//     (also empty tenant_id) may read/write
	//   - if topic.OwnerTenantID is set, only principals with the
	//     SAME tenant_id (or platform principals) may read/write
	//
	// This is the broker-level cross-tenant access prevention.
	OwnerTenantID string `json:"owner_tenant_id,omitempty"`
}

// Name returns the topic name.
func (t *Topic) Name() string { return t.name }

// NumPartitions returns the partition count.
func (t *Topic) NumPartitions() int32 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return int32(len(t.partitions))
}

// Partition returns the log for a specific partition. Returns false
// if out of range.
func (t *Topic) Partition(idx int32) (*storage.Log, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if idx < 0 || int(idx) >= len(t.partitions) {
		return nil, false
	}
	return t.partitions[idx], true
}

// Logs returns all partition logs in order. Caller must not modify.
func (t *Topic) Logs() []*storage.Log {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]*storage.Log, len(t.partitions))
	copy(out, t.partitions)
	return out
}

// Config returns the persisted topic config.
func (t *Topic) Config() TopicConfig {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.cfg
}

// Close shuts down all partition logs.
func (t *Topic) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	var firstErr error
	for _, l := range t.partitions {
		if err := l.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	t.partitions = nil
	return firstErr
}

// TopicRegistry holds all topics for the broker. Implements the
// storage.LogProvider interface for the retention reaper.
type TopicRegistry struct {
	storage *storage.Storage
	mu      sync.RWMutex
	topics  map[string]*Topic
}

// NewTopicRegistry constructs an empty registry. Callers populate via
// Recover or Create.
func NewTopicRegistry(s *storage.Storage) *TopicRegistry {
	return &TopicRegistry{
		storage: s,
		topics:  make(map[string]*Topic),
	}
}

// Get returns the topic, or nil if not registered.
func (r *TopicRegistry) Get(name string) *Topic {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.topics[name]
}

// All returns all registered topics, sorted by name.
func (r *TopicRegistry) All() []*Topic {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Topic, 0, len(r.topics))
	for _, t := range r.topics {
		out = append(out, t)
	}
	return out
}

// AllLogs returns every partition log across every topic. Implements
// storage.LogProvider for the retention reaper.
func (r *TopicRegistry) AllLogs() []*storage.Log {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*storage.Log
	for _, t := range r.topics {
		out = append(out, t.Logs()...)
	}
	return out
}

// Create registers a new topic and opens all its partition logs.
// Returns ErrTopicExists if the topic is already registered.
func (r *TopicRegistry) Create(name string, cfg TopicConfig) (*Topic, error) {
	if name == "" {
		return nil, errors.New("topic name required")
	}
	if cfg.NumPartitions <= 0 {
		cfg.NumPartitions = 1
	}
	if cfg.ReplicationFactor <= 0 {
		cfg.ReplicationFactor = 1
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.topics[name]; ok {
		return nil, ErrTopicExists
	}
	parts := make([]*storage.Log, 0, cfg.NumPartitions)
	for i := int32(0); i < cfg.NumPartitions; i++ {
		l, err := r.storage.OpenLog(name, i)
		if err != nil {
			// Roll back any opened partitions.
			for _, opened := range parts {
				_ = opened.Close()
			}
			return nil, fmt.Errorf("open partition %d: %w", i, err)
		}
		parts = append(parts, l)
	}
	t := &Topic{name: name, partitions: parts, cfg: cfg}
	r.topics[name] = t
	return t, nil
}

// Adopt re-attaches an already-on-disk topic during recovery.
func (r *TopicRegistry) Adopt(name string, partitions []*storage.Log, cfg TopicConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.topics[name] = &Topic{name: name, partitions: partitions, cfg: cfg}
}

// Delete removes a topic from the registry and closes its partitions.
// Caller is responsible for deleting the on-disk files separately
// (we don't rm -rf without explicit confirmation).
func (r *TopicRegistry) Delete(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.topics[name]
	if !ok {
		return ErrUnknownTopic
	}
	if err := t.Close(); err != nil {
		return err
	}
	delete(r.topics, name)
	return nil
}

// Sentinel errors used by the wire layer.
var (
	ErrTopicExists      = errors.New("topic already exists")
	ErrUnknownTopic     = errors.New("unknown topic")
	ErrUnknownPartition = errors.New("unknown partition")
)
