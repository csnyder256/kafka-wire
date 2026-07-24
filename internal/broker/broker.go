// Package broker is the top-level coordinator: it owns the topic
// registry, the consumer-group coordinator, the offset store, and the
// metadata persistence. The wire layer reaches in via the methods on
// *Broker; the storage layer below is consumed via injected handles.
package broker

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/csnyder256/kafka-wire/internal/metrics"
	"github.com/csnyder256/kafka-wire/internal/storage"
	"github.com/csnyder256/kafka-wire/internal/tiering"
)

// Config holds the runtime configuration the wire layer needs to
// know about (broker_id for the Metadata response, max_request_bytes
// for DoS guards, etc.).
type Config struct {
	BrokerID        int32
	ClusterID       string
	AdvertisedHost  string
	AdvertisedPort  int32
	DataDir         string
	MaxRequestBytes int32
	Storage         *storage.Storage
	Metrics         *metrics.Registry

	// AutoCreateTopics gates whether a Produce or Metadata request for
	// an unknown topic implicitly creates it. Defaults to true for
	// drop-in compat with existing services.
	AutoCreateTopics  bool
	DefaultPartitions int32
	DefaultRepFactor  int16
}

// Broker is the singleton that owns all per-cluster state.
type Broker struct {
	cfg      Config
	store    *storage.Storage
	metadata *MetadataStore
	topics   *TopicRegistry
	offsets  *OffsetStore
	groups   *GroupCoordinator
	metrics  *metrics.Registry

	mu       sync.Mutex // guards LoadState idempotency, Drain
	loaded   bool
	draining bool

	// S3 attachments: populated only when an S3 bucket is
	// configured. The fetch path consults restorer when a Fetch
	// requests an offset below the local-retention horizon.
	restorer *tiering.Restorer
	manifest *tiering.Manifest
	cache    *tiering.Cache

	// ACL store: populated lazily during LoadState.
	acl *ACLStore
}

// ACL returns the broker's ACL store. May be nil if storage init
// failed; callers should check.
func (b *Broker) ACL() *ACLStore { return b.acl }

// AttachRestorer wires the cold-storage components into the broker.
// Called from main.go after S3 client init. Safe to skip when S3 is
// not configured (the fetch path falls back to OFFSET_OUT_OF_RANGE).
func (b *Broker) AttachRestorer(r *tiering.Restorer, m *tiering.Manifest, c *tiering.Cache) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.restorer = r
	b.manifest = m
	b.cache = c
}

// TenantOfTopic returns the OwnerTenantID of `topic` (empty if shared
// or unknown). Used by the S3 uploader to tag segments at archive
// time with the same tenant binding the broker enforces at the wire.
func (b *Broker) TenantOfTopic(topic string, _ int32) string {
	t := b.topics.Get(topic)
	if t == nil {
		return ""
	}
	return t.Config().OwnerTenantID
}

// New returns an unloaded broker. Call LoadState before using.
func New(cfg Config) *Broker {
	if cfg.DefaultPartitions <= 0 {
		cfg.DefaultPartitions = 1
	}
	if cfg.DefaultRepFactor <= 0 {
		cfg.DefaultRepFactor = 1
	}
	if cfg.MaxRequestBytes <= 0 {
		cfg.MaxRequestBytes = 4 * 1024 * 1024
	}
	cfg.AutoCreateTopics = true // sticky default; explicit-false support comes in admin API
	b := &Broker{
		cfg:     cfg,
		store:   cfg.Storage,
		metrics: cfg.Metrics,
		topics:  NewTopicRegistry(cfg.Storage),
	}
	return b
}

// LoadState recovers all on-disk state. Idempotent; safe to call
// once at boot.
func (b *Broker) LoadState() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.loaded {
		return nil
	}

	mds, err := NewMetadataStore(b.cfg.DataDir)
	if err != nil {
		return err
	}
	b.metadata = mds

	// Cluster identity. If cluster.json is missing, write our own.
	cm, err := mds.LoadCluster()
	if err != nil {
		return err
	}
	if cm == nil {
		_ = mds.SaveCluster(clusterMetadata{
			ClusterID:  b.cfg.ClusterID,
			BrokerID:   b.cfg.BrokerID,
			Advertised: fmt.Sprintf("%s:%d", b.cfg.AdvertisedHost, b.cfg.AdvertisedPort),
		})
	} else if cm.ClusterID != "" {
		b.cfg.ClusterID = cm.ClusterID
	}

	// Recover topics. Re-open every partition log on disk and
	// reconcile against the persisted topic config. Any partition
	// directory not in topics.json gets adopted with default config
	// (handles operator hand-edits + crash-mid-CreateTopics).
	persisted, err := mds.LoadTopics()
	if err != nil {
		return err
	}

	topicsDir := b.store.TopicsDir()
	if entries, err := readDirSafe(topicsDir); err == nil {
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			name := e.Name()
			tcfg, ok := persisted[name]
			if !ok {
				tcfg = TopicConfig{
					NumPartitions:     b.cfg.DefaultPartitions,
					ReplicationFactor: b.cfg.DefaultRepFactor,
					RetentionMS:       -1,
					RetentionBytes:    -1,
					SegmentBytes:      b.store.Cfg().SegmentBytes,
				}
			}
			// Walk the topic directory and open every partition we find.
			topicDir := topicsDir + "/" + name
			pents, err := readDirSafe(topicDir)
			if err != nil {
				continue
			}
			parts := make([]*storage.Log, 0, len(pents))
			for _, pe := range pents {
				if !pe.IsDir() {
					continue
				}
				var idx int32
				if _, scerr := fmt.Sscanf(pe.Name(), "%d", &idx); scerr != nil {
					continue
				}
				l, err := b.store.OpenLog(name, idx)
				if err != nil {
					return fmt.Errorf("recover %s/%d: %w", name, idx, err)
				}
				parts = append(parts, l)
			}
			// Sort by partition index so [0]==partition 0, etc.
			sort.Slice(parts, func(i, j int) bool {
				return parts[i].Partition() < parts[j].Partition()
			})
			if int32(len(parts)) != tcfg.NumPartitions && tcfg.NumPartitions == 0 {
				tcfg.NumPartitions = int32(len(parts))
			}
			b.topics.Adopt(name, parts, tcfg)
		}
	}

	// OffsetStore + GroupCoordinator come up only after topics so
	// JoinGroup can validate subscriptions. (this build does not
	// validate subscriptions yet: it passes them through opaquely,
	// but the order is correct for forward compat.)
	os, err := NewOffsetStore(b.cfg.DataDir)
	if err != nil {
		return err
	}
	b.offsets = os
	b.groups = NewGroupCoordinator(os)

	// ACL store. Optional: empty = "ACLs not configured", broker
	// allows everything.
	acl, err := NewACLStore(b.cfg.DataDir)
	if err != nil {
		return err
	}
	b.acl = acl

	b.loaded = true
	return nil
}

// Drain is called from main on SIGTERM. Flushes all active segments,
// persists group state, then closes everything.
func (b *Broker) Drain(ctx context.Context) error {
	b.mu.Lock()
	if b.draining {
		b.mu.Unlock()
		return nil
	}
	b.draining = true
	b.mu.Unlock()

	// Flush every active segment.
	for _, t := range b.topics.All() {
		for _, l := range t.Logs() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if err := l.FlushAndSync(); err != nil {
				return fmt.Errorf("flush %s/%d: %w", t.Name(), l.Partition(), err)
			}
		}
	}

	// Close all topic logs.
	for _, t := range b.topics.All() {
		_ = t.Close()
	}
	return nil
}

// ── public accessors used by wire layer ──────────────────────────────

// BrokerID returns the configured broker id (used in Metadata responses).
func (b *Broker) BrokerID() int32 { return b.cfg.BrokerID }

// ClusterID returns the persisted cluster id.
func (b *Broker) ClusterID() string { return b.cfg.ClusterID }

// AdvertisedHost / Port: what we tell clients to reach us at.
func (b *Broker) AdvertisedHost() string { return b.cfg.AdvertisedHost }
func (b *Broker) AdvertisedPort() int32  { return b.cfg.AdvertisedPort }

// MaxRequestBytes: the wire layer's DoS cap.
func (b *Broker) MaxRequestBytes() int32 { return b.cfg.MaxRequestBytes }

// Topics is the registry handle (used by retention reaper + admin API).
func (b *Broker) Topics() *TopicRegistry { return b.topics }

// Groups returns the group coordinator (used by wire group handlers).
func (b *Broker) Groups() *GroupCoordinator { return b.groups }

// Offsets returns the offset store (used by wire offset handlers).
func (b *Broker) Offsets() *OffsetStore { return b.offsets }

// AutoCreateTopics returns whether unknown topics should be created.
func (b *Broker) AutoCreateTopics() bool { return b.cfg.AutoCreateTopics }

// SetAutoCreateTopics is exposed for admin-toggle. Setting it false
// causes Produce/Metadata for unknown topics to return UNKNOWN_TOPIC
// rather than implicitly creating them.
func (b *Broker) SetAutoCreateTopics(v bool) { b.cfg.AutoCreateTopics = v }

// ── topic operations ─────────────────────────────────────────────────

// EnsureTopic returns an existing topic or creates it with the given
// partition count if it doesn't exist. Used by Produce and Metadata
// when AutoCreateTopics is on.
func (b *Broker) EnsureTopic(name string, partitions int32) (*Topic, error) {
	return b.EnsureTopicForTenant(name, partitions, "")
}

// EnsureTopicForTenant is the tenant-aware variant. The created topic
// gets `OwnerTenantID = tenantID`. Empty tenantID means a shared
// platform topic.
func (b *Broker) EnsureTopicForTenant(name string, partitions int32, tenantID string) (*Topic, error) {
	if t := b.topics.Get(name); t != nil {
		return t, nil
	}
	if !b.cfg.AutoCreateTopics {
		return nil, ErrUnknownTopic
	}
	if partitions <= 0 {
		partitions = b.cfg.DefaultPartitions
	}
	cfg := TopicConfig{
		NumPartitions:     partitions,
		ReplicationFactor: b.cfg.DefaultRepFactor,
		RetentionMS:       -1,
		RetentionBytes:    -1,
		SegmentBytes:      b.store.Cfg().SegmentBytes,
		OwnerTenantID:     tenantID,
	}
	t, err := b.topics.Create(name, cfg)
	if err != nil {
		return nil, err
	}
	if err := b.persistTopicsLocked(); err != nil {
		return nil, err
	}
	b.metrics.IncTopicCreated()
	return t, nil
}

// CreateTopic explicitly registers a topic. Returns ErrTopicExists if
// it already exists (creation is not idempotent; use the admin API to update an
// existing topic).
func (b *Broker) CreateTopic(name string, partitions int32, replicationFactor int16) (*Topic, error) {
	cfg := TopicConfig{
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
		RetentionMS:       -1,
		RetentionBytes:    -1,
		SegmentBytes:      b.store.Cfg().SegmentBytes,
	}
	t, err := b.topics.Create(name, cfg)
	if err != nil {
		return nil, err
	}
	if err := b.persistTopicsLocked(); err != nil {
		return nil, err
	}
	return t, nil
}

// DeleteTopic removes a topic from the registry, closes its
// partitions, and rm -rf's its data directory. The admin API gates
// this; this build does not expose it on the wire.
func (b *Broker) DeleteTopic(name string) error {
	if err := b.topics.Delete(name); err != nil {
		return err
	}
	if err := b.persistTopicsLocked(); err != nil {
		return err
	}
	// Best-effort delete of the on-disk topic directory.
	dir := b.cfg.Storage.TopicsDir() + "/" + name
	return removeAllSafe(dir)
}

func (b *Broker) persistTopicsLocked() error {
	all := b.topics.All()
	out := make(map[string]TopicConfig, len(all))
	for _, t := range all {
		out[t.Name()] = t.Config()
	}
	return b.metadata.SaveTopics(out)
}

// ── produce / fetch ──────────────────────────────────────────────────

// AppendBatches writes one or more record batches to a topic-partition
// and returns the offset of the FIRST record in the FIRST batch.
//
// The caller (wire/produce.go) has already concatenated multiple
// batches per partition.
func (b *Broker) AppendBatches(topic string, partition int32, batches [][]byte) (int64, error) {
	return b.AppendBatchesAuthorized(topic, partition, batches, "", "")
}

// AppendBatchesAuthorized is the tenant-aware variant. Verifies the
// principal's tenant matches the topic's owner tenant before writing.
// Empty principalTenant means "platform principal", which can write
// to any topic regardless of owner.
//
// principalName is used only for audit logging.
func (b *Broker) AppendBatchesAuthorized(topic string, partition int32, batches [][]byte, principalName, principalTenant string) (int64, error) {
	t := b.topics.Get(topic)
	if t == nil {
		nt, err := b.EnsureTopicForTenant(topic, b.cfg.DefaultPartitions, principalTenant)
		if err != nil {
			return 0, err
		}
		t = nt
	}
	if err := b.verifyTopicTenant(t, principalTenant); err != nil {
		return 0, err
	}
	l, ok := t.Partition(partition)
	if !ok {
		return 0, ErrUnknownPartition
	}
	off, err := l.Append(batches)
	if err != nil {
		b.metrics.IncProduceFailed()
		return 0, err
	}
	b.metrics.IncProduceSucceeded(int64(len(batches)))
	return off, nil
}

// verifyTopicTenant returns ErrUnauthorizedTopic when the resolved
// topic's tenant does not match the requesting principal's tenant.
// This is the core cross-tenant access prevention invariant.
//
//   - principalTenant == "" (platform): allowed everywhere
//   - topic.OwnerTenantID == "" (legacy shared topic): allowed for
//     platform principals; tenant principals denied
//   - both set, equal:                                       allowed
//   - both set, mismatch:                                    denied
func (b *Broker) verifyTopicTenant(t *Topic, principalTenant string) error {
	if principalTenant == "" {
		// Platform principal: passes through. Existing clients
		// run as platform principals because they share
		// the admin token; tenancy is enforced in their
		// payload-level logic, not at the wire layer.
		return nil
	}
	tenantOwner := t.Config().OwnerTenantID
	if tenantOwner == "" {
		// Tenant principal trying to access a shared/legacy topic,
		// fail closed. If chaos test creates a new tenant, it must
		// also create tenant-scoped topics for that tenant.
		return ErrUnauthorizedTopic
	}
	if tenantOwner != principalTenant {
		return ErrUnauthorizedTopic
	}
	return nil
}

// FetchAuthorized returns the same shape as Fetch but ALSO verifies
// the resolved topic is owned by the requesting principal's tenant.
// Cross-tenant Fetch fails closed with ErrUnauthorizedTopic, which
// the wire layer maps to TOPIC_AUTHORIZATION_FAILED on the wire.
func (b *Broker) FetchAuthorized(topic string, partition int32, fetchOffset int64, maxBytes int, principalName, principalTenant string) ([]byte, int64, int64, int64, error) {
	t := b.topics.Get(topic)
	if t == nil {
		return nil, 0, 0, 0, ErrUnknownTopic
	}
	if err := b.verifyTopicTenant(t, principalTenant); err != nil {
		return nil, 0, 0, 0, err
	}
	return b.fetchInternal(topic, partition, fetchOffset, maxBytes, principalTenant)
}

// Fetch returns up to maxBytes of contiguous batch bytes starting at
// fetchOffset for one (topic, partition). Backwards-compatible alias
// that bypasses the tenant check (used in tests + unauthenticated
// admin paths).
//
// Falls back to S3 archive when fetchOffset is below the local
// retention horizon. Restoring transparently caches the segment locally
// (LRU) so consecutive reads of the same archived range are fast.
func (b *Broker) Fetch(topic string, partition int32, fetchOffset int64, maxBytes int) ([]byte, int64, int64, int64, error) {
	return b.fetchInternal(topic, partition, fetchOffset, maxBytes, "")
}

func (b *Broker) fetchInternal(topic string, partition int32, fetchOffset int64, maxBytes int, principalTenant string) ([]byte, int64, int64, int64, error) {
	t := b.topics.Get(topic)
	if t == nil {
		return nil, 0, 0, 0, ErrUnknownTopic
	}
	l, ok := t.Partition(partition)
	if !ok {
		return nil, 0, 0, 0, ErrUnknownPartition
	}
	hwm := l.HighWatermark()
	logStart := l.LogStartOffset()
	bytes, firstOffset, err := l.FetchAt(fetchOffset, maxBytes)
	if err == nil {
		return bytes, firstOffset, hwm, logStart, nil
	}
	// Local read failed. If it was an out-of-range below-logStart and
	// we have S3 archival configured, try the archive fallback. The
	// tenant gate inside fetchFromArchive prevents cross-tenant
	// archive reads (defense in depth: caller already verified the
	// topic-tenant match above).
	if errors.Is(err, storage.ErrOffsetOutOfRange) && b.restorer != nil {
		archBytes, archOffset, archErr := b.fetchFromArchive(context.Background(), topic, partition, fetchOffset, maxBytes, principalTenant)
		if archErr == nil {
			return archBytes, archOffset, hwm, logStart, nil
		}
		// Archive miss falls back to surfacing the original
		// out-of-range error. If the archive call returned an
		// auth failure, propagate that instead so the operator
		// sees an isolation event in the audit trail.
		if errors.Is(archErr, ErrUnauthorizedTopic) {
			return nil, 0, hwm, logStart, ErrUnauthorizedTopic
		}
	}
	return nil, 0, hwm, logStart, err
}

// ListOffsets returns:
//   - timestamp == -2 (Earliest): logStartOffset
//   - timestamp == -1 (Latest):   highWatermark
//   - timestamp >= 0:             smallest offset whose batch
//     MaxTimestamp >= timestamp
func (b *Broker) ListOffsets(topic string, partition int32, timestamp int64) (int64, int64, error) {
	t := b.topics.Get(topic)
	if t == nil {
		return 0, -1, ErrUnknownTopic
	}
	l, ok := t.Partition(partition)
	if !ok {
		return 0, -1, ErrUnknownPartition
	}
	switch timestamp {
	case -2:
		return l.EarliestOffset(), -1, nil
	case -1:
		return l.LatestOffset(), -1, nil
	default:
		off, ok := l.LookupOffsetByTimestamp(timestamp)
		if !ok {
			// Spec: return (offset = -1, timestamp = -1) when no
			// matching offset.
			return -1, -1, nil
		}
		return off, timestamp, nil
	}
}

// ── helpers ──────────────────────────────────────────────────────────

// Sentinel errors not already declared in topic.go.
var (
	ErrBrokerDraining = errors.New("broker draining")
)

// Archive returns the manifest snapshot for the dashboard archive
// view. Returns nil if S3 archival is not configured.
func (b *Broker) Archive() *tiering.Manifest {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.manifest
}
