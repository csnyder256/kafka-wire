package broker

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Persisted metadata. Two flat JSON files under $DATA_DIR/metadata:
//
//   topics.json: topic registry (name -> TopicConfig)
//   cluster.json: cluster_id, broker_id, advertised endpoint
//
// On boot, broker.LoadState reads these and re-attaches every
// partition log already on disk. New topics get persisted via
// CreateTopics.

type clusterMetadata struct {
	ClusterID  string `json:"cluster_id"`
	BrokerID   int32  `json:"broker_id"`
	Advertised string `json:"advertised_listener"`
	Format     int    `json:"format_version"`
}

type topicsMetadata struct {
	Format int                    `json:"format_version"`
	Topics map[string]TopicConfig `json:"topics"`
}

const metadataFormat = 1

// MetadataStore handles read/write of cluster + topics JSON.
type MetadataStore struct {
	dir string
}

// NewMetadataStore initializes the metadata directory.
func NewMetadataStore(dataDir string) (*MetadataStore, error) {
	dir := filepath.Join(dataDir, "metadata")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("metadata: mkdir: %w", err)
	}
	return &MetadataStore{dir: dir}, nil
}

// LoadCluster reads cluster.json. Returns (nil, nil) if absent (caller
// initializes from config).
func (m *MetadataStore) LoadCluster() (*clusterMetadata, error) {
	path := filepath.Join(m.dir, "cluster.json")
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var c clusterMetadata
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("parse cluster.json: %w", err)
	}
	return &c, nil
}

// SaveCluster writes cluster.json atomically.
func (m *MetadataStore) SaveCluster(c clusterMetadata) error {
	c.Format = metadataFormat
	return m.atomicWrite("cluster.json", c)
}

// LoadTopics reads topics.json. Returns an empty registry if absent.
func (m *MetadataStore) LoadTopics() (map[string]TopicConfig, error) {
	path := filepath.Join(m.dir, "topics.json")
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return make(map[string]TopicConfig), nil
	}
	if err != nil {
		return nil, err
	}
	var tm topicsMetadata
	if err := json.Unmarshal(raw, &tm); err != nil {
		return nil, fmt.Errorf("parse topics.json: %w", err)
	}
	if tm.Topics == nil {
		tm.Topics = make(map[string]TopicConfig)
	}
	return tm.Topics, nil
}

// SaveTopics overwrites topics.json atomically.
func (m *MetadataStore) SaveTopics(topics map[string]TopicConfig) error {
	tm := topicsMetadata{Format: metadataFormat, Topics: topics}
	return m.atomicWrite("topics.json", tm)
}

func (m *MetadataStore) atomicWrite(name string, data interface{}) error {
	raw, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal %s: %w", name, err)
	}
	final := filepath.Join(m.dir, name)
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, final)
}
