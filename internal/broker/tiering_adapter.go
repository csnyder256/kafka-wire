package broker

import (
	"time"

	"github.com/csnyder256/kafka-wire/internal/storage"
	"github.com/csnyder256/kafka-wire/internal/tiering"
)

// segmentSource is the bridge between *storage.Segment + its owning
// (topic, partition) and the tiering.SegmentSource interface the uploader
// consumes. Defined here (in broker/) to keep storage/ free of
// per-partition metadata concerns.
type segmentSource struct {
	topic     string
	partition int32
	seg       *storage.Segment
}

func (s segmentSource) Topic() string        { return s.topic }
func (s segmentSource) Partition() int32     { return s.partition }
func (s segmentSource) BaseOffset() int64    { return s.seg.BaseOffset() }
func (s segmentSource) NextOffset() int64    { return s.seg.NextOffset() }
func (s segmentSource) Size() int64          { return s.seg.Size() }
func (s segmentSource) LogPath() string      { return s.seg.LogPath() }
func (s segmentSource) CreatedAt() time.Time { return s.seg.CreatedAt() }

// AttachArchive wires the cold-storage tier into the broker and returns the
// uploader the caller should run.
//
// It exists so the tenant resolver is set in exactly one place, because an
// archived segment has to carry the tenant that owns its topic: the restore
// path refuses to serve a manifest entry whose TenantID does not match the
// requesting principal's. The resolver is the broker's own topic-ownership
// lookup, so the tenant recorded in the archive is the same binding the wire
// layer already enforces on Produce and Fetch.
//
// Leaving it unset (which is what happened before: the field existed, the
// lookup existed, and nothing joined them) uploaded every segment with an
// empty TenantID to the shared key layout, and the gate in fetchFromArchive
// then rejected every tenant principal with TOPIC_AUTHORIZATION_FAILED --
// cold storage read back as a permission error for the tenant that owned the
// data.
func (b *Broker) AttachArchive(store tiering.Store, manifest *tiering.Manifest, cache *tiering.Cache, ucfg tiering.Config, mreg tiering.Metrics) *tiering.Uploader {
	ucfg.TenantResolver = b.TenantOfTopic
	uploader := tiering.NewUploader(ucfg, store, manifest, mreg)
	b.AttachRestorer(tiering.NewRestorer("", cache, manifest, store, mreg), manifest, cache)
	return uploader
}

// AllSealedSegments returns one SegmentSource per sealed segment
// across every topic+partition. Implements tiering.LogProvider.
func (r *TopicRegistry) AllSealedSegments() []tiering.SegmentSource {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []tiering.SegmentSource
	for _, t := range r.topics {
		for _, l := range t.partitions {
			for _, seg := range l.SealedSegments() {
				out = append(out, segmentSource{
					topic:     t.name,
					partition: l.Partition(),
					seg:       seg,
				})
			}
		}
	}
	return out
}
