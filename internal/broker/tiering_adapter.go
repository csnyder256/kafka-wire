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
