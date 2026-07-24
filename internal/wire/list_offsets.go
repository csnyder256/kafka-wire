package wire

import (
	"errors"

	"github.com/csnyder256/kafka-wire/internal/broker"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// handleListOffsets answers "give me the earliest / latest offset for
// this partition, or the offset whose batch first reached this
// timestamp." Used by consumers on init (auto.offset.reset)
// and by the dashboard for time-travel queries.
//
// Special timestamp values:
//
//	-2 = LIST_EARLIEST
//	-1 = LIST_LATEST
//	>=0 = "smallest offset whose batch.MaxTimestamp >= ts"
func (d *Dispatcher) handleListOffsets(state *connState, hdr RequestHeader, body []byte) error {
	req := kmsg.NewPtrListOffsetsRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return err
	}

	resp := kmsg.NewPtrListOffsetsResponse()
	resp.SetVersion(hdr.APIVersion)

	acl := d.brk.ACL()
	for _, t := range req.Topics {
		// Offsets disclose how much traffic a topic carries, so they follow
		// the same read grant the records themselves do.
		if acl != nil && !acl.AuthorizeTopic(state.saslPrincipal, t.Topic, "read") {
			rt := kmsg.ListOffsetsResponseTopic{Topic: t.Topic}
			for _, part := range t.Partitions {
				rt.Partitions = append(rt.Partitions, kmsg.ListOffsetsResponseTopicPartition{
					Partition: part.Partition,
					ErrorCode: errCodeTopicAuthorizationFailed,
				})
			}
			resp.Topics = append(resp.Topics, rt)
			continue
		}
		rt := kmsg.ListOffsetsResponseTopic{Topic: t.Topic}
		for _, p := range t.Partitions {
			rp := kmsg.ListOffsetsResponseTopicPartition{
				Partition: p.Partition,
			}
			off, ts, err := d.brk.ListOffsets(t.Topic, p.Partition, p.Timestamp)
			if err != nil {
				if errors.Is(err, broker.ErrUnknownTopic) || errors.Is(err, broker.ErrUnknownPartition) {
					rp.ErrorCode = errCodeUnknownTopicOrPart
				} else {
					rp.ErrorCode = errCodeCorruptMessage
				}
				rt.Partitions = append(rt.Partitions, rp)
				continue
			}
			rp.ErrorCode = errCodeNone
			rp.Timestamp = ts
			rp.Offset = off
			rt.Partitions = append(rt.Partitions, rp)
		}
		resp.Topics = append(resp.Topics, rt)
	}
	return d.writeKmsgResponse(state, hdr, resp, req.IsFlexible())
}
