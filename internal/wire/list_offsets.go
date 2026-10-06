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
				rp := kmsg.NewListOffsetsResponseTopicPartition()
				rp.Partition = part.Partition
				rp.ErrorCode = errCodeTopicAuthorizationFailed
				rt.Partitions = append(rt.Partitions, rp)
			}
			resp.Topics = append(resp.Topics, rt)
			continue
		}
		rt := kmsg.ListOffsetsResponseTopic{Topic: t.Topic}
		for _, p := range t.Partitions {
			// Construct through kmsg's constructor rather than a bare
			// struct literal. ListOffsetsResponseTopicPartition.Default()
			// seeds Timestamp and Offset to -1 and LeaderEpoch to -1, the
			// protocol's "no such offset / no such epoch" markers. A
			// zero-value literal reports offset 0 / timestamp 0 / epoch 0
			// instead, so an error or an empty partition would carry a
			// value a consumer cannot distinguish from a real one.
			rp := kmsg.NewListOffsetsResponseTopicPartition()
			rp.Partition = p.Partition
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
			// v0 answers with an array of offsets where v1+ use the scalar
			// Offset field. The encoder writes exactly one of the two for
			// the negotiated version, so fill the array too: leaving it
			// nil handed every v0 client an empty offset list, which it
			// reads as "no offset available" even for a plain latest-offset
			// query.
			rp.OldStyleOffsets = []int64{off}
			rt.Partitions = append(rt.Partitions, rp)
		}
		resp.Topics = append(resp.Topics, rt)
	}
	return d.writeKmsgResponse(state, hdr, resp, req.IsFlexible())
}
