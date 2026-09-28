package wire

import (
	"sort"

	"github.com/csnyder256/kafka-wire/internal/broker"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// OffsetCommit: group offsets get persisted via the offset store.
// Each (topic, partition) pair carries an offset to commit.
func (d *Dispatcher) handleOffsetCommit(state *connState, hdr RequestHeader, body []byte) error {
	req := kmsg.NewPtrOffsetCommitRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return err
	}

	resp := kmsg.NewPtrOffsetCommitResponse()
	resp.SetVersion(hdr.APIVersion)

	acl := d.brk.ACL()
	if acl != nil && !acl.AuthorizeGroup(state.saslPrincipal, req.Group, "read") {
		// Build per-topic refusal so the client sees per-partition codes.
		for _, t := range req.Topics {
			rt := kmsg.OffsetCommitResponseTopic{Topic: t.Topic}
			for _, p := range t.Partitions {
				rt.Partitions = append(rt.Partitions, kmsg.OffsetCommitResponseTopicPartition{
					Partition: p.Partition,
					ErrorCode: errCodeGroupAuthorizationFailed,
				})
			}
			resp.Topics = append(resp.Topics, rt)
		}
		return d.writeKmsgResponse(state, hdr, resp, req.IsFlexible())
	}

	store := d.brk.Offsets()
	for _, t := range req.Topics {
		rt := kmsg.OffsetCommitResponseTopic{Topic: t.Topic}
		// Per-topic authorize too: a group might be allowed but the
		// topic blocked.
		if acl != nil && !acl.AuthorizeTopic(state.saslPrincipal, t.Topic, "read") {
			for _, p := range t.Partitions {
				rt.Partitions = append(rt.Partitions, kmsg.OffsetCommitResponseTopicPartition{
					Partition: p.Partition,
					ErrorCode: errCodeTopicAuthorizationFailed,
				})
			}
			resp.Topics = append(resp.Topics, rt)
			continue
		}
		for _, p := range t.Partitions {
			rp := kmsg.OffsetCommitResponseTopicPartition{Partition: p.Partition}
			meta := ""
			if p.Metadata != nil {
				meta = *p.Metadata
			}
			leaderEpoch := int32(0)
			// LeaderEpoch became required in v6; older versions omit
			// it. If kmsg reports 0 we just persist 0.
			if p.LeaderEpoch != 0 {
				leaderEpoch = p.LeaderEpoch
			}
			err := store.CommitOffset(req.Group, t.Topic, p.Partition, p.Offset, leaderEpoch, meta)
			if err != nil {
				rp.ErrorCode = errCodeCorruptMessage
			} else {
				rp.ErrorCode = errCodeNone
			}
			rt.Partitions = append(rt.Partitions, rp)
		}
		resp.Topics = append(resp.Topics, rt)
	}
	return d.writeKmsgResponse(state, hdr, resp, req.IsFlexible())
}

// offsetLeaderEpoch picks the leader epoch to report for a committed-offset
// lookup, following the protocol's "unknown" convention.
//
// OffsetFetch v5+ writes LeaderEpoch unconditionally, and kmsg's own Default()
// seeds it to -1 for exactly one reason: a partition with no commit recorded
// must not report a real-looking epoch. The handler builds its partitions with
// a bare struct literal and never calls Default(), so for a missing commit the
// store's zero value (0) would be written instead, and a consumer resuming
// from that tuple would start at the right offset carrying a leader epoch that
// was never committed.
//
// The store reports "no commit recorded" as offset -1 (see
// OffsetStore.FetchOffset); a real committed offset is never negative, so that
// is the test. Real commits keep whatever epoch the client committed, including
// 0, which is accurate on a single-node broker.
func offsetLeaderEpoch(co broker.CommittedOffset) int32 {
	if co.Offset < 0 {
		return -1
	}
	return co.LeaderEpoch
}

// OffsetFetch: return committed offsets for the requested
// (topic, partition) tuples. If the request omits Topics entirely,
// return ALL topics committed by the group.
func (d *Dispatcher) handleOffsetFetch(state *connState, hdr RequestHeader, body []byte) error {
	req := kmsg.NewPtrOffsetFetchRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return err
	}

	resp := kmsg.NewPtrOffsetFetchResponse()
	resp.SetVersion(hdr.APIVersion)

	store := d.brk.Offsets()
	groupID := req.Group

	// v8+ supports multi-group fetch via req.Groups; we still answer
	// the singleton case correctly. If req.Groups is non-empty, build
	// the v8 response shape.
	if len(req.Groups) > 0 {
		for _, g := range req.Groups {
			rg := kmsg.OffsetFetchResponseGroup{
				Group: g.Group,
			}
			for _, t := range g.Topics {
				rt := kmsg.OffsetFetchResponseGroupTopic{Topic: t.Topic}
				for _, partition := range t.Partitions {
					co := store.FetchOffset(g.Group, t.Topic, partition)
					rp := kmsg.OffsetFetchResponseGroupTopicPartition{
						Partition: partition,
						Offset:    co.Offset,
					}
					if co.Metadata != "" {
						rp.Metadata = stringPtr(co.Metadata)
					}
					rp.LeaderEpoch = offsetLeaderEpoch(co)
					rt.Partitions = append(rt.Partitions, rp)
				}
				rg.Topics = append(rg.Topics, rt)
			}
			resp.Groups = append(resp.Groups, rg)
		}
		return d.writeKmsgResponse(state, hdr, resp, req.IsFlexible())
	}

	// A null or absent topic list means "every topic this group has
	// committed". That is the form kafka-consumer-groups.sh --describe,
	// AdminClient.listConsumerGroupOffsets and consumer-lag dashboards send;
	// iterating an empty list would answer all of them with silence.
	if len(req.Topics) == 0 {
		for topic, parts := range store.AllOffsets(groupID) {
			rt := kmsg.OffsetFetchResponseTopic{Topic: topic}
			partitions := make([]int32, 0, len(parts))
			for p := range parts {
				partitions = append(partitions, p)
			}
			sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
			for _, p := range partitions {
				co := parts[p]
				rp := kmsg.OffsetFetchResponseTopicPartition{
					Partition:   p,
					Offset:      co.Offset,
					LeaderEpoch: offsetLeaderEpoch(co),
				}
				if co.Metadata != "" {
					rp.Metadata = stringPtr(co.Metadata)
				}
				rt.Partitions = append(rt.Partitions, rp)
			}
			resp.Topics = append(resp.Topics, rt)
		}
		sort.Slice(resp.Topics, func(i, j int) bool { return resp.Topics[i].Topic < resp.Topics[j].Topic })
		return d.writeKmsgResponse(state, hdr, resp, req.IsFlexible())
	}

	// Explicit topic list.
	for _, t := range req.Topics {
		rt := kmsg.OffsetFetchResponseTopic{Topic: t.Topic}
		for _, partition := range t.Partitions {
			co := store.FetchOffset(groupID, t.Topic, partition)
			rp := kmsg.OffsetFetchResponseTopicPartition{
				Partition: partition,
				Offset:    co.Offset,
			}
			if co.Metadata != "" {
				rp.Metadata = stringPtr(co.Metadata)
			}
			rp.LeaderEpoch = offsetLeaderEpoch(co)
			rt.Partitions = append(rt.Partitions, rp)
		}
		resp.Topics = append(resp.Topics, rt)
	}
	return d.writeKmsgResponse(state, hdr, resp, req.IsFlexible())
}
