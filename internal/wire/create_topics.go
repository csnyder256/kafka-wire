package wire

import (
	"errors"

	"github.com/csnyder256/kafka-wire/internal/broker"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// handleCreateTopics handles explicit topic creation (admin API +
// auto-create-on-Produce path when the producer pre-creates).
//
// this build ignores per-topic configs in the request, a future version wires
// retention.ms/bytes overrides via DescribeConfigs/AlterConfigs.
func (d *Dispatcher) handleCreateTopics(state *connState, hdr RequestHeader, body []byte) error {
	req := kmsg.NewPtrCreateTopicsRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return err
	}

	resp := kmsg.NewPtrCreateTopicsResponse()
	resp.SetVersion(hdr.APIVersion)

	for _, t := range req.Topics {
		rt := kmsg.CreateTopicsResponseTopic{
			Topic: t.Topic,
		}
		partitions := t.NumPartitions
		if partitions <= 0 {
			partitions = 1
		}
		repFactor := t.ReplicationFactor
		if repFactor <= 0 {
			repFactor = 1
		}

		if req.ValidateOnly {
			// this build: no validation rules to apply yet (we accept
			// any name + partition count). Mirror Kafka's behavior
			// of returning success without actually creating.
			rt.ErrorCode = errCodeNone
			resp.Topics = append(resp.Topics, rt)
			continue
		}

		if _, err := d.brk.CreateTopic(t.Topic, partitions, repFactor); err != nil {
			if errors.Is(err, broker.ErrTopicExists) {
				rt.ErrorCode = errCodeTopicAlreadyExists
			} else {
				rt.ErrorCode = errCodeInvalidPartitions
				rt.ErrorMessage = stringPtr(err.Error())
			}
			resp.Topics = append(resp.Topics, rt)
			continue
		}

		rt.ErrorCode = errCodeNone
		rt.NumPartitions = partitions
		rt.ReplicationFactor = repFactor
		resp.Topics = append(resp.Topics, rt)
	}

	return d.writeKmsgResponse(state, hdr, resp, req.IsFlexible())
}

// handleDeleteTopics is exposed for admin use. this build does not use it
// from the existing services, but admin tooling may.
func (d *Dispatcher) handleDeleteTopics(state *connState, hdr RequestHeader, body []byte) error {
	req := kmsg.NewPtrDeleteTopicsRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return err
	}
	resp := kmsg.NewPtrDeleteTopicsResponse()
	resp.SetVersion(hdr.APIVersion)

	// v6+ uses TopicNames + Topics (with topic UUIDs); earlier versions
	// used Topics ([]string). Handle both: if Topics is populated,
	// dereference each name; if TopicNames is populated, use that.
	var names []string
	for _, n := range req.TopicNames {
		names = append(names, n)
	}
	for _, t := range req.Topics {
		if t.Topic != nil {
			names = append(names, *t.Topic)
		}
	}

	for _, name := range names {
		rt := kmsg.DeleteTopicsResponseTopic{
			Topic: stringPtr(name),
		}
		if err := d.brk.DeleteTopic(name); err != nil {
			if errors.Is(err, broker.ErrUnknownTopic) {
				rt.ErrorCode = errCodeUnknownTopicOrPart
			} else {
				rt.ErrorCode = errCodeCorruptMessage
				rt.ErrorMessage = stringPtr(err.Error())
			}
		} else {
			rt.ErrorCode = errCodeNone
		}
		resp.Topics = append(resp.Topics, rt)
	}

	return d.writeKmsgResponse(state, hdr, resp, req.IsFlexible())
}
