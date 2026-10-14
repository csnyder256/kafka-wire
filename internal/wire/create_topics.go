package wire

import (
	"errors"
	"fmt"

	"github.com/csnyder256/kafka-wire/internal/broker"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// CreateTopics carries two values that look alike and mean different things:
// -1 is KIP-464's "use the broker default" (v4+), and any OTHER non-positive
// value is a count the broker cannot honor. Kafka answers INVALID_PARTITIONS /
// INVALID_REPLICATION_FACTOR for the latter. Reading the two as one value is
// what let this handler turn a request for 0 partitions into a one-partition
// topic and answer success, so the client was told it had created a topic that
// does not exist.
const (
	brokerDefaultPartitions  int32 = -1
	brokerDefaultReplication int16 = -1
)

// preflightCreateTopic resolves what a create of `t` would do, without doing
// it: the partition count and replication factor it would use, or the error
// code and message it would return.
//
// It is shared by the real create and by a validate-only request on purpose.
// A dry run exists to answer "would this work", so if it does not run the same
// checks the create runs it is worse than no dry run at all: the handler used
// to answer success for any request, including a name the broker refuses, and
// reported 0 partitions for the shape it would have created.
//
// The rules are Kafka's:
//
//   - -1 means the broker default (KIP-464).
//   - any other non-positive count is invalid, never a default.
//   - the name must be usable as a directory and an object key.
//   - a partition count over the broker's cap is refused.
//   - an existing topic cannot be created again, which a dry run must also say.
func (d *Dispatcher) preflightCreateTopic(t kmsg.CreateTopicsRequestTopic) (partitions int32, repFactor int16, code int16, msg string) {
	partitions, repFactor = t.NumPartitions, t.ReplicationFactor
	switch {
	case partitions == brokerDefaultPartitions:
		partitions = 1
	case partitions <= 0:
		return 0, 0, errCodeInvalidPartitions,
			fmt.Sprintf("numPartitions %d: a partition count must be positive, or -1 for the broker default", t.NumPartitions)
	}
	switch {
	case repFactor == brokerDefaultReplication:
		repFactor = 1
	case repFactor <= 0:
		return 0, 0, errCodeInvalidReplicationFactor,
			fmt.Sprintf("replicationFactor %d: it must be positive, or -1 for the broker default", t.ReplicationFactor)
	}
	// The name becomes a directory name and an object key, so it is validated
	// for the same reason the create path validates it. Both map a rejected
	// name to INVALID_TOPIC, which is what keeps the dry run predictive.
	if err := broker.ValidateName("topic", t.Topic); err != nil {
		return 0, 0, errCodeInvalidTopic, err.Error()
	}
	if partitions > broker.MaxPartitionsPerTopic {
		return 0, 0, errCodeInvalidTopic,
			fmt.Sprintf("numPartitions %d: the limit is %d", partitions, broker.MaxPartitionsPerTopic)
	}
	if d.brk.Topics().Get(t.Topic) != nil {
		return 0, 0, errCodeTopicAlreadyExists, broker.ErrTopicExists.Error()
	}
	return partitions, repFactor, errCodeNone, ""
}

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

	acl := d.brk.ACL()
	for _, t := range req.Topics {
		// Built through kmsg's constructor rather than a bare struct literal:
		// CreateTopicsResponseTopic.Default() seeds NumPartitions and
		// ReplicationFactor to -1, the protocol's "not reported" marker. The
		// Go zero value of each is 0, which a client cannot tell from a real
		// count of zero, so an error entry used to report a topic with no
		// partitions and no replicas.
		rt := kmsg.NewCreateTopicsResponseTopic()
		rt.Topic = t.Topic
		// Kafka admin clients consume this successful response as a list.
		// An empty configuration set is valid; a null list breaks Java Admin.
		rt.Configs = []kmsg.CreateTopicsResponseTopicConfig{}

		// Creating a topic is a write to the cluster's namespace. Without
		// this check a least-privileged principal could create topics it has
		// no grant for, which makes every other topic ACL advisory.
		if acl != nil && !acl.AuthorizeTopic(state.saslPrincipal, t.Topic, "write") {
			rt.ErrorCode = errCodeTopicAuthorizationFailed
			resp.Topics = append(resp.Topics, rt)
			continue
		}

		partitions, repFactor, code, msg := d.preflightCreateTopic(t)
		if code != errCodeNone {
			rt.ErrorCode = code
			rt.ErrorMessage = stringPtr(msg)
			resp.Topics = append(resp.Topics, rt)
			continue
		}

		if req.ValidateOnly {
			// A dry run stops here and answers with the counts a real create
			// would use. It changes nothing.
			rt.ErrorCode = errCodeNone
			rt.NumPartitions = partitions
			rt.ReplicationFactor = repFactor
			resp.Topics = append(resp.Topics, rt)
			continue
		}

		if _, err := d.brk.CreateTopic(t.Topic, partitions, repFactor); err != nil {
			rt.ErrorCode = createTopicsErrorCode(err)
			rt.ErrorMessage = stringPtr(err.Error())
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

// createTopicsErrorCode maps a broker create error to the code the client
// expects for it.
func createTopicsErrorCode(err error) int16 {
	switch {
	case errors.Is(err, broker.ErrTopicExists):
		return errCodeTopicAlreadyExists
	case errors.Is(err, broker.ErrInvalidName):
		// A rejected name or partition count is not a partition-count
		// problem specifically, and clients surface the code.
		return errCodeInvalidTopic
	default:
		return errCodeInvalidPartitions
	}
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
	names := append([]string(nil), req.TopicNames...)
	for _, t := range req.Topics {
		if t.Topic != nil {
			names = append(names, *t.Topic)
		}
	}

	acl := d.brk.ACL()
	for _, name := range names {
		rt := kmsg.DeleteTopicsResponseTopic{
			Topic: stringPtr(name),
		}
		// Deleting is the most destructive operation on the wire, so it is
		// the last place an unauthorized principal should reach.
		if acl != nil && !acl.AuthorizeTopic(state.saslPrincipal, name, "write") {
			rt.ErrorCode = errCodeTopicAuthorizationFailed
			resp.Topics = append(resp.Topics, rt)
			continue
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
