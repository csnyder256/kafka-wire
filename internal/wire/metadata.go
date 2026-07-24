package wire

import (
	"github.com/twmb/franz-go/pkg/kmsg"
)

// handleMetadata answers the Metadata API. Returns:
//   - the broker list (just us, in single-node mode)
//   - the topic list and per-partition leader/replica info
//
// Clients use Metadata to discover broker endpoints on bootstrap
// and to refresh after any topic-related error. The response shape
// is large but each field is mechanical.
func (d *Dispatcher) handleMetadata(state *connState, hdr RequestHeader, body []byte) error {
	req := kmsg.NewPtrMetadataRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return err
	}

	resp := kmsg.NewPtrMetadataResponse()
	resp.SetVersion(hdr.APIVersion)
	resp.ClusterID = stringPtr(d.brk.ClusterID())
	resp.ControllerID = d.brk.BrokerID()

	// Broker list: single-node, so just us.
	resp.Brokers = []kmsg.MetadataResponseBroker{{
		NodeID: d.brk.BrokerID(),
		Host:   d.brk.AdvertisedHost(),
		Port:   d.brk.AdvertisedPort(),
	}}

	// Topic list. Per Kafka spec:
	//   - req.Topics nil OR empty: return ALL topics
	//   - req.Topics has explicit names: return only those
	//   - if AllowAutoTopicCreation=true and topic missing, create it
	var requested []string
	includeAll := req.Topics == nil || len(req.Topics) == 0
	if !includeAll {
		for _, t := range req.Topics {
			if t.Topic != nil {
				requested = append(requested, *t.Topic)
			}
		}
	}

	// Build the response topic list.
	registry := d.brk.Topics()
	var names []string
	if includeAll {
		for _, t := range registry.All() {
			names = append(names, t.Name())
		}
	} else {
		names = requested
	}

	autoCreate := req.AllowAutoTopicCreation && d.brk.AutoCreateTopics()

	for _, name := range names {
		t := registry.Get(name)
		if t == nil && autoCreate {
			created, err := d.brk.EnsureTopic(name, 1)
			if err == nil {
				t = created
			}
		}
		if t == nil {
			resp.Topics = append(resp.Topics, kmsg.MetadataResponseTopic{
				Topic:     stringPtr(name),
				ErrorCode: errCodeUnknownTopicOrPart,
			})
			continue
		}

		mt := kmsg.MetadataResponseTopic{
			Topic:     stringPtr(name),
			ErrorCode: errCodeNone,
			// math.MinInt32 is the protocol's "not included" marker for
			// authorized operations. Zero would mean "authorized for
			// nothing", which makes admin UIs grey out every action.
			AuthorizedOperations: -2147483648,
		}
		for _, l := range t.Logs() {
			p := kmsg.MetadataResponseTopicPartition{
				Partition:       l.Partition(),
				Leader:          d.brk.BrokerID(),
				LeaderEpoch:     0,
				Replicas:        []int32{d.brk.BrokerID()},
				ISR:             []int32{d.brk.BrokerID()},
				OfflineReplicas: nil,
				ErrorCode:       errCodeNone,
			}
			mt.Partitions = append(mt.Partitions, p)
		}
		resp.Topics = append(resp.Topics, mt)
	}

	return d.writeKmsgResponse(state, hdr, resp, req.IsFlexible())
}

// stringPtr is a helper since kmsg uses *string for nullable strings.
func stringPtr(s string) *string { return &s }
