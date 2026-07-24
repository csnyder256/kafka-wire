package wire

import (
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ApiVersions advertises the request types we support, with version
// ranges we serve. clients use this response to pick a
// negotiated version per API for the rest of the session.
//
// Versions chosen to match the plan's compatibility budget:
//   Produce         v3-v9     (v3+ uses v2 record batches)
//   Fetch           v4-v11    (v11 caps before topic-IDs in v12+)
//   ListOffsets     v0-v5
//   Metadata        v0-v9
//   OffsetCommit    v0-v8
//   OffsetFetch     v0-v6
//   FindCoordinator v0-v3
//   JoinGroup       v0-v7
//   Heartbeat       v0-v4
//   LeaveGroup      v0-v4
//   SyncGroup       v0-v5
//   SaslHandshake   v0-v1
//   SaslAuthenticate v0-v2
//   ApiVersions     v0-v3
//   CreateTopics    v0-v6
//   DeleteTopics    v0-v5
//   DescribeConfigs v0-v4
//   DescribeGroups  v0-v5
//   ListGroups      v0-v4

type apiVersionRange struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}

var advertisedAPIs = []apiVersionRange{
	{APIKey: int16(kmsg.Produce), MinVersion: 3, MaxVersion: 9},
	{APIKey: int16(kmsg.Fetch), MinVersion: 4, MaxVersion: 11},
	{APIKey: int16(kmsg.ListOffsets), MinVersion: 0, MaxVersion: 5},
	{APIKey: int16(kmsg.Metadata), MinVersion: 0, MaxVersion: 9},
	{APIKey: int16(kmsg.OffsetCommit), MinVersion: 0, MaxVersion: 8},
	{APIKey: int16(kmsg.OffsetFetch), MinVersion: 0, MaxVersion: 6},
	{APIKey: int16(kmsg.FindCoordinator), MinVersion: 0, MaxVersion: 3},
	{APIKey: int16(kmsg.JoinGroup), MinVersion: 0, MaxVersion: 7},
	{APIKey: int16(kmsg.Heartbeat), MinVersion: 0, MaxVersion: 4},
	{APIKey: int16(kmsg.LeaveGroup), MinVersion: 0, MaxVersion: 4},
	{APIKey: int16(kmsg.SyncGroup), MinVersion: 0, MaxVersion: 5},
	{APIKey: int16(kmsg.SASLHandshake), MinVersion: 0, MaxVersion: 1},
	{APIKey: int16(kmsg.SASLAuthenticate), MinVersion: 0, MaxVersion: 2},
	{APIKey: int16(kmsg.ApiVersions), MinVersion: 0, MaxVersion: 3},
	{APIKey: int16(kmsg.CreateTopics), MinVersion: 0, MaxVersion: 6},
	{APIKey: int16(kmsg.DeleteTopics), MinVersion: 0, MaxVersion: 5},
	{APIKey: int16(kmsg.DescribeConfigs), MinVersion: 0, MaxVersion: 4},
	{APIKey: int16(kmsg.DescribeGroups), MinVersion: 0, MaxVersion: 5},
	{APIKey: int16(kmsg.ListGroups), MinVersion: 0, MaxVersion: 4},
}

func (d *Dispatcher) handleAPIVersions(state *connState, hdr RequestHeader, body []byte) error {
	// Newer Kafka clients sometimes send a v0 ApiVersions probe even
	// against a v3-aware broker. We don't actually need to decode the
	// request body: the response is the same regardless.
	resp := kmsg.NewPtrApiVersionsResponse()
	resp.SetVersion(hdr.APIVersion)
	resp.ErrorCode = errCodeNone
	resp.ApiKeys = make([]kmsg.ApiVersionsResponseApiKey, 0, len(advertisedAPIs))
	for _, a := range advertisedAPIs {
		resp.ApiKeys = append(resp.ApiKeys, kmsg.ApiVersionsResponseApiKey{
			ApiKey:     a.APIKey,
			MinVersion: a.MinVersion,
			MaxVersion: a.MaxVersion,
		})
	}
	// ApiVersions response uses the v0 (non-flexible) header even on
	// flexible-supporting versions: Kafka special-cases this so old
	// brokers can still negotiate with new clients without parsing
	// the tagged-fields header byte. Hardcode false.
	return d.writeKmsgResponse(state, hdr, resp, false)
}
