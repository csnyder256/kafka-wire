package e2e

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestMetadataEmptyTopicListDoesNotLeakTheNamespace drives a Metadata request
// with an empty (non-null) topic array through the real listener and asserts,
// from the decoded response bytes, that the broker returned no topics.
//
// The topic array is nullable from v1: a null array means "all topics", and an
// empty array means "no topics at all". franz-go's broker-only metadata
// refresh (Client.fetchBrokerMetadata, behind Ping and the periodic broker
// reload) sends exactly the empty form, with the comment "brokers only, no
// opinion on topics", and its storeCachedMeta has a matching guard for
// `req.Topics != nil && len(req.Topics) == 0`. The broker used to treat
// `len(topics) == 0` as "all topics", so that refresh -- which explicitly asks
// for no topics -- came back carrying the whole topic namespace and every
// partition of it.
//
// The second half is the control: the null form still returns everything, so
// narrowing the empty case must not narrow "list all topics".
func TestMetadataEmptyTopicListDoesNotLeakTheNamespace(t *testing.T) {
	b := startBroker(t)
	cl := newClient(t, b.addr)
	createTopic(t, cl, "meta.empty.a", 1)
	createTopic(t, cl, "meta.empty.b", 2)

	req := kmsg.NewPtrMetadataRequest()
	req.SetVersion(9)
	req.Topics = []kmsg.MetadataRequestTopic{} // non-null empty: "no topics"

	body, err := rawRequest(b.addr, int16(kmsg.Metadata), 9, req.AppendTo(nil))
	if err != nil {
		t.Fatalf("metadata round trip: %v", err)
	}
	resp := kmsg.NewPtrMetadataResponse()
	resp.SetVersion(9)
	if err := resp.ReadFrom(body); err != nil {
		t.Fatalf("decoding the metadata response: %v", err)
	}
	if len(resp.Topics) != 0 {
		names := make([]string, 0, len(resp.Topics))
		for _, tp := range resp.Topics {
			if tp.Topic != nil {
				names = append(names, *tp.Topic)
			}
		}
		t.Fatalf("an empty (non-null) topic array returned %d topic(s) %v; the array is nullable from v1, so empty means no topics, not all of them",
			len(resp.Topics), names)
	}
	if len(resp.Brokers) == 0 {
		t.Fatalf("the response carried no brokers; a broker-only metadata request still needs them")
	}

	all := kmsg.NewPtrMetadataRequest()
	all.SetVersion(9)
	all.Topics = nil // null: "all topics"
	body, err = rawRequest(b.addr, int16(kmsg.Metadata), 9, all.AppendTo(nil))
	if err != nil {
		t.Fatalf("all-topics metadata round trip: %v", err)
	}
	respAll := kmsg.NewPtrMetadataResponse()
	respAll.SetVersion(9)
	if err := respAll.ReadFrom(body); err != nil {
		t.Fatalf("decoding the all-topics metadata response: %v", err)
	}
	if len(respAll.Topics) < 2 {
		t.Fatalf("a null topic array returned %d topic(s), want at least the 2 that were created", len(respAll.Topics))
	}
}
