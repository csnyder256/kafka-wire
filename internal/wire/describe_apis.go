package wire

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// DescribeConfigs surfaces topic + broker configs for the dashboard.
// this build returns a minimal subset (retention.ms, segment.bytes,
// num.partitions, replication.factor); a future version wires the full set.
func (d *Dispatcher) handleDescribeConfigs(state *connState, hdr RequestHeader, body []byte) error {
	req := kmsg.NewPtrDescribeConfigsRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return err
	}
	resp := kmsg.NewPtrDescribeConfigsResponse()
	resp.SetVersion(hdr.APIVersion)

	for _, r := range req.Resources {
		rr := kmsg.DescribeConfigsResponseResource{
			ResourceType: r.ResourceType,
			ResourceName: r.ResourceName,
		}
		switch r.ResourceType {
		case kmsg.ConfigResourceTypeTopic:
			t := d.brk.Topics().Get(r.ResourceName)
			if t == nil {
				rr.ErrorCode = errCodeUnknownTopicOrPart
				resp.Resources = append(resp.Resources, rr)
				continue
			}
			cfg := t.Config()
			rr.Configs = append(rr.Configs,
				configEntry("retention.ms", fmt.Sprintf("%d", cfg.RetentionMS)),
				configEntry("retention.bytes", fmt.Sprintf("%d", cfg.RetentionBytes)),
				configEntry("segment.bytes", fmt.Sprintf("%d", cfg.SegmentBytes)),
				configEntry("num.partitions", fmt.Sprintf("%d", cfg.NumPartitions)),
				configEntry("replication.factor", fmt.Sprintf("%d", cfg.ReplicationFactor)),
			)
		case kmsg.ConfigResourceTypeBroker:
			rr.Configs = append(rr.Configs,
				configEntry("broker.id", fmt.Sprintf("%d", d.brk.BrokerID())),
				configEntry("cluster.id", d.brk.ClusterID()),
				configEntry("auto.create.topics.enable", fmt.Sprintf("%v", d.brk.AutoCreateTopics())),
			)
		default:
			rr.ErrorCode = errCodeUnsupportedVersion
		}
		resp.Resources = append(resp.Resources, rr)
	}
	return d.writeKmsgResponse(state, hdr, resp, req.IsFlexible())
}

// DescribeGroups returns the full state of one or more consumer groups.
func (d *Dispatcher) handleDescribeGroups(state *connState, hdr RequestHeader, body []byte) error {
	req := kmsg.NewPtrDescribeGroupsRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return err
	}
	resp := kmsg.NewPtrDescribeGroupsResponse()
	resp.SetVersion(hdr.APIVersion)

	all := d.brk.Groups().AllGroups()
	byID := make(map[string]int, len(all))
	for i, g := range all {
		byID[g.GroupID] = i
	}

	for _, gid := range req.Groups {
		if acl := d.brk.ACL(); acl != nil && !acl.AuthorizeGroup(state.saslPrincipal, gid, "read") {
			resp.Groups = append(resp.Groups, kmsg.DescribeGroupsResponseGroup{
				Group:                gid,
				ErrorCode:            errCodeGroupAuthorizationFailed,
				AuthorizedOperations: -2147483648,
			})
			continue
		}
		rg := kmsg.DescribeGroupsResponseGroup{
			Group: gid,
			// See the note in metadata.go: zero here reads as "authorized
			// for nothing" rather than "not reported".
			AuthorizedOperations: -2147483648,
		}
		idx, ok := byID[gid]
		if !ok {
			rg.State = "Empty"
			resp.Groups = append(resp.Groups, rg)
			continue
		}
		snap := all[idx]
		rg.State = snap.State
		rg.ProtocolType = "consumer"
		rg.Protocol = snap.Protocol
		for _, m := range snap.Members {
			rg.Members = append(rg.Members, kmsg.DescribeGroupsResponseGroupMember{
				MemberID:   m.MemberID,
				ClientID:   m.ClientID,
				ClientHost: m.ClientHost,
			})
		}
		resp.Groups = append(resp.Groups, rg)
	}
	return d.writeKmsgResponse(state, hdr, resp, req.IsFlexible())
}

// ListGroups returns every known group's id + protocol type.
func (d *Dispatcher) handleListGroups(state *connState, hdr RequestHeader, body []byte) error {
	req := kmsg.NewPtrListGroupsRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return err
	}
	resp := kmsg.NewPtrListGroupsResponse()
	resp.SetVersion(hdr.APIVersion)
	for _, g := range d.brk.Groups().AllGroups() {
		resp.Groups = append(resp.Groups, kmsg.ListGroupsResponseGroup{
			Group:        g.GroupID,
			ProtocolType: "consumer",
			GroupState:   g.State,
		})
	}
	return d.writeKmsgResponse(state, hdr, resp, req.IsFlexible())
}

func configEntry(name, value string) kmsg.DescribeConfigsResponseResourceConfig {
	return kmsg.DescribeConfigsResponseResourceConfig{
		Name:  name,
		Value: stringPtr(value),
	}
}
