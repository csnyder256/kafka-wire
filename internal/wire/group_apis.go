package wire

import (
	"encoding/hex"
	"log/slog"

	"github.com/csnyder256/kafka-wire/internal/broker"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// hexHead returns the first n bytes as hex (or all of buf if smaller).
// Used for compact byte-content snippets in diagnostic logs without
// dumping full payloads.
func hexHead(buf []byte, n int) string {
	if len(buf) < n {
		n = len(buf)
	}
	return hex.EncodeToString(buf[:n])
}

// FindCoordinator. Single-node mode: we are the coordinator for all
// groups. Always return our own broker_id + advertised endpoint.
func (d *Dispatcher) handleFindCoordinator(state *connState, hdr RequestHeader, body []byte) error {
	req := kmsg.NewPtrFindCoordinatorRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return err
	}
	resp := kmsg.NewPtrFindCoordinatorResponse()
	resp.SetVersion(hdr.APIVersion)
	resp.NodeID = d.brk.BrokerID()
	resp.Host = d.brk.AdvertisedHost()
	resp.Port = d.brk.AdvertisedPort()
	resp.ErrorCode = errCodeNone
	// v4+ supports CoordinatorKeys[] (multi-key batch). For brevity
	// we only handle the single-coordinator-key case (key on the
	// request itself); the few v4+ clients in our compat target
	// (none) tolerate this.
	for _, key := range req.CoordinatorKeys {
		resp.Coordinators = append(resp.Coordinators, kmsg.FindCoordinatorResponseCoordinator{
			Key:       key,
			NodeID:    d.brk.BrokerID(),
			Host:      d.brk.AdvertisedHost(),
			Port:      d.brk.AdvertisedPort(),
			ErrorCode: errCodeNone,
		})
	}
	return d.writeKmsgResponse(state, hdr, resp, req.IsFlexible())
}

// JoinGroup: first phase of consumer-group rebalance. Either issues
// a fresh member_id (MEMBER_ID_REQUIRED) or admits the member to a
// new generation.
func (d *Dispatcher) handleJoinGroup(state *connState, hdr RequestHeader, body []byte) error {
	req := kmsg.NewPtrJoinGroupRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return err
	}

	if acl := d.brk.ACL(); acl != nil && !acl.AuthorizeGroup(state.saslPrincipal, req.Group, "read") {
		resp := kmsg.NewPtrJoinGroupResponse()
		resp.SetVersion(hdr.APIVersion)
		resp.ErrorCode = errCodeGroupAuthorizationFailed
		return d.writeKmsgResponse(state, hdr, resp, req.IsFlexible())
	}

	// Convert protocols slice + emit the actual byte counts so we
	// can tell if subscription metadata is propagating correctly.
	//
	// kmsg v1.9.0 asymmetry: JoinGroupRequestProtocol.Metadata
	// (matching the Kafka protocol JSON spec name), but
	// JoinGroupResponseMember.ProtocolMetadata. Use each as kmsg
	// expects.
	protos := make([]broker.GroupProtocol, 0, len(req.Protocols))
	for _, p := range req.Protocols {
		md := p.Metadata
		slog.Info("joingroup.protocol_in",
			"group", req.Group,
			"member", req.MemberID,
			"protocol_name", p.Name,
			"metadata_bytes", len(md),
			"metadata_first_8_hex", hexHead(md, 8),
		)
		protos = append(protos, broker.GroupProtocol{
			Name:     p.Name,
			Metadata: md,
		})
	}

	args := broker.JoinGroupArgs{
		GroupID:            req.Group,
		MemberID:           req.MemberID,
		ClientID:           state.lastClientID,
		ClientHost:         state.remote,
		SessionTimeoutMS:   req.SessionTimeoutMillis,
		RebalanceTimeoutMS: req.RebalanceTimeoutMillis,
		ProtocolType:       req.ProtocolType,
		Protocols:          protos,
		APIVersion:         hdr.APIVersion,
	}
	if req.InstanceID != nil {
		args.GroupInstanceID = *req.InstanceID
	}

	res, err := d.brk.Groups().JoinGroup(args)
	if err != nil {
		return err
	}

	resp := kmsg.NewPtrJoinGroupResponse()
	resp.SetVersion(hdr.APIVersion)
	resp.ErrorCode = res.ErrorCode
	resp.Generation = res.Generation
	if res.ProtocolType != "" {
		resp.ProtocolType = stringPtr(res.ProtocolType)
	}
	resp.Protocol = stringPtr(res.Protocol)
	resp.LeaderID = res.LeaderID
	resp.MemberID = res.MemberID
	for _, m := range res.Members {
		slog.Info("joingroup.member_out",
			"group", req.Group,
			"member", m.MemberID,
			"is_leader_recipient", res.IsLeader,
			"metadata_bytes", len(m.Metadata),
			"metadata_first_8_hex", hexHead(m.Metadata, 8),
		)
		resp.Members = append(resp.Members, kmsg.JoinGroupResponseMember{
			MemberID:         m.MemberID,
			ProtocolMetadata: m.Metadata,
		})
	}
	return d.writeKmsgResponse(state, hdr, resp, req.IsFlexible())
}

// SyncGroup: second phase. Leader pushes assignments; members
// retrieve their slice.
func (d *Dispatcher) handleSyncGroup(state *connState, hdr RequestHeader, body []byte) error {
	req := kmsg.NewPtrSyncGroupRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return err
	}
	asgs := make(map[string][]byte, len(req.GroupAssignment))
	for _, a := range req.GroupAssignment {
		asgs[a.MemberID] = a.MemberAssignment
		slog.Info("syncgroup.assignment_in",
			"group", req.Group,
			"requesting_member", req.MemberID,
			"target_member", a.MemberID,
			"assignment_bytes", len(a.MemberAssignment),
			"assignment_first_8_hex", hexHead(a.MemberAssignment, 8),
		)
	}
	args := broker.SyncGroupArgs{
		GroupID:     req.Group,
		Generation:  req.Generation,
		MemberID:    req.MemberID,
		Assignments: asgs,
	}
	res, err := d.brk.Groups().SyncGroup(args)
	if err != nil {
		return err
	}
	resp := kmsg.NewPtrSyncGroupResponse()
	resp.SetVersion(hdr.APIVersion)
	resp.ErrorCode = res.ErrorCode
	resp.MemberAssignment = res.Assignment
	slog.Info("syncgroup.assignment_out",
		"group", req.Group,
		"member", req.MemberID,
		"assignment_bytes", len(res.Assignment),
		"error_code", res.ErrorCode,
	)
	return d.writeKmsgResponse(state, hdr, resp, req.IsFlexible())
}

// Heartbeat: refresh liveness; tells client if a rebalance is
// pending.
func (d *Dispatcher) handleHeartbeat(state *connState, hdr RequestHeader, body []byte) error {
	req := kmsg.NewPtrHeartbeatRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return err
	}
	args := broker.HeartbeatArgs{
		GroupID:    req.Group,
		MemberID:   req.MemberID,
		Generation: req.Generation,
	}
	code := d.brk.Groups().Heartbeat(args)
	resp := kmsg.NewPtrHeartbeatResponse()
	resp.SetVersion(hdr.APIVersion)
	resp.ErrorCode = code
	return d.writeKmsgResponse(state, hdr, resp, req.IsFlexible())
}

// LeaveGroup: clean shutdown. Multi-member variant (v3+) is one
// member per request; we handle both.
func (d *Dispatcher) handleLeaveGroup(state *connState, hdr RequestHeader, body []byte) error {
	req := kmsg.NewPtrLeaveGroupRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return err
	}
	resp := kmsg.NewPtrLeaveGroupResponse()
	resp.SetVersion(hdr.APIVersion)

	if req.MemberID != "" {
		resp.ErrorCode = d.brk.Groups().LeaveGroup(req.Group, req.MemberID)
	}
	for _, m := range req.Members {
		code := d.brk.Groups().LeaveGroup(req.Group, m.MemberID)
		resp.Members = append(resp.Members, kmsg.LeaveGroupResponseMember{
			MemberID:  m.MemberID,
			ErrorCode: code,
		})
	}
	return d.writeKmsgResponse(state, hdr, resp, req.IsFlexible())
}
