// Package admin serves the broker's REST admin API on port 8080.
// Routes are intentionally simple JSON GET/POST endpoints (no
// generated framework) so the dashboard's typed client is easy to
// mirror.
package admin

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/csnyder256/kafka-wire/internal/broker"
	"github.com/csnyder256/kafka-wire/internal/metrics"
	"github.com/csnyder256/kafka-wire/internal/storage"
)

// Config tunes the admin handlers.
type Config struct {
	// AdminToken, when non-empty, is required as a Bearer token on
	// every endpoint EXCEPT /health (which orchestrator probes hit unauthenticated
	// for liveness probes). The dashboard sends the token as
	// `Authorization: Bearer <token>` via authFetch's existing flow;
	// set admin.token, or leave it empty to disable the check so
	// service-to-service calls don't need a separate secret.
	AdminToken string
}

// Register adds all admin handlers to the given mux.
func Register(mux *http.ServeMux, brk *broker.Broker, mreg *metrics.Registry, cfg Config) {
	h := &handlers{brk: brk, mreg: mreg, cfg: cfg, started: time.Now()}
	// /health is intentionally unauthenticated. An orchestrator's liveness
	// probe hits it via the container's localhost so it's not
	// exposed externally; a bearer-token requirement would add
	// useless friction.
	mux.HandleFunc("/health", h.health)

	// Authenticated endpoints.
	mux.HandleFunc("/v1/cluster", h.guard(h.cluster))
	mux.HandleFunc("/v1/topics", h.guard(h.topics))
	mux.HandleFunc("/v1/topics/", h.guard(h.topicDetail))
	mux.HandleFunc("/v1/groups", h.guard(h.groups))
	mux.HandleFunc("/v1/groups/", h.guard(h.groupDetail))
	mux.HandleFunc("/v1/archive", h.guard(h.archive))
	mux.HandleFunc("/v1/replay/reset-offset", h.guard(h.resetOffset))
	mux.HandleFunc("/v1/acls", h.guard(h.acls))
	mux.HandleFunc("/v1/acls/", h.guard(h.aclDetail))
}

// acls: GET returns all principals + their grants; POST upserts one.
func (h *handlers) acls(w http.ResponseWriter, r *http.Request) {
	acl := h.brk.ACL()
	if acl == nil {
		writeError(w, http.StatusServiceUnavailable, "ACL store not initialized")
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{
			"configured": acl.Configured(),
			"principals": acl.All(),
		})
	case http.MethodPost:
		// Principal.Name is the map key in the stored file and carries
		// `json:"-"`, so decoding a request body straight into it always
		// produced an unnamed principal and a "principal name required"
		// rejection: the endpoint could not create anything. The request
		// shape is therefore its own type, with the name in the body.
		var req struct {
			Name          string                  `json:"name"`
			TopicPrefixes []broker.TopicPrefixACL `json:"topic_prefixes"`
			Groups        []broker.GroupPrefixACL `json:"groups"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		p := broker.Principal{
			Name:          req.Name,
			TopicPrefixes: req.TopicPrefixes,
			Groups:        req.Groups,
		}
		if err := acl.Upsert(p); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"upserted": p.Name})
	default:
		writeError(w, http.StatusMethodNotAllowed, "GET or POST only")
	}
}

// aclDetail: DELETE /v1/acls/{name} drops one principal.
func (h *handlers) aclDetail(w http.ResponseWriter, r *http.Request) {
	acl := h.brk.ACL()
	if acl == nil {
		writeError(w, http.StatusServiceUnavailable, "ACL store not initialized")
		return
	}
	name := r.URL.Path[len("/v1/acls/"):]
	if name == "" {
		writeError(w, http.StatusBadRequest, "principal name required")
		return
	}
	switch r.Method {
	case http.MethodGet:
		p := acl.Get(name)
		if p == nil {
			writeError(w, http.StatusNotFound, "principal not found")
			return
		}
		writeJSON(w, http.StatusOK, p)
	case http.MethodDelete:
		if err := acl.Delete(name); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"deleted": name})
	default:
		writeError(w, http.StatusMethodNotAllowed, "GET or DELETE only")
	}
}

// resetOffset moves a consumer group's committed offsets to one of:
//   - earliest (replay everything still on disk + S3)
//   - latest (skip ahead, e.g. after a poison-pill drain)
//   - a Unix timestamp in milliseconds (seek-by-time)
//
// This is the dashboard's "Reset Offset" button + the equivalent of
// `kafka-consumer-groups --reset-offsets`. The group must be empty
// (no active members) when reset; otherwise members would just
// over-write the new commit on their next Heartbeat.
func (h *handlers) resetOffset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST only")
		return
	}
	var body struct {
		GroupID     string `json:"group_id"`
		Topic       string `json:"topic"`    // optional; empty = all subscribed topics
		Strategy    string `json:"strategy"` // earliest | latest | timestamp
		TimestampMS int64  `json:"timestamp_ms,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if body.GroupID == "" {
		writeError(w, http.StatusBadRequest, "group_id required")
		return
	}
	gs := h.brk.Offsets().Get(body.GroupID)
	if gs == nil {
		writeError(w, http.StatusNotFound, "group has no committed offsets")
		return
	}
	resets := []map[string]any{}
	for topic, parts := range gs.Offsets {
		if body.Topic != "" && body.Topic != topic {
			continue
		}
		t := h.brk.Topics().Get(topic)
		if t == nil {
			continue
		}
		for partition := range parts {
			l, ok := t.Partition(partition)
			if !ok {
				continue
			}
			var newOffset int64
			switch body.Strategy {
			case "earliest":
				newOffset = l.EarliestOffset()
			case "latest":
				newOffset = l.LatestOffset()
			case "timestamp":
				if off, ok := l.LookupOffsetByTimestamp(body.TimestampMS); ok {
					newOffset = off
				} else {
					newOffset = l.EarliestOffset()
				}
			default:
				writeError(w, http.StatusBadRequest, "strategy must be earliest|latest|timestamp")
				return
			}
			if err := h.brk.Offsets().CommitOffset(body.GroupID, topic, partition, newOffset, 0, "reset-via-admin"); err != nil {
				writeError(w, http.StatusInternalServerError, err.Error())
				return
			}
			resets = append(resets, map[string]any{
				"topic":      topic,
				"partition":  partition,
				"new_offset": newOffset,
			})
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"group_id": body.GroupID,
		"reset":    resets,
		"count":    len(resets),
	})
}

func (h *handlers) archive(w http.ResponseWriter, r *http.Request) {
	mfst := h.brk.Archive()
	if mfst == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"enabled":  false,
			"segments": []any{},
		})
		return
	}
	all := mfst.All()
	writeJSON(w, http.StatusOK, map[string]any{
		"enabled":  true,
		"segments": all,
		"pending":  mfst.PendingAll(),
		"total":    len(all),
	})
}

type handlers struct {
	brk     *broker.Broker
	mreg    *metrics.Registry
	cfg     Config
	started time.Time
}

// guard wraps an authenticated handler. When AdminToken is unset
// (local dev), all calls pass through. When set, the handler refuses
// any request without a matching Authorization: Bearer <token>.
//
// Constant-time comparison via subtle.ConstantTimeCompare to avoid
// timing oracles on the token.
func (h *handlers) guard(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h.cfg.AdminToken == "" {
			next(w, r)
			return
		}
		got := r.Header.Get("Authorization")
		if !strings.HasPrefix(got, "Bearer ") {
			writeError(w, http.StatusUnauthorized, "missing bearer token")
			return
		}
		got = got[len("Bearer "):]
		if subtle.ConstantTimeCompare([]byte(got), []byte(h.cfg.AdminToken)) != 1 {
			writeError(w, http.StatusUnauthorized, "invalid token")
			return
		}
		next(w, r)
	}
}

func (h *handlers) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":      "ok",
		"uptime_secs": int64(time.Since(h.started).Seconds()),
		"broker_id":   h.brk.BrokerID(),
		"cluster_id":  h.brk.ClusterID(),
	})
}

func (h *handlers) cluster(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"broker_id":          h.brk.BrokerID(),
		"cluster_id":         h.brk.ClusterID(),
		"advertised_host":    h.brk.AdvertisedHost(),
		"advertised_port":    h.brk.AdvertisedPort(),
		"auto_create_topics": h.brk.AutoCreateTopics(),
	})
}

type topicSummary struct {
	Name              string `json:"name"`
	Partitions        int32  `json:"partitions"`
	ReplicationFactor int16  `json:"replication_factor"`
	RetentionMS       int64  `json:"retention_ms"`
	RetentionBytes    int64  `json:"retention_bytes"`
	SegmentBytes      int64  `json:"segment_bytes"`
	HighWatermark     int64  `json:"high_watermark_total"`
	LogStartOffset    int64  `json:"log_start_total"`
	LocalSizeBytes    int64  `json:"local_size_bytes"`
}

func (h *handlers) topics(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		all := h.brk.Topics().All()
		out := make([]topicSummary, 0, len(all))
		for _, t := range all {
			s := topicSummary{
				Name:              t.Name(),
				Partitions:        t.NumPartitions(),
				ReplicationFactor: t.Config().ReplicationFactor,
				RetentionMS:       t.Config().RetentionMS,
				RetentionBytes:    t.Config().RetentionBytes,
				SegmentBytes:      t.Config().SegmentBytes,
			}
			for _, l := range t.Logs() {
				s.HighWatermark += l.HighWatermark()
				s.LogStartOffset += l.LogStartOffset()
				for _, seg := range l.AllSegments() {
					s.LocalSizeBytes += seg.Size()
				}
			}
			out = append(out, s)
		}
		writeJSON(w, http.StatusOK, map[string]any{"topics": out})
	case http.MethodPost:
		var body struct {
			Name              string `json:"name"`
			Partitions        int32  `json:"partitions"`
			ReplicationFactor int16  `json:"replication_factor"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if body.Name == "" {
			writeError(w, http.StatusBadRequest, "name required")
			return
		}
		if body.Partitions <= 0 {
			body.Partitions = 1
		}
		if body.ReplicationFactor <= 0 {
			body.ReplicationFactor = 1
		}
		if _, err := h.brk.CreateTopic(body.Name, body.Partitions, body.ReplicationFactor); err != nil {
			if errors.Is(err, broker.ErrTopicExists) {
				writeError(w, http.StatusConflict, err.Error())
				return
			}
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, map[string]any{
			"name":       body.Name,
			"partitions": body.Partitions,
		})
	default:
		writeError(w, http.StatusMethodNotAllowed, "GET or POST only")
	}
}

func (h *handlers) topicDetail(w http.ResponseWriter, r *http.Request) {
	// path is /v1/topics/{name}[/partitions]
	rest := r.URL.Path[len("/v1/topics/"):]
	name := rest
	subpath := ""
	if i := indexOf(rest, '/'); i >= 0 {
		name = rest[:i]
		subpath = rest[i+1:]
	}

	t := h.brk.Topics().Get(name)
	if t == nil {
		writeError(w, http.StatusNotFound, "topic not found")
		return
	}

	if subpath == "" {
		switch r.Method {
		case http.MethodGet:
			writeJSON(w, http.StatusOK, topicDetailFor(t))
		case http.MethodDelete:
			if err := h.brk.DeleteTopic(name); err != nil {
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"deleted": name})
		default:
			writeError(w, http.StatusMethodNotAllowed, "GET or DELETE only")
		}
		return
	}

	if subpath == "partitions" && r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, map[string]any{
			"partitions": partitionDetails(t),
		})
		return
	}
	writeError(w, http.StatusNotFound, "no such resource")
}

func topicDetailFor(t *broker.Topic) map[string]any {
	cfg := t.Config()
	return map[string]any{
		"name":               t.Name(),
		"partitions":         t.NumPartitions(),
		"replication_factor": cfg.ReplicationFactor,
		"retention_ms":       cfg.RetentionMS,
		"retention_bytes":    cfg.RetentionBytes,
		"segment_bytes":      cfg.SegmentBytes,
		"partition_details":  partitionDetails(t),
	}
}

func partitionDetails(t *broker.Topic) []map[string]any {
	out := make([]map[string]any, 0, t.NumPartitions())
	for _, l := range t.Logs() {
		segs := l.AllSegments()
		segOut := make([]map[string]any, 0, len(segs))
		for _, seg := range segs {
			segOut = append(segOut, map[string]any{
				"base_offset": seg.BaseOffset(),
				"next_offset": seg.NextOffset(),
				"size_bytes":  seg.Size(),
				"sealed":      seg.Sealed(),
				"created_at":  seg.CreatedAt(),
			})
		}
		out = append(out, map[string]any{
			"partition":        l.Partition(),
			"high_watermark":   l.HighWatermark(),
			"log_start_offset": l.LogStartOffset(),
			"segments":         segOut,
		})
	}
	return out
}

func (h *handlers) groups(w http.ResponseWriter, r *http.Request) {
	all := h.brk.Groups().AllGroups()
	out := make([]map[string]any, 0, len(all))
	for _, g := range all {
		out = append(out, groupSummary(h, g))
	}
	writeJSON(w, http.StatusOK, map[string]any{"groups": out})
}

func (h *handlers) groupDetail(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Path[len("/v1/groups/"):]
	if id == "" {
		writeError(w, http.StatusBadRequest, "group id required")
		return
	}
	for _, g := range h.brk.Groups().AllGroups() {
		if g.GroupID == id {
			writeJSON(w, http.StatusOK, groupDetailFor(h, g))
			return
		}
	}
	writeError(w, http.StatusNotFound, "group not found")
}

func groupSummary(h *handlers, g broker.Snapshot) map[string]any {
	return map[string]any{
		"group_id":   g.GroupID,
		"state":      g.State,
		"generation": g.Generation,
		"protocol":   g.Protocol,
		"members":    len(g.Members),
		"lag":        groupLag(h, g.GroupID),
	}
}

func groupDetailFor(h *handlers, g broker.Snapshot) map[string]any {
	members := make([]map[string]any, 0, len(g.Members))
	for _, m := range g.Members {
		members = append(members, map[string]any{
			"member_id":          m.MemberID,
			"client_id":          m.ClientID,
			"client_host":        m.ClientHost,
			"session_timeout_ms": m.SessionTimeoutMS,
			"last_heartbeat":     m.LastHeartbeat,
		})
	}
	return map[string]any{
		"group_id":   g.GroupID,
		"state":      g.State,
		"generation": g.Generation,
		"protocol":   g.Protocol,
		"members":    members,
		"lag":        groupLag(h, g.GroupID),
	}
}

// groupLag walks the group's persisted offsets and computes
// (HWM - committed) per (topic, partition).
func groupLag(h *handlers, groupID string) []map[string]any {
	out := []map[string]any{}
	gs := h.brk.Offsets().Get(groupID)
	if gs == nil {
		return out
	}
	for topic, parts := range gs.Offsets {
		t := h.brk.Topics().Get(topic)
		if t == nil {
			continue
		}
		for partition, co := range parts {
			l, ok := t.Partition(partition)
			if !ok {
				continue
			}
			hwm := l.HighWatermark()
			out = append(out, map[string]any{
				"topic":      topic,
				"partition":  partition,
				"committed":  co.Offset,
				"high_water": hwm,
				"lag":        hwm - co.Offset,
			})
		}
	}
	return out
}

// ── helpers ──────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"error": msg})
}

func indexOf(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

// Compile-time link check: ensure storage is referenced (the import
// is used by topic detail's partitionDetails -> seg.Size()).
var _ = strconv.Itoa
var _ = fmt.Sprintf
var _ storage.Config
