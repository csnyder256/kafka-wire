package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// ACL store: per-principal access control to topics + groups.
//
// Format on disk at /data/metadata/acls.json:
//
//   {
//     "format_version": 1,
//     "principals": {
//       "tenant-7e3f2a": {
//         "tenant_id": "7e3f2a",
//         "topic_prefixes": [
//           {"prefix": "tenant.7e3f2a.", "ops": ["read", "write"]}
//         ],
//         "groups": [
//           {"id_prefix": "tenant.7e3f2a.", "ops": ["read"]}
//         ]
//       },
//       "platform-internal": {
//         "tenant_id": null,
//         "topic_prefixes": [{"prefix": "", "ops": ["read", "write"]}],
//         "groups":         [{"id_prefix": "",  "ops": ["read"]}]
//       }
//     }
//   }
//
// Two principal flavors:
//
// - **Tenant principals** (`tenant_id` set): the broker tags every
//   produced batch with `principal.tenant_id`. Fetches verify the
//   resolved segment's tenant matches the requesting principal's
//   tenant. Cross-tenant access fails closed at multiple layers.
//
// - **Platform principals** (`tenant_id` null): allowed to access
//   any topic / group matching their prefix list. The four existing
//   services (`ingestion`, `contract-normalization`, `clause-extraction`,
//   `relay`) all run as ONE platform principal because they share
//   the admin token. Tenant separation, where it exists, lives in payloads,
//   not at the wire layer.
//
// `topic_prefixes` and `groups` are matched as STRING PREFIXES, never
// as substring or regex. Empty prefix `""` = match anything. This
// keeps the matcher simple enough to reason about and impossible to
// trick with traversal characters.
//
// SASL is required when ACLs are configured. A connection without an
// authenticated principal can only do ApiVersions / SaslHandshake /
// SaslAuthenticate. After SASL completes, the principal name pins
// down which ACL entry applies.

const (
	OpRead  = "read"
	OpWrite = "write"
)

// ACLStore is the singleton persisted ACL registry.
type ACLStore struct {
	dir   string
	mu    sync.RWMutex
	state map[string]*Principal
}

// Principal is one identity allowed to connect.
type Principal struct {
	Name          string         `json:"-"` // map key; not duplicated in JSON
	TenantID      *string        `json:"tenant_id,omitempty"`
	TopicPrefixes []TopicPrefixACL `json:"topic_prefixes"`
	Groups        []GroupPrefixACL `json:"groups"`
}

// TopicPrefixACL grants ops on every topic whose name starts with Prefix.
type TopicPrefixACL struct {
	Prefix string   `json:"prefix"`
	Ops    []string `json:"ops"` // subset of {"read", "write"}
}

// GroupPrefixACL grants ops on every consumer group whose id starts with IDPrefix.
type GroupPrefixACL struct {
	IDPrefix string   `json:"id_prefix"`
	Ops      []string `json:"ops"`
}

// NewACLStore opens (creating if absent) the ACL JSON file. Empty
// state = "ACLs not configured" (broker accepts any authenticated
// principal as platform-level if SASL is on, or any anonymous
// connection if SASL is off: the behavior when ACLs are off).
func NewACLStore(dataDir string) (*ACLStore, error) {
	dir := filepath.Join(dataDir, "metadata")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("acl: mkdir: %w", err)
	}
	s := &ACLStore{dir: dir, state: make(map[string]*Principal)}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

// Path is the on-disk location for acls.json.
func (s *ACLStore) Path() string { return filepath.Join(s.dir, "acls.json") }

func (s *ACLStore) load() error {
	raw, err := os.ReadFile(s.Path())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("acl: read: %w", err)
	}
	var wrapper struct {
		Format     int                   `json:"format_version"`
		Principals map[string]*Principal `json:"principals"`
	}
	if err := json.Unmarshal(raw, &wrapper); err != nil {
		return fmt.Errorf("acl: parse: %w", err)
	}
	for name, p := range wrapper.Principals {
		if p == nil {
			continue
		}
		p.Name = name
		s.state[name] = p
	}
	return nil
}

// Save atomically writes acls.json. Returns immediately for an
// in-memory mutation followed by a flush.
func (s *ACLStore) Save() error {
	s.mu.RLock()
	wrapper := struct {
		Format     int                   `json:"format_version"`
		Principals map[string]*Principal `json:"principals"`
	}{Format: 1, Principals: s.state}
	s.mu.RUnlock()
	raw, err := json.MarshalIndent(wrapper, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.Path() + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.Path())
}

// Configured returns true if any ACL entries exist. Used by the
// authorizer to decide whether to enforce or fall through to
// "anything goes" behavior used when ACLs are off.
func (s *ACLStore) Configured() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.state) > 0
}

// Get returns the principal record, or nil if no entry exists.
func (s *ACLStore) Get(name string) *Principal {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state[name]
}

// All returns a snapshot of all principals (for admin endpoint).
func (s *ACLStore) All() []*Principal {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Principal, 0, len(s.state))
	for _, p := range s.state {
		out = append(out, p)
	}
	return out
}

// Upsert adds or replaces a principal definition.
func (s *ACLStore) Upsert(p Principal) error {
	if p.Name == "" {
		return errors.New("acl: principal name required")
	}
	for _, tp := range p.TopicPrefixes {
		for _, op := range tp.Ops {
			if op != OpRead && op != OpWrite {
				return fmt.Errorf("acl: invalid topic op %q", op)
			}
		}
	}
	for _, gp := range p.Groups {
		for _, op := range gp.Ops {
			if op != OpRead {
				return fmt.Errorf("acl: invalid group op %q (only 'read' supported)", op)
			}
		}
	}
	s.mu.Lock()
	pCopy := p
	s.state[p.Name] = &pCopy
	s.mu.Unlock()
	return s.Save()
}

// Delete removes a principal.
func (s *ACLStore) Delete(name string) error {
	s.mu.Lock()
	delete(s.state, name)
	s.mu.Unlock()
	return s.Save()
}

// AuthorizeTopic returns true if `principal` may perform `op` on
// `topic`. The empty principal name means "no SASL principal", used
// when SASL is disabled. In that case ACLs are also disabled, so we
// allow.
func (s *ACLStore) AuthorizeTopic(principal, topic, op string) bool {
	if !s.Configured() {
		return true
	}
	if principal == "" {
		return false
	}
	p := s.Get(principal)
	if p == nil {
		return false
	}
	for _, tp := range p.TopicPrefixes {
		if !strings.HasPrefix(topic, tp.Prefix) {
			continue
		}
		for _, allowed := range tp.Ops {
			if allowed == op {
				return true
			}
		}
	}
	return false
}

// AuthorizeGroup returns true if `principal` may join/operate on
// the consumer group `groupID`.
func (s *ACLStore) AuthorizeGroup(principal, groupID, op string) bool {
	if !s.Configured() {
		return true
	}
	if principal == "" {
		return false
	}
	p := s.Get(principal)
	if p == nil {
		return false
	}
	for _, gp := range p.Groups {
		if !strings.HasPrefix(groupID, gp.IDPrefix) {
			continue
		}
		for _, allowed := range gp.Ops {
			if allowed == op {
				return true
			}
		}
	}
	return false
}

// TenantOf returns the tenant_id bound to a principal, or "" if the
// principal is platform-level (no tenant) or unknown. Used by the
// archive uploader to tag segments with their tenant on write, and
// by the Fetch path to verify the requesting principal owns the
// resolved segment.
func (s *ACLStore) TenantOf(principal string) string {
	if principal == "" {
		return ""
	}
	p := s.Get(principal)
	if p == nil || p.TenantID == nil {
		return ""
	}
	return *p.TenantID
}

// ErrUnauthorized is returned by handlers when the authorizer denies
// access. Maps to the Kafka error TOPIC_AUTHORIZATION_FAILED (29) or
// GROUP_AUTHORIZATION_FAILED (30) on the wire.
var (
	ErrUnauthorizedTopic = errors.New("topic authorization failed")
	ErrUnauthorizedGroup = errors.New("group authorization failed")
)
