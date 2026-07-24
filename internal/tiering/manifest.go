// Package s3 handles tiered storage: hot segments live on local disk
// (the data directory), cold segments are uploaded to S3-compatible
// object storage (AWS S3, Cloudflare R2, MinIO, Backblaze B2).
//
// How it fits together:
//
//   - Uploader: scans every topic's sealed segments, uploads to S3
//     via multipart, writes a manifest entry with sha256.
//   - Cache:    on-disk LRU under /data/cache; restored segments live
//     here briefly to satisfy fetches before being evicted.
//   - Restorer: when a Fetch lands on an offset whose segment is no
//     longer local, downloads the segment from S3 to the cache and
//     serves it. Transparent to the caller.
//
// Resumability: archive uploads checkpoint per-part. A restart
// mid-upload resumes from the last completed part on next boot.
package tiering

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// SegmentEntry is one archived segment's metadata.
//
// CRITICAL multi-tenant invariants:
//
//   - TenantID is captured at archive time from the OwnerTenantID
//     of the topic at the moment of upload. It NEVER changes after
//     the entry is written; tampering invalidates HMACSignature.
//
//   - HMACSignature is HMAC-SHA-256 over the canonical ownership
//     tuple (tenant_id || topic || partition || base_offset || sha256)
//     using the broker's archive HMAC key. Verified on restore. A
//     mismatch is a hard failure: chaos engine treats it as a
//     tenant-leak detection event.
type SegmentEntry struct {
	Topic         string    `json:"topic"`
	Partition     int32     `json:"partition"`
	BaseOffset    int64     `json:"base_offset"`
	NextOffset    int64     `json:"next_offset"`
	SizeBytes     int64     `json:"size_bytes"`
	SHA256        string    `json:"sha256"`
	S3Key         string    `json:"s3_key"`
	UploadedAt    time.Time `json:"uploaded_at"`
	TenantID      string    `json:"tenant_id,omitempty"`
	HMACSignature string    `json:"hmac_signature,omitempty"`
}

// PendingUpload tracks an in-flight multipart upload so we can resume
// after a restart.
//
// NextOffset / TenantID / SHA256 are carried so boot-time reconcile can
// rebuild a full SegmentEntry when the upload COMPLETED but the process
// died before the manifest write (fields are additive; zero values on
// pre-upgrade checkpoints).
type PendingUpload struct {
	Topic      string              `json:"topic"`
	Partition  int32               `json:"partition"`
	BaseOffset int64               `json:"base_offset"`
	NextOffset int64               `json:"next_offset,omitempty"`
	TenantID   string              `json:"tenant_id,omitempty"`
	SHA256     string              `json:"sha256,omitempty"`
	S3Key      string              `json:"s3_key"`
	UploadID   string              `json:"upload_id"`
	Parts      []PendingUploadPart `json:"parts"`
	StartedAt  time.Time           `json:"started_at"`
}

type PendingUploadPart struct {
	PartNumber int32  `json:"part_number"`
	ETag       string `json:"etag"`
	SizeBytes  int64  `json:"size_bytes"`
}

// Manifest persists the archive state to disk so a restart sees the
// same view of "what's in S3" without re-listing the bucket.
//
// Two files under /data/metadata/:
//
//	archive.json: completed segments (SegmentEntry list)
//	archive_pending.json: in-flight multipart uploads
//
// Both use atomic write-temp-then-rename. The Manifest's mu guards
// the in-memory slice + map; flushes happen synchronously after every
// mutation so a crash never loses an uploaded segment.
type Manifest struct {
	dir string
	mu  sync.Mutex

	completed []SegmentEntry
	pending   map[string]*PendingUpload // key = s3_key
}

// OpenManifest opens (creating if absent) the manifest files.
func OpenManifest(dataDir string) (*Manifest, error) {
	dir := filepath.Join(dataDir, "metadata")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("manifest mkdir: %w", err)
	}
	m := &Manifest{
		dir:     dir,
		pending: make(map[string]*PendingUpload),
	}
	if err := m.load(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Manifest) load() error {
	if raw, err := os.ReadFile(filepath.Join(m.dir, "archive.json")); err == nil {
		var wrapper struct {
			Format   int            `json:"format_version"`
			Segments []SegmentEntry `json:"segments"`
		}
		if err := json.Unmarshal(raw, &wrapper); err != nil {
			// A corrupt manifest must not brick the broker: this error
			// used to bubble to main.go's os.Exit(1), taking the whole
			// broker down until an operator hand-deleted the file.
			// Quarantine the corrupt bytes, salvage every complete
			// segment entry, rewrite a clean file, and keep booting.
			// Segments whose entries could not be salvaged remain in S3
			// and are re-listed by operators from the quarantine copy.
			salvaged := salvageSegments(raw)
			quarantine := filepath.Join(m.dir, "archive.json.corrupt")
			_ = os.WriteFile(quarantine, raw, 0o644)
			slog.Error("s3.manifest.archive_corrupt_salvaged",
				"err", err, "quarantine", quarantine,
				"salvaged_entries", len(salvaged))
			m.completed = salvaged
			if ferr := m.flushCompleted(); ferr != nil {
				return fmt.Errorf("rewrite salvaged archive.json: %w", ferr)
			}
		} else {
			m.completed = wrapper.Segments
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	if raw, err := os.ReadFile(filepath.Join(m.dir, "archive_pending.json")); err == nil {
		var wrapper struct {
			Format  int                       `json:"format_version"`
			Pending map[string]*PendingUpload `json:"pending"`
		}
		if err := json.Unmarshal(raw, &wrapper); err != nil {
			// Pending state is resumable bookkeeping, not the record of
			// what's archived. On corruption: quarantine + reset. The
			// uploader re-uploads any sealed segment still on disk, and
			// abortStale cleans up orphaned multipart uploads in S3.
			quarantine := filepath.Join(m.dir, "archive_pending.json.corrupt")
			_ = os.WriteFile(quarantine, raw, 0o644)
			slog.Error("s3.manifest.pending_corrupt_reset",
				"err", err, "quarantine", quarantine)
			m.pending = make(map[string]*PendingUpload)
			if ferr := m.flushPending(); ferr != nil {
				return fmt.Errorf("rewrite reset archive_pending.json: %w", ferr)
			}
		} else if wrapper.Pending != nil {
			m.pending = wrapper.Pending
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	return nil
}

// salvageSegments pulls every complete SegmentEntry out of a corrupt or
// truncated archive.json. Atomic write-temp-then-rename makes truncation
// unlikely, but a full disk or a bad volume can still hand us garbage,
// and every parseable entry we recover is an archived segment that stays
// reachable without a manual S3 re-list.
func salvageSegments(raw []byte) []SegmentEntry {
	dec := json.NewDecoder(bytes.NewReader(raw))
	var out []SegmentEntry
	for {
		tok, err := dec.Token()
		if err != nil {
			return out
		}
		if key, ok := tok.(string); ok && key == "segments" {
			tok2, err2 := dec.Token()
			if err2 != nil {
				return out
			}
			if d, ok := tok2.(json.Delim); !ok || d != '[' {
				return out
			}
			for dec.More() {
				var e SegmentEntry
				if err := dec.Decode(&e); err != nil {
					return out
				}
				out = append(out, e)
			}
			return out
		}
	}
}

// AddCompleted records a successful upload + flushes archive.json.
func (m *Manifest) AddCompleted(e SegmentEntry) error {
	m.mu.Lock()
	m.completed = append(m.completed, e)
	delete(m.pending, e.S3Key)
	m.mu.Unlock()
	return m.flushBoth()
}

// SetPending records a multipart upload's checkpoint state.
func (m *Manifest) SetPending(p *PendingUpload) error {
	m.mu.Lock()
	m.pending[p.S3Key] = p
	m.mu.Unlock()
	return m.flushPending()
}

// GetPending returns the checkpoint for an in-flight upload.
func (m *Manifest) GetPending(s3Key string) *PendingUpload {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pending[s3Key]
}

// PendingAll returns all in-flight uploads (for resume on startup).
func (m *Manifest) PendingAll() []*PendingUpload {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*PendingUpload, 0, len(m.pending))
	for _, p := range m.pending {
		out = append(out, p)
	}
	return out
}

// AbortPending removes a pending upload (the caller has already issued
// AbortMultipartUpload to S3).
func (m *Manifest) AbortPending(s3Key string) error {
	m.mu.Lock()
	delete(m.pending, s3Key)
	m.mu.Unlock()
	return m.flushPending()
}

// Lookup returns the manifest entry for a (topic, partition, baseOffset),
// or false if not archived.
func (m *Manifest) Lookup(topic string, partition int32, baseOffset int64) (SegmentEntry, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.completed {
		if e.Topic == topic && e.Partition == partition && e.BaseOffset == baseOffset {
			return e, true
		}
	}
	return SegmentEntry{}, false
}

// All returns all archived segments. Used by the dashboard.
func (m *Manifest) All() []SegmentEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]SegmentEntry, len(m.completed))
	copy(out, m.completed)
	return out
}

// AllForTopic filters by topic.
func (m *Manifest) AllForTopic(topic string) []SegmentEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []SegmentEntry{}
	for _, e := range m.completed {
		if e.Topic == topic {
			out = append(out, e)
		}
	}
	return out
}

func (m *Manifest) flushCompleted() error {
	m.mu.Lock()
	wrapper := struct {
		Format   int            `json:"format_version"`
		Segments []SegmentEntry `json:"segments"`
	}{Format: 1, Segments: m.completed}
	m.mu.Unlock()
	return atomicWrite(filepath.Join(m.dir, "archive.json"), wrapper)
}

func (m *Manifest) flushPending() error {
	m.mu.Lock()
	wrapper := struct {
		Format  int                       `json:"format_version"`
		Pending map[string]*PendingUpload `json:"pending"`
	}{Format: 1, Pending: m.pending}
	m.mu.Unlock()
	return atomicWrite(filepath.Join(m.dir, "archive_pending.json"), wrapper)
}

func (m *Manifest) flushBoth() error {
	if err := m.flushCompleted(); err != nil {
		return err
	}
	return m.flushPending()
}

func atomicWrite(path string, data interface{}) error {
	raw, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal %s: %w", path, err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// SegmentKey returns the canonical S3 object key for an archived
// segment. Format depends on tenancy:
//
//	tenant-scoped:  <prefix>/tenants/<tenant_id>/<topic>/<partition>/<base_offset>.log
//	shared:         <prefix>/<topic>/<partition>/<base_offset>.log
//
// The tenant prefix gives operators a visual + IAM-policy boundary:
// a tenant-bucket-policy can pin write/read to its own subtree, and
// an AWS audit trail (CloudTrail GetObject) directly correlates with
// tenant identity.
//
// CRITICAL: this function is the ONLY place that constructs an
// archive key. Path traversal characters are forbidden in topic /
// tenant names elsewhere; the chaos engine actively probes this.
func SegmentKey(prefix, topic string, partition int32, baseOffset int64) string {
	return SegmentKeyTenant(prefix, "", topic, partition, baseOffset)
}

// SegmentKeyTenant is the tenant-aware path builder. tenantID == ""
// produces the legacy shared layout.
func SegmentKeyTenant(prefix, tenantID, topic string, partition int32, baseOffset int64) string {
	prefix = strings.TrimSuffix(prefix, "/")
	if tenantID == "" {
		return fmt.Sprintf("%s/%s/%d/%020d.log", prefix, topic, partition, baseOffset)
	}
	return fmt.Sprintf("%s/tenants/%s/%s/%d/%020d.log", prefix, tenantID, topic, partition, baseOffset)
}

// SegmentSignature computes the broker's HMAC-SHA-256 over the
// ownership tuple. Returned as hex. Both sign-on-upload and verify-
// on-restore use this exact function so a mismatch is unambiguous.
func SegmentSignature(hmacKey []byte, tenantID, topic string, partition int32, baseOffset int64, sha256Hex string) string {
	mac := hmac.New(sha256.New, hmacKey)
	// Canonical message: pipe-delimited, no JSON, no whitespace.
	// Order is fixed for the lifetime of the broker, changing it is
	// a wire-format break.
	fmt.Fprintf(mac, "v1|%s|%s|%d|%d|%s", tenantID, topic, partition, baseOffset, sha256Hex)
	return hex.EncodeToString(mac.Sum(nil))
}

// VerifySignature returns true if the entry's HMACSignature matches
// the broker's HMAC of its ownership tuple. Empty hmacKey means HMAC
// is disabled (legacy entries from before signing was wired up; pass
// through). Empty entry.HMACSignature with non-empty key means the
// entry was written before HMAC was configured, also passes through.
//
// The chaos engine sets the key from day one, so all chaos-test
// segments carry valid signatures and tampering surfaces immediately.
func (e *SegmentEntry) VerifySignature(hmacKey []byte) bool {
	if len(hmacKey) == 0 || e.HMACSignature == "" {
		return true
	}
	want := SegmentSignature(hmacKey, e.TenantID, e.Topic, e.Partition, e.BaseOffset, e.SHA256)
	return subtle.ConstantTimeCompare([]byte(want), []byte(e.HMACSignature)) == 1
}

// ErrNotArchived signals a Fetch missed both local + manifest.
var ErrNotArchived = errors.New("segment not archived")
