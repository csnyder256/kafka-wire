// Package webhook implements a per-topic HTTP forwarder. Each
// configured topic gets an internal consumer that reads each batch
// and POSTs the records to a configured endpoint. Useful for fanning
// events out to external systems (Slack, PagerDuty, custom webhooks)
// without writing a separate consumer.
//
// Config lives at /data/metadata/webhooks.json:
//
//   {
//     "format_version": 1,
//     "subscriptions": [
//       {
//         "topic": "orders.events",
//         "url": "https://hooks.slack.com/services/...",
//         "method": "POST",
//         "headers": { "X-Custom": "value" },
//         "filter_event_type": "document.uploaded",
//         "auto_offset_reset": "latest"
//       }
//     ]
//   }
//
// Each subscription runs as a goroutine reading from the broker via
// the same Kafka wire protocol its clients use. Failures
// retry with exponential backoff up to 5 minutes; persistent failures
// log a warning and are visible via /v1/webhooks admin endpoint.
package webhook

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Subscription is one webhook config.
type Subscription struct {
	Topic           string            `json:"topic"`
	URL             string            `json:"url"`
	Method          string            `json:"method,omitempty"`
	Headers         map[string]string `json:"headers,omitempty"`
	FilterEventType string            `json:"filter_event_type,omitempty"`
	AutoOffsetReset string            `json:"auto_offset_reset,omitempty"` // earliest|latest
	GroupID         string            `json:"group_id,omitempty"`
}

// Config is the on-disk webhook config shape.
type Config struct {
	FormatVersion int            `json:"format_version"`
	Subscriptions []Subscription `json:"subscriptions"`
}

// Manager owns the goroutines for every subscription. Reload-able at
// runtime via Reload(), caller passes a fresh path scan and we
// diff/start/stop.
type Manager struct {
	dataDir string
	mu      sync.Mutex
	cancels map[string]context.CancelFunc // keyed by topic+url
	stats   map[string]*stats

	// dlqMu serializes DLQ file writes (rare; one lock is plenty).
	dlqMu sync.Mutex
	// dlqBusy marks subscriptions with a redelivery in flight (guarded
	// by dlqMu). A second concurrent redelivery for the same key would
	// race the first for the claim file, so it is refused instead.
	dlqBusy map[string]bool
}

// dlqEntry is one exhausted delivery, persisted as a JSONL line under
// /data/metadata/webhook-dlq/. Before the DLQ existed, a record whose
// retries all failed was simply DROPPED, the subscriber's offset had
// already advanced, so the event was gone with only a log line left.
type dlqEntry struct {
	Topic     string    `json:"topic"`
	Partition int32     `json:"partition"`
	Offset    int64     `json:"offset"`
	Key       []byte    `json:"key,omitempty"`
	Value     []byte    `json:"value"`
	Timestamp time.Time `json:"timestamp"`
	FailedAt  time.Time `json:"failed_at"`
	LastError string    `json:"last_error"`
}

type stats struct {
	mu              sync.Mutex
	delivered       int64
	failed          int64
	lastError       string
	lastDeliveredAt time.Time
}

// NewManager initializes a webhook manager.
func NewManager(dataDir string) *Manager {
	return &Manager{
		dataDir: dataDir,
		cancels: make(map[string]context.CancelFunc),
		stats:   make(map[string]*stats),
		dlqBusy: make(map[string]bool),
	}
}

// Path returns the on-disk config file path.
func (m *Manager) Path() string {
	return filepath.Join(m.dataDir, "metadata", "webhooks.json")
}

// LoadConfig reads webhooks.json. Missing file returns an empty
// config (no subscriptions).
func (m *Manager) LoadConfig() (*Config, error) {
	raw, err := os.ReadFile(m.Path())
	if errors.Is(err, os.ErrNotExist) {
		return &Config{FormatVersion: 1}, nil
	}
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parse webhooks.json: %w", err)
	}
	return &cfg, nil
}

// SaveConfig atomically writes webhooks.json.
func (m *Manager) SaveConfig(cfg *Config) error {
	cfg.FormatVersion = 1
	raw, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	tmp := m.Path() + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, m.Path())
}

// Subscriber reads from a topic. Implemented by a small
// internal-consumer adapter (broker.NewLocalConsumer in production;
// stubs in tests).
type Subscriber interface {
	Subscribe(ctx context.Context, topic, group, offsetReset string) (<-chan Record, error)
}

// Record is one message delivered by the subscriber.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp time.Time
}

// Run starts the manager. Spawns one goroutine per subscription.
// Cancellation propagates through ctx.
func (m *Manager) Run(ctx context.Context, sub Subscriber) error {
	cfg, err := m.LoadConfig()
	if err != nil {
		return err
	}
	for _, s := range cfg.Subscriptions {
		m.start(ctx, sub, s)
	}
	return nil
}

func (m *Manager) start(parent context.Context, sub Subscriber, s Subscription) {
	if s.URL == "" || s.Topic == "" {
		slog.Warn("webhook.skip_invalid", "topic", s.Topic, "url", s.URL)
		return
	}
	key := s.Topic + "::" + s.URL
	m.mu.Lock()
	if _, exists := m.cancels[key]; exists {
		m.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(parent)
	m.cancels[key] = cancel
	m.stats[key] = &stats{}
	m.mu.Unlock()

	go m.runOne(ctx, sub, s, key)
}

func (m *Manager) runOne(ctx context.Context, sub Subscriber, s Subscription, key string) {
	group := s.GroupID
	if group == "" {
		group = "kafka-wire-webhook-" + s.Topic
	}
	offsetReset := s.AutoOffsetReset
	if offsetReset == "" {
		offsetReset = "latest"
	}
	method := s.Method
	if method == "" {
		method = "POST"
	}

	ch, err := sub.Subscribe(ctx, s.Topic, group, offsetReset)
	if err != nil {
		slog.Error("webhook.subscribe_failed", "topic", s.Topic, "err", err)
		return
	}

	client := &http.Client{Timeout: 30 * time.Second}
	for {
		select {
		case <-ctx.Done():
			return
		case rec, ok := <-ch:
			if !ok {
				return
			}
			if s.FilterEventType != "" && !matchesEventType(rec.Value, s.FilterEventType) {
				continue
			}
			if err := m.deliver(ctx, client, method, s, rec); err != nil {
				m.stats[key].markFailed(err)
				// Exhausted retries: park the record in the DLQ instead
				// of dropping it (the consumer offset has already moved).
				if derr := m.appendDLQ(key, rec, err); derr != nil {
					slog.Error("webhook.dlq_append_failed", "topic", s.Topic, "err", derr)
				} else {
					slog.Warn("webhook.delivery_dead_lettered",
						"topic", s.Topic, "offset", rec.Offset, "err", err)
				}
			} else {
				m.stats[key].markDelivered()
			}
		}
	}
}

func envelopeBytes(rec Record) ([]byte, error) {
	envelope := map[string]any{
		"topic":     rec.Topic,
		"partition": rec.Partition,
		"offset":    rec.Offset,
		"timestamp": rec.Timestamp.UnixMilli(),
		"key":       string(rec.Key),
		"value":     json.RawMessage(rec.Value),
	}
	return json.Marshal(envelope)
}

// deliverOnce is a single HTTP attempt. Returns (retryable, error);
// retryable=false means a 4xx-style config problem where hammering the
// endpoint again with the same payload cannot help.
func (m *Manager) deliverOnce(ctx context.Context, client *http.Client, method string, s Subscription, raw []byte) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, method, s.URL, bytes.NewReader(raw))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "kafka-wire-webhook/1.0")
	for k, v := range s.Headers {
		req.Header.Set(k, v)
	}
	resp, derr := client.Do(req)
	if derr != nil {
		return true, derr
	}
	// Drain body so connection can be reused.
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return false, nil
	}
	return resp.StatusCode < 400 || resp.StatusCode >= 500, fmt.Errorf("HTTP %d", resp.StatusCode)
}

func (m *Manager) deliver(ctx context.Context, client *http.Client, method string, s Subscription, rec Record) error {
	raw, err := envelopeBytes(rec)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}

	// Retry with exponential backoff capped at 5 minutes.
	delays := []time.Duration{0, time.Second, 5 * time.Second, 30 * time.Second, 2 * time.Minute, 5 * time.Minute}
	var lastErr error
	for attempt, delay := range delays {
		if delay > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
		retryable, derr := m.deliverOnce(ctx, client, method, s, raw)
		if derr == nil {
			return nil
		}
		lastErr = derr
		if !retryable && attempt >= 1 {
			// 4xx is usually a config error; don't retry indefinitely.
			return lastErr
		}
	}
	return lastErr
}

// matchesEventType peeks at the JSON payload's `event_type` field
// without parsing the whole value (cheap path; rejects non-JSON).
func matchesEventType(value []byte, want string) bool {
	var probe struct {
		EventType string `json:"event_type"`
	}
	if err := json.Unmarshal(value, &probe); err != nil {
		return false
	}
	return probe.EventType == want
}

func (s *stats) markDelivered() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.delivered++
	s.lastDeliveredAt = time.Now()
}

func (s *stats) markFailed(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failed++
	s.lastError = err.Error()
}

// Snapshot returns the current stats per subscription. Used by the
// admin /v1/webhooks endpoint.
type SubscriptionStatus struct {
	Topic           string    `json:"topic"`
	URL             string    `json:"url"`
	Delivered       int64     `json:"delivered"`
	Failed          int64     `json:"failed"`
	DeadLettered    int       `json:"dead_lettered"`
	LastError       string    `json:"last_error,omitempty"`
	LastDeliveredAt time.Time `json:"last_delivered_at,omitempty"`
}

// Snapshot returns a sorted-by-topic list of subscription status.
func (m *Manager) Snapshot() []SubscriptionStatus {
	m.mu.Lock()
	keys := make([]string, 0, len(m.stats))
	statByKey := make(map[string]*stats, len(m.stats))
	for key, s := range m.stats {
		keys = append(keys, key)
		statByKey[key] = s
	}
	m.mu.Unlock()

	out := make([]SubscriptionStatus, 0, len(keys))
	for _, key := range keys {
		s := statByKey[key]
		parts := splitKey(key)
		s.mu.Lock()
		st := SubscriptionStatus{
			Topic:           parts[0],
			URL:             parts[1],
			Delivered:       s.delivered,
			Failed:          s.failed,
			LastError:       s.lastError,
			LastDeliveredAt: s.lastDeliveredAt,
		}
		s.mu.Unlock()
		// Counted outside m.mu / s.mu: dlqCount takes dlqMu and reads a
		// file; holding the stats locks across file IO would stall the
		// hot delivery path.
		st.DeadLettered = m.dlqCount(key)
		out = append(out, st)
	}
	return out
}

func splitKey(k string) [2]string {
	for i := 0; i+1 < len(k); i++ {
		if k[i] == ':' && k[i+1] == ':' {
			return [2]string{k[:i], k[i+2:]}
		}
	}
	return [2]string{k, ""}
}

/* ── Dead-letter queue ──────────────────────────────────────────────── */

func (m *Manager) dlqDir() string {
	return filepath.Join(m.dataDir, "metadata", "webhook-dlq")
}

// dlqPath maps a subscription key to its JSONL file. The key contains a
// URL, so hash it instead of fighting filesystem-hostile characters.
func (m *Manager) dlqPath(key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(m.dlqDir(), hex.EncodeToString(sum[:8])+".jsonl")
}

func (m *Manager) appendDLQ(key string, rec Record, deliverErr error) error {
	m.dlqMu.Lock()
	defer m.dlqMu.Unlock()
	if err := os.MkdirAll(m.dlqDir(), 0o755); err != nil {
		return err
	}
	entry := dlqEntry{
		Topic:     rec.Topic,
		Partition: rec.Partition,
		Offset:    rec.Offset,
		Key:       rec.Key,
		Value:     rec.Value,
		Timestamp: rec.Timestamp,
		FailedAt:  time.Now().UTC(),
		LastError: deliverErr.Error(),
	}
	raw, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(m.dlqPath(key), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(append(raw, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

// dlqCount returns the number of parked records for a subscription key.
// Includes any .processing claim file so records mid-redelivery (or
// left behind by a crashed redelivery) stay visible in Snapshot.
func (m *Manager) dlqCount(key string) int {
	m.dlqMu.Lock()
	defer m.dlqMu.Unlock()
	path := m.dlqPath(key)
	return countJSONLLines(path) + countJSONLLines(path+".processing")
}

func countJSONLLines(path string) int {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close()
	n := 0
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		if len(bytes.TrimSpace(sc.Bytes())) > 0 {
			n++
		}
	}
	return n
}

// appendRawLocked appends raw JSONL bytes to path, fsyncing before
// return. Caller must hold dlqMu.
func appendRawLocked(path string, data []byte) error {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if !bytes.HasSuffix(data, []byte("\n")) {
		data = append(data, '\n')
	}
	if _, err := f.Write(data); err != nil {
		return err
	}
	return f.Sync()
}

// RedeliverDLQ replays every parked record for (topic, url). Successes
// are removed; failures are kept (with a fresh error + timestamp). The
// admin endpoint triggers this once the downstream is healthy again.
//
// The DLQ file is CLAIMED by renaming it to <path>.processing under
// dlqMu before any network work: live deliveries that dead-letter
// while the replay runs append to a fresh file at the canonical path,
// and failures from the claimed batch are appended back afterwards
// (never a whole-file rewrite). The earlier read-process-rewrite shape
// silently dropped any record appendDLQ parked during the unlocked
// network phase. Crash mid-claim leaves <path>.processing behind; the
// next redelivery folds it back in, so records can duplicate after a
// crash but are never lost (at-least-once, the right DLQ bias).
func (m *Manager) RedeliverDLQ(ctx context.Context, topic, url string) (redelivered, remaining int, err error) {
	key := topic + "::" + url
	cfg, err := m.LoadConfig()
	if err != nil {
		return 0, 0, err
	}
	var sub *Subscription
	for i := range cfg.Subscriptions {
		if cfg.Subscriptions[i].Topic == topic && cfg.Subscriptions[i].URL == url {
			sub = &cfg.Subscriptions[i]
			break
		}
	}
	if sub == nil {
		return 0, 0, fmt.Errorf("no subscription for topic %q url %q", topic, url)
	}
	method := sub.Method
	if method == "" {
		method = "POST"
	}

	m.dlqMu.Lock()
	if m.dlqBusy[key] {
		m.dlqMu.Unlock()
		return 0, 0, fmt.Errorf("redelivery already in progress for topic %q url %q", topic, url)
	}
	path := m.dlqPath(key)
	claimed := path + ".processing"
	// Fold back a leftover claim from a crashed prior run before taking
	// a new one; renaming over it would drop those records.
	if leftover, lerr := os.ReadFile(claimed); lerr == nil {
		if aerr := appendRawLocked(path, leftover); aerr != nil {
			m.dlqMu.Unlock()
			return 0, 0, aerr
		}
		if aerr := os.Remove(claimed); aerr != nil {
			m.dlqMu.Unlock()
			return 0, 0, aerr
		}
	} else if !errors.Is(lerr, os.ErrNotExist) {
		// An unreadable claim must abort: renaming over it would
		// destroy the only copy of the crashed batch.
		m.dlqMu.Unlock()
		return 0, 0, lerr
	}
	if rerr := os.Rename(path, claimed); rerr != nil {
		m.dlqMu.Unlock()
		if errors.Is(rerr, os.ErrNotExist) {
			return 0, 0, nil
		}
		return 0, 0, rerr
	}
	m.dlqBusy[key] = true
	m.dlqMu.Unlock()
	defer func() {
		m.dlqMu.Lock()
		delete(m.dlqBusy, key)
		m.dlqMu.Unlock()
	}()

	// Safe to read without the lock: nothing else touches the claim file
	// while dlqBusy is held. On error the claim survives for fold-back.
	raw, rerr := os.ReadFile(claimed)
	if rerr != nil {
		return 0, 0, rerr
	}

	client := &http.Client{Timeout: 30 * time.Second}
	var kept [][]byte
	lines := bytes.Split(raw, []byte("\n"))
	for li, line := range lines {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		if ctx.Err() != nil {
			// Canceled: fold this and every remaining line back
			// unprocessed. Breaking without keeping them would delete
			// their only copy when the claim file is removed below.
			for _, rest := range lines[li:] {
				rest = bytes.TrimSpace(rest)
				if len(rest) > 0 {
					kept = append(kept, append([]byte(nil), rest...))
				}
			}
			break
		}
		var e dlqEntry
		if uerr := json.Unmarshal(line, &e); uerr != nil {
			// Unparseable line: keep it verbatim rather than lose data.
			kept = append(kept, append([]byte(nil), line...))
			continue
		}
		rec := Record{Topic: e.Topic, Partition: e.Partition, Offset: e.Offset,
			Key: e.Key, Value: e.Value, Timestamp: e.Timestamp}
		// Single attempt per record: the operator explicitly asked for a
		// replay NOW; a failure goes back to the queue instead of walking
		// the multi-minute live-path backoff chain per dead record.
		raw2, merr := envelopeBytes(rec)
		var derr error
		if merr != nil {
			derr = merr
		} else {
			_, derr = m.deliverOnce(ctx, client, method, *sub, raw2)
		}
		if derr != nil {
			e.FailedAt = time.Now().UTC()
			e.LastError = derr.Error()
			if reser, mErr2 := json.Marshal(e); mErr2 == nil {
				kept = append(kept, reser)
			} else {
				kept = append(kept, append([]byte(nil), line...))
			}
		} else {
			redelivered++
		}
	}

	m.dlqMu.Lock()
	if len(kept) > 0 {
		// APPEND failures back: the canonical file may have gained fresh
		// dead letters during the network phase and must not be replaced.
		out := append(bytes.Join(kept, []byte("\n")), '\n')
		if werr := appendRawLocked(path, out); werr != nil {
			// Claim file kept: the batch stays recoverable via fold-back
			// (successes will re-send next run; duplicates over loss).
			m.dlqMu.Unlock()
			return redelivered, len(kept), werr
		}
	}
	_ = os.Remove(claimed)
	m.dlqMu.Unlock()
	return redelivered, len(kept), nil
}
