package storage

import (
	"log/slog"
	"time"
)

// RetentionConfig governs the periodic reaper. The reaper evicts sealed
// segments by age (segment creation time) and by partition size.
//
// With cold storage on, Archived is set and no rule deletes a segment the
// uploader has not yet put in the archive: while uploads are failing a
// partition grows on local disk instead of losing records the archive never
// received. LocalRetentionMS then trims the local copies of archived
// segments early; reads below the local log are served from the archive.
type RetentionConfig struct {
	RetentionMS    int64         // age cap in ms; 0 = unlimited
	RetentionBytes int64         // size cap; 0 or negative = unlimited
	Tick           time.Duration // sweep interval

	// Archived reports whether a sealed segment is durably in cold storage.
	// Nil when cold storage is off.
	Archived func(topic string, partition int32, baseOffset int64) bool
	// LocalRetentionMS deletes the local copy of an archived segment once it
	// is this old (archive.localretention). 0 disables it. Needs Archived.
	LocalRetentionMS int64
}

// LogProvider is the minimum surface RunRetention needs from the
// broker. Implemented by *broker.TopicRegistry.
type LogProvider interface {
	AllLogs() []*Log
}

// RunRetention loops forever, sweeping every sweepInterval(cfg).
// Safe to spawn from main.go without goroutine-leak concerns because
// process exit kills it.
func RunRetention(provider LogProvider, cfg RetentionConfig) {
	tick := time.NewTicker(sweepInterval(cfg))
	defer tick.Stop()
	for range tick.C {
		sweepOnce(provider, cfg, time.Now())
	}
}

// sweepInterval is cfg.Tick (60s by default), shortened so that no retention
// window is overshot by more than half of itself, and never below a second.
func sweepInterval(cfg RetentionConfig) time.Duration {
	d := cfg.Tick
	if d <= 0 {
		d = 60 * time.Second
	}
	for _, ms := range []int64{cfg.RetentionMS, cfg.LocalRetentionMS} {
		if half := time.Duration(ms) * time.Millisecond / 2; ms > 0 && half < d {
			d = max(half, time.Second)
		}
	}
	return d
}

func sweepOnce(provider LogProvider, cfg RetentionConfig, now time.Time) {
	logs := provider.AllLogs()
	for _, l := range logs {
		segs := l.SealedSegments()
		if len(segs) == 0 {
			continue
		}

		// Only an oldest-first prefix of the sealed segments can go. With
		// cold storage on, that prefix ends at the first segment the
		// uploader has not archived yet.
		deletable := len(segs)
		if cfg.Archived != nil {
			for i, seg := range segs {
				if !cfg.Archived(l.Topic(), l.Partition(), seg.BaseOffset()) {
					deletable = i
					break
				}
			}
		}

		// Walk the prefix oldest-first, stopping at the first segment
		// inside every age window. The segments are in BaseOffset order
		// (== creation order) so the first "kept" segment is also the
		// cutoff.
		var cutoff int64 = -1
		for _, seg := range segs[:deletable] {
			ageMS := now.Sub(seg.CreatedAt()).Milliseconds()
			expired := cfg.RetentionMS > 0 && ageMS > cfg.RetentionMS
			if cfg.Archived != nil && cfg.LocalRetentionMS > 0 && ageMS > cfg.LocalRetentionMS {
				expired = true
			}
			if !expired {
				break
			}
			cutoff = seg.NextOffset()
		}

		if cfg.RetentionBytes > 0 {
			var totalBytes int64
			for _, seg := range l.AllSegments() {
				totalBytes += seg.Size()
			}
			// Either rule may delete a segment, so keep whichever cutoff
			// reaches further.
			for i := 0; totalBytes > cfg.RetentionBytes && i < deletable; i++ {
				totalBytes -= segs[i].Size()
				if next := segs[i].NextOffset(); next > cutoff {
					cutoff = next
				}
			}
		}

		if cutoff < 0 {
			continue
		}
		deleted, err := l.DeleteSegmentsBefore(cutoff)
		if err != nil {
			slog.Warn("retention.delete_failed",
				"topic", l.Topic(),
				"partition", l.Partition(),
				"cutoff", cutoff,
				"err", err,
			)
			continue
		}
		if deleted > 0 {
			slog.Info("retention.swept",
				"topic", l.Topic(),
				"partition", l.Partition(),
				"deleted", deleted,
				"new_log_start", l.LogStartOffset(),
			)
		}
	}
}
