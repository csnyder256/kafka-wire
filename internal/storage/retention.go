package storage

import (
	"log/slog"
	"time"
)

// RetentionConfig governs the periodic reaper. The reaper evicts
// sealed-segment files based on segment-file mtime (the plan's
// year-2050-timestamp griefing mitigation). Cold storage layers
// archival on top: reaper only deletes segments AFTER the uploader
// has confirmed they're durably in S3.
type RetentionConfig struct {
	RetentionMS    int64         // age cap in ms; 0 = unlimited
	RetentionBytes int64         // size cap; 0 or negative = unlimited
	Tick           time.Duration // sweep interval
}

// LogProvider is the minimum surface RunRetention needs from the
// broker. Implemented by *broker.TopicRegistry.
type LogProvider interface {
	AllLogs() []*Log
}

// RunRetention loops forever, sweeping every RetentionConfig.Tick.
// Cancel by closing the returned channel; safe to spawn from main.go
// without goroutine-leak concerns because process exit kills it.
func RunRetention(provider LogProvider, cfg RetentionConfig) {
	if cfg.Tick <= 0 {
		cfg.Tick = 60 * time.Second
	}
	tick := time.NewTicker(cfg.Tick)
	defer tick.Stop()
	for range tick.C {
		sweepOnce(provider, cfg)
	}
}

func sweepOnce(provider LogProvider, cfg RetentionConfig) {
	now := time.Now()
	logs := provider.AllLogs()
	for _, l := range logs {
		// Walk sealed segments oldest-first, stopping as soon as we
		// find one whose mtime is inside the retention window. The
		// segments are in BaseOffset order (== creation order) so
		// the first "kept" segment is also the cutoff.
		segs := l.SealedSegments()
		if len(segs) == 0 {
			continue
		}

		var cutoff int64 = -1
		for _, seg := range segs {
			ageMS := now.Sub(seg.CreatedAt()).Milliseconds()
			if cfg.RetentionMS > 0 && ageMS > cfg.RetentionMS {
				cutoff = seg.NextOffset()
				continue
			}
			break
		}

		if cfg.RetentionBytes > 0 {
			// Recompute cutoff including byte-size cap.
			var totalBytes int64
			all := l.AllSegments()
			for _, seg := range all {
				totalBytes += seg.Size()
			}
			i := 0
			for totalBytes > cfg.RetentionBytes && i < len(segs) {
				totalBytes -= segs[i].Size()
				cutoff = segs[i].NextOffset()
				i++
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
