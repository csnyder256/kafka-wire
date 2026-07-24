package storage

import (
	"log/slog"
	"time"
)

// RunSyncer fsyncs every partition's active segment on a timer.
//
// This is what storage.fsyncmode "interval" actually means. Without it, data
// written to the log sits in the operating system's page cache until the OS
// chooses to write it back, which on an idle machine can be a long time, and
// the setting would be a promise the broker does not keep.
//
// It is deliberately a whole-partition sweep rather than per-append
// bookkeeping: one fsync covers every write since the last one, so the cost
// is bounded by the tick rate rather than by the write rate.
func RunSyncer(provider LogProvider, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for range tick.C {
		for _, l := range provider.AllLogs() {
			if err := l.FlushAndSync(); err != nil {
				// A failing fsync means the next crash loses more than the
				// operator was told it would, so it is worth saying loudly.
				slog.Warn("storage.fsync_failed",
					"topic", l.Topic(), "partition", l.Partition(), "err", err)
			}
		}
	}
}
