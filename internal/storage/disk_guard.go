//go:build !windows

package storage

import (
	"errors"
	"log/slog"
	"sync/atomic"
	"syscall"
	"time"
)

// DiskGuard periodically checks free space on the data volume and
// publishes a "writes paused" flag the broker consults before each
// append. Trips at the configured threshold (default: 10% free); auto-
// recovers when free space returns above the threshold.
//
// Only available on Unix; Windows
// compiles to a no-op guard.
type DiskGuard struct {
	dir       string
	threshold float64 // fraction free below which writes pause (0.10 = 10%)
	paused    atomic.Bool
	stop      chan struct{}

	// OnState, when set, receives every check's outcome so the pause
	// flag is observable (Prometheus gauge) instead of log-only.
	OnState func(paused bool, freeFraction float64)
}

// NewDiskGuard returns a DiskGuard. Threshold of 0 uses the default 0.10.
func NewDiskGuard(dir string, threshold float64) *DiskGuard {
	if threshold <= 0 || threshold >= 1 {
		threshold = 0.10
	}
	return &DiskGuard{
		dir:       dir,
		threshold: threshold,
		stop:      make(chan struct{}),
	}
}

// Run loops every 30s checking statfs of the data dir.
func (d *DiskGuard) Run() {
	tick := time.NewTicker(30 * time.Second)
	defer tick.Stop()
	d.checkOnce()
	for {
		select {
		case <-d.stop:
			return
		case <-tick.C:
			d.checkOnce()
		}
	}
}

// Stop signals the loop to exit.
func (d *DiskGuard) Stop() {
	close(d.stop)
}

// Paused reports whether writes are currently paused for low disk.
// Hot path; pure atomic load.
func (d *DiskGuard) Paused() bool {
	return d.paused.Load()
}

func (d *DiskGuard) checkOnce() {
	free, total, err := diskStats(d.dir)
	if err != nil {
		// Don't pause writes on a stat error, that would be a worse
		// failure mode than a real disk issue. Log and move on.
		slog.Warn("storage.disk_guard.statfs_failed", "dir", d.dir, "err", err)
		return
	}
	if total == 0 {
		return
	}
	frac := float64(free) / float64(total)
	defer func() {
		if d.OnState != nil {
			d.OnState(d.paused.Load(), frac)
		}
	}()
	if frac < d.threshold {
		if !d.paused.Load() {
			d.paused.Store(true)
			slog.Warn("storage.disk_guard.writes_paused",
				"dir", d.dir, "free_fraction", frac, "threshold", d.threshold)
		}
		return
	}
	if d.paused.Load() {
		d.paused.Store(false)
		slog.Info("storage.disk_guard.writes_resumed",
			"dir", d.dir, "free_fraction", frac)
	}
}

// ErrDiskFull is returned by Append paths when the guard has tripped.
var ErrDiskFull = errors.New("disk usage above threshold; writes paused")

// diskStats returns (free_bytes, total_bytes). Linux/Mac via syscall;
// Windows path returns (0, 0, nil) so the guard becomes a no-op (the
// docker-compose dev path on Windows hosts maps to a Linux container,
// so this is fine).
func diskStats(dir string) (uint64, uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, 0, err
	}
	free := uint64(st.Bavail) * uint64(st.Bsize)
	total := uint64(st.Blocks) * uint64(st.Bsize)
	return free, total, nil
}
