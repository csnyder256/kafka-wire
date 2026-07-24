//go:build windows

package storage

import "errors"

// Windows stub. The broker runs in alpine containers in production,
// so the windows build is dev-only (e.g., go test from a Windows
// host) and a no-op guard is fine.

type DiskGuard struct {
	// OnState mirrors the Unix guard's field so callers can wire it
	// unconditionally; never invoked by the no-op guard.
	OnState func(paused bool, freeFraction float64)
}

func NewDiskGuard(dir string, threshold float64) *DiskGuard { return &DiskGuard{} }

func (d *DiskGuard) Run()         {}
func (d *DiskGuard) Stop()        {}
func (d *DiskGuard) Paused() bool { return false }

var ErrDiskFull = errors.New("disk usage above threshold; writes paused")
