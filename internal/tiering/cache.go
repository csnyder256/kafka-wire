package tiering

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Cache is the on-disk LRU for restored segments. Lives at /data/cache/.
// Eviction policy: when the cache exceeds MaxBytes, evict by least-
// recently-used (atime, fall back to mtime if filesystem doesn't track
// atime: most modern Linux filesystems with `relatime` mount track
// atime per file even if not on every read).
//
// Each cached segment is stored as:
//
//	<cache_dir>/<topic>/<partition>/<base_offset>.log
//
// matching the S3 key structure for clarity.
type Cache struct {
	dir      string
	maxBytes int64
	mu       sync.Mutex
}

// NewCache creates the cache directory and bounds it at maxBytes.
func NewCache(dir string, maxBytes int64) (*Cache, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("cache mkdir: %w", err)
	}
	if maxBytes <= 0 {
		maxBytes = 2 * 1024 * 1024 * 1024 // 2 GiB default
	}
	return &Cache{dir: dir, maxBytes: maxBytes}, nil
}

// Path returns the cache file path for a (tenant, topic, partition,
// base) tuple. Tenant is part of the path so cross-tenant cache bleed
// is impossible by construction.
//
// Tenant-scoped:  <cache>/tenants/<tenant>/<topic>/<partition>/<base>.log
// Shared:         <cache>/<topic>/<partition>/<base>.log
//
// CRITICAL: the cache key is the FULL tuple (tenant, topic, partition,
// base). Two segments with the same topic name but different tenants
// would have collided under the old scheme; under the new scheme
// they're at different paths.
func (c *Cache) Path(topic string, partition int32, baseOffset int64) string {
	return c.PathTenant("", topic, partition, baseOffset)
}

// PathTenant is the tenant-aware path builder.
func (c *Cache) PathTenant(tenant, topic string, partition int32, baseOffset int64) string {
	if tenant == "" {
		return filepath.Join(c.dir, topic, fmt.Sprintf("%d", partition), fmt.Sprintf("%020d.log", baseOffset))
	}
	return filepath.Join(c.dir, "tenants", tenant, topic, fmt.Sprintf("%d", partition), fmt.Sprintf("%020d.log", baseOffset))
}

// Has returns true if the cache contains this segment's file.
func (c *Cache) Has(topic string, partition int32, baseOffset int64) bool {
	return c.HasTenant("", topic, partition, baseOffset)
}

// HasTenant is the tenant-scoped variant.
func (c *Cache) HasTenant(tenant, topic string, partition int32, baseOffset int64) bool {
	_, err := os.Stat(c.PathTenant(tenant, topic, partition, baseOffset))
	return err == nil
}

// Put writes a restored segment to the cache. `data` is the entire
// segment body. Verifies sha256 before commit (reject corrupted
// downloads: better an error than silent data corruption).
func (c *Cache) Put(topic string, partition int32, baseOffset int64, data []byte, expectedSha256 string) error {
	return c.PutTenant("", topic, partition, baseOffset, data, expectedSha256)
}

// PutTenant is the tenant-aware variant.
func (c *Cache) PutTenant(tenant, topic string, partition int32, baseOffset int64, data []byte, expectedSha256 string) error {
	if expectedSha256 != "" {
		actual := sha256.Sum256(data)
		actualHex := hex.EncodeToString(actual[:])
		if actualHex != expectedSha256 {
			return fmt.Errorf("sha256 mismatch: got %s want %s", actualHex[:12], expectedSha256[:12])
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	path := c.PathTenant(tenant, topic, partition, baseOffset)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return c.evictLockedIfNeeded()
}

// PutTenantStream writes a restored segment from a reader, hashing as it
// streams to a temp file. The whole point over PutTenant is memory: a
// multi-GiB segment restore must never be buffered in RAM (the old
// io.ReadAll path OOM'd the broker under restore storms), and the S3
// download must not hold c.mu (a slow download would block every cache
// operation). Only the rename + eviction take the lock. Returns the
// byte count written.
func (c *Cache) PutTenantStream(tenant, topic string, partition int32, baseOffset int64, r io.Reader, expectedSha256 string) (int64, error) {
	path := c.PathTenant(tenant, topic, partition, baseOffset)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return 0, err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return 0, err
	}
	hasher := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, hasher), r)
	cerr := f.Close()
	if err != nil || cerr != nil {
		_ = os.Remove(tmp)
		if err == nil {
			err = cerr
		}
		return 0, fmt.Errorf("stream to cache tmp: %w", err)
	}
	if expectedSha256 != "" {
		actualHex := hex.EncodeToString(hasher.Sum(nil))
		if actualHex != expectedSha256 {
			_ = os.Remove(tmp)
			return 0, fmt.Errorf("sha256 mismatch: got %s want %s", actualHex[:12], expectedSha256[:12])
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return 0, err
	}
	return n, c.evictLockedIfNeeded()
}

// Open returns a *os.File handle to the cached segment. Caller must
// close it. Touches the file's atime via os.Chtimes so eviction
// counts the read.
func (c *Cache) Open(topic string, partition int32, baseOffset int64) (*os.File, error) {
	return c.OpenTenant("", topic, partition, baseOffset)
}

// OpenTenant is the tenant-aware variant.
func (c *Cache) OpenTenant(tenant, topic string, partition int32, baseOffset int64) (*os.File, error) {
	path := c.PathTenant(tenant, topic, partition, baseOffset)
	now := time.Now()
	_ = os.Chtimes(path, now, now)
	return os.Open(path)
}

// ReadAll loads the cached segment fully into memory. Used by Fetch
// when the segment is small (< 4 MiB); larger segments stream via Open.
func (c *Cache) ReadAll(topic string, partition int32, baseOffset int64) ([]byte, error) {
	f, err := c.Open(topic, partition, baseOffset)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

// evictLockedIfNeeded enforces the size cap. Caller must hold c.mu.
func (c *Cache) evictLockedIfNeeded() error {
	type fileMeta struct {
		path  string
		size  int64
		atime time.Time
	}
	var files []fileMeta
	var total int64
	err := filepath.Walk(c.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || filepath.Ext(path) != ".log" {
			return nil
		}
		fm := fileMeta{path: path, size: info.Size(), atime: info.ModTime()}
		files = append(files, fm)
		total += info.Size()
		return nil
	})
	if err != nil {
		return err
	}
	if total <= c.maxBytes {
		return nil
	}

	// LRU: oldest atime first. Files younger than the grace window are
	// never victims: a just-restored segment can otherwise be evicted
	// by a concurrent Put before its Fetch ever opens it, forcing an
	// immediate re-restore loop under cache pressure.
	const evictionGrace = 60 * time.Second
	cutoff := time.Now().Add(-evictionGrace)
	sort.Slice(files, func(i, j int) bool { return files[i].atime.Before(files[j].atime) })
	for total > c.maxBytes && len(files) > 0 {
		victim := files[0]
		files = files[1:]
		if victim.atime.After(cutoff) {
			continue
		}
		if err := os.Remove(victim.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		total -= victim.size
	}
	return nil
}

// SizeBytes returns the current cache size by walking the dir.
func (c *Cache) SizeBytes() (int64, error) {
	var total int64
	err := filepath.Walk(c.dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || filepath.Ext(info.Name()) != ".log" {
			return nil
		}
		total += info.Size()
		return nil
	})
	return total, err
}
