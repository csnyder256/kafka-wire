package objstore

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// FS archives to a directory. It exists for three reasons: it makes the whole
// cold-storage feature testable with no network and no container, it gives
// self-hosters a real "archive to the NAS" tier without running an object
// store, and it is the reference against which the s3 driver's behavior is
// compared in the shared conformance suite.
//
// Layout under Root:
//
//	objects/<key>            the finished object
//	objects/<key>.meta       its user metadata, as JSON
//	uploads/<uploadID>/NNNNN one part per file, plus a "key" marker
type FS struct {
	root string
	mu   sync.Mutex
}

// NewFS returns a filesystem-backed Store rooted at dir.
func NewFS(dir string) (*FS, error) {
	if dir == "" {
		return nil, errors.New("objstore/fs: archive.fs.path is empty")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("objstore/fs: %w", err)
	}
	for _, sub := range []string{"objects", "uploads"} {
		if err := os.MkdirAll(filepath.Join(abs, sub), 0o750); err != nil {
			return nil, fmt.Errorf("objstore/fs: create %s: %w", sub, err)
		}
	}
	// Fail at construction rather than at the first archive, an hour later,
	// in a background goroutine nobody is watching.
	probe := filepath.Join(abs, ".kafka-wire-write-probe")
	if err := os.WriteFile(probe, []byte("ok"), 0o640); err != nil {
		return nil, fmt.Errorf("objstore/fs: %s is not writable: %w", abs, err)
	}
	_ = os.Remove(probe)
	return &FS{root: abs}, nil
}

func (f *FS) Name() string { return "fs" }

// objectPath maps a key to a path, refusing anything that would escape the
// root. Keys come from topic names, which come from clients.
func (f *FS) objectPath(key string) (string, error) {
	clean := filepath.Clean("/" + strings.ReplaceAll(key, "\\", "/"))
	p := filepath.Join(f.root, "objects", filepath.FromSlash(clean))
	if !strings.HasPrefix(p, filepath.Join(f.root, "objects")+string(os.PathSeparator)) {
		return "", fmt.Errorf("objstore/fs: key %q escapes the archive root", key)
	}
	return p, nil
}

func (f *FS) Stat(_ context.Context, key string) (ObjectInfo, error) {
	p, err := f.objectPath(key)
	if err != nil {
		return ObjectInfo{}, err
	}
	st, err := os.Stat(p)
	if errors.Is(err, os.ErrNotExist) {
		return ObjectInfo{}, ErrNotFound
	}
	if err != nil {
		return ObjectInfo{}, err
	}
	info := ObjectInfo{Key: key, Size: st.Size(), Metadata: map[string]string{}}
	if b, err := os.ReadFile(p + ".meta"); err == nil {
		_ = json.Unmarshal(b, &info.Metadata)
	}
	info.ETag = info.Metadata["etag"]
	return info, nil
}

func (f *FS) Get(_ context.Context, key string) (io.ReadCloser, error) {
	p, err := f.objectPath(key)
	if err != nil {
		return nil, err
	}
	fh, err := os.Open(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	return fh, err
}

func (f *FS) Delete(_ context.Context, key string) error {
	p, err := f.objectPath(key)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Remove(p + ".meta"); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (f *FS) uploadDir(id string) string {
	return filepath.Join(f.root, "uploads", id)
}

func (f *FS) CreateMultipart(_ context.Context, key string, meta map[string]string) (string, error) {
	if _, err := f.objectPath(key); err != nil {
		return "", err
	}
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	id := hex.EncodeToString(buf)
	dir := f.uploadDir(id)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", err
	}
	m, _ := json.Marshal(map[string]any{"key": key, "meta": meta})
	if err := os.WriteFile(filepath.Join(dir, "upload.json"), m, 0o640); err != nil {
		return "", err
	}
	return id, nil
}

func (f *FS) UploadPart(_ context.Context, key, uploadID string, number int, r io.Reader, size int64) (Part, error) {
	if number < 1 || number > MaxPartsPerUpload {
		return Part{}, fmt.Errorf("objstore/fs: part number %d out of range 1..%d", number, MaxPartsPerUpload)
	}
	dir := f.uploadDir(uploadID)
	if _, err := os.Stat(dir); err != nil {
		return Part{}, fmt.Errorf("objstore/fs: unknown upload %q", uploadID)
	}
	// Write to a temp name then rename, so a crash mid-part never leaves a
	// short part that ListParts would later believe.
	tmp := filepath.Join(dir, fmt.Sprintf(".part-%05d.tmp", number))
	fh, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o640)
	if err != nil {
		return Part{}, err
	}
	n, err := io.Copy(fh, io.LimitReader(r, size))
	if cerr := fh.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return Part{}, err
	}
	if n != size {
		_ = os.Remove(tmp)
		return Part{}, fmt.Errorf("objstore/fs: part %d short write: wrote %d of %d bytes", number, n, size)
	}
	final := filepath.Join(dir, fmt.Sprintf("%05d.part", number))
	if err := os.Rename(tmp, final); err != nil {
		return Part{}, err
	}
	return Part{Number: number, ETag: fmt.Sprintf("fs-%05d-%d", number, n), Size: n}, nil
}

func (f *FS) ListParts(_ context.Context, _, uploadID string) ([]Part, error) {
	dir := f.uploadDir(uploadID)
	ents, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var parts []Part
	for _, e := range ents {
		if !strings.HasSuffix(e.Name(), ".part") {
			continue
		}
		num, err := strconv.Atoi(strings.TrimSuffix(e.Name(), ".part"))
		if err != nil {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		parts = append(parts, Part{
			Number: num,
			ETag:   fmt.Sprintf("fs-%05d-%d", num, info.Size()),
			Size:   info.Size(),
		})
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].Number < parts[j].Number })
	return parts, nil
}

func (f *FS) CompleteMultipart(_ context.Context, key, uploadID string, parts []Part) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	dir := f.uploadDir(uploadID)
	raw, err := os.ReadFile(filepath.Join(dir, "upload.json"))
	if err != nil {
		return fmt.Errorf("objstore/fs: unknown upload %q", uploadID)
	}
	var hdr struct {
		Key  string            `json:"key"`
		Meta map[string]string `json:"meta"`
	}
	_ = json.Unmarshal(raw, &hdr)

	dst, err := f.objectPath(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
		return err
	}

	sorted := append([]Part(nil), parts...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Number < sorted[j].Number })

	tmp := dst + ".assembling"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	var total int64
	for _, p := range sorted {
		in, err := os.Open(filepath.Join(dir, fmt.Sprintf("%05d.part", p.Number)))
		if err != nil {
			_ = out.Close()
			_ = os.Remove(tmp)
			return fmt.Errorf("objstore/fs: part %d missing at completion: %w", p.Number, err)
		}
		n, err := io.Copy(out, in)
		_ = in.Close()
		if err != nil {
			_ = out.Close()
			_ = os.Remove(tmp)
			return err
		}
		total += n
	}
	// The object is only durable once its bytes are on the platter; a rename
	// over a half-flushed file would be a silently corrupt archive.
	if err := out.Sync(); err != nil {
		_ = out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if hdr.Meta == nil {
		hdr.Meta = map[string]string{}
	}
	hdr.Meta["etag"] = fmt.Sprintf("fs-%d", total)
	if b, err := json.Marshal(hdr.Meta); err == nil {
		_ = os.WriteFile(dst+".meta", b, 0o640)
	}
	return os.RemoveAll(dir)
}

func (f *FS) AbortMultipart(_ context.Context, _, uploadID string) error {
	return os.RemoveAll(f.uploadDir(uploadID))
}
