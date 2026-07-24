// Package objstore is the seam between kafka-wire and cold storage.
//
// The interface deliberately mentions no vendor and imports no vendor SDK.
// Everything above this package (the tiering uploader, the restore-on-fetch
// path, the manifest) is written against Store alone, so adding a backend
// never touches broker logic and removing one never leaves a dangling type.
//
// Three backends ship:
//
//	none  the default. Cold storage is off; everything stays on local disk.
//	fs    a directory. Point it at an NFS or SMB mount and you have durable
//	      off-box archival with no object store at all.
//	s3    any S3-compatible store: AWS, MinIO, Ceph, Cloudflare R2, Backblaze
//	      B2, Wasabi, Garage, SeaweedFS, Tigris, Storj, Google Cloud Storage
//	      through its XML interop endpoint, and the rest.
//
// Multipart is part of the interface rather than hidden inside the drivers
// because segments are large and uploads get interrupted. Exposing
// CreateMultipart, ListParts and CompleteMultipart is what lets an upload
// that died halfway resume from the last completed part instead of starting
// the whole segment again.
package objstore

import (
	"context"
	"errors"
	"io"
)

// ErrNotFound is returned by Stat and Get when the key does not exist. Every
// backend must map its own not-found representation onto this, so callers
// never have to know whether they are talking to S3, a filesystem, or a test
// double.
var ErrNotFound = errors.New("objstore: object not found")

// ObjectInfo is what a backend can tell us about a stored object without
// downloading it.
type ObjectInfo struct {
	Key      string
	Size     int64
	ETag     string
	Metadata map[string]string
}

// Part identifies one uploaded piece of a multipart upload. ETag is opaque:
// it is whatever the backend handed back, and it is given straight back to
// the backend at completion time.
type Part struct {
	Number int
	ETag   string
	Size   int64
}

// Store is the whole contract a cold-storage backend must satisfy.
//
// Implementations must be safe for concurrent use: the uploader runs several
// segment uploads at once and the restorer serves consumer fetches from other
// goroutines.
type Store interface {
	// Name identifies the backend in logs and in the admin API.
	Name() string

	// Stat returns metadata for a key, or ErrNotFound.
	Stat(ctx context.Context, key string) (ObjectInfo, error)

	// Get opens the object for reading. The caller closes it.
	Get(ctx context.Context, key string) (io.ReadCloser, error)

	// Delete removes a key. Deleting a key that is already gone is not an
	// error, so retention sweeps are idempotent.
	Delete(ctx context.Context, key string) error

	// CreateMultipart begins a multipart upload and returns its id.
	CreateMultipart(ctx context.Context, key string, meta map[string]string) (uploadID string, err error)

	// UploadPart uploads one part. Part numbers start at 1. Every part
	// except the final one must be the same size: Cloudflare R2 enforces
	// this, and the uploader satisfies it by construction.
	UploadPart(ctx context.Context, key, uploadID string, number int, r io.Reader, size int64) (Part, error)

	// ListParts returns the parts already accepted for an upload. This is
	// what makes resume possible after a restart.
	ListParts(ctx context.Context, key, uploadID string) ([]Part, error)

	// CompleteMultipart finalizes the upload from the given parts.
	CompleteMultipart(ctx context.Context, key, uploadID string, parts []Part) error

	// AbortMultipart discards an incomplete upload and its parts. Backends
	// that bill for incomplete uploads make this matter.
	AbortMultipart(ctx context.Context, key, uploadID string) error
}

// MaxPartsPerUpload is the ceiling every S3 implementation enforces. It is
// declared here rather than in the s3 driver because the configuration
// validator uses it to reject an impossible segment-to-part ratio before a
// single byte is transferred.
const MaxPartsPerUpload = 10000

// MinPartSize is the smallest non-final part size S3-compatible stores
// accept.
const MinPartSize = 5 * 1024 * 1024
