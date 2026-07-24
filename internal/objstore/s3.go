package objstore

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3Config describes any S3-compatible store. The field names are chosen to
// match what providers call these things in their own documentation, so a
// user copying values out of a Cloudflare or Backblaze console does not have
// to translate.
type S3Config struct {
	Bucket       string
	Endpoint     string // empty means AWS
	Region       string
	Addressing   string // auto | path | virtual
	AccessKey    string
	SecretKey    string
	SessionToken string
	Insecure     bool // plain HTTP
	CAFile       string
	SkipVerify   bool
	StorageClass string
}

// S3 is the S3-compatible backend.
//
// It is built on minio-go rather than the AWS SDK on purpose. Since
// aws-sdk-go-v2's service/s3 v1.73.0 the SDK computes a CRC32 checksum on
// every upload by default and switches the request to
// Content-Encoding: aws-chunked with a streaming trailer. Stores that do not
// implement that trailer, which at various points has included Hetzner, OVH,
// Garage, SeaweedFS, older Backblaze B2, older Cloudflare R2, older MinIO and
// Google Cloud Storage's XML endpoint, either reject the request or store the
// chunk framing as object bytes. minio-go has never had that behavior, and it
// pulls one module instead of seventeen.
type S3 struct {
	core   *minio.Core
	bucket string
	cfg    S3Config
}

// NewS3 constructs the backend and verifies it can actually reach the bucket,
// so a misconfiguration surfaces at startup rather than an hour later in a
// background upload.
func NewS3(ctx context.Context, cfg S3Config) (*S3, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("objstore/s3: archive.s3.bucket is empty")
	}

	endpoint, secure := cfg.Endpoint, !cfg.Insecure
	if endpoint == "" {
		endpoint = "s3.amazonaws.com"
	} else {
		// Accept a full URL, because that is how every provider prints it,
		// and let the scheme decide TLS unless insecure was set explicitly.
		if strings.HasPrefix(endpoint, "http://") {
			endpoint, secure = strings.TrimPrefix(endpoint, "http://"), false
		} else if strings.HasPrefix(endpoint, "https://") {
			endpoint, secure = strings.TrimPrefix(endpoint, "https://"), true
		}
		endpoint = strings.TrimSuffix(endpoint, "/")
	}
	if cfg.Insecure {
		secure = false
	}

	var lookup minio.BucketLookupType
	switch cfg.Addressing {
	case "", "auto":
		lookup = minio.BucketLookupAuto
	case "path":
		lookup = minio.BucketLookupPath
	case "virtual", "dns":
		lookup = minio.BucketLookupDNS
	default:
		return nil, fmt.Errorf("objstore/s3: archive.s3.addressing is %q; valid values are auto, path, virtual", cfg.Addressing)
	}

	creds, err := buildCredentials(cfg)
	if err != nil {
		return nil, err
	}

	opts := &minio.Options{
		Creds:        creds,
		Secure:       secure,
		Region:       cfg.Region,
		BucketLookup: lookup,
	}
	if secure && (cfg.CAFile != "" || cfg.SkipVerify) {
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if cfg.SkipVerify {
			tlsCfg.InsecureSkipVerify = true
		}
		if cfg.CAFile != "" {
			pem, err := os.ReadFile(cfg.CAFile)
			if err != nil {
				return nil, fmt.Errorf("objstore/s3: archive.s3.cafile: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("objstore/s3: archive.s3.cafile %s contains no usable certificate", cfg.CAFile)
			}
			tlsCfg.RootCAs = pool
		}
		opts.Transport = &http.Transport{TLSClientConfig: tlsCfg}
	}

	core, err := minio.NewCore(endpoint, opts)
	if err != nil {
		return nil, fmt.Errorf("objstore/s3: %w", err)
	}

	s := &S3{core: core, bucket: cfg.Bucket, cfg: cfg}

	// Reachability probe. BucketExists is one round trip and turns the three
	// most common misconfigurations into a named error at boot.
	ok, err := core.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("objstore/s3: cannot reach bucket %q at %s: %w\n"+
			"  Common causes: wrong archive.s3.endpoint, credentials not visible to this process,\n"+
			"  archive.s3.addressing needing to be \"path\" (MinIO, Ceph, SeaweedFS),\n"+
			"  or archive.s3.region not matching the bucket",
			cfg.Bucket, endpoint, err)
	}
	if !ok {
		return nil, fmt.Errorf("objstore/s3: bucket %q does not exist at %s, or the credentials cannot see it", cfg.Bucket, endpoint)
	}
	return s, nil
}

// buildCredentials implements a precedence a user can predict: explicit
// configuration first, then the standard AWS_* environment variables that
// every S3-compatible vendor tells people to use, then the shared credentials
// file, then the instance metadata endpoint. Refusing to read AWS_* just
// because the store is not AWS would be gratuitous friction.
func buildCredentials(cfg S3Config) (*credentials.Credentials, error) {
	if cfg.AccessKey != "" {
		if cfg.SecretKey == "" {
			return nil, errors.New("objstore/s3: archive.s3.accesskey is set but archive.s3.secretkey is empty")
		}
		return credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, cfg.SessionToken), nil
	}
	return credentials.NewChainCredentials([]credentials.Provider{
		&credentials.EnvAWS{},
		&credentials.FileAWSCredentials{},
		&credentials.IAM{Client: &http.Client{Transport: http.DefaultTransport}},
	}), nil
}

func (s *S3) Name() string { return "s3" }

// notFound maps every way a store can say "no such object" onto ErrNotFound.
func notFound(err error) bool {
	if err == nil {
		return false
	}
	switch minio.ToErrorResponse(err).Code {
	case "NoSuchKey", "NotFound", "NoSuchUpload":
		return true
	}
	return false
}

func (s *S3) Stat(ctx context.Context, key string) (ObjectInfo, error) {
	oi, err := s.core.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if notFound(err) {
		return ObjectInfo{}, ErrNotFound
	}
	if err != nil {
		return ObjectInfo{}, err
	}
	meta := map[string]string{}
	for k, v := range oi.UserMetadata {
		meta[strings.ToLower(k)] = v
	}
	return ObjectInfo{Key: key, Size: oi.Size, ETag: normalizeETag(oi.ETag), Metadata: meta}, nil
}

func (s *S3) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	rc, _, _, err := s.core.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if notFound(err) {
		return nil, ErrNotFound
	}
	return rc, err
}

func (s *S3) Delete(ctx context.Context, key string) error {
	err := s.core.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
	if notFound(err) {
		return nil
	}
	return err
}

func (s *S3) putOptions(meta map[string]string) minio.PutObjectOptions {
	opts := minio.PutObjectOptions{StorageClass: s.cfg.StorageClass}
	if len(meta) > 0 {
		opts.UserMetadata = meta
	}
	return opts
}

func (s *S3) CreateMultipart(ctx context.Context, key string, meta map[string]string) (string, error) {
	return s.core.NewMultipartUpload(ctx, s.bucket, key, s.putOptions(meta))
}

// normalizeETag strips the surrounding quotes S3 puts around entity tags.
//
// This is not cosmetic. The same store returns a part's ETag unquoted from
// PutObjectPart and quoted from ListObjectParts, so code that uploads a part,
// restarts, lists what survived and compares the two would never find a
// match, and resume would silently degrade into re-uploading every segment
// from the beginning. The objstore conformance suite asserts the two agree.
func normalizeETag(s string) string {
	return strings.Trim(s, `"`)
}

func (s *S3) UploadPart(ctx context.Context, key, uploadID string, number int, r io.Reader, size int64) (Part, error) {
	if number < 1 || number > MaxPartsPerUpload {
		return Part{}, fmt.Errorf("objstore/s3: part number %d out of range 1..%d", number, MaxPartsPerUpload)
	}
	p, err := s.core.PutObjectPart(ctx, s.bucket, key, uploadID, number, r, size, minio.PutObjectPartOptions{})
	if err != nil {
		return Part{}, err
	}
	return Part{Number: p.PartNumber, ETag: normalizeETag(p.ETag), Size: p.Size}, nil
}

func (s *S3) ListParts(ctx context.Context, key, uploadID string) ([]Part, error) {
	var out []Part
	marker := 0
	for {
		res, err := s.core.ListObjectParts(ctx, s.bucket, key, uploadID, marker, 1000)
		if notFound(err) {
			return nil, ErrNotFound
		}
		if err != nil {
			return nil, err
		}
		for _, p := range res.ObjectParts {
			out = append(out, Part{Number: p.PartNumber, ETag: normalizeETag(p.ETag), Size: p.Size})
		}
		if !res.IsTruncated {
			return out, nil
		}
		marker = res.NextPartNumberMarker
	}
}

func (s *S3) CompleteMultipart(ctx context.Context, key, uploadID string, parts []Part) error {
	cp := make([]minio.CompletePart, 0, len(parts))
	for _, p := range parts {
		cp = append(cp, minio.CompletePart{PartNumber: p.Number, ETag: p.ETag})
	}
	_, err := s.core.CompleteMultipartUpload(ctx, s.bucket, key, uploadID, cp, minio.PutObjectOptions{})
	return err
}

func (s *S3) AbortMultipart(ctx context.Context, key, uploadID string) error {
	err := s.core.AbortMultipartUpload(ctx, s.bucket, key, uploadID)
	if notFound(err) {
		return nil
	}
	return err
}
