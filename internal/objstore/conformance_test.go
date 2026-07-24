package objstore_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/csnyder256/kafka-wire/internal/objstore"
)

// runConformance is the whole point of the objstore package: one suite that
// every backend must pass identically. A backend that diverges here would
// make cold storage behave differently depending on where it points, which is
// exactly the class of bug a "pluggable" abstraction is supposed to prevent.
//
// It runs unconditionally against the filesystem backend, and against a real
// S3-compatible server whenever one is reachable (see TestS3Conformance).
func runConformance(t *testing.T, name string, st objstore.Store) {
	t.Helper()
	ctx := context.Background()

	t.Run(name+"/StatMissingIsErrNotFound", func(t *testing.T) {
		_, err := st.Stat(ctx, "definitely/not/here.log")
		if !errors.Is(err, objstore.ErrNotFound) {
			t.Fatalf("Stat on a missing key = %v, want ErrNotFound", err)
		}
	})

	t.Run(name+"/GetMissingIsErrNotFound", func(t *testing.T) {
		_, err := st.Get(ctx, "definitely/not/here.log")
		if !errors.Is(err, objstore.ErrNotFound) {
			t.Fatalf("Get on a missing key = %v, want ErrNotFound", err)
		}
	})

	t.Run(name+"/DeleteMissingIsNotAnError", func(t *testing.T) {
		// Retention sweeps re-run. Deleting something already gone must be
		// a no-op or every sweep after the first logs a spurious failure.
		if err := st.Delete(ctx, "definitely/not/here.log"); err != nil {
			t.Fatalf("Delete on a missing key = %v, want nil", err)
		}
	})

	t.Run(name+"/RoundTripMultipart", func(t *testing.T) {
		key := "topics/round.trip/0/00000000000000000000.log"
		// Two full parts plus a short final part, which is the shape every
		// real segment upload takes.
		partSize := objstore.MinPartSize
		payload := randomBytes(t, partSize*2+1234)

		id, err := st.CreateMultipart(ctx, key, map[string]string{"sha256": "abc123"})
		if err != nil {
			t.Fatalf("CreateMultipart: %v", err)
		}
		var parts []objstore.Part
		for i, off := 1, 0; off < len(payload); i++ {
			end := off + partSize
			if end > len(payload) {
				end = len(payload)
			}
			chunk := payload[off:end]
			p, err := st.UploadPart(ctx, key, id, i, bytes.NewReader(chunk), int64(len(chunk)))
			if err != nil {
				t.Fatalf("UploadPart %d: %v", i, err)
			}
			if p.Number != i {
				t.Fatalf("UploadPart returned number %d, want %d", p.Number, i)
			}
			if p.ETag == "" {
				t.Fatalf("UploadPart %d returned an empty ETag; completion needs it", i)
			}
			parts = append(parts, p)
			off = end
		}
		if err := st.CompleteMultipart(ctx, key, id, parts); err != nil {
			t.Fatalf("CompleteMultipart: %v", err)
		}

		info, err := st.Stat(ctx, key)
		if err != nil {
			t.Fatalf("Stat after complete: %v", err)
		}
		if info.Size != int64(len(payload)) {
			t.Fatalf("stored size %d, want %d", info.Size, len(payload))
		}

		rc, err := st.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		got, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		// Byte equality, not length equality. Assembling parts in the wrong
		// order produces the right length and the wrong file.
		if !bytes.Equal(got, payload) {
			t.Fatalf("round trip corrupted the object: %d bytes back, equal=%v", len(got), bytes.Equal(got, payload))
		}

		if err := st.Delete(ctx, key); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if _, err := st.Stat(ctx, key); !errors.Is(err, objstore.ErrNotFound) {
			t.Fatalf("after Delete, Stat = %v, want ErrNotFound", err)
		}
	})

	t.Run(name+"/ListPartsEnablesResume", func(t *testing.T) {
		// This is the behavior that makes an interrupted upload resumable.
		// If ListParts does not report what was already accepted, the
		// uploader has to re-send the entire segment.
		key := "topics/resume.me/0/00000000000000001000.log"
		partSize := objstore.MinPartSize
		payload := randomBytes(t, partSize*3)

		id, err := st.CreateMultipart(ctx, key, nil)
		if err != nil {
			t.Fatalf("CreateMultipart: %v", err)
		}
		// Upload the first two parts, then pretend the process died.
		var sent []objstore.Part
		for i := 1; i <= 2; i++ {
			chunk := payload[(i-1)*partSize : i*partSize]
			p, err := st.UploadPart(ctx, key, id, i, bytes.NewReader(chunk), int64(len(chunk)))
			if err != nil {
				t.Fatalf("UploadPart %d: %v", i, err)
			}
			sent = append(sent, p)
		}

		listed, err := st.ListParts(ctx, key, id)
		if err != nil {
			t.Fatalf("ListParts: %v", err)
		}
		if len(listed) != 2 {
			t.Fatalf("ListParts returned %d parts, want 2; resume is impossible without this", len(listed))
		}
		for i, p := range listed {
			if p.Number != i+1 {
				t.Fatalf("ListParts[%d].Number = %d, want %d (must be sorted ascending)", i, p.Number, i+1)
			}
			if p.Size != int64(partSize) {
				t.Fatalf("ListParts[%d].Size = %d, want %d", i, p.Size, partSize)
			}
			if p.ETag != sent[i].ETag {
				t.Fatalf("ListParts[%d].ETag = %q, want %q; a resumed upload completes with these", i, p.ETag, sent[i].ETag)
			}
		}

		// Resume: send only the missing part, then complete with the listed
		// parts plus the new one.
		chunk := payload[2*partSize:]
		p3, err := st.UploadPart(ctx, key, id, 3, bytes.NewReader(chunk), int64(len(chunk)))
		if err != nil {
			t.Fatalf("UploadPart 3: %v", err)
		}
		if err := st.CompleteMultipart(ctx, key, id, append(listed, p3)); err != nil {
			t.Fatalf("CompleteMultipart after resume: %v", err)
		}
		rc, err := st.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		got, _ := io.ReadAll(rc)
		rc.Close()
		if !bytes.Equal(got, payload) {
			t.Fatal("resumed upload did not reassemble to the original bytes")
		}
		_ = st.Delete(ctx, key)
	})

	t.Run(name+"/AbortDiscardsParts", func(t *testing.T) {
		key := "topics/abort.me/0/00000000000000002000.log"
		id, err := st.CreateMultipart(ctx, key, nil)
		if err != nil {
			t.Fatalf("CreateMultipart: %v", err)
		}
		chunk := randomBytes(t, objstore.MinPartSize)
		if _, err := st.UploadPart(ctx, key, id, 1, bytes.NewReader(chunk), int64(len(chunk))); err != nil {
			t.Fatalf("UploadPart: %v", err)
		}
		if err := st.AbortMultipart(ctx, key, id); err != nil {
			t.Fatalf("AbortMultipart: %v", err)
		}
		// The object must not exist, and aborting twice must not explode.
		if _, err := st.Stat(ctx, key); !errors.Is(err, objstore.ErrNotFound) {
			t.Fatalf("after abort, Stat = %v, want ErrNotFound", err)
		}
		if err := st.AbortMultipart(ctx, key, id); err != nil {
			t.Fatalf("second AbortMultipart = %v, want nil (must be idempotent)", err)
		}
	})

	t.Run(name+"/RejectsOutOfRangePartNumbers", func(t *testing.T) {
		key := "topics/bad.parts/0/00000000000000003000.log"
		id, err := st.CreateMultipart(ctx, key, nil)
		if err != nil {
			t.Fatalf("CreateMultipart: %v", err)
		}
		defer st.AbortMultipart(ctx, key, id)
		for _, bad := range []int{0, -1, objstore.MaxPartsPerUpload + 1} {
			if _, err := st.UploadPart(ctx, key, id, bad, bytes.NewReader([]byte("x")), 1); err == nil {
				t.Errorf("UploadPart accepted part number %d, which no S3 store allows", bad)
			}
		}
	})

	t.Run(name+"/BinarySafety", func(t *testing.T) {
		// Segments are opaque bytes. A backend that mangles NUL bytes, high
		// bytes, or anything resembling a text encoding would silently
		// corrupt every record batch.
		key := "topics/binary.safety/0/00000000000000004000.log"
		payload := make([]byte, 256)
		for i := range payload {
			payload[i] = byte(i)
		}
		id, err := st.CreateMultipart(ctx, key, nil)
		if err != nil {
			t.Fatalf("CreateMultipart: %v", err)
		}
		p, err := st.UploadPart(ctx, key, id, 1, bytes.NewReader(payload), int64(len(payload)))
		if err != nil {
			t.Fatalf("UploadPart: %v", err)
		}
		if err := st.CompleteMultipart(ctx, key, id, []objstore.Part{p}); err != nil {
			t.Fatalf("CompleteMultipart: %v", err)
		}
		rc, err := st.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		got, _ := io.ReadAll(rc)
		rc.Close()
		if !bytes.Equal(got, payload) {
			t.Fatal("every byte value 0..255 must survive a round trip unchanged")
		}
		_ = st.Delete(ctx, key)
	})
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func TestFSConformance(t *testing.T) {
	st, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	runConformance(t, "fs", st)
}

func TestFSRejectsPathEscape(t *testing.T) {
	st, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// Topic names come from clients, and they end up in archive keys.
	for _, key := range []string{"../escaped.log", "a/../../escaped.log", "..\\escaped.log"} {
		if _, err := st.Stat(context.Background(), key); err == nil {
			t.Errorf("Stat(%q) should refuse to leave the archive root", key)
		} else if errors.Is(err, objstore.ErrNotFound) {
			// Cleaned to a path inside the root and legitimately absent.
			continue
		}
	}
}

func TestFSRefusesUnwritableRoot(t *testing.T) {
	if _, err := objstore.NewFS(""); err == nil {
		t.Fatal("an empty archive.fs.path must be refused at construction")
	}
}

// TestS3Conformance runs the identical suite against a real S3-compatible
// server. It is skipped unless KAFKA_WIRE_TEST_S3_ENDPOINT is set, so the
// default `go test ./...` stays hermetic. The CI e2e job and the local
// docker-compose test profile both set it.
//
//	docker run -d -p 9000:9000 -e MINIO_ROOT_USER=minioadmin \
//	  -e MINIO_ROOT_PASSWORD=minioadmin quay.io/minio/minio server /data
//	KAFKA_WIRE_TEST_S3_ENDPOINT=http://127.0.0.1:9000 go test ./internal/objstore/
func TestS3Conformance(t *testing.T) {
	endpoint := os.Getenv("KAFKA_WIRE_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set KAFKA_WIRE_TEST_S3_ENDPOINT to run the S3 conformance suite against a real server")
	}
	cfg := objstore.S3Config{
		Bucket:     envOr("KAFKA_WIRE_TEST_S3_BUCKET", "kafka-wire-test"),
		Endpoint:   endpoint,
		Region:     envOr("KAFKA_WIRE_TEST_S3_REGION", "us-east-1"),
		Addressing: envOr("KAFKA_WIRE_TEST_S3_ADDRESSING", "path"),
		AccessKey:  envOr("KAFKA_WIRE_TEST_S3_ACCESS_KEY", "minioadmin"),
		SecretKey:  envOr("KAFKA_WIRE_TEST_S3_SECRET_KEY", "minioadmin"),
	}
	ensureBucket(t, cfg)
	st, err := objstore.NewS3(context.Background(), cfg)
	if err != nil {
		t.Fatalf("connecting to %s: %v", endpoint, err)
	}
	runConformance(t, fmt.Sprintf("s3(%s)", endpoint), st)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// ensureBucket creates the test bucket if it is missing. NewS3 deliberately
// refuses to create buckets (a broker that invents storage on your behalf is
// a broker that hides a typo in the bucket name), so the test does it.
func ensureBucket(t *testing.T, cfg objstore.S3Config) {
	t.Helper()
	ep := cfg.Endpoint
	secure := true
	if strings.HasPrefix(ep, "http://") {
		ep, secure = strings.TrimPrefix(ep, "http://"), false
	} else if strings.HasPrefix(ep, "https://") {
		ep = strings.TrimPrefix(ep, "https://")
	}
	cl, err := minio.New(ep, &minio.Options{
		Creds:        credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure:       secure,
		Region:       cfg.Region,
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		t.Fatalf("test bucket setup: %v", err)
	}
	ctx := context.Background()
	ok, err := cl.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		t.Fatalf("test bucket setup: %v", err)
	}
	if !ok {
		if err := cl.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{Region: cfg.Region}); err != nil {
			t.Fatalf("test bucket setup: %v", err)
		}
	}
}
