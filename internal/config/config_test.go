package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Every `def` tag must parse. This is the test that lets Default() panic
// instead of returning an error, and it is why the tag can be the single
// source of truth for the docs and the runtime default at once.
func TestDefaultsAllParse(t *testing.T) {
	c := Default()
	if c.Listeners.Kafka != "127.0.0.1:9092" {
		t.Fatalf("listeners.kafka default = %q", c.Listeners.Kafka)
	}
	if c.Storage.SegmentBytes != 1<<30 {
		t.Fatalf("segmentbytes default = %d, want %d", c.Storage.SegmentBytes, int64(1<<30))
	}
	if c.Storage.SegmentAge != 168*time.Hour {
		t.Fatalf("segmentage default = %v", c.Storage.SegmentAge)
	}
	if c.Archive.Backend != "none" {
		t.Fatalf("archive.backend default = %q, want none", c.Archive.Backend)
	}
	// The default configuration must itself be valid, or a first-time user
	// cannot start the broker with no config at all.
	l := &Loaded{Config: c}
	if err := l.Validate(); err != nil {
		t.Fatalf("the compiled-in defaults do not validate: %v", err)
	}
}

// The env mapping has to be reversible, which is the property that lets
// `config print --format env` be lossless and keeps us out of the escaping
// ladders other projects needed.
func TestEnvNamesAreUnambiguous(t *testing.T) {
	seen := map[string]string{}
	for _, f := range Fields() {
		if prev, dup := seen[f.Env]; dup {
			t.Errorf("env var %s is produced by both %q and %q", f.Env, prev, f.Path)
		}
		seen[f.Env] = f.Path
		for _, seg := range strings.Split(f.Path, ".") {
			if strings.ContainsAny(seg, "_-") {
				t.Errorf("key path segment %q in %q contains _ or -, which breaks the reversible env mapping", seg, f.Path)
			}
		}
	}
	if len(seen) == 0 {
		t.Fatal("no fields discovered")
	}
}

// Every field must be documented. A setting nobody can find is a setting
// that generates a support issue.
func TestEveryFieldIsDocumented(t *testing.T) {
	for _, f := range Fields() {
		if strings.TrimSpace(f.Doc) == "" {
			t.Errorf("%s has no doc tag", f.Path)
		}
	}
}

func TestParseSize(t *testing.T) {
	cases := map[string]int64{
		"0": 0, "1024": 1024, "-1": -1,
		"1KiB": 1 << 10, "1MiB": 1 << 20, "1GiB": 1 << 30, "1TiB": 1 << 40,
		"512MB": 512 << 20, "2G": 2 << 30, "8m": 8 << 20, "1b": 1,
		"1.5MiB": 1572864,
	}
	for in, want := range cases {
		got, err := ParseSize(in)
		if err != nil {
			t.Errorf("ParseSize(%q): %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("ParseSize(%q) = %d, want %d", in, got, want)
		}
	}
	if _, err := ParseSize("banana"); err == nil {
		t.Error("ParseSize(banana) should fail")
	}
}

// A bare integer means milliseconds, because Kafka's own settings are named
// in milliseconds and users copy values across without converting.
func TestParseDurationAcceptsBareMilliseconds(t *testing.T) {
	d, err := ParseDuration("604800000")
	if err != nil {
		t.Fatal(err)
	}
	if d != 168*time.Hour {
		t.Fatalf("604800000 = %v, want 168h", d)
	}
	if d, err = ParseDuration("30s"); err != nil || d != 30*time.Second {
		t.Fatalf("30s = %v, %v", d, err)
	}
	if _, err := ParseDuration("soon"); err == nil {
		t.Error("ParseDuration(soon) should fail")
	}
}

func env(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func TestEnvOverridesFile(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "kafka-wire.yaml")
	os.WriteFile(cfgPath, []byte("listeners:\n  kafka: \"127.0.0.1:1111\"\nstorage:\n  segmentbytes: 512MiB\n"), 0o644)

	l, err := Load(Options{
		File:      cfgPath,
		LookupEnv: env(map[string]string{"KAFKA_WIRE_LISTENERS_KAFKA": "127.0.0.1:2222"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if l.Config.Listeners.Kafka != "127.0.0.1:2222" {
		t.Errorf("env should beat file, got %q", l.Config.Listeners.Kafka)
	}
	if l.Config.Storage.SegmentBytes != 512<<20 {
		t.Errorf("file value not applied: %d", l.Config.Storage.SegmentBytes)
	}
	if l.Sources["listeners.kafka"] != "env:KAFKA_WIRE_LISTENERS_KAFKA" {
		t.Errorf("provenance wrong: %q", l.Sources["listeners.kafka"])
	}
	if !strings.HasPrefix(l.Sources["storage.segmentbytes"], "file:") {
		t.Errorf("provenance wrong: %q", l.Sources["storage.segmentbytes"])
	}
	if l.Sources["cluster.id"] != "default" {
		t.Errorf("untouched key should read as default, got %q", l.Sources["cluster.id"])
	}
}

// Secrets arrive as mounted files in every orchestrator worth using, and
// those files almost always end in a newline that is not part of the secret.
func TestFileIndirectionTrimsTrailingNewline(t *testing.T) {
	dir := t.TempDir()
	secret := filepath.Join(dir, "token")
	os.WriteFile(secret, []byte("s3cr3t-value\n"), 0o600)

	l, err := Load(Options{
		File:      filepath.Join(dir, "absent.yaml"),
		LookupEnv: env(map[string]string{"KAFKA_WIRE_ADMIN_TOKEN_FILE": secret}),
		ReadFile:  os.ReadFile,
	})
	if err == nil {
		// An explicit --config that does not exist must fail; assert that
		// separately below. Reuse a valid path here.
		t.Fatal("expected explicit missing config file to be an error")
	}

	l, err = Load(Options{
		LookupEnv: env(map[string]string{"KAFKA_WIRE_ADMIN_TOKEN_FILE": secret}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if l.Config.Admin.Token != "s3cr3t-value" {
		t.Errorf("token = %q, want the value without its trailing newline", l.Config.Admin.Token)
	}
	if l.Sources["admin.token"] != "file-env:KAFKA_WIRE_ADMIN_TOKEN_FILE" {
		t.Errorf("provenance wrong: %q", l.Sources["admin.token"])
	}
}

func TestBothEnvFormsIsAnError(t *testing.T) {
	_, err := Load(Options{LookupEnv: env(map[string]string{
		"KAFKA_WIRE_ADMIN_TOKEN":      "inline",
		"KAFKA_WIRE_ADMIN_TOKEN_FILE": "/some/path",
	})})
	if err == nil || !strings.Contains(err.Error(), "pick one") {
		t.Fatalf("want a both-set error, got %v", err)
	}
}

func TestUnknownKeySuggests(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "c.yaml")
	os.WriteFile(p, []byte("archive:\n  s3:\n    buckett: nope\n"), 0o644)
	_, err := Load(Options{File: p, LookupEnv: env(nil)})
	if err == nil {
		t.Fatal("unknown key should fail")
	}
	if !strings.Contains(err.Error(), "archive.s3.bucket") {
		t.Fatalf("want a suggestion naming archive.s3.bucket, got: %v", err)
	}
}

// A typo in a value must not be silently swallowed. The original code
// returned the default on a parse error, which made a typo invisible.
func TestBadValueIsAnErrorNotADefault(t *testing.T) {
	_, err := Load(Options{LookupEnv: env(map[string]string{
		"KAFKA_WIRE_STORAGE_SEGMENTBYTES": "1GBB",
	})})
	if err == nil {
		t.Fatal("a malformed size must fail loudly, not fall back to the default")
	}
	if !strings.Contains(err.Error(), "KAFKA_WIRE_STORAGE_SEGMENTBYTES") {
		t.Fatalf("the error must name the variable at fault, got: %v", err)
	}
}

func TestOpenBindWithoutAuthIsRefused(t *testing.T) {
	_, err := Load(Options{LookupEnv: env(map[string]string{
		"KAFKA_WIRE_LISTENERS_KAFKA": "0.0.0.0:9092",
	})})
	if err == nil {
		t.Fatal("binding to 0.0.0.0 with no auth must be refused by default")
	}
	for _, want := range []string{"auth.saslenabled", "auth.allowanon", "127.0.0.1:9092"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal must tell the user how to fix it; missing %q in:\n%v", want, err)
		}
	}
	// And the escape hatch must work, because plenty of people really are on
	// a private network.
	if _, err := Load(Options{LookupEnv: env(map[string]string{
		"KAFKA_WIRE_LISTENERS_KAFKA": "0.0.0.0:9092",
		"KAFKA_WIRE_AUTH_ALLOWANON":  "true",
	})}); err != nil {
		t.Fatalf("allowanon should permit an open bind: %v", err)
	}
}

func TestPartCountCeilingIsCaughtBeforeUpload(t *testing.T) {
	_, err := Load(Options{LookupEnv: env(map[string]string{
		"KAFKA_WIRE_ARCHIVE_BACKEND":      "s3",
		"KAFKA_WIRE_ARCHIVE_S3_BUCKET":    "b",
		"KAFKA_WIRE_ARCHIVE_S3_PARTSIZE":  "5MiB",
		"KAFKA_WIRE_STORAGE_SEGMENTBYTES": "500GiB",
		"KAFKA_WIRE_AUTH_ALLOWANON":       "true",
	})})
	if err == nil {
		t.Fatal("a segment/part ratio above 10000 must be refused at startup")
	}
	if !strings.Contains(err.Error(), "10000") {
		t.Fatalf("error should explain the 10000-part limit: %v", err)
	}
}

func TestTLSMustBeAllOrNothing(t *testing.T) {
	_, err := Load(Options{LookupEnv: env(map[string]string{
		"KAFKA_WIRE_TLS_CERTFILE": "/tmp/x.pem",
	})})
	if err == nil || !strings.Contains(err.Error(), "silently serve plaintext") {
		t.Fatalf("half-configured TLS must be refused, got %v", err)
	}
}

func TestArchiveRetentionOrdering(t *testing.T) {
	_, err := Load(Options{LookupEnv: env(map[string]string{
		"KAFKA_WIRE_ARCHIVE_BACKEND":        "fs",
		"KAFKA_WIRE_ARCHIVE_FS_PATH":        "/tmp/arch",
		"KAFKA_WIRE_ARCHIVE_AGE":            "6h",
		"KAFKA_WIRE_ARCHIVE_LOCALRETENTION": "1h",
	})})
	if err == nil || !strings.Contains(err.Error(), "losing data") {
		t.Fatalf("localretention below archive age loses data and must be refused, got %v", err)
	}
}

func TestArchiveBackendCoherence(t *testing.T) {
	if _, err := Load(Options{LookupEnv: env(map[string]string{
		"KAFKA_WIRE_ARCHIVE_BACKEND": "s3",
	})}); err == nil {
		t.Error("archive.backend=s3 without a bucket must fail")
	}
	if _, err := Load(Options{LookupEnv: env(map[string]string{
		"KAFKA_WIRE_ARCHIVE_BACKEND": "gcs",
	})}); err == nil {
		t.Error("an unknown archive backend must fail")
	}
}
