package storage

import (
	"strings"
	"testing"
)

// A batch's attribute bits are not decoration: two of them make a claim about a
// transaction coordinator, and kafka-wire does not have one. These tests pin the
// refusal, because accepting those bits is how a read_committed consumer gets
// handed records from an aborted transaction.

func TestValidateBatchAttributes_RejectsTransactionClaims(t *testing.T) {
	cases := []struct {
		name    string
		attrs   int16
		wantSub string
	}{
		{"transactional", AttrTransactional, "transaction"},
		{"control batch", AttrControlBatch, "control marker"},
		{"both", AttrTransactional | AttrControlBatch, "control marker"},
		// The bits must be rejected in company too: a batch that is also
		// compressed or carries log-append timestamps is still a lie.
		{"transactional+snappy", AttrTransactional | CompressionSnappy, "transaction"},
		{"transactional+logappend", AttrTransactional | AttrTimestampType, "transaction"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := ValidateBatchAttributes(c.attrs)
			if err == nil {
				t.Fatalf("attributes 0x%02x were accepted; this broker has no transaction coordinator to honor them", c.attrs)
			}
			if !strings.Contains(err.Error(), c.wantSub) {
				t.Fatalf("error %q does not name the offending bit (want %q)", err, c.wantSub)
			}
		})
	}
}

func TestValidateBatchAttributes_AcceptsWhatItCanHonor(t *testing.T) {
	cases := []struct {
		name  string
		attrs int16
	}{
		{"plain", 0},
		{"gzip", CompressionGzip},
		{"snappy", CompressionSnappy},
		{"lz4", CompressionLZ4},
		{"zstd", CompressionZstd},
		// Log-append timestamps are a claim the broker could keep but does not
		// make today; that is a separate decision from the transaction bits.
		// This pins that it stays accepted, so the transaction refusal does not
		// quietly grow into a broader one.
		{"logappend timestamp", AttrTimestampType},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := ValidateBatchAttributes(c.attrs); err != nil {
				t.Fatalf("attributes 0x%02x were refused: %v", c.attrs, err)
			}
		})
	}
}

// TestBatchHeaderTransactionAccessors pins the bit tests the refusal is built
// from, since reading the wrong bit would make the validator look correct while
// checking nothing.
func TestBatchHeaderTransactionAccessors(t *testing.T) {
	cases := []struct {
		attrs   int16
		txn     bool
		control bool
		codec   int8
		what    string
	}{
		{0x0000, false, false, CompressionNone, "plain"},
		{AttrTransactional, true, false, CompressionNone, "bit 4"},
		{AttrControlBatch, false, true, CompressionNone, "bit 5"},
		{AttrTransactional | AttrControlBatch, true, true, CompressionNone, "bits 4+5"},
		{CompressionZstd | AttrTransactional, true, false, CompressionZstd, "codec + bit 4"},
	}
	for _, c := range cases {
		h := BatchHeader{Attributes: c.attrs}
		if h.IsTransactional() != c.txn {
			t.Errorf("%s: IsTransactional() = %v, want %v", c.what, h.IsTransactional(), c.txn)
		}
		if h.IsControlBatch() != c.control {
			t.Errorf("%s: IsControlBatch() = %v, want %v", c.what, h.IsControlBatch(), c.control)
		}
		if h.Compression() != c.codec {
			t.Errorf("%s: Compression() = %d, want %d", c.what, h.Compression(), c.codec)
		}
	}
}

// TestValidateBatchForAppend covers the shared pre-flight gate. Its purpose is
// that Log.Append can reject a whole request before writing any of it, so the
// per-partition error code it returns is truthful.
func TestValidateBatchForAppend(t *testing.T) {
	good := makeBatch(t, 0, 1, 0, 0)
	if err := ValidateBatchForAppend(good); err != nil {
		t.Fatalf("a plain batch was refused: %v", err)
	}

	if err := ValidateBatchForAppend(good[:MinBatchSize-1]); err == nil {
		t.Error("a batch below the minimum size was accepted")
	}
	if err := ValidateBatchForAppend(make([]byte, MaxBatchSize+1)); err == nil {
		t.Error("a batch above the maximum size was accepted")
	}

	// A corrupt body under an intact header: the CRC must catch it.
	corrupt := append([]byte(nil), good...)
	corrupt[v2BodyStart+1] ^= 0xff
	if err := ValidateBatchForAppend(corrupt); err == nil {
		t.Error("a batch with a broken CRC was accepted")
	}

	// The transaction bits, through the same gate.
	for _, attrs := range []int16{AttrTransactional, AttrControlBatch} {
		err := ValidateBatchForAppend(makeBatch(t, 0, 1, attrs, 0))
		if err == nil {
			t.Fatalf("attributes 0x%02x passed the append gate", attrs)
		}
		if !strings.Contains(err.Error(), "transaction") {
			t.Errorf("error %q does not name the transaction claim", err)
		}
	}
}

// TestLogAppendRejectsWholeRequestBeforeWriting is the property that matters at
// the wire layer: a Produce response carries one error code per partition, so a
// client that reads a failure there believes nothing from the request landed. If
// an earlier batch of the same request had been written, a retry would duplicate
// it. The transaction bits are the newest reason a batch can be refused, so this
// pins that the refusal is all-or-nothing.
func TestLogAppendRejectsWholeRequestBeforeWriting(t *testing.T) {
	s, err := Open(Config{DataDir: t.TempDir(), SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	l, err := s.OpenLog("txn.atomic", 0)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	t.Cleanup(func() { l.Close() })

	good := makeBatch(t, 0, 1, 0, 0)
	bad := makeBatch(t, 0, 1, AttrTransactional, 0)

	if _, err := l.Append([][]byte{good, bad}); err == nil {
		t.Fatal("a request containing a transactional batch was accepted")
	}

	// Nothing from that request may be on disk.
	if got := l.LogStartOffset(); got != 0 {
		t.Errorf("log start = %d after a refused request, want 0", got)
	}
	var size int64
	for _, seg := range l.AllSegments() {
		size += seg.Size()
	}
	if size != 0 {
		t.Fatalf("a refused request left %d bytes on disk; the per-partition error code a client reads would be a lie", size)
	}

	// And the partition still accepts real writes afterwards.
	if _, err := l.Append([][]byte{good}); err != nil {
		t.Fatalf("a valid batch was refused after the failed request: %v", err)
	}
}
