package storage

import (
	"encoding/binary"
	"testing"
)

// Hand-built valid v2 record batch covering offsets 100..102 (3 records).
// We don't actually decode the records, only the header, so the
// tail bytes can be anything as long as the CRC matches.
func makeBatch(t *testing.T, baseOffset int64, recordCount int, attributes int16, ts int64) []byte {
	t.Helper()
	recordsTail := []byte{0x01, 0x02, 0x03, 0x04}         // opaque per-test
	totalBatchLen := v2HeaderSize - 12 + len(recordsTail) // post-BatchLength bytes
	buf := make([]byte, 12+totalBatchLen)
	// BaseOffset (8)
	binary.BigEndian.PutUint64(buf[0:8], uint64(baseOffset))
	// BatchLength (4), bytes after this field
	binary.BigEndian.PutUint32(buf[8:12], uint32(totalBatchLen))
	// PartitionLeaderEpoch (4)
	binary.BigEndian.PutUint32(buf[12:16], 0)
	// Magic (1)
	buf[16] = v2MagicByte
	// CRC (4), placeholder, computed below
	// Attributes (2)
	binary.BigEndian.PutUint16(buf[21:23], uint16(attributes))
	// LastOffsetDelta (4)
	binary.BigEndian.PutUint32(buf[23:27], uint32(recordCount-1))
	// FirstTimestamp (8)
	binary.BigEndian.PutUint64(buf[27:35], uint64(ts))
	// MaxTimestamp (8)
	binary.BigEndian.PutUint64(buf[35:43], uint64(ts+10))
	// ProducerID (8)
	binary.BigEndian.PutUint64(buf[43:51], ^uint64(0)) // -1 (unset)
	// ProducerEpoch (2)
	binary.BigEndian.PutUint16(buf[51:53], 0xFFFF) // unset
	// BaseSequence (4)
	binary.BigEndian.PutUint32(buf[53:57], 0xFFFFFFFF) // unset
	// RecordCount (4)
	binary.BigEndian.PutUint32(buf[57:61], uint32(recordCount))
	// Opaque records bytes
	copy(buf[61:], recordsTail)
	// CRC over body (Attributes onward).
	crc := CRC32C(buf[v2BodyStart:])
	binary.BigEndian.PutUint32(buf[v2CRCOffset:v2CRCOffset+v2CRCSize], crc)
	return buf
}

func TestParseBatchHeader_Valid(t *testing.T) {
	batch := makeBatch(t, 100, 3, 0, 1714234567000)
	h, err := ParseBatchHeader(batch)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if h.BaseOffset != 100 {
		t.Errorf("BaseOffset = %d, want 100", h.BaseOffset)
	}
	if h.LastOffsetDelta != 2 {
		t.Errorf("LastOffsetDelta = %d, want 2", h.LastOffsetDelta)
	}
	if h.LastOffset() != 102 {
		t.Errorf("LastOffset = %d, want 102", h.LastOffset())
	}
	if h.RecordCount != 3 {
		t.Errorf("RecordCount = %d, want 3", h.RecordCount)
	}
	if h.Magic != v2MagicByte {
		t.Errorf("Magic = %d, want %d", h.Magic, v2MagicByte)
	}
}

func TestParseBatchHeader_TooShort(t *testing.T) {
	_, err := ParseBatchHeader(make([]byte, 30))
	if err == nil {
		t.Fatal("expected error for short input")
	}
}

func TestParseBatchHeader_BadMagic(t *testing.T) {
	batch := makeBatch(t, 0, 1, 0, 0)
	batch[16] = 99 // corrupt magic
	_, err := ParseBatchHeader(batch)
	if err == nil {
		t.Fatal("expected error for bad magic byte")
	}
}

func TestValidateCRC_RoundTrip(t *testing.T) {
	batch := makeBatch(t, 0, 1, 0, 0)
	if err := ValidateCRC(batch); err != nil {
		t.Fatalf("CRC validation failed on freshly-built batch: %v", err)
	}
}

func TestValidateCRC_Corrupted(t *testing.T) {
	batch := makeBatch(t, 0, 1, 0, 0)
	// Corrupt one byte in the body.
	batch[v2BodyStart+5] ^= 0x01
	if err := ValidateCRC(batch); err == nil {
		t.Fatal("expected CRC mismatch after corrupting body")
	}
}

// Critical: rewriting BaseOffset must NOT invalidate the CRC.
// The whole storage engine is built on this Kafka design property.
func TestRewriteBaseOffset_PreservesCRC(t *testing.T) {
	batch := makeBatch(t, 0, 1, 0, 0)
	if err := ValidateCRC(batch); err != nil {
		t.Fatalf("pre-rewrite CRC should be valid: %v", err)
	}
	RewriteBaseOffset(batch, 9_999_999)
	if err := ValidateCRC(batch); err != nil {
		t.Fatalf("post-rewrite CRC should STILL be valid (Kafka design): %v", err)
	}
	h, _ := ParseBatchHeader(batch)
	if h.BaseOffset != 9_999_999 {
		t.Fatalf("BaseOffset rewrite didn't take: got %d", h.BaseOffset)
	}
}

func TestSetPartitionLeaderEpoch_PreservesCRC(t *testing.T) {
	batch := makeBatch(t, 0, 1, 0, 0)
	SetPartitionLeaderEpoch(batch, 42)
	if err := ValidateCRC(batch); err != nil {
		t.Fatalf("epoch rewrite should not invalidate CRC: %v", err)
	}
}

func TestValidateCompressionCodec_AllValidCodecs(t *testing.T) {
	for codec, name := range map[int8]string{
		CompressionNone:   "none",
		CompressionGzip:   "gzip",
		CompressionSnappy: "snappy",
		CompressionLZ4:    "lz4",
		CompressionZstd:   "zstd",
	} {
		attrs := int16(codec) & AttrCompressionMask
		if err := ValidateCompressionCodec(attrs); err != nil {
			t.Errorf("codec %s (%d) should be accepted: %v", name, codec, err)
		}
	}
}

func TestValidateCompressionCodec_Invalid(t *testing.T) {
	if err := ValidateCompressionCodec(int16(7)); err == nil {
		t.Fatal("codec 7 (unknown) should be rejected")
	}
}

func TestBatchHeader_Compression(t *testing.T) {
	// Set the gzip codec bit.
	batch := makeBatch(t, 0, 1, int16(CompressionGzip), 0)
	h, err := ParseBatchHeader(batch)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if h.Compression() != CompressionGzip {
		t.Fatalf("Compression() = %d, want %d", h.Compression(), CompressionGzip)
	}
}

func TestBatchHeader_TotalSize(t *testing.T) {
	batch := makeBatch(t, 0, 1, 0, 0)
	h, _ := ParseBatchHeader(batch)
	if h.TotalSize() != len(batch) {
		t.Fatalf("TotalSize = %d, want len(batch)=%d", h.TotalSize(), len(batch))
	}
}
