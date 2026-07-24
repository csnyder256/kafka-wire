package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Kafka v2 record batch format. We never decode individual records;
// we only parse the batch HEADER so we know offsets, length, and CRC
// for indexing and validation. Bytes flow through to clients verbatim
// via sendfile(2).
//
// Layout (Kafka v2):
//   BaseOffset            int64    (8)
//   BatchLength           int32    (4), bytes after this field
//   PartitionLeaderEpoch  int32    (4)
//   Magic                 int8     (1), must be 2
//   CRC                   int32    (4), CRC32C over body after this field
//   Attributes            int16    (2)
//   LastOffsetDelta       int32    (4)
//   FirstTimestamp        int64    (8)
//   MaxTimestamp          int64    (8)
//   ProducerID            int64    (8)
//   ProducerEpoch         int16    (2)
//   BaseSequence          int32    (4)
//   RecordCount           int32    (4)
//   Records               varies: opaque
//
// Total fixed header before Records = 61 bytes.

const (
	v2HeaderSize    = 61
	v2MagicByte     = 2
	v2BatchLenStart = 8 // bytes into the on-disk record where batch_length is
	v2BatchLenSize  = 4
	v2MagicOffset   = 16 // BaseOffset(8) + BatchLength(4) + PartitionLeaderEpoch(4)
	v2CRCOffset     = 17
	v2CRCSize       = 4
	v2BodyStart     = 21 // first byte covered by CRC (Attributes onward)
)

// MinBatchSize is the smallest legitimate v2 batch on the wire (header
// only, zero records). We refuse anything smaller.
const MinBatchSize = v2HeaderSize

// MaxBatchSize is the conservative cap we enforce regardless of
// client-side max.message.bytes setting. a future version may make this
// configurable; for now 4MiB matches Kafka's default and our DoS
// guard in the wire layer.
const MaxBatchSize = 4 * 1024 * 1024

// BatchHeader is the parsed header fields we care about.
type BatchHeader struct {
	BaseOffset      int64
	BatchLength     int32 // bytes after BatchLength itself
	Magic           int8
	CRC             uint32
	Attributes      int16
	LastOffsetDelta int32
	FirstTimestamp  int64
	MaxTimestamp    int64
	ProducerID      int64
	ProducerEpoch   int16
	BaseSequence    int32
	RecordCount     int32
}

// LastOffset is the highest offset contained in this batch.
func (h BatchHeader) LastOffset() int64 {
	return h.BaseOffset + int64(h.LastOffsetDelta)
}

// TotalSize is the on-disk size of the batch, including BaseOffset
// and BatchLength prefix.
func (h BatchHeader) TotalSize() int {
	return 12 + int(h.BatchLength)
}

// Compression bits live in the bottom 3 bits of Attributes.
const (
	AttrCompressionMask = 0x07
	CompressionNone     = 0
	CompressionGzip     = 1
	CompressionSnappy   = 2
	CompressionLZ4      = 3
	CompressionZstd     = 4
)

// Compression returns the codec used by this batch.
func (h BatchHeader) Compression() int8 {
	return int8(h.Attributes & AttrCompressionMask)
}

// ParseBatchHeader decodes the v2 header from buf. buf must contain
// at least v2HeaderSize bytes; len > v2HeaderSize is fine.
func ParseBatchHeader(buf []byte) (BatchHeader, error) {
	if len(buf) < v2HeaderSize {
		return BatchHeader{}, fmt.Errorf("batch header: need %d bytes, have %d", v2HeaderSize, len(buf))
	}
	h := BatchHeader{
		BaseOffset:      int64(binary.BigEndian.Uint64(buf[0:8])),
		BatchLength:     int32(binary.BigEndian.Uint32(buf[8:12])),
		Magic:           int8(buf[16]),
		CRC:             binary.BigEndian.Uint32(buf[17:21]),
		Attributes:      int16(binary.BigEndian.Uint16(buf[21:23])),
		LastOffsetDelta: int32(binary.BigEndian.Uint32(buf[23:27])),
		FirstTimestamp:  int64(binary.BigEndian.Uint64(buf[27:35])),
		MaxTimestamp:    int64(binary.BigEndian.Uint64(buf[35:43])),
		ProducerID:      int64(binary.BigEndian.Uint64(buf[43:51])),
		ProducerEpoch:   int16(binary.BigEndian.Uint16(buf[51:53])),
		BaseSequence:    int32(binary.BigEndian.Uint32(buf[53:57])),
		RecordCount:     int32(binary.BigEndian.Uint32(buf[57:61])),
	}
	if h.Magic != v2MagicByte {
		return h, fmt.Errorf("unsupported magic byte %d (want %d)", h.Magic, v2MagicByte)
	}
	if h.BatchLength < int32(v2HeaderSize-12) {
		return h, fmt.Errorf("batch length %d too small", h.BatchLength)
	}
	if h.BatchLength > MaxBatchSize {
		return h, fmt.Errorf("batch length %d exceeds max %d", h.BatchLength, MaxBatchSize)
	}
	return h, nil
}

// ValidateCRC computes CRC32C over the body bytes (Attributes onward,
// which is offset v2BodyStart) and compares with the header's CRC.
//
// `batch` is the entire on-disk batch starting at BaseOffset.
func ValidateCRC(batch []byte) error {
	if len(batch) < v2HeaderSize {
		return errors.New("batch too short to CRC-check")
	}
	wantCRC := binary.BigEndian.Uint32(batch[v2CRCOffset : v2CRCOffset+v2CRCSize])
	gotCRC := CRC32C(batch[v2BodyStart:])
	if gotCRC != wantCRC {
		return fmt.Errorf("CRC mismatch: got %x want %x", gotCRC, wantCRC)
	}
	return nil
}

// RewriteBaseOffset patches BaseOffset in an in-memory batch buffer.
// Producers send batches with BaseOffset=0 (relative); we rewrite to
// the absolute partition offset before persistence.
//
// CRITICAL: rewriting BaseOffset does NOT invalidate the CRC, because
// the CRC is computed over Attributes-onward, it does not cover the
// fields before Attributes (BaseOffset, BatchLength, PartitionLeaderEpoch,
// Magic, CRC itself). This is by Kafka design specifically so brokers
// can assign offsets without recomputing CRCs.
func RewriteBaseOffset(batch []byte, baseOffset int64) {
	binary.BigEndian.PutUint64(batch[0:8], uint64(baseOffset))
}

// SetPartitionLeaderEpoch patches the leader-epoch field. Single-node
// brokers always use 0; this exists for forward compatibility.
func SetPartitionLeaderEpoch(batch []byte, epoch int32) {
	binary.BigEndian.PutUint32(batch[12:16], uint32(epoch))
}
