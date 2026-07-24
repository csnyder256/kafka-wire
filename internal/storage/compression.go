package storage

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
)

// Compression codec validation + decompression.
//
// The broker doesn't need to decompress to PERSIST batches, record
// batches are stored verbatim and shipped to consumers verbatim. The
// only places we'd need to decode the inner records are:
//
//   1. Content-aware filtering (not in scope; would only matter if we
//      add server-side filtering hooks).
//   2. Per-record timestamp inspection (we use batch.MaxTimestamp
//      from the UNcompressed header, so this is moot in practice).
//
// What we DO need to validate is the codec ID at Produce time,
// otherwise a producer can claim a non-existent codec (e.g. ID 7)
// and consumers would see corrupt data with a misleading error.

// ValidateCompressionCodec checks the codec ID in a batch's
// Attributes is one we support (or "none"). Producers using an
// unknown codec are rejected at Produce time with CorruptMessage.
func ValidateCompressionCodec(attributes int16) error {
	codec := int8(attributes & AttrCompressionMask)
	switch codec {
	case CompressionNone, CompressionGzip, CompressionSnappy, CompressionZstd:
		return nil
	case CompressionLZ4:
		// LZ4 is in the protocol enumeration but this
		// some clients doesn't ship LZ4 by default. Accept it
		// since we pass-through bytes; a future version may reject it if
		// we discover unexpected behavior.
		return nil
	}
	return fmt.Errorf("unsupported compression codec %d", codec)
}

// Decompress unpacks a compressed records blob from inside a v2
// batch. Provided for completeness; nothing calls it today.
//
// `compressed` is the batch body AFTER the v2 header. The caller is
// responsible for slicing past the 61-byte header.
func Decompress(codec int8, compressed []byte) ([]byte, error) {
	switch codec {
	case CompressionNone:
		return compressed, nil
	case CompressionGzip:
		gz, err := gzip.NewReader(bytes.NewReader(compressed))
		if err != nil {
			return nil, fmt.Errorf("gzip new reader: %w", err)
		}
		defer func() { _ = gz.Close() }()
		out, err := io.ReadAll(gz)
		if err != nil {
			return nil, fmt.Errorf("gzip read: %w", err)
		}
		return out, nil
	case CompressionSnappy:
		out, err := snappy.Decode(nil, compressed)
		if err != nil {
			return nil, fmt.Errorf("snappy decode: %w", err)
		}
		return out, nil
	case CompressionZstd:
		dec, err := zstd.NewReader(nil)
		if err != nil {
			return nil, fmt.Errorf("zstd new decoder: %w", err)
		}
		defer dec.Close()
		out, err := dec.DecodeAll(compressed, nil)
		if err != nil {
			return nil, fmt.Errorf("zstd decode: %w", err)
		}
		return out, nil
	case CompressionLZ4:
		return nil, errors.New("LZ4 decompression not implemented (not needed while batches are stored verbatim)")
	}
	return nil, fmt.Errorf("unknown codec %d", codec)
}
