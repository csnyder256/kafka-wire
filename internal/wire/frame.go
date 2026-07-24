package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// Kafka request frame on the wire:
//
//   [int32 size]              total size of the rest of the frame
//   [int16 api_key]
//   [int16 api_version]
//   [int32 correlation_id]
//   [string client_id]        nullable string (int16 length, then UTF-8)
//   [...]                     request-specific body
//
// Response frame:
//
//   [int32 size]
//   [int32 correlation_id]
//   [...]                     response body
//
// The client_id and tagged-fields handling for KIP-482 (flexible
// versions) is the responsibility of the kmsg decoders we delegate to;
// frame.go only handles the outermost size prefix and the
// api_key/api_version/correlation_id we need for dispatch routing.

const (
	maxFrameSize = 16 * 1024 * 1024 // 16MB hard cap on a single request
)

// RequestHeader captures the dispatch-relevant fields from the
// request header. The body bytes (everything after the header and
// client_id) are passed to the typed decoder.
type RequestHeader struct {
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      string
}

// readRequest reads one length-prefixed frame from conn. Returns the
// header + the remaining body bytes (after client_id parse).
//
// `maxBytes` is the hard cap from the broker's MaxRequestBytes
// config. We refuse anything larger than that BEFORE allocating the
// body buffer: DoS surface mitigation.
func readRequest(conn net.Conn, maxBytes int32, deadline time.Duration) (RequestHeader, []byte, error) {
	var sizeBuf [4]byte

	// Reset deadline per request.
	if deadline > 0 {
		_ = conn.SetReadDeadline(time.Now().Add(deadline))
	}

	if _, err := io.ReadFull(conn, sizeBuf[:]); err != nil {
		return RequestHeader{}, nil, err
	}
	size := int32(binary.BigEndian.Uint32(sizeBuf[:]))
	if size <= 0 {
		return RequestHeader{}, nil, fmt.Errorf("invalid frame size %d", size)
	}
	if size > maxBytes {
		return RequestHeader{}, nil, fmt.Errorf("frame size %d exceeds max %d", size, maxBytes)
	}
	if size > maxFrameSize {
		return RequestHeader{}, nil, fmt.Errorf("frame size %d exceeds hard cap %d", size, maxFrameSize)
	}

	frame := make([]byte, size)
	if _, err := io.ReadFull(conn, frame); err != nil {
		return RequestHeader{}, nil, err
	}

	if len(frame) < 8 {
		return RequestHeader{}, nil, errors.New("frame shorter than fixed header")
	}

	hdr := RequestHeader{
		APIKey:        int16(binary.BigEndian.Uint16(frame[0:2])),
		APIVersion:    int16(binary.BigEndian.Uint16(frame[2:4])),
		CorrelationID: int32(binary.BigEndian.Uint32(frame[4:8])),
	}

	// Parse client_id (nullable string). Format: int16 length; -1
	// means null.
	if len(frame) < 10 {
		return RequestHeader{}, nil, errors.New("frame too short for client_id length")
	}
	clientIDLen := int16(binary.BigEndian.Uint16(frame[8:10]))
	bodyStart := 10
	if clientIDLen >= 0 {
		end := bodyStart + int(clientIDLen)
		if end > len(frame) {
			return RequestHeader{}, nil, errors.New("frame too short for client_id bytes")
		}
		hdr.ClientID = string(frame[bodyStart:end])
		bodyStart = end
	}

	// Flexible versions (KIP-482) put a tagged-fields section after
	// client_id. It belongs to the REQUEST HEADER, not to the body, and the
	// kmsg body decoders do not consume it. Leaving it in place shifts the
	// body by at least one byte and the decode fails with a message about
	// the request not containing enough data.
	//
	// This is easy to miss because clients that negotiate down to
	// pre-flexible versions never send it, so a broker can look completely
	// healthy against one client library and refuse every request from
	// another.
	if isFlexibleRequest(hdr.APIKey, hdr.APIVersion) {
		n, err := skipTaggedFields(frame[bodyStart:])
		if err != nil {
			return RequestHeader{}, nil, fmt.Errorf("api key %d v%d: %w", hdr.APIKey, hdr.APIVersion, err)
		}
		bodyStart += n
	}

	body := frame[bodyStart:]
	return hdr, body, nil
}

// isFlexibleRequest reports whether this API and version use the flexible
// request header. kmsg owns the per-API version table, so this asks it rather
// than duplicating a list that would rot on the next protocol revision.
func isFlexibleRequest(apiKey, apiVersion int16) bool {
	req := kmsg.RequestForKey(apiKey)
	if req == nil {
		return false
	}
	req.SetVersion(apiVersion)
	return req.IsFlexible()
}

// skipTaggedFields consumes a KIP-482 tagged-fields section and reports how
// many bytes it occupied. The section is an unsigned varint count followed by
// that many (tag, length, bytes) triples. Nearly always the count is zero and
// this is a single 0x00 byte, but a client is entitled to send real tags and
// a broker that assumes one byte would desynchronize the stream.
func skipTaggedFields(b []byte) (int, error) {
	count, n, err := uvarint(b)
	if err != nil {
		return 0, fmt.Errorf("tagged fields: %w", err)
	}
	off := n
	for i := uint64(0); i < count; i++ {
		_, n, err := uvarint(b[off:]) // tag
		if err != nil {
			return 0, fmt.Errorf("tagged field %d tag: %w", i, err)
		}
		off += n
		size, n, err := uvarint(b[off:])
		if err != nil {
			return 0, fmt.Errorf("tagged field %d size: %w", i, err)
		}
		off += n
		if uint64(off)+size > uint64(len(b)) {
			return 0, errors.New("tagged field runs past the end of the frame")
		}
		off += int(size)
	}
	return off, nil
}

// uvarint decodes an unsigned varint, refusing the over-long encodings a
// hostile client could use to force an oversized allocation downstream.
func uvarint(b []byte) (uint64, int, error) {
	var x uint64
	var s uint
	for i := 0; i < len(b); i++ {
		c := b[i]
		if i == 9 && c > 1 {
			return 0, 0, errors.New("varint overflows 64 bits")
		}
		if c < 0x80 {
			return x | uint64(c)<<s, i + 1, nil
		}
		x |= uint64(c&0x7f) << s
		s += 7
	}
	return 0, 0, errors.New("truncated varint")
}

// writeResponse writes a response frame. `correlationID` echoes the
// request's. `body` is the encoded response (already includes any
// flexible-version header tag bytes).
func writeResponse(conn net.Conn, correlationID int32, body []byte, flexibleHeader bool, deadline time.Duration) error {
	if deadline > 0 {
		_ = conn.SetWriteDeadline(time.Now().Add(deadline))
	}

	// Response header: [int32 correlation_id] [opt: tagged-fields byte for flexible].
	// For flexible response headers we add a single 0x00 (empty
	// tagged-fields varint) after the correlation ID. ApiVersions
	// response is a special case: even flexible-versions of
	// ApiVersions use the v0 response header (no tagged fields). The
	// caller passes flexibleHeader=false for that.
	var headerSize int = 4
	if flexibleHeader {
		headerSize = 5
	}
	totalSize := int32(headerSize + len(body))

	// Single contiguous write so we don't tear the frame.
	out := make([]byte, 4+headerSize+len(body))
	binary.BigEndian.PutUint32(out[0:4], uint32(totalSize))
	binary.BigEndian.PutUint32(out[4:8], uint32(correlationID))
	if flexibleHeader {
		out[8] = 0 // empty tagged-fields varint
		copy(out[9:], body)
	} else {
		copy(out[8:], body)
	}
	_, err := conn.Write(out)
	return err
}
