package e2e

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

// Raw Kafka framing helpers shared by the tests that assert on wire bytes
// rather than on a client library's interpretation of them.

// rawRequest writes one Kafka request frame and returns the response body with
// the correlation id (and any response-header tagged-fields byte) stripped.
func rawRequest(addr string, apiKey, apiVersion int16, payload []byte) ([]byte, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))

	body := make([]byte, 0, 12+len(payload))
	body = binary.BigEndian.AppendUint16(body, uint16(apiKey))
	body = binary.BigEndian.AppendUint16(body, uint16(apiVersion))
	body = binary.BigEndian.AppendUint32(body, 1) // correlation id
	body = binary.BigEndian.AppendUint16(body, 0) // null client_id
	if flexibleRequest(apiKey, apiVersion) {
		body = append(body, 0) // empty request-header tagged fields
	}
	body = append(body, payload...)

	frame := binary.BigEndian.AppendUint32(nil, uint32(len(body)))
	frame = append(frame, body...)
	if _, err := conn.Write(frame); err != nil {
		return nil, err
	}

	var sizeBuf [4]byte
	if _, err := io.ReadFull(conn, sizeBuf[:]); err != nil {
		return nil, err
	}
	size := int32(binary.BigEndian.Uint32(sizeBuf[:]))
	if size <= 0 || size > 64*1024*1024 {
		return nil, fmt.Errorf("bad response frame size %d", size)
	}
	resp := make([]byte, size)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return nil, err
	}
	if len(resp) < 4 {
		return nil, fmt.Errorf("response frame shorter than the correlation id")
	}
	out := resp[4:]
	// ApiVersions is the one API whose response header is never flexible
	// (KIP-511), which is why this is not simply keyed off the request version.
	if apiKey != apiKeyAPIVersions && flexibleRequest(apiKey, apiVersion) {
		if len(out) == 0 {
			return nil, fmt.Errorf("flexible response is missing its header tags")
		}
		out = out[1:]
	}
	return out, nil
}

// flexibleRequest mirrors kmsg's per-API flexible-version table for the APIs
// these tests send.
func flexibleRequest(apiKey, apiVersion int16) bool {
	switch apiKey {
	case apiKeyProduce:
		return apiVersion >= 9
	case apiKeyFetch:
		return apiVersion >= 12
	case apiKeyAPIVersions:
		return apiVersion >= 3
	case apiKeyCreateTopics:
		return apiVersion >= 5
	case apiKeyInitProducerID:
		return apiVersion >= 2
	}
	return false
}

func uvarintAt(b []byte, pos int) (uint64, int) {
	var x uint64
	var s uint
	for i := pos; i < len(b); i++ {
		c := b[i]
		if c < 0x80 {
			return x | uint64(c)<<s, i - pos + 1
		}
		x |= uint64(c&0x7f) << s
		s += 7
	}
	return x, len(b) - pos
}

// skipTags consumes a tagged-fields section at pos and returns the position
// after it.
func skipTags(b []byte, pos int) int {
	if pos >= len(b) {
		return pos
	}
	count, w := uvarintAt(b, pos)
	pos += w
	for i := uint64(0); i < count && pos < len(b); i++ {
		_, w1 := uvarintAt(b, pos)
		pos += w1
		size, w2 := uvarintAt(b, pos)
		pos += w2 + int(size)
	}
	if pos > len(b) {
		return len(b)
	}
	return pos
}

func appendVarint(b []byte, v int64) []byte { return binary.AppendVarint(b, v) }

func appendInt8(b []byte, v int8) []byte { return append(b, byte(v)) }

func appendInt16(b []byte, v int16) []byte {
	return binary.BigEndian.AppendUint16(b, uint16(v))
}

func appendInt32(b []byte, v int32) []byte {
	return binary.BigEndian.AppendUint32(b, uint32(v))
}

func appendInt64(b []byte, v int64) []byte {
	return binary.BigEndian.AppendUint64(b, uint64(v))
}

func appendString(b []byte, s string) []byte {
	b = appendInt16(b, int16(len(s)))
	return append(b, s...)
}

func appendCompactString(b []byte, s string) []byte {
	b = appendUVarint(b, uint32(len(s))+1)
	return append(b, s...)
}

func appendUVarint(b []byte, v uint32) []byte { return binary.AppendUvarint(b, uint64(v)) }
