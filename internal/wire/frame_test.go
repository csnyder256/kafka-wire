package wire

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

// pipeConn feeds bytes to readRequest without a real socket.
func pipeConn(t *testing.T, payload []byte) net.Conn {
	t.Helper()
	client, server := net.Pipe()
	go func() {
		_, _ = client.Write(payload)
		// Leave the connection open; readRequest reads exactly one frame.
	}()
	t.Cleanup(func() { client.Close(); server.Close() })
	return server
}

// frame builds a request frame: size, api key, version, correlation id,
// client id, then whatever trailer the caller supplies.
func frame(apiKey, apiVersion int16, correlationID int32, clientID string, trailer []byte) []byte {
	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, apiKey)
	binary.Write(&body, binary.BigEndian, apiVersion)
	binary.Write(&body, binary.BigEndian, correlationID)
	binary.Write(&body, binary.BigEndian, int16(len(clientID)))
	body.WriteString(clientID)
	body.Write(trailer)

	out := make([]byte, 4+body.Len())
	binary.BigEndian.PutUint32(out[0:4], uint32(body.Len()))
	copy(out[4:], body.Bytes())
	return out
}

// ApiVersions v0 is not flexible, so every byte after the client id belongs
// to the body and none of it may be eaten as header tags.
func TestReadRequestNonFlexibleKeepsWholeBody(t *testing.T) {
	want := []byte{0xde, 0xad, 0xbe, 0xef}
	conn := pipeConn(t, frame(18, 0, 7, "test-client", want))

	hdr, body, err := readRequest(conn, 1<<20, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if hdr.APIKey != 18 || hdr.APIVersion != 0 || hdr.CorrelationID != 7 {
		t.Fatalf("header = %+v", hdr)
	}
	if hdr.ClientID != "test-client" {
		t.Fatalf("client id = %q", hdr.ClientID)
	}
	if !bytes.Equal(body, want) {
		t.Fatalf("body = %x, want %x", body, want)
	}
}

// The regression this whole file exists for. CreateTopics v6 is flexible, so
// a single empty tagged-fields byte sits between the client id and the body.
// Passing it through as body data shifts every field by one and the request
// fails to decode, which is what made modern clients see the connection drop.
func TestReadRequestFlexibleStripsHeaderTags(t *testing.T) {
	want := []byte{0x01, 0x02, 0x03}
	trailer := append([]byte{0x00}, want...) // empty tag section, then body
	conn := pipeConn(t, frame(19, 6, 42, "kgo", trailer))

	_, body, err := readRequest(conn, 1<<20, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, want) {
		t.Fatalf("body = %x, want %x (the header tagged-fields byte must not reach the decoder)", body, want)
	}
}

// A client may legitimately send real tagged fields, so the section has to be
// parsed rather than assumed to be one byte.
func TestReadRequestFlexibleStripsPopulatedTags(t *testing.T) {
	want := []byte{0xaa, 0xbb}
	// One tag: tag=1, length=3, three bytes of payload.
	tags := []byte{0x01, 0x01, 0x03, 0x07, 0x08, 0x09}
	conn := pipeConn(t, frame(19, 6, 1, "c", append(tags, want...)))

	_, body, err := readRequest(conn, 1<<20, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, want) {
		t.Fatalf("body = %x, want %x", body, want)
	}
}

func TestReadRequestRejectsOversizeFrame(t *testing.T) {
	payload := frame(18, 0, 1, "c", bytes.Repeat([]byte{0}, 4096))
	conn := pipeConn(t, payload)
	if _, _, err := readRequest(conn, 64, time.Second); err == nil {
		t.Fatal("a frame larger than the configured limit must be refused")
	}
}

func TestReadRequestRejectsNegativeSize(t *testing.T) {
	bad := make([]byte, 4)
	binary.BigEndian.PutUint32(bad, uint32(0xFFFFFFFF)) // -1
	conn := pipeConn(t, bad)
	if _, _, err := readRequest(conn, 1<<20, time.Second); err == nil {
		t.Fatal("a negative frame size must be refused, not used as a length")
	}
}

// A null client id is encoded as length -1 and carries no bytes.
func TestReadRequestNullClientID(t *testing.T) {
	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, int16(18))
	binary.Write(&body, binary.BigEndian, int16(0))
	binary.Write(&body, binary.BigEndian, int32(3))
	binary.Write(&body, binary.BigEndian, int16(-1))
	body.Write([]byte{0x77})
	out := make([]byte, 4+body.Len())
	binary.BigEndian.PutUint32(out[0:4], uint32(body.Len()))
	copy(out[4:], body.Bytes())

	hdr, payload, err := readRequest(pipeConn(t, out), 1<<20, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if hdr.ClientID != "" {
		t.Fatalf("client id = %q, want empty for a null string", hdr.ClientID)
	}
	if !bytes.Equal(payload, []byte{0x77}) {
		t.Fatalf("body = %x", payload)
	}
}

// A truncated client id must be an error rather than a slice out of range.
// This is a network-facing parser, so every length on the wire is hostile
// until proven otherwise.
func TestReadRequestRejectsTruncatedClientID(t *testing.T) {
	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, int16(18))
	binary.Write(&body, binary.BigEndian, int16(0))
	binary.Write(&body, binary.BigEndian, int32(1))
	binary.Write(&body, binary.BigEndian, int16(500)) // claims 500 bytes
	body.WriteString("short")
	out := make([]byte, 4+body.Len())
	binary.BigEndian.PutUint32(out[0:4], uint32(body.Len()))
	copy(out[4:], body.Bytes())

	if _, _, err := readRequest(pipeConn(t, out), 1<<20, time.Second); err == nil {
		t.Fatal("a client id length past the end of the frame must be refused")
	}
}

func TestSkipTaggedFieldsRejectsRunaway(t *testing.T) {
	// One tag claiming a 200-byte payload inside a 4-byte buffer.
	if _, err := skipTaggedFields([]byte{0x01, 0x01, 0xc8, 0x00}); err == nil {
		t.Fatal("a tagged field longer than the remaining frame must be refused")
	}
	if _, err := skipTaggedFields([]byte{0x80}); err == nil {
		t.Fatal("a truncated varint must be refused")
	}
}

func TestUvarint(t *testing.T) {
	cases := map[uint64][]byte{
		0:     {0x00},
		1:     {0x01},
		127:   {0x7f},
		128:   {0x80, 0x01},
		300:   {0xac, 0x02},
		16384: {0x80, 0x80, 0x01},
	}
	for want, enc := range cases {
		got, n, err := uvarint(enc)
		if err != nil {
			t.Errorf("uvarint(%x): %v", enc, err)
			continue
		}
		if got != want || n != len(enc) {
			t.Errorf("uvarint(%x) = %d in %d bytes, want %d in %d", enc, got, n, want, len(enc))
		}
	}
}

func TestIsFlexibleRequestMatchesProtocol(t *testing.T) {
	cases := []struct {
		key, version int16
		flexible     bool
		name         string
	}{
		{18, 0, false, "ApiVersions v0"},
		{18, 3, true, "ApiVersions v3"},
		{19, 4, false, "CreateTopics v4"},
		{19, 6, true, "CreateTopics v6"},
		{1, 11, false, "Fetch v11"},
		{0, 8, false, "Produce v8"},
	}
	for _, c := range cases {
		if got := isFlexibleRequest(c.key, c.version); got != c.flexible {
			t.Errorf("%s: isFlexibleRequest = %v, want %v", c.name, got, c.flexible)
		}
	}
	// An api key nothing knows about must not panic.
	if isFlexibleRequest(30000, 0) {
		t.Error("an unknown api key should not be reported as flexible")
	}
}

func TestWriteResponseHeaderShape(t *testing.T) {
	for _, flexible := range []bool{false, true} {
		client, server := net.Pipe()
		done := make(chan []byte, 1)
		go func() {
			buf := make([]byte, 64)
			n, _ := client.Read(buf)
			done <- buf[:n]
		}()
		if err := writeResponse(server, 99, []byte{0xAB, 0xCD}, flexible, time.Second); err != nil {
			t.Fatal(err)
		}
		got := <-done
		client.Close()
		server.Close()

		size := binary.BigEndian.Uint32(got[0:4])
		corr := binary.BigEndian.Uint32(got[4:8])
		if corr != 99 {
			t.Errorf("flexible=%v: correlation id = %d", flexible, corr)
		}
		wantSize := uint32(6) // 4 correlation + 2 body
		if flexible {
			wantSize = 7 // plus the tagged-fields byte
			if got[8] != 0x00 {
				t.Errorf("flexible response header must carry an empty tag byte, got %#x", got[8])
			}
		}
		if size != wantSize {
			t.Errorf("flexible=%v: size prefix = %d, want %d", flexible, size, wantSize)
		}
	}
}
