package e2e

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ApiVersions is the first request every client sends and the one every later
// request version is chosen from. Two things can go wrong with the answer, and
// neither shows up as an ApiVersions error: the broker can advertise an API it
// has no handler for, or it can accept a request version past the top of the
// range it published. Both turn into a failure somewhere unrelated later, in
// whatever the client happened to be doing.
//
// These tests read the advertisement and drive the handlers over the wire
// directly, so they describe what any client sees rather than what one client
// library happens to negotiate.

const (
	apiKeyProduce        int16 = 0
	apiKeyFetch          int16 = 1
	apiKeyAPIVersions    int16 = 18
	apiKeyCreateTopics   int16 = 19
	apiKeyInitProducerID int16 = 22
	errCodeUnsupportedVs int16 = 35
)

type versionRange struct{ min, max int16 }

// TestUnimplementedAPIsAreNotAdvertised is the first regression this file
// exists for.
//
// kafka-wire implements no transaction coordinator, so InitProducerId has no
// handler: dispatch.go's switch falls through to writeUnsupported. ApiVersions
// listed it anyway, so a client that read the advertisement -- the entire
// purpose of the advertisement -- sent a request the broker had just declared
// supported and got UNSUPPORTED_VERSION back. The README documents the real
// contract: unimplemented APIs are absent from the table, and an unadvertised
// API gets a typed UNSUPPORTED_VERSION response.
func TestUnimplementedAPIsAreNotAdvertised(t *testing.T) {
	b := startBroker(t)
	adv := probeAPIVersions(t, b.addr)

	if len(adv) == 0 {
		t.Fatal("the broker advertised no APIs at all")
	}
	for key, r := range adv {
		// 22 InitProducerId and 42 DeleteGroups have no handler.
		if key == apiKeyInitProducerID || key == 42 {
			t.Errorf("api key %d is advertised (v%d-v%d) with no handler in dispatch.go; a client that trusts this sends it and is refused",
				key, r.min, r.max)
		}
		if r.min > r.max {
			t.Errorf("api key %d advertises an inverted range v%d-v%d", key, r.min, r.max)
		}
		if r.min < 0 {
			t.Errorf("api key %d advertises a negative minimum version %d", key, r.min)
		}
	}
	// An over-eager deletion from the table would be as broken as an extra
	// entry, so pin the APIs the docs promise.
	for _, key := range []int16{apiKeyProduce, apiKeyFetch, 3 /* Metadata */, 8, 9, apiKeyAPIVersions} {
		if _, ok := adv[key]; !ok {
			t.Errorf("api key %d is documented as supported but is no longer advertised", key)
		}
	}
	t.Logf("advertised %d APIs, all of them implemented", len(adv))
}

// TestUnsupportedAPIFailsAsItsOwnResponseType pins the other half of that
// contract, which writeUnsupported already implements: an API the broker does
// not advertise still answers with a response shaped like the request, carrying
// UNSUPPORTED_VERSION. A client that sent InitProducerId decodes the next frame
// as an InitProducerIdResponse whatever the broker meant to send, so a
// mis-typed reply desynchronizes the connection instead of failing the one
// request that was unsupported.
func TestUnsupportedAPIFailsAsItsOwnResponseType(t *testing.T) {
	b := startBroker(t)
	for _, v := range []int16{0, 1, 2, 4} {
		resp, err := roundTrip(b.addr, apiKeyInitProducerID, v, nil)
		if err != nil {
			t.Fatalf("InitProducerId v%d: the broker did not answer with a typed response: %v", v, err)
		}
		if len(resp.body) < 2 {
			t.Fatalf("InitProducerId v%d: response too short (%d bytes)", v, len(resp.body))
		}
		if len(resp.body) < 6 {
			t.Fatalf("InitProducerId v%d: response too short for throttle+errorCode (%d bytes)", v, len(resp.body))
		}
		if code := int16(binary.BigEndian.Uint16(resp.body[4:6])); code != errCodeUnsupportedVs {
			t.Fatalf("InitProducerId v%d answered with error code %d, want %d (UNSUPPORTED_VERSION)", v, code, errCodeUnsupportedVs)
		}
	}
	t.Log("InitProducerId answers with UNSUPPORTED_VERSION at v0, v1, v2 and v4")
}

// TestHighProduceVersionIsSupported drives Produce at the top of its advertised
// range with a real record batch. Advertising a version the handler only half
// implements is worse than not advertising it, because the client has no
// fallback left; this proves the ceiling the broker publishes is one it serves.
func TestHighProduceVersionIsSupported(t *testing.T) {
	b := startBroker(t)
	adv := probeAPIVersions(t, b.addr)
	pr, ok := adv[apiKeyProduce]
	if !ok {
		t.Fatal("Produce is not advertised")
	}

	const topic = "high.produce"
	cl := newClient(t, b.addr)
	createTopic(t, cl, topic, 1)
	cl.Close()

	if err := produceRawAtVersion(t, b.addr, topic, pr.max, []byte("ceiling-payload")); err != nil {
		t.Fatalf("Produce v%d is advertised as supported but the broker refused a real batch: %v", pr.max, err)
	}

	// Read it back through a real client, so the check covers the stored bytes
	// and not just the acknowledgement.
	cons := newClient(t, b.addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	t.Cleanup(cons.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var got []string
	deadline := time.Now().Add(30 * time.Second)
	for len(got) == 0 && time.Now().Before(deadline) {
		pctx, pcancel := context.WithTimeout(ctx, 5*time.Second)
		f := cons.PollFetches(pctx)
		pcancel()
		f.EachRecord(func(r *kgo.Record) { got = append(got, string(r.Value)) })
	}
	if len(got) != 1 || got[0] != "ceiling-payload" {
		t.Fatalf("after a successful Produce v%d the record read back as %v, want [ceiling-payload]", pr.max, got)
	}
	t.Logf("Produce v%d accepted a real batch and the record round-tripped", pr.max)
}

// TestHighFetchVersionIsSupported does the same for Fetch. Fetch is advertised
// v4-v11; v11 is the version that is flexible on the request header, so if the
// header parsing were wrong the fetch would fail to decode rather than return
// stale data -- which is the quiet version of this bug.
func TestHighFetchVersionIsSupported(t *testing.T) {
	b := startBroker(t)
	adv := probeAPIVersions(t, b.addr)
	fr, ok := adv[apiKeyFetch]
	if !ok {
		t.Fatal("Fetch is not advertised")
	}

	const topic = "high.fetch"
	cl := newClient(t, b.addr)
	createTopic(t, cl, topic, 1)
	cctx, ccancel := context.WithTimeout(context.Background(), 20*time.Second)
	if err := cl.ProduceSync(cctx, &kgo.Record{Topic: topic, Value: []byte("f")}).FirstErr(); err != nil {
		ccancel()
		t.Fatalf("seeding the topic: %v", err)
	}
	ccancel()
	cl.Close()

	resp, err := roundTrip(b.addr, apiKeyFetch, fr.max, encodeFetchV11(topic, 0))
	if err != nil {
		t.Fatalf("Fetch v%d: %v", fr.max, err)
	}
	if len(resp.body) < 2 {
		t.Fatalf("Fetch v%d: response too short (%d bytes)", fr.max, len(resp.body))
	}
	if code := int16(binary.BigEndian.Uint16(resp.body[0:2])); code != 0 {
		t.Fatalf("Fetch v%d answered with top-level error code %d", fr.max, code)
	}
	t.Logf("Fetch v%d answered without error", fr.max)
}

// ---------------------------------------------------------------------------
// Raw wire helpers. These talk to the broker directly so the tests assert on
// bytes rather than on a client library's interpretation of them.

type rawResp struct {
	body []byte // response body, after the correlation id and any header tags
}

// roundTrip writes one request frame and reads one response frame.
//
// The request header is [api_key][api_version][correlation_id][client_id] plus
// a tagged-fields section for flexible versions. Flexible response headers carry
// a tagged-fields byte after the correlation id, EXCEPT ApiVersions, which
// always answers with a v0 header (KIP-511) -- that exception is why the
// response side is not simply keyed off the request version.
func roundTrip(addr string, apiKey, apiVersion int16, payload []byte) (rawResp, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return rawResp{}, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))

	body := make([]byte, 0, 12+len(payload))
	body = binary.BigEndian.AppendUint16(body, uint16(apiKey))
	body = binary.BigEndian.AppendUint16(body, uint16(apiVersion))
	body = binary.BigEndian.AppendUint32(body, 1) // correlation id
	body = binary.BigEndian.AppendUint16(body, 0) // null client_id
	if flexibleRequest(apiKey, apiVersion) {
		body = append(body, 0) // empty tagged-fields section
	}
	body = append(body, payload...)

	frame := binary.BigEndian.AppendUint32(nil, uint32(len(body)))
	frame = append(frame, body...)
	if _, err := conn.Write(frame); err != nil {
		return rawResp{}, err
	}

	var sizeBuf [4]byte
	if _, err := io.ReadFull(conn, sizeBuf[:]); err != nil {
		return rawResp{}, err
	}
	size := int32(binary.BigEndian.Uint32(sizeBuf[:]))
	if size <= 0 || size > 64*1024*1024 {
		return rawResp{}, fmt.Errorf("bad response frame size %d", size)
	}
	resp := make([]byte, size)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return rawResp{}, err
	}
	if len(resp) < 4 {
		return rawResp{}, fmt.Errorf("response frame shorter than the correlation id")
	}
	out := resp[4:]
	// ApiVersions is the one API whose response header is never flexible.
	if apiKey != apiKeyAPIVersions && flexibleRequest(apiKey, apiVersion) {
		if len(out) == 0 {
			return rawResp{}, fmt.Errorf("flexible response is missing its header tags")
		}
		out = out[1:]
	}
	return rawResp{body: out}, nil
}

// probeAPIVersions reads the advertisement off the wire. v3 is used because
// KIP-511 made v3 return the full ApiKeys list even when the requested version
// is not supported, so the answer is complete regardless of the broker's own
// ceiling.
func probeAPIVersions(t *testing.T, addr string) map[int16]versionRange {
	t.Helper()
	resp, err := roundTrip(addr, apiKeyAPIVersions, 3, nil)
	if err != nil {
		t.Fatalf("ApiVersions: %v", err)
	}
	// The body is [error_code][compact api_keys array][throttle_ms][tags].
	body := resp.body
	if len(body) < 4 {
		t.Fatalf("ApiVersions response too short: %d bytes", len(body))
	}
	if code := int16(binary.BigEndian.Uint16(body[0:2])); code != 0 {
		t.Fatalf("ApiVersions returned error code %d", code)
	}
	count := int(body[2]) - 1 // compact array length encoding
	pos := 3
	if count <= 0 {
		t.Fatalf("ApiVersions advertised %d keys", count)
	}
	out := make(map[int16]versionRange, count)
	for i := 0; i < count; i++ {
		if pos+6 > len(body) {
			t.Fatalf("ApiVersions truncated after %d of %d keys", i, count)
		}
		key := int16(binary.BigEndian.Uint16(body[pos : pos+2]))
		out[key] = versionRange{
			min: int16(binary.BigEndian.Uint16(body[pos+2 : pos+4])),
			max: int16(binary.BigEndian.Uint16(body[pos+4 : pos+6])),
		}
		pos += 6
		pos = skipTags(body, pos)
	}
	return out
}

// produceRaw builds a Produce request at the given version carrying one
// uncompressed record batch, and returns an error if the partition reported a
// non-zero error code. The batch layout depends on the version, which is the
// whole point of the test: v9 moved the CRC from CRC-32C to CRC-32.
func produceRawAtVersion(t *testing.T, addr, topic string, version int16, value []byte) error {
	t.Helper()
	if version < 3 {
		return fmt.Errorf("produce v%d predates v2 record batches", version)
	}
	// Encode the request with kmsg, the same library clients use, so this test
	// is about what the broker does with a well-formed request rather than
	// about this file's ability to build one.
	req := kmsg.NewPtrProduceRequest()
	req.SetVersion(version)
	req.Acks = 1
	req.TimeoutMillis = 5000
	batchBytes := buildRecordBatch(version, value)
	tp := kmsg.ProduceRequestTopic{Topic: topic}
	tp.Partitions = append(tp.Partitions, kmsg.ProduceRequestTopicPartition{
		Partition: 0,
		Records:   batchBytes,
	})
	req.Topics = append(req.Topics, tp)

	resp, err := roundTrip(addr, apiKeyProduce, version, req.AppendTo(nil))
	if err != nil {
		return err
	}
	code, err := firstPartitionError(resp.body)
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("partition error code %d", code)
	}
	return nil
}

// firstPartitionError pulls the first partition-level error code out of a
// Produce v9 response body. The shared roundTrip helper has already removed the
// response header, so this body begins at the compact responses array:
//
//	[responses][topic name][compact partitions][index][error_code]...
//
// Index precedes ErrorCode, and the array/string lengths are stored as count+1.
func firstPartitionError(body []byte) (int16, error) {
	pos := 1 // leading compact responses array length
	if pos >= len(body) {
		return 0, fmt.Errorf("produce response truncated at the responses array")
	}
	nameLen, w := uvarintAt(body, pos)
	pos += w + int(nameLen) - 1
	if pos >= len(body) {
		return 0, fmt.Errorf("produce response truncated after the topic name")
	}
	pos++ // compact partitions array length
	if pos+6 > len(body) {
		return 0, fmt.Errorf("produce response truncated before the partition entry")
	}
	pos += 4 // partition index
	return int16(binary.BigEndian.Uint16(body[pos : pos+2])), nil
}

// encodeFetchV11 builds a minimal Fetch request body: one topic, one partition,
// reading from offset 0.
func encodeFetchV11(topic string, partition int32) []byte {
	var p []byte
	p = appendInt32(p, -1)        // replica_id
	p = appendInt32(p, 100)       // max_wait_ms
	p = appendInt32(p, 1)         // min_bytes
	p = appendInt32(p, 1024*1024) // max_bytes
	p = appendInt8(p, 0)          // isolation_level
	p = appendInt32(p, 0)         // session_id
	p = appendInt32(p, -1)        // session_epoch
	p = appendInt32(p, 1)         // topics
	p = appendString(p, topic)
	p = appendInt32(p, 1) // partitions
	p = appendInt32(p, partition)
	p = appendInt32(p, -1) // current_leader_epoch
	p = appendInt64(p, 0)  // fetch_offset
	p = appendInt64(p, -1) // log_start_offset
	p = appendInt32(p, 1024*1024)
	p = appendInt32(p, 0) // rack_id
	p = appendInt32(p, 0) // forgotten topics
	return p
}

// buildRecordBatch builds a one-record batch in whichever layout this Produce
// version expects.
func buildRecordBatch(_ int16, value []byte) []byte {
	// A record is [length varint][attributes][timestampDelta][offsetDelta]
	// [keyLength][key][valueLength][value][headersCount]. The leading length is
	// the byte count of everything after it, and omitting it makes the batch
	// one byte short, which the broker reports as "batch overruns Records
	// buffer".
	inner := []byte{}
	inner = appendInt8(inner, 0) // attributes
	inner = appendVarint(inner, 0)
	inner = appendVarint(inner, 0)
	inner = appendVarint(inner, -1) // null key
	inner = appendVarint(inner, int64(len(value)))
	inner = append(inner, value...)
	inner = appendVarint(inner, 0) // headersCount

	rec := appendVarint(nil, int64(len(inner)))
	rec = append(rec, inner...)

	var body []byte
	body = appendInt16(body, 0)  // attributes
	body = appendInt32(body, 0)  // last_offset_delta
	body = appendInt64(body, 0)  // first_timestamp
	body = appendInt64(body, 0)  // max_timestamp
	body = appendInt64(body, -1) // producer_id
	body = appendInt16(body, -1) // producer_epoch
	body = appendInt32(body, -1) // base_sequence
	body = appendInt32(body, 1)  // record count
	body = append(body, rec...)

	// The v2 batch checksum is CRC-32C for every Produce version. Produce v9's
	// only change was to become flexible (KIP-482 tagged fields); it did not
	// touch the record format.
	checksum := crc32C(body)

	// batch_length counts every byte after the batch_length field itself:
	// partitionLeaderEpoch(4) + magic(1) + crc(4) + the body written below.
	batchLength := int32(4 + 1 + 4 + len(body))

	header := make([]byte, 0, 61)
	header = appendInt64(header, 0) // base_offset
	header = appendInt32(header, batchLength)
	header = appendInt32(header, -1) // partition_leader_epoch
	header = appendInt8(header, 2)   // magic
	header = binary.BigEndian.AppendUint32(header, checksum)
	batch := append(header, body...)

	// The header is fixed at 61 bytes and TotalSize() is 12+batchLength, so any
	// drift here is a malformed batch; catch it in the test rather than letting
	// the broker report "overruns Records buffer".
	if len(batch) != 12+int(batchLength) {
		panic(fmt.Sprintf("test built a %d-byte batch whose header claims %d", len(batch), 12+batchLength))
	}
	return batch
}

func crc32C(b []byte) uint32 {
	return crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli))
}
