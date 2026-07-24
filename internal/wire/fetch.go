package wire

import (
	"errors"
	"log/slog"
	"time"

	"github.com/csnyder256/kafka-wire/internal/broker"
	"github.com/csnyder256/kafka-wire/internal/storage"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// handleFetch dispatches a Fetch request. Each (topic, partition)
// pair specifies a fetchOffset and partitionMaxBytes; we return up
// to that many bytes of contiguous record-batch bytes starting at
// or after fetchOffset.
//
// Long-poll semantics:
//   - If MinBytes > 0 and we have <MinBytes available, wait up to
//     MaxWaitMillis for more data to arrive.
//   - On wakeup or timeout, return whatever we have.
//
// Long-poll is implemented via a coarse polling loop (50ms ticks)
// rather than a wakeup-on-Append condition variable. This is fine
// for moderate rates and latency-tolerant consumers
// and keeps the storage layer's lock surface minimal. a future version may switch to a
// per-partition broadcast condition variable.
func (d *Dispatcher) handleFetch(state *connState, hdr RequestHeader, body []byte) error {
	req := kmsg.NewPtrFetchRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return err
	}

	// Kept at Debug: at INFO this logs on every fetch AND every
	// long-poll iteration, which is hundreds of lines a second per
	// consumer.
	requestedTopics := make([]string, 0, len(req.Topics))
	for _, t := range req.Topics {
		requestedTopics = append(requestedTopics, t.Topic)
	}
	slog.Debug("fetch.recv",
		"principal", state.saslPrincipal,
		"client", state.lastClientID,
		"version", hdr.APIVersion,
		"min_bytes", req.MinBytes,
		"max_wait_ms", req.MaxWaitMillis,
		"topics", requestedTopics,
	)

	resp := kmsg.NewPtrFetchResponse()
	resp.SetVersion(hdr.APIVersion)

	deadline := time.Now().Add(time.Duration(req.MaxWaitMillis) * time.Millisecond)
	totalBytes := int64(0)
	pollDelay := 10 * time.Millisecond

	acl := d.brk.ACL()
	for {
		resp.Topics = resp.Topics[:0]
		totalBytes = 0
		for _, t := range req.Topics {
			topicResp := kmsg.FetchResponseTopic{
				Topic: t.Topic,
			}
			// Authorizer: principal must have `read` on this topic.
			// Tenant ownership of resolved segments is verified
			// inside Broker.Fetch via the segment-tenant tag check.
			if acl != nil && !acl.AuthorizeTopic(state.saslPrincipal, t.Topic, "read") {
				for _, p := range t.Partitions {
					topicResp.Partitions = append(topicResp.Partitions, kmsg.FetchResponseTopicPartition{
						Partition: p.Partition,
						ErrorCode: errCodeTopicAuthorizationFailed,
					})
				}
				resp.Topics = append(resp.Topics, topicResp)
				continue
			}
			for _, p := range t.Partitions {
				pr := kmsg.FetchResponseTopicPartition{
					Partition: p.Partition,
				}
				bytes, _, hwm, logStart, err := d.brk.FetchAuthorized(t.Topic, p.Partition, p.FetchOffset, int(p.PartitionMaxBytes), state.saslPrincipal, state.tenantID)
				if err != nil {
					switch {
					case errors.Is(err, broker.ErrUnknownTopic), errors.Is(err, broker.ErrUnknownPartition):
						pr.ErrorCode = errCodeUnknownTopicOrPart
					case errors.Is(err, broker.ErrUnauthorizedTopic):
						pr.ErrorCode = errCodeTopicAuthorizationFailed
					case errors.Is(err, storage.ErrOffsetOutOfRange):
						pr.ErrorCode = errCodeOffsetOutOfRange
					default:
						pr.ErrorCode = errCodeCorruptMessage
					}
					pr.HighWatermark = hwm
					pr.LogStartOffset = logStart
					topicResp.Partitions = append(topicResp.Partitions, pr)
					continue
				}
				pr.ErrorCode = errCodeNone
				pr.HighWatermark = hwm
				pr.LastStableOffset = hwm
				pr.LogStartOffset = logStart
				// -1 means "no preferred replica, keep reading from me".
				//
				// The protocol default for this field is -1, but the Go zero
				// value is 0, and 0 is a legitimate broker id. A client that
				// reads 0 here dutifully tries to fetch from broker 0, which
				// does not exist in a single-node cluster whose broker id is
				// 1, so it silently retries the same offset forever while the
				// broker log shows it happily serving the records every time.
				//
				// The field only exists in Fetch v11 and above, so a client
				// that negotiates lower never sees it. That is precisely how
				// a bug like this survives in production.
				pr.PreferredReadReplica = -1
				// Explicitly empty (not nil) so kmsg encodes a 0-length
				// array rather than null. some clients expect an
				// array marker even when there are no aborted txns;
				// a nil here can de-sync the records-field offset and
				// cause CorruptRecord on the next field.
				pr.AbortedTransactions = []kmsg.FetchResponseTopicPartitionAbortedTransaction{}
				// Same defensive pattern for RecordBatches: kmsg
				// encodes nil as INT32(-1) (null marker), but some clients
				// 0.12's MemoryRecords constructor on some paths
				// constructs a records buffer from those bytes anyway
				// and tries to parse: yielding 'Record size < 14' on
				// what should have been a no-op empty fetch. Force
				// empty (length-0) when there's no data.
				if bytes == nil {
					pr.RecordBatches = []byte{}
				} else {
					pr.RecordBatches = bytes
				}
				totalBytes += int64(len(bytes))
				// Hex-dump the first 32 bytes so we can verify the v2
				// batch header on the wire matches what clients expect.
				// Layout: BaseOffset[0:8] BatchLength[8:12] PartLeaderEpoch[12:16] Magic[16] CRC[17:21] Attrs[21:23] LastOffsetDelta[23:27] FirstTs[27:35]
				slog.Debug("fetch.partition_ok",
					"topic", t.Topic,
					"partition", p.Partition,
					"fetch_offset", p.FetchOffset,
					"max_bytes", p.PartitionMaxBytes,
					"returned_bytes", len(bytes),
					"hwm", hwm,
					"log_start", logStart,
					"head32_hex", hexHead(bytes, 32),
				)
				topicResp.Partitions = append(topicResp.Partitions, pr)
			}
			resp.Topics = append(resp.Topics, topicResp)
		}

		// Long-poll: if MinBytes is set and we don't have enough, wait and
		// retry until MaxWaitMillis. Adaptive backoff: start at 10ms for
		// snappy delivery when data is about to arrive, double up to a
		// 250ms ceiling so an idle consumer doesn't re-scan every
		// partition 20x/sec for the whole wait window (the old fixed 50ms
		// spin), and never sleep past the deadline.
		if req.MinBytes > 0 && totalBytes < int64(req.MinBytes) {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				break
			}
			sleep := pollDelay
			if sleep > remaining {
				sleep = remaining
			}
			time.Sleep(sleep)
			if pollDelay < 250*time.Millisecond {
				pollDelay *= 2
			}
			continue
		}
		break
	}

	d.metrics.AddFetchBytes(totalBytes)
	// Encode the response body once here so we can hex-dump the first
	// chunk for wire-protocol debugging. writeKmsgResponse re-encodes
	// internally: small price for being able to verify field
	// alignment against what clients expect.
	encoded := resp.AppendTo(nil)
	slog.Debug("fetch.send",
		"total_bytes", totalBytes,
		"flexible_header", req.IsFlexible(),
		"version", hdr.APIVersion,
		"body_len", len(encoded),
		"body_head128_hex", hexHead(encoded, 128),
	)
	return d.writeKmsgResponse(state, hdr, resp, req.IsFlexible())
}
