package wire

import (
	"errors"
	"log/slog"

	"github.com/csnyder256/kafka-wire/internal/broker"
	"github.com/csnyder256/kafka-wire/internal/storage"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// handleProduce dispatches a Produce request. Each topic-partition's
// records arrive as a single concatenated bytes blob containing one
// or more record batches. We do NOT decode individual records: we
// validate the batch headers, rewrite BaseOffsets to absolute
// partition offsets, and append verbatim.
//
// this build implements `acks=1` semantics (the some clients default):
// we return as soon as the bytes hit the active segment's page cache.
// fsync happens asynchronously via the storage's group-commit timer.
// `acks=-1 / all` is treated identically in single-node mode (no
// replicas to wait for); `acks=0` skips the response entirely.
func (d *Dispatcher) handleProduce(state *connState, hdr RequestHeader, body []byte) error {
	req := kmsg.NewPtrProduceRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return err
	}

	// acks == 0 means "no response". Spec: don't write a response
	// frame at all, just process and move on.
	silent := req.Acks == 0

	resp := kmsg.NewPtrProduceResponse()
	resp.SetVersion(hdr.APIVersion)
	resp.ThrottleMillis = 0

	acl := d.brk.ACL()
	for _, topicReq := range req.Topics {
		respTopic := kmsg.ProduceResponseTopic{
			Topic: topicReq.Topic,
		}

		// Authorizer gate: the connection's principal must have
		// `write` ACL on this topic. Tenant-aware enforcement
		// happens deeper in storage (segment is tagged with
		// principal.tenant_id at append time).
		if acl != nil && !acl.AuthorizeTopic(state.saslPrincipal, topicReq.Topic, "write") {
			for _, partReq := range topicReq.Partitions {
				respTopic.Partitions = append(respTopic.Partitions, kmsg.ProduceResponseTopicPartition{
					Partition:    partReq.Partition,
					ErrorCode:    errCodeTopicAuthorizationFailed,
					BaseOffset:   -1,
					ErrorMessage: stringPtr("principal lacks write ACL"),
				})
			}
			resp.Topics = append(resp.Topics, respTopic)
			continue
		}

		for _, partReq := range topicReq.Partitions {
			partResp := kmsg.ProduceResponseTopicPartition{
				Partition: partReq.Partition,
				// -1 means "the broker did not overwrite your timestamp".
				// The Go zero value 0 is a real timestamp (1970-01-01), and
				// clients that read it hand that back as the record's
				// timestamp instead of the one the producer set.
				LogAppendTime: -1,
			}

			if len(partReq.Records) == 0 {
				partResp.ErrorCode = errCodeCorruptMessage
				partResp.BaseOffset = -1
				respTopic.Partitions = append(respTopic.Partitions, partResp)
				continue
			}

			// Records is one or more concatenated v2 record batches.
			// Split them so the storage layer can index each one's
			// timestamp + offset range.
			batches, err := splitBatches(partReq.Records)
			if err != nil {
				partResp.ErrorCode = errCodeCorruptMessage
				partResp.BaseOffset = -1
				partResp.ErrorMessage = stringPtr(err.Error())
				respTopic.Partitions = append(respTopic.Partitions, partResp)
				continue
			}

			firstOffset, err := d.brk.AppendBatchesAuthorized(topicReq.Topic, partReq.Partition, batches, state.saslPrincipal, state.tenantID)
			if err != nil {
				slog.Warn("produce.append_failed",
					"topic", topicReq.Topic,
					"partition", partReq.Partition,
					"err", err,
				)
				partResp.ErrorCode = mapAppendError(err)
				partResp.BaseOffset = -1
				partResp.ErrorMessage = stringPtr(err.Error())
				respTopic.Partitions = append(respTopic.Partitions, partResp)
				continue
			}

			slog.Info("produce.append_ok",
				"topic", topicReq.Topic,
				"partition", partReq.Partition,
				"batches", len(batches),
				"first_offset", firstOffset,
				"records_bytes", len(partReq.Records),
			)
			partResp.ErrorCode = errCodeNone
			partResp.BaseOffset = firstOffset
			partResp.LogStartOffset = -1
			respTopic.Partitions = append(respTopic.Partitions, partResp)
		}
		resp.Topics = append(resp.Topics, respTopic)
	}

	if silent {
		return nil
	}
	return d.writeKmsgResponse(state, hdr, resp, req.IsFlexible())
}

// splitBatches scans `data` (a Records blob from a Produce request)
// and yields one slice per record-batch boundary. Each returned slice
// shares memory with the input: caller must not retain past the
// next read.
func splitBatches(data []byte) ([][]byte, error) {
	var out [][]byte
	pos := 0
	for pos < len(data) {
		if pos+61 > len(data) {
			return nil, errors.New("trailing bytes shorter than batch header")
		}
		hdr, err := storage.ParseBatchHeader(data[pos : pos+61])
		if err != nil {
			return nil, err
		}
		end := pos + hdr.TotalSize()
		if end > len(data) {
			return nil, errors.New("batch overruns Records buffer")
		}
		// Copy the batch bytes: the storage layer will rewrite
		// BaseOffset in place, so we must own the bytes.
		batch := make([]byte, end-pos)
		copy(batch, data[pos:end])
		out = append(out, batch)
		pos = end
	}
	return out, nil
}

// mapAppendError translates broker-internal errors to Kafka error codes.
func mapAppendError(err error) int16 {
	switch {
	case errors.Is(err, broker.ErrUnknownTopic):
		return errCodeUnknownTopicOrPart
	case errors.Is(err, broker.ErrUnknownPartition):
		return errCodeUnknownTopicOrPart
	case errors.Is(err, broker.ErrUnauthorizedTopic):
		return errCodeTopicAuthorizationFailed
	case errors.Is(err, broker.ErrUnauthorizedGroup):
		return errCodeGroupAuthorizationFailed
	case errors.Is(err, broker.ErrBrokerDraining):
		return errCodeNotLeaderForPartition
	default:
		return errCodeCorruptMessage
	}
}
