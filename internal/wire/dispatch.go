package wire

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/csnyder256/kafka-wire/internal/broker"
	"github.com/csnyder256/kafka-wire/internal/metrics"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Config tunes the dispatcher.
type Config struct {
	MaxRequestBytes int32
	IdleReadTimeout time.Duration
	WriteTimeout    time.Duration
	SaslEnabled     bool
	UsersFile       string
	// MaxConnections caps concurrent client connections. 0 = unlimited.
	// On a small box, a hostile client could spin up
	// thousands of TCP connections; capping at e.g. 1024 keeps the
	// FD count bounded and the kernel's TCP table healthy.
	MaxConnections int
}

// Dispatcher accepts raw TCP connections, parses Kafka frames, and
// routes to per-API handlers. One Dispatcher serves the whole broker;
// each connection runs in its own goroutine.
type Dispatcher struct {
	brk     *broker.Broker
	metrics *metrics.Registry
	cfg     Config

	// SCRAM credentials store, populated from cfg.UsersFile.
	// nil means SASL is disabled.
	scram *scramServer

	connID    atomic.Int64
	connCount atomic.Int32
}

// NewDispatcher constructs the dispatcher. Loads SCRAM credentials
// from disk if SASL is enabled.
func NewDispatcher(brk *broker.Broker, mreg *metrics.Registry, cfg Config) *Dispatcher {
	if cfg.IdleReadTimeout <= 0 {
		cfg.IdleReadTimeout = 5 * time.Minute
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 30 * time.Second
	}
	if cfg.MaxRequestBytes <= 0 {
		cfg.MaxRequestBytes = 4 * 1024 * 1024
	}
	d := &Dispatcher{brk: brk, metrics: mreg, cfg: cfg}
	if cfg.SaslEnabled && cfg.UsersFile != "" {
		s, err := loadScramServer(cfg.UsersFile)
		if err != nil {
			slog.Warn("wire.scram_load_failed", "err", err)
		} else {
			d.scram = s
		}
	}
	return d
}

// Serve handles one connection until EOF or fatal error. Per-connection
// state (auth, in-flight requests) lives in the local connState.
func (d *Dispatcher) Serve(ctx context.Context, conn net.Conn) {
	id := d.connID.Add(1)
	defer conn.Close()

	if d.cfg.MaxConnections > 0 {
		if int(d.connCount.Add(1)) > d.cfg.MaxConnections {
			d.connCount.Add(-1)
			slog.Warn("wire.connection_limit", "max", d.cfg.MaxConnections, "remote", conn.RemoteAddr())
			return
		}
		defer d.connCount.Add(-1)
	}

	state := &connState{
		conn:         conn,
		remote:       conn.RemoteAddr().String(),
		saslComplete: !d.cfg.SaslEnabled, // pre-authenticated when SASL is off
		dispatcher:   d,
	}
	d.metrics.IncConnections()
	defer d.metrics.DecConnections()

	slog.Debug("wire.connect", "conn_id", id, "remote", state.remote)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		hdr, body, err := readRequest(conn, d.cfg.MaxRequestBytes, d.cfg.IdleReadTimeout)
		if err != nil {
			if errors.Is(err, io.EOF) || isClosedConn(err) {
				return
			}
			if isTimeout(err) {
				slog.Debug("wire.idle_close", "conn_id", id)
				return
			}
			slog.Warn("wire.read_failed", "conn_id", id, "err", err)
			return
		}
		state.lastClientID = hdr.ClientID
		start := time.Now()
		err = d.handle(ctx, state, hdr, body)
		d.metrics.ObserveRequest(int(hdr.APIKey), time.Since(start), err)
		if err != nil {
			slog.Warn("wire.handler_failed",
				"conn_id", id,
				"api_key", hdr.APIKey,
				"api_version", hdr.APIVersion,
				"correlation_id", hdr.CorrelationID,
				"err", err,
			)
			return
		}
	}
}

// connState carries per-connection mutable state across requests.
type connState struct {
	conn          net.Conn
	remote        string
	lastClientID  string
	saslComplete  bool
	saslMechanism string
	saslPrincipal string      // SASL-authenticated identity name; "" pre-auth
	tenantID      string      // resolved from the ACL store on auth; "" means unscoped
	saslState     interface{} // *scramConversation; opaque to dispatch
	dispatcher    *Dispatcher
}

// handle dispatches one request. Returns a non-nil error only on
// FATAL conditions (connection should close); per-request business
// errors are returned to the client inside the response body via
// the per-API error code.
func (d *Dispatcher) handle(ctx context.Context, state *connState, hdr RequestHeader, body []byte) error {
	apiKey := kmsg.Key(hdr.APIKey)

	// SASL gate. Pre-auth, only ApiVersions, SaslHandshake, and
	// SaslAuthenticate are allowed.
	if !state.saslComplete {
		switch apiKey {
		case kmsg.ApiVersions, kmsg.SASLHandshake, kmsg.SASLAuthenticate:
			// allowed
		default:
			return errors.New("request received before SASL completion")
		}
	}

	switch apiKey {
	case kmsg.ApiVersions:
		return d.handleAPIVersions(state, hdr, body)
	case kmsg.SASLHandshake:
		return d.handleSASLHandshake(state, hdr, body)
	case kmsg.SASLAuthenticate:
		return d.handleSASLAuthenticate(state, hdr, body)
	case kmsg.Metadata:
		return d.handleMetadata(state, hdr, body)
	case kmsg.Produce:
		return d.handleProduce(state, hdr, body)
	case kmsg.Fetch:
		return d.handleFetch(state, hdr, body)
	case kmsg.ListOffsets:
		return d.handleListOffsets(state, hdr, body)
	case kmsg.CreateTopics:
		return d.handleCreateTopics(state, hdr, body)
	case kmsg.DeleteTopics:
		return d.handleDeleteTopics(state, hdr, body)
	case kmsg.FindCoordinator:
		return d.handleFindCoordinator(state, hdr, body)
	case kmsg.JoinGroup:
		return d.handleJoinGroup(state, hdr, body)
	case kmsg.SyncGroup:
		return d.handleSyncGroup(state, hdr, body)
	case kmsg.Heartbeat:
		return d.handleHeartbeat(state, hdr, body)
	case kmsg.LeaveGroup:
		return d.handleLeaveGroup(state, hdr, body)
	case kmsg.OffsetCommit:
		return d.handleOffsetCommit(state, hdr, body)
	case kmsg.OffsetFetch:
		return d.handleOffsetFetch(state, hdr, body)
	case kmsg.DescribeConfigs:
		return d.handleDescribeConfigs(state, hdr, body)
	case kmsg.DescribeGroups:
		return d.handleDescribeGroups(state, hdr, body)
	case kmsg.ListGroups:
		return d.handleListGroups(state, hdr, body)
	}

	return d.writeUnsupported(state, hdr)
}

// writeUnsupported answers an API this broker does not implement.
//
// The reply must be shaped like the response type the client asked for. A
// client that sent InitProducerId decodes the next frame as an
// InitProducerIdResponse no matter what the broker meant to send, so replying
// with a different message type corrupts the connection and produces a
// confusing error somewhere unrelated. kmsg can build the correct empty
// response for any known key, so use that and set UNSUPPORTED_VERSION on it.
//
// Clients are supposed to consult ApiVersions and never ask for an
// unimplemented API, but a broker should not depend on every client being
// well behaved.
func (d *Dispatcher) writeUnsupported(state *connState, hdr RequestHeader) error {
	resp := kmsg.ResponseForKey(hdr.APIKey)
	if resp == nil {
		// A key kmsg has never heard of cannot be answered coherently.
		// Closing is the honest outcome; inventing a frame is not.
		return fmt.Errorf("unknown api key %d", hdr.APIKey)
	}
	resp.SetVersion(hdr.APIVersion)
	setErrorCode(resp, errCodeUnsupportedVersion)
	return d.writeKmsgResponse(state, hdr, resp, resp.IsFlexible())
}

// setErrorCode sets a top-level ErrorCode field when the response type has
// one. Many do; those that do not still get a well-formed empty response,
// which is enough for the client to fail cleanly rather than hang.
func setErrorCode(resp kmsg.Response, code int16) {
	v := reflect.ValueOf(resp)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return
	}
	f := v.Elem().FieldByName("ErrorCode")
	if f.IsValid() && f.CanSet() && f.Kind() == reflect.Int16 {
		f.SetInt(int64(code))
	}
}

// writeKmsgResponse encodes a kmsg.Response to the connection.
// flexibleHeader indicates whether to include the tagged-fields byte
// in the response header (per KIP-482).
func (d *Dispatcher) writeKmsgResponse(state *connState, hdr RequestHeader, resp kmsg.Response, flexibleHeader bool) error {
	body := resp.AppendTo(nil)
	return writeResponse(state.conn, hdr.CorrelationID, body, flexibleHeader, d.cfg.WriteTimeout)
}

// isClosedConn reports whether err is just a client going away.
//
// Clients disconnect constantly: a consumer exits, a pooled connection is
// recycled, a container is rescheduled. None of that is a broker problem, and
// logging it at warning level trains operators to ignore the warning channel
// and produces bug reports about a broker that is working correctly. The
// Windows strings are here because its sockets report an abortive close with
// entirely different wording from POSIX.
func isClosedConn(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	s := err.Error()
	for _, frag := range []string{
		"use of closed",
		"connection reset by peer",
		"broken pipe",
		"An established connection was aborted",      // Windows, WSAECONNABORTED
		"An existing connection was forcibly closed", // Windows, WSAECONNRESET
		"forcibly closed by the remote host",
	} {
		if strings.Contains(s, frag) {
			return true
		}
	}
	return false
}

func isTimeout(err error) bool {
	var ne net.Error
	if errors.As(err, &ne) {
		return ne.Timeout()
	}
	return false
}

// Kafka error codes we use throughout the wire layer. We define just
// the ones we hand back; kmsg has the full set if/when needed.
const (
	errCodeNone                       int16 = 0
	errCodeOffsetOutOfRange           int16 = 1
	errCodeCorruptMessage             int16 = 2
	errCodeUnknownTopicOrPart         int16 = 3
	errCodeRequestTimedOut            int16 = 7
	errCodeMessageTooLarge            int16 = 10
	errCodeNotLeaderForPartition      int16 = 6
	errCodeUnknownMember              int16 = 25
	errCodeMemberIDRequired           int16 = 79
	errCodeRebalanceInProgress        int16 = 27
	errCodeIllegalGeneration          int16 = 22
	errCodeInvalidGroupID             int16 = 24
	errCodeNotCoordinator             int16 = 16
	errCodeUnsupportedVersion         int16 = 35
	errCodeTopicAlreadyExists         int16 = 36
	errCodeInvalidPartitions          int16 = 37
	errCodeInvalidTopic               int16 = 17
	errCodeSaslAuthFailed             int16 = 58
	errCodeUnsupportedSaslMech        int16 = 33
	errCodeTopicAuthorizationFailed   int16 = 29
	errCodeGroupAuthorizationFailed   int16 = 30
	errCodeClusterAuthorizationFailed int16 = 31
)
