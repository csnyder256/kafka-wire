// Package metrics exposes Prometheus counters/histograms scraped at
// /metrics by the admin HTTP listener. Designed to idle at near-zero
// CPU: counters increment in O(1), histograms use pre-bucketed
// summary, no per-request allocation.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registry wraps the Prometheus collectors. One instance per broker.
type Registry struct {
	registry *prometheus.Registry

	// Connections (gauge).
	connections prometheus.Gauge

	// Per-API request counts/latency.
	requestCount   *prometheus.CounterVec
	requestErrors  *prometheus.CounterVec
	requestLatency *prometheus.HistogramVec

	// Produce/Fetch volume.
	batchesAppended prometheus.Counter
	produceFailed   prometheus.Counter
	fetchBytes      prometheus.Counter

	// Topic state.
	topicsCreated prometheus.Counter

	// Cold storage tiering.
	s3Uploaded     prometheus.Counter
	s3UploadFailed prometheus.Counter
	s3Restored     prometheus.Counter

	// Disk guard state. The pause flag previously lived only in logs,
	// so operators discovered a paused broker from producer errors
	// instead of an alert on this gauge.
	diskWritesPaused prometheus.Gauge
	diskFreeFraction prometheus.Gauge
}

// New constructs a Registry with all collectors registered.
func New() *Registry {
	r := &Registry{
		registry: prometheus.NewRegistry(),
	}
	r.connections = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kafka_wire_connections",
		Help: "Currently open Kafka-protocol connections.",
	})
	r.requestCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_wire_requests_total",
		Help: "Kafka-protocol requests handled, by API key.",
	}, []string{"api_key"})
	r.requestErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_wire_request_errors_total",
		Help: "Kafka-protocol requests that errored, by API key.",
	}, []string{"api_key"})
	r.requestLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kafka_wire_request_latency_seconds",
		Help:    "Kafka request latency, by API key.",
		Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
	}, []string{"api_key"})
	r.batchesAppended = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_wire_batches_appended_total",
		Help: "Record batches successfully appended to any partition.",
	})
	r.produceFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_wire_produce_failed_total",
		Help: "Append failures (per Produce request, not per batch).",
	})
	r.fetchBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_wire_fetch_bytes_total",
		Help: "Bytes shipped to consumers via Fetch.",
	})
	r.topicsCreated = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_wire_topics_created_total",
		Help: "Topics created (auto + explicit) since boot.",
	})
	r.s3Uploaded = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_wire_archive_segments_uploaded_total",
		Help: "Sealed segments successfully uploaded to S3.",
	})
	r.s3UploadFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_wire_archive_uploads_failed_total",
		Help: "S3 multipart uploads that failed (will be retried).",
	})
	r.s3Restored = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_wire_archive_segments_restored_total",
		Help: "Segments downloaded from S3 to satisfy a Fetch.",
	})
	r.diskWritesPaused = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kafka_wire_disk_writes_paused",
		Help: "1 while the disk guard has paused appends (low free space).",
	})
	r.diskFreeFraction = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kafka_wire_disk_free_fraction",
		Help: "Fraction of the data volume currently free (0-1).",
	})
	r.registry.MustRegister(
		r.connections,
		r.requestCount,
		r.requestErrors,
		r.requestLatency,
		r.batchesAppended,
		r.produceFailed,
		r.fetchBytes,
		r.topicsCreated,
		r.s3Uploaded,
		r.s3UploadFailed,
		r.s3Restored,
		r.diskWritesPaused,
		r.diskFreeFraction,
	)
	return r
}

// SetDiskState publishes the disk guard's view: whether appends are
// paused and how much of the data volume is free.
func (r *Registry) SetDiskState(paused bool, freeFraction float64) {
	if paused {
		r.diskWritesPaused.Set(1)
	} else {
		r.diskWritesPaused.Set(0)
	}
	r.diskFreeFraction.Set(freeFraction)
}

// Handler is the http.Handler for /metrics.
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.registry, promhttp.HandlerOpts{})
}

// IncConnections increments the connection gauge.
func (r *Registry) IncConnections() { r.connections.Inc() }

// DecConnections decrements the connection gauge.
func (r *Registry) DecConnections() { r.connections.Dec() }

// ObserveRequest records the latency + error status of one
// Kafka-protocol request.
func (r *Registry) ObserveRequest(apiKey int, latency time.Duration, err error) {
	label := apiKeyLabel(apiKey)
	r.requestCount.WithLabelValues(label).Inc()
	if err != nil {
		r.requestErrors.WithLabelValues(label).Inc()
	}
	r.requestLatency.WithLabelValues(label).Observe(latency.Seconds())
}

// IncProduceSucceeded counts a successful Produce response.
func (r *Registry) IncProduceSucceeded(batchCount int64) {
	r.batchesAppended.Add(float64(batchCount))
}

// IncProduceFailed counts a Produce failure.
func (r *Registry) IncProduceFailed() { r.produceFailed.Inc() }

// AddFetchBytes records bytes shipped to a consumer.
func (r *Registry) AddFetchBytes(n int64) {
	r.fetchBytes.Add(float64(n))
}

// IncTopicCreated counts a topic creation.
func (r *Registry) IncTopicCreated() { r.topicsCreated.Inc() }

// IncS3Uploaded counts a successful S3 archival.
func (r *Registry) IncS3Uploaded() { r.s3Uploaded.Inc() }

// IncS3UploadFailed counts a failed S3 upload (will retry).
func (r *Registry) IncS3UploadFailed() { r.s3UploadFailed.Inc() }

// IncS3Restored counts a download-from-S3 to satisfy a Fetch.
func (r *Registry) IncS3Restored() { r.s3Restored.Inc() }

// apiKeyLabel maps numeric API keys to human-readable labels for the
// `api_key` Prometheus label. Missing keys get a generic numeric
// fallback so we can still see traffic on unrecognized APIs.
func apiKeyLabel(k int) string {
	switch k {
	case 0:
		return "Produce"
	case 1:
		return "Fetch"
	case 2:
		return "ListOffsets"
	case 3:
		return "Metadata"
	case 8:
		return "OffsetCommit"
	case 9:
		return "OffsetFetch"
	case 10:
		return "FindCoordinator"
	case 11:
		return "JoinGroup"
	case 12:
		return "Heartbeat"
	case 13:
		return "LeaveGroup"
	case 14:
		return "SyncGroup"
	case 15:
		return "DescribeGroups"
	case 16:
		return "ListGroups"
	case 17:
		return "SaslHandshake"
	case 18:
		return "ApiVersions"
	case 19:
		return "CreateTopics"
	case 20:
		return "DeleteTopics"
	case 32:
		return "DescribeConfigs"
	case 36:
		return "SaslAuthenticate"
	default:
		return "other"
	}
}
