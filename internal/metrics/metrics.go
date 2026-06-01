// Package metrics defines all Prometheus metrics exposed by the Velo gateway.
//
// Metrics are grouped by subsystem so a Grafana dashboard can pivot off
// "velo_<subsystem>_<name>" naming. All histograms use latency buckets tuned
// for LLM workloads (TTFT and full-response).
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	Registry = prometheus.NewRegistry()

	RequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "velo_requests_total",
		Help: "Total HTTP requests received by the gateway.",
	}, []string{"path", "method", "status"})

	RequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "velo_request_duration_seconds",
		Help:    "End-to-end request latency at the gateway.",
		Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
	}, []string{"path", "status"})

	TimeToFirstToken = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "velo_time_to_first_token_seconds",
		Help:    "Time from request receipt to first SSE chunk forwarded to client.",
		Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
	})

	TokensGenerated = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "velo_tokens_generated_total",
		Help: "Completion tokens streamed through the gateway.",
	}, []string{"backend", "model"})

	// Scheduler metrics - the centerpiece of the system.

	QueueDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "velo_scheduler_queue_depth",
		Help: "Current number of requests waiting in the scheduler queue.",
	})

	BatchSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "velo_scheduler_batch_size",
		Help:    "Distribution of dispatched batch sizes.",
		Buckets: []float64{1, 2, 4, 8, 16, 32, 64},
	})

	BatchWaitTime = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "velo_scheduler_wait_seconds",
		Help:    "Time individual requests waited in the queue before dispatch.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5},
	})

	BatchesDispatched = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "velo_scheduler_batches_dispatched_total",
		Help: "Total number of batches dispatched.",
	})

	SchedulerRejections = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "velo_scheduler_rejections_total",
		Help: "Requests rejected because the scheduler queue was full.",
	})

	// Cache metrics.

	CacheHits = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "velo_cache_hits_total",
		Help: "Semantic cache hits.",
	})
	CacheMisses = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "velo_cache_misses_total",
		Help: "Semantic cache misses.",
	})
	CacheLookupSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "velo_cache_lookup_seconds",
		Help:    "Time spent in semantic-cache nearest-neighbor lookup.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25},
	})
	CacheStoreSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "velo_cache_entries",
		Help: "Approximate number of entries in the semantic cache.",
	})

	// Rate-limit metrics.

	RateLimitRejections = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "velo_ratelimit_rejections_total",
		Help: "Requests rejected by the rate limiter.",
	}, []string{"api_key"})

	// Router / backend metrics.

	BackendUp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "velo_backend_up",
		Help: "1 if backend is healthy, 0 otherwise.",
	}, []string{"backend"})

	BackendLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "velo_backend_latency_seconds",
		Help:    "Latency of upstream backend calls.",
		Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
	}, []string{"backend"})

	BackendErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "velo_backend_errors_total",
		Help: "Errors observed talking to a backend.",
	}, []string{"backend", "reason"})

	BackendRetries = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "velo_backend_retries_total",
		Help: "Number of times the router retried a failed backend on another.",
	})
)

func init() {
	Registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		RequestsTotal,
		RequestDuration,
		TimeToFirstToken,
		TokensGenerated,
		QueueDepth,
		BatchSize,
		BatchWaitTime,
		BatchesDispatched,
		SchedulerRejections,
		CacheHits,
		CacheMisses,
		CacheLookupSeconds,
		CacheStoreSize,
		RateLimitRejections,
		BackendUp,
		BackendLatency,
		BackendErrors,
		BackendRetries,
	)
}

// Handler returns an http.Handler that serves the Velo Prometheus registry.
func Handler() http.Handler {
	return promhttp.HandlerFor(Registry, promhttp.HandlerOpts{Registry: Registry})
}
