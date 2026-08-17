// Package metrics defines every Prometheus series the services emit and
// serves the scrape endpoint. Centralized so the dashboard JSON and the
// metric names cannot drift apart silently.
package metrics

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Serve exposes /metrics on addr in a background goroutine.
func Serve(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Printf("metrics: %v", err)
		}
	}()
}

// Ingest side.
var (
	IngestItems = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dealstream_ingest_items_total",
		Help: "Feed items processed, by retailer and outcome (accepted, stale_skipped, or a quarantine reason).",
	}, []string{"retailer", "outcome"})

	IngestPages = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dealstream_ingest_pages_total",
		Help: "Feed pages fetched, by retailer and phase (catalog or offers).",
	}, []string{"retailer", "phase"})

	IngestFeedErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dealstream_ingest_feed_errors_total",
		Help: "Feed fetch or processing failures, by retailer.",
	}, []string{"retailer"})

	IngestResyncs = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dealstream_ingest_resyncs_total",
		Help: "Full catalog resyncs forced by an expired cursor, by retailer.",
	}, []string{"retailer"})

	// Freshness is derived in PromQL as time() minus this, so a silent
	// retailer's staleness keeps climbing. A gauge of "seconds since"
	// would freeze at its last value exactly when the feed dies.
	IngestLastAccept = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dealstream_ingest_last_accept_timestamp_seconds",
		Help: "Unix timestamp of the newest accepted item's feed timestamp, by retailer.",
	}, []string{"retailer"})

	IngestBatchDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dealstream_ingest_batch_seconds",
		Help:    "Postgres upsert batch duration.",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 12),
	}, []string{"retailer"})

	CHPending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dealstream_ch_pending_rows",
		Help: "Price events buffered for the next ClickHouse flush.",
	})

	CHFlushes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dealstream_ch_flushes_total",
		Help: "ClickHouse batch flushes by result.",
	}, []string{"result"})

	CHRowsWritten = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dealstream_ch_rows_written_total",
		Help: "Price events written to ClickHouse.",
	})
)

// API side.
var (
	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dealstream_http_request_seconds",
		Help:    "API request duration by route and status class.",
		Buckets: prometheus.ExponentialBuckets(0.002, 2, 13),
	}, []string{"route", "status"})

	CacheOps = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dealstream_cache_ops_total",
		Help: "Read-through cache outcomes.",
	}, []string{"result"})

	DealsRefresh = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dealstream_deals_refresh_seconds",
		Help:    "Deals materialization duration by result.",
		Buckets: prometheus.ExponentialBuckets(0.05, 2, 10),
	}, []string{"result"})
)

// Feedgen side.
var (
	FeedRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dealstream_feedgen_requests_total",
		Help: "Feed requests served, by retailer, endpoint, and status.",
	}, []string{"retailer", "endpoint", "status"})

	FeedUpdates = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dealstream_feedgen_updates_total",
		Help: "Simulated offer updates emitted, by retailer.",
	}, []string{"retailer"})
)
