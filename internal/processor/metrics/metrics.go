package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	Processed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "txn_processed_total",
		Help: "Transactions fully processed, by decision.",
	}, []string{"decision"})

	RuleHits = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "txn_rule_hits_total",
		Help: "Fraud rule hits, by rule.",
	}, []string{"rule"})

	Duplicates = promauto.NewCounter(prometheus.CounterOpts{
		Name: "txn_duplicate_total",
		Help: "Redelivered transactions dropped by the idempotency guard.",
	})

	Retries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "txn_retry_total",
		Help: "Processing attempts that failed with a transient error and were retried.",
	})

	DeadLettered = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "txn_dlq_total",
		Help: "Messages routed to the dead-letter topic, by reason.",
	}, []string{"reason"})

	PublishFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "txn_publish_failures_total",
		Help: "Failed writes of decision events to Kafka.",
	})

	CommitFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "txn_offset_commit_failures_total",
		Help: "Failed Kafka offset commits (retried on the next tick).",
	})

	ProcessingLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "txn_processing_latency_seconds",
		Help:    "Time spent in the worker: rules, publish and bookkeeping.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14),
	})

	EndToEndLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "txn_end_to_end_latency_seconds",
		Help:    "Time from Kafka append (producer timestamp) to decision published.",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 12),
	})

	ConsumerLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "txn_consumer_lag",
		Help: "Messages between the group's position and the partition head.",
	})

	WorkerQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "txn_worker_queue_depth",
		Help: "Messages buffered across all worker queues.",
	})

	UncommittedMessages = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "txn_uncommitted_messages",
		Help: "Fetched messages whose offset has not yet been committed.",
	})
)
