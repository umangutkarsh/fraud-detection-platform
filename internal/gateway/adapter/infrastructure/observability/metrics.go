// Package observability defines the gateway's Prometheus instruments. Labels
// use route TEMPLATES (e.g. /transactions/{id}), never raw URLs or client IDs,
// to keep cardinality bounded.
package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_http_requests_total", Help: "HTTP requests handled.",
	}, []string{"route", "method", "status"})

	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "gateway_http_request_duration_seconds", Help: "Request latency.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	}, []string{"route", "method"})

	RateLimited = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_rate_limited_total", Help: "Requests rejected with 429, by scope.",
	}, []string{"scope"})

	RateLimitFallback = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_ratelimit_fallback_total", Help: "Checks served by the local limiter because Redis was unavailable.",
	})

	BreakerState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gateway_circuit_breaker_state", Help: "0=closed 1=half-open 2=open.",
	}, []string{"name"})

	TransactionsAccepted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_transactions_accepted_total", Help: "Transactions published to Kafka.",
	})

	TransactionsRejected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_transactions_rejected_total", Help: "Transactions not accepted, by reason.",
	}, []string{"reason"})
)

// BreakerStateValue maps gobreaker state names to the gauge value.
func BreakerStateValue(state string) float64 {
	switch state {
	case "half-open":
		return 1
	case "open":
		return 2
	default:
		return 0
	}
}
