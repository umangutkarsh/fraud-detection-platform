// Package bootstrap is the composition root: the only place that knows about
// every concrete adapter.
package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"fraud-detection-platform/internal/gateway/adapter/application/rest/handler"
	"fraud-detection-platform/internal/gateway/adapter/application/rest/middleware"
	"fraud-detection-platform/internal/gateway/adapter/application/rest/routes"
	"fraud-detection-platform/internal/gateway/adapter/infrastructure/messaging"
	"fraud-detection-platform/internal/gateway/adapter/infrastructure/observability"
	"fraud-detection-platform/internal/gateway/adapter/infrastructure/ratelimit"
	"fraud-detection-platform/internal/gateway/adapter/infrastructure/redisclient"
	"fraud-detection-platform/internal/gateway/adapter/infrastructure/resilience"
	"fraud-detection-platform/internal/gateway/adapter/infrastructure/store"
	"fraud-detection-platform/internal/gateway/config"
	"fraud-detection-platform/internal/gateway/domain/service"
)

// App holds the two servers and everything that must be closed on shutdown.
type App struct {
	Public  *http.Server
	Admin   *http.Server
	closers []func() error
}

func New(cfg config.Config, log *slog.Logger) (*App, error) {
	app := &App{}
	fail := func(err error) (*App, error) { _ = app.Close(); return nil, err }

	rdb, err := redisclient.NewClient(redisclient.RedisOptions{
		Addr: cfg.RedisAddr, Password: cfg.RedisPassword, TLS: cfg.RedisTLS,
	})
	if err != nil {
		return nil, err
	}
	app.closers = append(app.closers, rdb.Close)

	onBreaker := func(name, from, to string) {
		observability.BreakerState.WithLabelValues(name).Set(observability.BreakerStateValue(to))
		log.Warn("circuit breaker state change", "breaker", name, "from", from, "to", to)
	}

	// Kafka publisher, wrapped in a circuit breaker.
	kp, err := messaging.NewKafkaPublisher(messaging.KafkaOptions{
		Brokers: cfg.KafkaBrokers, Topic: cfg.TransactionsTopic, TLS: cfg.KafkaTLS,
		SASLMechanism: cfg.KafkaSASLMechanism, SASLUsername: cfg.KafkaSASLUsername, SASLPassword: cfg.KafkaSASLPassword,
	})
	if err != nil {
		return fail(err)
	}
	publisher := resilience.NewBreakerPublisher(kp, onBreaker)
	app.closers = append(app.closers, publisher.Close)

	// Rate limiters: Redis token bucket, falling back to a local one.
	newLimiter := func(scope string, rps float64, burst int) *ratelimit.Fallback {
		local := ratelimit.NewLocal(rps, burst)
		app.closers = append(app.closers, func() error { local.Close(); return nil })
		return ratelimit.NewFallback(ratelimit.NewRedis(rdb, "rl:"+scope+":", rps, burst), local, log,
			func() { observability.RateLimitFallback.Inc() })
	}
	ipLimiter := newLimiter("ip", cfg.IPRate, cfg.IPBurst)
	clientLimiter := newLimiter("client", cfg.ClientRate, cfg.ClientBurst)

	decisionStore := store.NewRedisDecisions(rdb)

	allow, err := middleware.NewIPAllowlist(cfg.AllowedCIDRs)
	if err != nil {
		return fail(err)
	}
	if allow.AllowsAll() {
		log.Warn("ALLOWED_CIDRS is empty: every source IP is allowed. Set it for production B2B use.")
	}

	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	// Trust X-Forwarded-For only from the listed load balancers. gin.Default()
	// trusts every client, which lets anyone spoof their IP.
	if err := router.SetTrustedProxies(cfg.TrustedProxies); err != nil {
		return fail(fmt.Errorf("TRUSTED_PROXIES: %w", err))
	}

	routes.Register(router, routes.Deps{
		Log:           log,
		Allowlist:     allow,
		Auth:          middleware.NewAuthenticator(cfg.JWTSecret, cfg.JWTIssuer, cfg.JWTAudience, log),
		IPLimiter:     ipLimiter,
		ClientLimiter: clientLimiter,
		Transactions:  handler.NewTransactionHandler(service.NewIngestionService(publisher, decisionStore, log), log),
		Decisions:     handler.NewDecisionHandler(service.NewDecisionService(decisionStore), log),
	})

	app.Public = &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second, // slowloris protection
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	app.Admin = &http.Server{
		Addr:              cfg.AdminAddr,
		Handler:           adminMux(rdb),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
	}
	return app, nil
}

// adminMux serves probes and metrics on a separate, non-public port.
func adminMux(rdb *redis.Client) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := rdb.Ping(ctx).Err(); err != nil {
			http.Error(w, "redis unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

// Close releases everything in reverse order of creation.
func (a *App) Close() error {
	var errs []error
	for i := len(a.closers) - 1; i >= 0; i-- {
		errs = append(errs, a.closers[i]())
	}
	return errors.Join(errs...)
}
