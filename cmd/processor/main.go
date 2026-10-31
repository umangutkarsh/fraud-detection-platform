package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"fraud-detection-platform/internal/processor/config"
	"fraud-detection-platform/internal/processor/pipeline"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := newLogger(cfg.LogLevel)
	slog.SetDefault(log)

	p, err := pipeline.New(cfg, log)
	if err != nil {
		return err
	}

	admin := newAdminServer(cfg.MetricsAddr, p)
	go func() {
		log.Info("admin server listening", "addr", cfg.MetricsAddr)
		if err := admin.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("admin server failed", "error", err)
		}
	}()

	p.Start()
	log.Info("pipeline running",
		"workers", cfg.WorkerCount, "input_topic", cfg.InputTopic,
		"decisions_topic", cfg.DecisionsTopic, "quarantine_topic", cfg.QuarantineTopic, "dlq_topic", cfg.DLQTopic)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	stop() // a second SIGINT now kills the process immediately

	log.Info("shutting down", "timeout", cfg.ShutdownTimeout)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	// Stop the pipeline first (it flips readiness to false), then the admin
	// server, so probes and the final scrape still work during the drain.
	stopErr := p.Stop(shutdownCtx)
	_ = admin.Shutdown(shutdownCtx)
	if stopErr != nil {
		return stopErr
	}
	log.Info("stopped cleanly")
	return nil
}

func newAdminServer(addr string, p *pipeline.Pipeline) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := p.Ready(ctx); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l})).With("service", "processor")
}
