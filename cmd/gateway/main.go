package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"fraud-detection-platform/internal/gateway/bootstrap"
	"fraud-detection-platform/internal/gateway/config"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return err
	}
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		lvl = slog.LevelInfo
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})).With("service", "api-gateway")
	slog.SetDefault(log)

	app, err := bootstrap.New(cfg, log)
	if err != nil {
		return err
	}

	serve := func(name string, srv *http.Server) {
		go func() {
			log.Info("listening", "server", name, "addr", srv.Addr)
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("server failed", "server", name, "error", err)
				os.Exit(1)
			}
		}()
	}
	serve("public", app.Public)
	serve("admin", app.Admin)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	stop()

	// Shutdown stops accepting new connections and waits for in-flight
	// requests, so a deploy does not drop customer traffic.
	log.Info("shutting down", "timeout", cfg.ShutdownTimeout)
	sctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := app.Public.Shutdown(sctx); err != nil {
		log.Error("public shutdown", "error", err)
	}
	_ = app.Admin.Shutdown(sctx)
	return app.Close()
}
