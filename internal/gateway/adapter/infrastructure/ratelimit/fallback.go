package ratelimit

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"fraud-detection-platform/internal/gateway/domain/port"
)

// Fallback uses the primary (Redis) limiter and degrades to the secondary
// (local) one when the primary errors or is slow. After a failure the primary
// is skipped for `cooldown`, so an outage costs one short timeout, not one per
// request. The gateway stays up and still limits (per node) while Redis is down.
type Fallback struct {
	primary, secondary port.RateLimiter
	timeout, cooldown  time.Duration
	skipUntil          atomic.Int64 // unix nanos
	onFallback         func()
	log                *slog.Logger
}

func NewFallback(primary, secondary port.RateLimiter, log *slog.Logger, onFallback func()) *Fallback {
	return &Fallback{
		primary: primary, secondary: secondary,
		timeout: 50 * time.Millisecond, cooldown: 5 * time.Second,
		onFallback: onFallback, log: log,
	}
}

func (f *Fallback) Allow(ctx context.Context, key string) (port.Decision, error) {
	if time.Now().UnixNano() >= f.skipUntil.Load() {
		pctx, cancel := context.WithTimeout(ctx, f.timeout)
		d, err := f.primary.Allow(pctx, key)
		cancel()
		if err == nil {
			return d, nil
		}
		f.skipUntil.Store(time.Now().Add(f.cooldown).UnixNano())
		f.log.Warn("distributed rate limiter unavailable, using local limiter", "error", err, "cooldown", f.cooldown)
	}
	if f.onFallback != nil {
		f.onFallback()
	}
	return f.secondary.Allow(ctx, key)
}
