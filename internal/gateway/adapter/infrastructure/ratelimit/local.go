// Package ratelimit provides three rate limiters behind port.RateLimiter:
// an in-process token bucket, a Redis-backed distributed token bucket, and a
// wrapper that degrades from the second to the first when Redis is unhealthy.
package ratelimit

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"fraud-detection-platform/internal/gateway/domain/port"
)

type localEntry struct {
	lim      *rate.Limiter
	lastSeen time.Time
}

// Local is a per-process token bucket. It is only correct for a single gateway
// instance (N replicas would each allow the full rate), so production uses it
// as the Redis fallback. Idle keys are evicted, otherwise one limiter per
// distinct IP would accumulate forever: a slow memory leak and a DoS vector.
type Local struct {
	mu      sync.Mutex
	entries map[string]*localEntry
	rate    rate.Limit
	burst   int
	ttl     time.Duration
	stop    chan struct{}
	once    sync.Once
}

func NewLocal(rps float64, burst int) *Local { return newLocal(rps, burst, 5*time.Minute) }

func newLocal(rps float64, burst int, ttl time.Duration) *Local {
	l := &Local{
		entries: make(map[string]*localEntry),
		rate:    rate.Limit(rps),
		burst:   burst,
		ttl:     ttl,
		stop:    make(chan struct{}),
	}
	go l.janitor()
	return l
}

func (l *Local) Allow(_ context.Context, key string) (port.Decision, error) {
	now := time.Now()

	l.mu.Lock()
	e, ok := l.entries[key]
	if !ok {
		e = &localEntry{lim: rate.NewLimiter(l.rate, l.burst)}
		l.entries[key] = e
	}
	e.lastSeen = now
	l.mu.Unlock()

	res := e.lim.ReserveN(now, 1)
	if !res.OK() {
		return port.Decision{Allowed: false}, nil
	}
	if delay := res.DelayFrom(now); delay > 0 {
		res.CancelAt(now) // we are rejecting, so give the token back
		return port.Decision{Allowed: false, RetryAfter: delay}, nil
	}
	return port.Decision{Allowed: true, Remaining: int(e.lim.TokensAt(now))}, nil
}

func (l *Local) janitor() {
	t := time.NewTicker(l.ttl / 2)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			l.evictIdle(time.Now().Add(-l.ttl))
		case <-l.stop:
			return
		}
	}
}

func (l *Local) evictIdle(cutoff time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for k, e := range l.entries {
		if e.lastSeen.Before(cutoff) {
			delete(l.entries, k)
		}
	}
}

func (l *Local) Close() { l.once.Do(func() { close(l.stop) }) }

// Size reports tracked keys (used by tests and metrics).
func (l *Local) Size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}
