package port

import (
	"context"
	"time"
)

// Decision is the outcome of a rate-limit check.
type Decision struct {
	Allowed    bool
	Remaining  int
	RetryAfter time.Duration
}

// RateLimiter returns an error only when the limiter itself is broken (for
// example Redis is down), which lets a wrapper fall back to another limiter.
type RateLimiter interface {
	Allow(ctx context.Context, key string) (Decision, error)
}
