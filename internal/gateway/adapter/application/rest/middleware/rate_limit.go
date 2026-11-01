package middleware

import (
	"log/slog"
	"math"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"fraud-detection-platform/internal/gateway/adapter/infrastructure/observability"
	"fraud-detection-platform/internal/gateway/domain/port"
)

// KeyFunc picks what to rate-limit on.
type KeyFunc func(*gin.Context) string

func KeyByIP(c *gin.Context) string     { return c.ClientIP() }
func KeyByClient(c *gin.Context) string { return ClientIDFrom(c) }

// RateLimit enforces a limiter and tells well-behaved clients when to retry
// (Retry-After, RateLimit-Remaining). It is used twice: once per IP BEFORE
// authentication (so floods are dropped before any JWT crypto), and once per
// authenticated client AFTER it.
func RateLimit(limiter port.RateLimiter, scope string, key KeyFunc, log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		d, err := limiter.Allow(c.Request.Context(), scope+":"+key(c))
		if err != nil {
			// Both limiters failed. Fail open for availability, but loudly.
			log.Error("rate limiter failed, allowing request", "scope", scope, "error", err)
			c.Next()
			return
		}
		c.Header("RateLimit-Remaining", strconv.Itoa(d.Remaining))
		if !d.Allowed {
			observability.RateLimited.WithLabelValues(scope).Inc()
			secs := int(math.Ceil(d.RetryAfter.Seconds()))
			c.Header("Retry-After", strconv.Itoa(max(secs, 1)))
			c.AbortWithStatusJSON(http.StatusTooManyRequests, ErrorBody(c, "too many requests"))
			return
		}
		c.Next()
	}
}
