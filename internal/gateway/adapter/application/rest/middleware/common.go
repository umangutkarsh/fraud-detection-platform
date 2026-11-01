package middleware

import (
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"regexp"
	"time"

	"github.com/gin-gonic/gin"

	"fraud-detection-platform/internal/gateway/adapter/infrastructure/observability"
)

const (
	ctxRequestID = "request_id"
	ctxClientID  = "client_id"
)

var validRequestID = regexp.MustCompile(`^[A-Za-z0-9._-]{8,64}$`)

// RequestID accepts a well-formed inbound X-Request-Id (so traces join up
// across services) or generates one. Malformed values are replaced, which
// stops log injection through that header.
func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader("X-Request-Id")
		if !validRequestID.MatchString(id) {
			var b [12]byte
			_, _ = rand.Read(b[:])
			id = hex.EncodeToString(b[:])
		}
		c.Set(ctxRequestID, id)
		c.Header("X-Request-Id", id)
		c.Next()
	}
}

func RequestIDFrom(c *gin.Context) string { return c.GetString(ctxRequestID) }
func ClientIDFrom(c *gin.Context) string  { return c.GetString(ctxClientID) }

// Recovery turns a panic into a logged 500 instead of a dropped connection.
func Recovery(log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if r := recover(); r != nil {
				log.Error("panic recovered", "panic", r, "request_id", RequestIDFrom(c), "path", c.Request.URL.Path)
				c.AbortWithStatusJSON(http.StatusInternalServerError, ErrorBody(c, "internal error"))
			}
		}()
		c.Next()
	}
}

// Observe records one structured log line and the Prometheus metrics per
// request. It logs the route template, never headers or bodies (which carry
// tokens and payment data).
func Observe(log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		elapsed := time.Since(start)
		status := c.Writer.Status()

		observability.HTTPRequests.WithLabelValues(route, c.Request.Method, statusClass(status)).Inc()
		observability.HTTPDuration.WithLabelValues(route, c.Request.Method).Observe(elapsed.Seconds())

		log.Info("request",
			"request_id", RequestIDFrom(c), "method", c.Request.Method, "route", route,
			"status", status, "duration_ms", elapsed.Milliseconds(),
			"client_id", ClientIDFrom(c), "client_ip", c.ClientIP(), "bytes", c.Writer.Size())
	}
}

func statusClass(s int) string {
	switch {
	case s >= 500:
		return "5xx"
	case s >= 400:
		return "4xx"
	case s >= 300:
		return "3xx"
	default:
		return "2xx"
	}
}

// ErrorBody is the uniform error envelope. Messages are generic on purpose;
// details go to the log, correlated by request_id.
func ErrorBody(c *gin.Context, msg string) gin.H {
	return gin.H{"error": msg, "request_id": RequestIDFrom(c)}
}
