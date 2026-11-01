package routes

import (
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"

	"fraud-detection-platform/internal/gateway/adapter/application/rest/handler"
	"fraud-detection-platform/internal/gateway/adapter/application/rest/middleware"
	"fraud-detection-platform/internal/gateway/domain/port"
)

// Deps are built once in bootstrap and injected, so routes can be tested
// with fakes and nothing here reads config or connects to anything.
type Deps struct {
	Log           *slog.Logger
	Allowlist     *middleware.IPAllowlist
	Auth          *middleware.Authenticator
	IPLimiter     port.RateLimiter
	ClientLimiter port.RateLimiter
	Transactions  *handler.TransactionHandler
	Decisions     *handler.DecisionHandler
}

// Register installs the middleware chain, ordered cheapest-first:
//
//	request id -> panic recovery -> logging/metrics
//	-> IP allowlist   (a CIDR check)
//	-> per-IP limit   (sheds floods before any crypto)
//	-> JWT auth       (HMAC verification)
//	-> per-client limit
//	-> per-route scope check
func Register(r *gin.Engine, d Deps) {
	r.Use(
		middleware.RequestID(),
		middleware.Recovery(d.Log),
		middleware.Observe(d.Log),
		d.Allowlist.Middleware(),
		middleware.RateLimit(d.IPLimiter, "ip", middleware.KeyByIP, d.Log),
		d.Auth.Middleware(),
		middleware.RateLimit(d.ClientLimiter, "client", middleware.KeyByClient, d.Log),
	)

	r.POST("/v1/transactions", middleware.RequireScope("transactions:write"), d.Transactions.Submit)
	r.GET("/v1/transactions/:id", middleware.RequireScope("transactions:read"), d.Decisions.Get)

	r.NoRoute(func(c *gin.Context) {
		c.JSON(http.StatusNotFound, middleware.ErrorBody(c, "not found"))
	})
}
