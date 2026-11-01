package handler

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"

	"fraud-detection-platform/internal/gateway/adapter/application/rest/middleware"
	"fraud-detection-platform/internal/gateway/domain/service"
)

type DecisionHandler struct {
	decisions *service.DecisionService
	log       *slog.Logger
}

func NewDecisionHandler(s *service.DecisionService, log *slog.Logger) *DecisionHandler {
	return &DecisionHandler{decisions: s, log: log}
}

// Get is GET /v1/transactions/:id, the read side of the async pipeline.
//
//	200 {"status":"approved"|"under_review", "evaluated_at":...}
//	202 {"status":"pending"}   accepted, not decided yet: poll again
//	404                        unknown id (or another merchant's id)
func (h *DecisionHandler) Get(c *gin.Context) {
	id := c.Param("id")
	if id == "" || len(id) > 128 {
		c.AbortWithStatusJSON(http.StatusBadRequest, middleware.ErrorBody(c, "invalid transaction id"))
		return
	}

	view, err := h.decisions.Lookup(c.Request.Context(), middleware.ClientIDFrom(c), id)
	switch {
	case err == nil && view.Status == service.StatusPending:
		c.Header("Retry-After", "1")
		c.JSON(http.StatusAccepted, gin.H{"id": view.ID, "status": view.Status, "request_id": middleware.RequestIDFrom(c)})
	case err == nil:
		c.JSON(http.StatusOK, gin.H{"id": view.ID, "status": view.Status, "evaluated_at": view.EvaluatedAt, "request_id": middleware.RequestIDFrom(c)})
	case errors.Is(err, service.ErrNotFound):
		c.AbortWithStatusJSON(http.StatusNotFound, middleware.ErrorBody(c, "transaction not found"))
	default:
		h.log.Error("decision lookup failed", "request_id", middleware.RequestIDFrom(c), "error", err)
		c.Header("Retry-After", "2")
		c.AbortWithStatusJSON(http.StatusServiceUnavailable, middleware.ErrorBody(c, "decision lookup temporarily unavailable"))
	}
}
