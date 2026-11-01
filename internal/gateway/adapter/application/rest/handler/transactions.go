package handler

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"

	"fraud-detection-platform/internal/contract"
	"fraud-detection-platform/internal/gateway/adapter/application/rest/middleware"
	"fraud-detection-platform/internal/gateway/adapter/infrastructure/observability"
	"fraud-detection-platform/internal/gateway/domain/service"
)

type TransactionHandler struct {
	ingest *service.IngestionService
	log    *slog.Logger
}

func NewTransactionHandler(s *service.IngestionService, log *slog.Logger) *TransactionHandler {
	return &TransactionHandler{ingest: s, log: log}
}

// Submit is POST /v1/transactions. It answers 202 once Kafka has durably
// stored the event; the fraud decision arrives asynchronously. Clients retry
// with the SAME transaction id on 5xx/timeout: the pipeline is idempotent on
// id, so a retry can never be processed twice.
func (h *TransactionHandler) Submit(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 64<<10)
	dec := json.NewDecoder(c.Request.Body)
	dec.DisallowUnknownFields()

	var tx contract.Transaction
	if err := dec.Decode(&tx); err != nil {
		observability.TransactionsRejected.WithLabelValues("malformed").Inc()
		c.AbortWithStatusJSON(http.StatusBadRequest, middleware.ErrorBody(c, "malformed JSON body"))
		return
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) { // reject trailing data
		observability.TransactionsRejected.WithLabelValues("malformed").Inc()
		c.AbortWithStatusJSON(http.StatusBadRequest, middleware.ErrorBody(c, "unexpected data after JSON body"))
		return
	}
	// Idempotency-Key is an accepted alias for the transaction id.
	if tx.ID == "" {
		tx.ID = c.GetHeader("Idempotency-Key")
	}

	accepted, err := h.ingest.Submit(c.Request.Context(), tx, middleware.ClientIDFrom(c))
	switch {
	case err == nil:
		observability.TransactionsAccepted.Inc()
		c.JSON(http.StatusAccepted, gin.H{"id": accepted.ID, "status": "accepted", "request_id": middleware.RequestIDFrom(c)})
	case errors.Is(err, contract.ErrInvalidTransaction):
		observability.TransactionsRejected.WithLabelValues("invalid").Inc()
		c.AbortWithStatusJSON(http.StatusUnprocessableEntity, gin.H{"error": err.Error(), "request_id": middleware.RequestIDFrom(c)})
	default:
		observability.TransactionsRejected.WithLabelValues("unavailable").Inc()
		h.log.Error("ingestion failed", "request_id", middleware.RequestIDFrom(c), "tx_id", accepted.ID, "error", err)
		c.Header("Retry-After", "5")
		c.AbortWithStatusJSON(http.StatusServiceUnavailable, middleware.ErrorBody(c, "ingestion temporarily unavailable, retry with the same id"))
	}
}
