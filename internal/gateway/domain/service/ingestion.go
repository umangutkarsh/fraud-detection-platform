package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"fraud-detection-platform/internal/contract"
	"fraud-detection-platform/internal/gateway/domain/port"
)

var ErrIngestionUnavailable = errors.New("transaction ingestion temporarily unavailable")

// IngestionService is the front door of the pipeline: validate, stamp the
// authenticated merchant, publish to Kafka, record a receipt, acknowledge.
type IngestionService struct {
	publisher port.EventPublisher
	store     port.DecisionStore
	log       *slog.Logger
	timeout   time.Duration
	now       func() time.Time
}

func NewIngestionService(p port.EventPublisher, store port.DecisionStore, log *slog.Logger) *IngestionService {
	return &IngestionService{publisher: p, store: store, log: log, timeout: 3 * time.Second, now: time.Now}
}

// Submit returns nil only after Kafka has acknowledged the event, so a 202 to
// the client really means "durably accepted".
func (s *IngestionService) Submit(ctx context.Context, tx contract.Transaction, merchantID string) (contract.Transaction, error) {
	now := s.now()
	tx.Normalise(now)
	tx.MerchantID = merchantID // always the authenticated caller, never client-supplied
	if err := tx.Validate(now); err != nil {
		return tx, err
	}

	payload, err := json.Marshal(tx)
	if err != nil {
		return tx, fmt.Errorf("marshal transaction: %w", err)
	}

	pctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	err = s.publisher.Publish(pctx, port.Event{
		Key:   tx.UserID, // same user -> same partition -> ordered processing downstream
		Value: payload,
		Headers: map[string]string{
			"event_type":     "transaction.submitted",
			"transaction_id": tx.ID,
			"merchant_id":    tx.MerchantID,
			"schema_version": contract.SchemaVersion,
		},
	})
	if err != nil {
		return tx, fmt.Errorf("%w: %v", ErrIngestionUnavailable, err)
	}

	// Best effort: the event is already durable. The receipt only lets a later
	// lookup say "pending" instead of "not found".
	rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), 200*time.Millisecond)
	defer rcancel()
	if err := s.store.PutReceipt(rctx, tx.MerchantID, tx.ID); err != nil {
		s.log.Warn("could not store receipt", "tx_id", tx.ID, "error", err)
	}
	return tx, nil
}
