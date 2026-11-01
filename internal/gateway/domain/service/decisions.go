package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"fraud-detection-platform/internal/gateway/domain/port"
)

var (
	ErrNotFound            = errors.New("transaction not found")
	ErrDecisionUnavailable = errors.New("decision store unavailable")
)

// Public statuses. Internal rule names, scores and the word "fraud" are NOT
// exposed to merchants: telling a fraudster exactly which rule fired lets
// them tune around it.
const (
	StatusPending     = "pending"
	StatusApproved    = "approved"
	StatusUnderReview = "under_review"
)

type DecisionView struct {
	ID          string
	Status      string
	EvaluatedAt *time.Time
}

type DecisionService struct{ store port.DecisionStore }

func NewDecisionService(store port.DecisionStore) *DecisionService {
	return &DecisionService{store: store}
}

// Lookup is tenant-safe by construction: the store key includes the caller's
// merchant id (from the JWT), so one merchant can never read another's
// decision, and a foreign id is indistinguishable from a missing one.
func (s *DecisionService) Lookup(ctx context.Context, merchantID, id string) (DecisionView, error) {
	res, err := s.store.GetDecision(ctx, merchantID, id)
	if err != nil {
		return DecisionView{}, fmt.Errorf("%w: %v", ErrDecisionUnavailable, err)
	}
	if res != nil {
		status := StatusApproved
		if res.IsFraudulent {
			status = StatusUnderReview
		}
		at := res.EvaluatedAt
		return DecisionView{ID: id, Status: status, EvaluatedAt: &at}, nil
	}

	seen, err := s.store.HasReceipt(ctx, merchantID, id)
	if err != nil {
		return DecisionView{}, fmt.Errorf("%w: %v", ErrDecisionUnavailable, err)
	}
	if seen {
		return DecisionView{ID: id, Status: StatusPending}, nil
	}
	return DecisionView{}, ErrNotFound
}
