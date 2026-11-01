package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"fraud-detection-platform/internal/contract"
)

func decided(fraud bool) *contract.FraudResult {
	return &contract.FraudResult{
		Transaction:  contract.Transaction{ID: "t-1", MerchantID: "m-1"},
		IsFraudulent: fraud, RiskScore: 90, Reasons: []string{"secret rule detail"},
		EvaluatedAt: time.Now(),
	}
}

func TestLookupStatuses(t *testing.T) {
	ctx := context.Background()

	st := NewFakeStore()
	svc := NewDecisionService(st)
	if _, err := svc.Lookup(ctx, "m-1", "t-1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown id must be ErrNotFound, got %v", err)
	}

	st.receipts["m-1|t-1"] = true
	if v, err := svc.Lookup(ctx, "m-1", "t-1"); err != nil || v.Status != StatusPending {
		t.Fatalf("accepted but undecided must be pending: %+v %v", v, err)
	}

	st.decision = decided(false)
	if v, _ := svc.Lookup(ctx, "m-1", "t-1"); v.Status != StatusApproved || v.EvaluatedAt == nil {
		t.Fatalf("want approved: %+v", v)
	}

	st.decision = decided(true)
	if v, _ := svc.Lookup(ctx, "m-1", "t-1"); v.Status != StatusUnderReview {
		t.Fatalf("a flagged transaction is reported as under_review, never as fraud: %+v", v)
	}
}

func TestLookupIsTenantScoped(t *testing.T) {
	st := NewFakeStore()
	st.decision = decided(false) // belongs to m-1
	st.receipts["m-1|t-1"] = true
	svc := NewDecisionService(st)

	if _, err := svc.Lookup(context.Background(), "m-2", "t-1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("another merchant must get ErrNotFound, got %v", err)
	}
}

func TestLookupStoreOutage(t *testing.T) {
	st := NewFakeStore()
	st.err = errors.New("redis down")
	if _, err := NewDecisionService(st).Lookup(context.Background(), "m", "t"); !errors.Is(err, ErrDecisionUnavailable) {
		t.Fatalf("want ErrDecisionUnavailable, got %v", err)
	}
}
