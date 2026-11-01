package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"fraud-detection-platform/internal/contract"
	"fraud-detection-platform/internal/gateway/domain/port"
)

type FakeStore struct {
	receipts map[string]bool
	decision *contract.FraudResult
	err      error
}

func NewFakeStore() *FakeStore { return &FakeStore{receipts: map[string]bool{}} }

func (f *FakeStore) GetDecision(_ context.Context, m, id string) (*contract.FraudResult, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.decision != nil && f.decision.Transaction.MerchantID == m && f.decision.Transaction.ID == id {
		return f.decision, nil
	}
	return nil, nil
}

func (f *FakeStore) PutReceipt(_ context.Context, m, id string) error {
	if f.err != nil {
		return f.err
	}
	f.receipts[m+"|"+id] = true
	return nil
}

func (f *FakeStore) HasReceipt(_ context.Context, m, id string) (bool, error) {
	return f.receipts[m+"|"+id], f.err
}

type recPublisher struct {
	events []port.Event
	err    error
}

func (r *recPublisher) Publish(_ context.Context, e port.Event) error {
	if r.err != nil {
		return r.err
	}
	r.events = append(r.events, e)
	return nil
}
func (r *recPublisher) Close() error { return nil }

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func goodTx() contract.Transaction {
	return contract.Transaction{ID: "t-1", UserID: "u-1", AmountMinor: 1999, Currency: "gbp", Country: "gb"}
}

func TestSubmitNormalisesStampsMerchantAndKeysByUser(t *testing.T) {
	p := &recPublisher{}
	svc := NewIngestionService(p, NewFakeStore(), quietLog())
	tx := goodTx()
	tx.MerchantID = "spoofed"

	out, err := svc.Submit(context.Background(), tx, "merchant-7")
	if err != nil {
		t.Fatal(err)
	}
	if len(p.events) != 1 || p.events[0].Key != "u-1" || p.events[0].Headers["transaction_id"] != "t-1" {
		t.Fatalf("unexpected event: %+v", p.events)
	}
	var sent contract.Transaction
	_ = json.Unmarshal(p.events[0].Value, &sent)
	if sent.MerchantID != "merchant-7" {
		t.Fatalf("merchant must come from the token, got %q", sent.MerchantID)
	}
	if sent.Currency != "GBP" || sent.Country != "GB" || sent.Timestamp.IsZero() || out.ID != "t-1" {
		t.Fatalf("payload not normalised: %+v", sent)
	}
}

func TestSubmitRejectsInvalidWithoutPublishing(t *testing.T) {
	p := &recPublisher{}
	svc := NewIngestionService(p, NewFakeStore(), quietLog())
	bad := goodTx()
	bad.AmountMinor = 0
	if _, err := svc.Submit(context.Background(), bad, "m"); !errors.Is(err, contract.ErrInvalidTransaction) {
		t.Fatalf("want ErrInvalidTransaction, got %v", err)
	}
	future := goodTx()
	future.Timestamp = time.Now().Add(time.Hour)
	if _, err := svc.Submit(context.Background(), future, "m"); !errors.Is(err, contract.ErrInvalidTransaction) {
		t.Fatalf("future timestamp must be rejected, got %v", err)
	}
	if len(p.events) != 0 {
		t.Fatal("invalid transactions must never reach Kafka")
	}
}

func TestSubmitSurfacesPublishFailureAsUnavailable(t *testing.T) {
	svc := NewIngestionService(&recPublisher{err: errors.New("broker down")}, NewFakeStore(), quietLog())
	if _, err := svc.Submit(context.Background(), goodTx(), "m"); !errors.Is(err, ErrIngestionUnavailable) {
		t.Fatalf("want ErrIngestionUnavailable, got %v", err)
	}
}

func TestReceiptWrittenOnlyAfterKafkaAck(t *testing.T) {
	st := NewFakeStore()
	svc := NewIngestionService(&recPublisher{}, st, quietLog())
	if _, err := svc.Submit(context.Background(), goodTx(), "m-1"); err != nil {
		t.Fatal(err)
	}
	if !st.receipts["m-1|t-1"] {
		t.Fatal("an accepted transaction must leave a receipt scoped to the merchant")
	}

	st2 := NewFakeStore()
	svc2 := NewIngestionService(&recPublisher{err: errors.New("down")}, st2, quietLog())
	_, _ = svc2.Submit(context.Background(), goodTx(), "m-1")
	if len(st2.receipts) != 0 {
		t.Fatal("no receipt may exist for a transaction Kafka did not accept")
	}
}

func TestReceiptFailureDoesNotFailTheRequest(t *testing.T) {
	st := NewFakeStore()
	st.err = errors.New("redis down")
	svc := NewIngestionService(&recPublisher{}, st, quietLog())
	if _, err := svc.Submit(context.Background(), goodTx(), "m-1"); err != nil {
		t.Fatalf("the event is durable in Kafka; a receipt failure must not surface: %v", err)
	}
}
