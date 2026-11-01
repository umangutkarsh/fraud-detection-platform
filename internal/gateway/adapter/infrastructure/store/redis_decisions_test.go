package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"fraud-detection-platform/internal/contract"
)

// The key layout is a contract with the processor. This test writes exactly
// what the processor writes and reads it back through the gateway adapter.
func TestReadsWhatTheProcessorWrites(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	s := NewRedisDecisions(rdb)
	ctx := context.Background()

	if got, err := s.GetDecision(ctx, "m-1", "t-1"); err != nil || got != nil {
		t.Fatalf("absent decision must be (nil, nil): %v %v", got, err)
	}

	res := contract.FraudResult{
		Transaction: contract.Transaction{ID: "t-1", MerchantID: "m-1"},
		Decision:    contract.DecisionQuarantined, IsFraudulent: true, RiskScore: 90, EvaluatedAt: time.Now().UTC(),
	}
	raw, _ := json.Marshal(res)
	mr.Set(contract.DecisionKey("m-1", "t-1"), string(raw))

	got, err := s.GetDecision(ctx, "m-1", "t-1")
	if err != nil || got == nil || !got.IsFraudulent {
		t.Fatalf("got %+v %v", got, err)
	}
	if other, _ := s.GetDecision(ctx, "m-2", "t-1"); other != nil {
		t.Fatal("another merchant must not see this decision")
	}
}

func TestReceipts(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	s := NewRedisDecisions(rdb)
	ctx := context.Background()

	if ok, _ := s.HasReceipt(ctx, "m-1", "t-1"); ok {
		t.Fatal("no receipt yet")
	}
	if err := s.PutReceipt(ctx, "m-1", "t-1"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.HasReceipt(ctx, "m-1", "t-1"); !ok {
		t.Fatal("receipt must exist")
	}
	if ok, _ := s.HasReceipt(ctx, "m-2", "t-1"); ok {
		t.Fatal("receipts are merchant-scoped")
	}
	if ttl := mr.TTL(contract.ReceiptKey("m-1", "t-1")); ttl <= 0 {
		t.Fatalf("receipts must expire, ttl=%v", ttl)
	}
}
