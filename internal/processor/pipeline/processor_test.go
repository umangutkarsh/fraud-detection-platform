package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fraud-detection-platform/internal/contract"
	"fraud-detection-platform/internal/processor/rules"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type FakePublisher struct {
	mu            sync.Mutex
	decisions     []contract.FraudResult
	dlq           []string
	failDecisions int
	failDLQ       int
}

func (f *FakePublisher) PublishDecision(_ context.Context, r contract.FraudResult) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failDecisions > 0 {
		f.failDecisions--
		return errors.New("broker unavailable")
	}
	f.decisions = append(f.decisions, r)
	return nil
}

func (f *FakePublisher) PublishDLQ(_ context.Context, _ kafka.Message, reason string, _ error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failDLQ > 0 {
		f.failDLQ--
		return errors.New("dlq unavailable")
	}
	f.dlq = append(f.dlq, reason)
	return nil
}

func kmsg(t *testing.T, tx contract.Transaction) kafka.Message {
	t.Helper()
	b, err := json.Marshal(tx)
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Key: []byte(tx.UserID), Value: b, Time: time.Now()}
}

func goodTx(id string) contract.Transaction {
	return contract.Transaction{ID: id, UserID: "user-" + id, MerchantID: "merchant-1", AmountMinor: 25_00, Currency: "GBP", Country: "GB", Timestamp: time.Now()}
}

func (f *FakePublisher) counts() (decisions, dlq int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.decisions), len(f.dlq)
}

func TestSameIDFromTwoMerchantsIsNotADuplicate(t *testing.T) {
	pub := &FakePublisher{}
	p, _ := NewTestProcessor(t, pub)

	a, b := goodTx("shared"), goodTx("shared")
	b.MerchantID, b.UserID = "merchant-2", "someone-else"
	for _, tx := range []contract.Transaction{a, b} {
		if err := p.Process(context.Background(), kmsg(t, tx)); err != nil {
			t.Fatal(err)
		}
	}
	if d, _ := pub.counts(); d != 2 {
		t.Fatalf("merchant ids are separate namespaces; want 2 decisions, got %d", d)
	}
}

func NewTestProcessor(t *testing.T, pub *FakePublisher) (*Processor, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	return &Processor{
		Eval:      rules.NewDefaultEngine(rdb, rules.DefaultFX(), rules.DefaultConfig()),
		Idemp:     NewIdempotency(rdb),
		Publisher: pub,
		Store:     NewRedisDecisionStore(rdb, time.Hour),
		Retry:     RetryPolicy{MaxAttempts: 3, Base: time.Millisecond, Max: 5 * time.Millisecond},
		Log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, mr
}

func TestApprovedTransactionIsPublishedOnce(t *testing.T) {
	pub := &FakePublisher{}
	p, _ := NewTestProcessor(t, pub)

	if err := p.Process(context.Background(), kmsg(t, goodTx("t1"))); err != nil {
		t.Fatal(err)
	}
	if d, q := pub.counts(); d != 1 || q != 0 {
		t.Fatalf("decisions=%d dlq=%d, want 1/0", d, q)
	}
	if pub.decisions[0].Decision != contract.DecisionApproved {
		t.Fatalf("decision = %q", pub.decisions[0].Decision)
	}
}

func TestRedeliveryAfterCompletionIsSkipped(t *testing.T) {
	pub := &FakePublisher{}
	p, _ := NewTestProcessor(t, pub)
	m := kmsg(t, goodTx("t1"))

	_ = p.Process(context.Background(), m)
	_ = p.Process(context.Background(), m) // Kafka redelivery
	if d, _ := pub.counts(); d != 1 {
		t.Fatalf("a redelivered transaction must not be decided twice, got %d decisions", d)
	}
}

func TestDecisionIsStoredForTheGatewayAfterPublish(t *testing.T) {
	pub := &FakePublisher{}
	p, mr := NewTestProcessor(t, pub)

	if err := p.Process(context.Background(), kmsg(t, goodTx("t1"))); err != nil {
		t.Fatal(err)
	}
	raw, err := mr.Get(contract.DecisionKey("merchant-1", "t1"))
	if err != nil {
		t.Fatalf("decision must be queryable after processing: %v", err)
	}
	var stored contract.FraudResult
	if err := json.Unmarshal([]byte(raw), &stored); err != nil || stored.Decision != contract.DecisionApproved {
		t.Fatalf("stored decision = %+v (%v)", stored, err)
	}
	if mr.Exists(contract.DecisionKey("merchant-2", "t1")) {
		t.Fatal("decisions must be scoped per merchant")
	}
}

// The core bug in the original: the idempotency key was set before the decision
// was published, so a failure in between lost the transaction forever.
func TestFailedPublishIsRetriedNotLost(t *testing.T) {
	pub := &FakePublisher{failDecisions: 2}
	p, _ := NewTestProcessor(t, pub)

	if err := p.Process(context.Background(), kmsg(t, goodTx("t1"))); err != nil {
		t.Fatal(err)
	}
	if d, q := pub.counts(); d != 1 || q != 0 {
		t.Fatalf("after two transient failures the decision must still be published once; decisions=%d dlq=%d", d, q)
	}
}

func TestRetriesExhaustedGoToDLQ(t *testing.T) {
	pub := &FakePublisher{failDecisions: 100}
	p, mr := NewTestProcessor(t, pub)

	if err := p.Process(context.Background(), kmsg(t, goodTx("t1"))); err != nil {
		t.Fatal(err)
	}
	if d, q := pub.counts(); d != 0 || q != 1 || pub.dlq[0] != "retries_exhausted" {
		t.Fatalf("want 1 retries_exhausted DLQ record, got decisions=%d dlq=%v", d, pub.dlq)
	}
	if mr.Exists(contract.IdempotencyKey("merchant-1", "t1")) {
		t.Fatal("the claim must be released so a later replay can succeed")
	}
}

type FlakyStore struct {
	inner DecisionStore
	fails int
}

func (f *FlakyStore) Save(ctx context.Context, r contract.FraudResult) error {
	if f.fails > 0 {
		f.fails--
		return errors.New("redis blip")
	}
	return f.inner.Save(ctx, r)
}

func TestStoreFailureIsRetriedSoGatewayNeverSeesPendingForever(t *testing.T) {
	pub := &FakePublisher{}
	p, miniRedis := NewTestProcessor(t, pub)
	p.Store = &FlakyStore{inner: p.Store, fails: 1}

	if err := p.Process(context.Background(), kmsg(t, goodTx("t1"))); err != nil {
		t.Fatal(err)
	}
	if !miniRedis.Exists(contract.DecisionKey("merchant-1", "t1")) {
		t.Fatal("decision must be stored after a transient store failure")
	}
	if _, q := pub.counts(); q != 0 {
		t.Fatal("one transient failure must not dead-letter the message")
	}
}
