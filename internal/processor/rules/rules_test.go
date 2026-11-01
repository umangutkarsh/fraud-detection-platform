package rules

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"fraud-detection-platform/internal/contract"
)

var base = time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)

func newRedis(t *testing.T) *redis.Client {
	t.Helper()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func tx(id, user string, pence int64, country string, at time.Time) contract.Transaction {
	return contract.Transaction{
		ID: id, UserID: user, MerchantID: "merchant-1",
		AmountMinor: pence, Currency: "GBP", Country: country, Timestamp: at,
	}
}

// Guards against the bug where VelocityBaseScore defaulted to 0, so velocity
// hits were reported as reasons but could never quarantine anything.
func TestDefaultConfigVelocityCanQuarantine(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.VelocityBaseScore <= 0 {
		t.Fatalf("VelocityBaseScore must be > 0, got %d", cfg.VelocityBaseScore)
	}
	if cfg.VelocityBaseScore*2 < cfg.QuarantineThreshold {
		t.Fatalf("a <500ms burst (score %d) should reach the quarantine threshold %d",
			cfg.VelocityBaseScore*2, cfg.QuarantineThreshold)
	}
}

func TestHighValue(t *testing.T) {
	cfg := DefaultConfig()
	r := NewHighValue(DefaultFX(), cfg.HighValueGBPMinor, cfg.CrossBorderRiskScore)
	ctx := context.Background()

	if hit, _ := r.Evaluate(ctx, tx("a", "u", 10_000_00, "GB", base)); hit != nil {
		t.Fatal("exactly £10,000 must not fire (rule is strictly greater-than)")
	}
	if hit, _ := r.Evaluate(ctx, tx("b", "u", 10_000_01, "GB", base)); hit == nil || hit.Score != cfg.CrossBorderRiskScore {
		t.Fatalf("expected score %d hit, got %+v", cfg.CrossBorderRiskScore, hit)
	}
	// At 0.76 GBP/USD: 12,000 USD ≈ £9,120 -> no hit, 15,000 USD ≈ £11,400 -> hit.
	usd := tx("c", "u", 12_000_00, "US", base)
	usd.Currency = "USD"
	if hit, _ := r.Evaluate(ctx, usd); hit != nil {
		t.Fatal("12,000 USD is below £10k and must not fire")
	}
	usd.AmountMinor = 15_000_00
	if hit, _ := r.Evaluate(ctx, usd); hit == nil {
		t.Fatal("15,000 USD is above £10k and must fire")
	}
}

func TestUnknownCurrencyIsPermanentError(t *testing.T) {
	r := NewHighValue(DefaultFX(), 1, 60)
	bad := tx("a", "u", 100, "GB", base)
	bad.Currency = "XXX"
	if _, err := r.Evaluate(context.Background(), bad); !errors.Is(err, contract.ErrInvalidTransaction) {
		t.Fatalf("want ErrInvalidTransaction, got %v", err)
	}
}

func TestVelocity(t *testing.T) {
	ctx := context.Background()
	r := NewVelocity(newRedis(t), 2*time.Second, 30)

	if hit, err := r.Evaluate(ctx, tx("1", "u1", 100, "GB", base)); err != nil || hit != nil {
		t.Fatalf("first transaction must not fire: %v %v", hit, err)
	}
	// Gap of exactly 500ms: inside the window, but not the <500ms double-score band.
	if hit, _ := r.Evaluate(ctx, tx("2", "u1", 100, "GB", base.Add(500*time.Millisecond))); hit == nil || hit.Score != 30 {
		t.Fatalf("second transaction within 2s must fire with base score 30, got %+v", hit)
	}
	// Gap under 500ms doubles the score.
	if hit, _ := r.Evaluate(ctx, tx("3", "u1", 100, "GB", base.Add(600*time.Millisecond))); hit == nil || hit.Score != 60 {
		t.Fatalf("transaction <500ms after the previous one must score 60, got %+v", hit)
	}
	if hit, _ := r.Evaluate(ctx, tx("4", "u1", 100, "GB", base.Add(5*time.Second))); hit != nil {
		t.Fatal("transaction 4.4s after the previous one must not fire")
	}
	if hit, _ := r.Evaluate(ctx, tx("5", "other", 100, "GB", base.Add(5*time.Second))); hit != nil {
		t.Fatal("users must be tracked independently")
	}
}

// A retry of the same transaction (after a failed publish) must give the same
// answer, not compare the transaction with itself.
func TestVelocityIsIdempotentOnRetry(t *testing.T) {
	ctx := context.Background()
	r := NewVelocity(newRedis(t), 2*time.Second, 30)

	first := tx("1", "u", 100, "GB", base)
	second := tx("2", "u", 100, "GB", base.Add(time.Second))

	for i := 0; i < 3; i++ { // retrying the user's first transaction: never a prior one
		if hit, _ := r.Evaluate(ctx, first); hit != nil {
			t.Fatalf("first transaction must never fire, even when retried (attempt %d)", i+1)
		}
	}
	for i := 0; i < 3; i++ { // retrying the latest transaction replays its flag
		if hit, _ := r.Evaluate(ctx, second); hit == nil {
			t.Fatalf("evaluation #%d of the same flagged transaction must keep firing", i+1)
		}
	}
}

func TestVelocityOutOfOrder(t *testing.T) {
	ctx := context.Background()
	r := NewVelocity(newRedis(t), 2*time.Second, 30)
	_, _ = r.Evaluate(ctx, tx("1", "u", 100, "GB", base.Add(time.Second)))
	if hit, _ := r.Evaluate(ctx, tx("2", "u", 100, "GB", base)); hit == nil {
		t.Fatal("an older event arriving late but within the window must still count")
	}
}

func TestCrossBorderWindow(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultConfig()
	r := NewCrossBorder(newRedis(t), DefaultFX(), cfg)

	big := int64(11_000_00)
	for i := 0; i < cfg.CrossBorderMaxTxns; i++ {
		id := string(rune('a' + i))
		if hit, err := r.Evaluate(ctx, tx(id, "u", big, "FR", base.Add(time.Duration(i)*time.Second))); err != nil || hit != nil {
			t.Fatalf("transaction %d must not fire yet: %v %v", i+1, hit, err)
		}
	}
	hit, err := r.Evaluate(ctx, tx("z", "u", big, "FR", base.Add(10*time.Second)))
	if err != nil || hit == nil || hit.Score != cfg.CrossBorderRiskScore {
		t.Fatalf("6th large foreign transaction must fire: %v %v", hit, err)
	}
	// Retrying the same transaction must not inflate the count.
	again, _ := r.Evaluate(ctx, tx("z", "u", big, "FR", base.Add(10*time.Second)))
	if again == nil {
		t.Fatal("retry of the flagged transaction must still fire")
	}
	// Outside the 2 minute window the old entries age out.
	if hit, _ := r.Evaluate(ctx, tx("late", "u", big, "FR", base.Add(10*time.Minute))); hit != nil {
		t.Fatal("window must slide; old transactions must not count")
	}
}

func TestCrossBorderIgnoresHomeAndSmall(t *testing.T) {
	ctx := context.Background()
	r := NewCrossBorder(newRedis(t), DefaultFX(), DefaultConfig())
	for i := 0; i < 10; i++ {
		if hit, _ := r.Evaluate(ctx, tx(string(rune('a'+i)), "u", 50_000_00, "GB", base)); hit != nil {
			t.Fatal("home-country transactions are out of scope for this rule")
		}
		if hit, _ := r.Evaluate(ctx, tx(string(rune('A'+i)), "u", 100, "FR", base)); hit != nil {
			t.Fatal("small foreign transactions are out of scope for this rule")
		}
	}
}

func TestEngineScoring(t *testing.T) {
	ctx := context.Background()
	e := NewDefaultEngine(newRedis(t), DefaultFX(), DefaultConfig())

	res, err := e.Evaluate(ctx, tx("1", "u", 50_00, "GB", base))
	if err != nil || res.Decision != contract.DecisionApproved || res.IsFraudulent || res.RiskScore != 0 {
		t.Fatalf("small domestic transaction should be approved: %+v %v", res, err)
	}

	// Same user 1s later: velocity base score 30 < 50 threshold -> still approved.
	res, _ = e.Evaluate(ctx, tx("2", "u", 50_00, "GB", base.Add(time.Second)))
	if res.RiskScore != 30 || res.IsFraudulent || len(res.Reasons) != 1 {
		t.Fatalf("1s velocity hit must score 30 and pass: %+v", res)
	}

	// Same user, same instant: velocity doubles to 60 -> quarantined.
	res, _ = e.Evaluate(ctx, tx("3", "u", 50_00, "GB", base.Add(time.Second)))
	if res.RiskScore != 60 || res.Decision != contract.DecisionQuarantined || !res.IsFraudulent {
		t.Fatalf("burst under 500ms must be quarantined with score 60: %+v", res)
	}
}

func TestDefaultConfigScoresAreWired(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	e := NewDefaultEngine(rdb, DefaultFX(), DefaultConfig())

	now := time.Now()
	tx := func(id string, amount int64) contract.Transaction {
		return contract.Transaction{ID: id, UserID: "u", MerchantID: "m", AmountMinor: amount,
			Currency: "GBP", Country: "GB", Timestamp: now}
	}

	// one very large transaction is flagged but not quarantined
	r, _ := e.Evaluate(context.Background(), tx("big", 20_000_00))
	if r.RiskScore != 40 || r.IsFraudulent {
		t.Fatalf("high value alone: score=%d quarantined=%v", r.RiskScore, r.IsFraudulent)
	}

	// two transactions at the same instant from one user are quarantined
	_, _ = e.Evaluate(context.Background(), contract.Transaction{ID: "a", UserID: "v", MerchantID: "m", AmountMinor: 2500, Currency: "GBP", Country: "GB", Timestamp: now})
	r, _ = e.Evaluate(context.Background(), contract.Transaction{ID: "b", UserID: "v", MerchantID: "m", AmountMinor: 2500, Currency: "GBP", Country: "GB", Timestamp: now})
	if !r.IsFraudulent || r.RiskScore == 0 {
		t.Fatalf("velocity must contribute to the score: %+v", r)
	}
}
