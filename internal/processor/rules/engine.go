package rules

import (
	"context"
	"fmt"
	"fraud-detection-platform/internal/contract"
	"time"
)

// Hit is a rule firing.
type Hit struct {
	Score  int
	Reason string
}

// Rule evaluates one transaction. A nil Hit means "no signal". Implementations
// must be safe for concurrent use and IDEMPOTENT: the pipeline retries failed
// attempts, so evaluating the same transaction twice must give the same answer.
type Rule interface {
	Name() string
	Evaluate(ctx context.Context, tx contract.Transaction) (*Hit, error)
}

// Config holds every tunable threshold.
type Config struct {
	HomeCountry            string
	HighValueGBPMinor      int64
	VelocityWindow         time.Duration
	AMLWindow              time.Duration
	CrossBorderMaxTxns     int
	CrossBorderMinGBPMinor int64
	QuarantineThreshold    int
}

func DefaultConfig() Config {
	return Config{
		HomeCountry:            "GB",
		HighValueGBPMinor:      10_000_00, // £10,000
		VelocityWindow:         2 * time.Second,
		AMLWindow:              2 * time.Minute,
		CrossBorderMaxTxns:     5,
		CrossBorderMinGBPMinor: 10_000_00,
		QuarantineThreshold:    50,
	}
}

type Engine struct {
	rules     []Rule
	threshold int
	now       func() time.Time
}

func NewEngine(threshold int, rules ...Rule) *Engine {
	return &Engine{rules: rules, threshold: threshold, now: time.Now}
}

func (e *Engine) Evaluate(ctx context.Context, txn contract.Transaction) (contract.FraudResult, error) {
	score := 0
	reasons := make([]string, 0, len(e.rules))
	fired := make([]string, 0, len(e.rules))

	for _, r := range e.rules {
		hit, err := r.Evaluate(ctx, txn)
		if err != nil {
			return contract.FraudResult{}, fmt.Errorf("rule %s: %w", r.Name(), err)
		}
		if hit == nil {
			continue
		}

		score += hit.Score
		reasons = append(reasons, hit.Reason)
		fired = append(fired, r.Name())
	}

	// Count only after every rule succeeded, so retries don't double count.
	for _, name := range fired {
		metrics.RuleHits.WithLabelValues(name).Inc()
	}

	// return &contract.FraudResult{}, nil
}
