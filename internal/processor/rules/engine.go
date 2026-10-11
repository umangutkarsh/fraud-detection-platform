package rules

import (
	"context"
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
}
