package rules

import (
	"context"
	"fmt"
	"fraud-detection-platform/internal/contract"
)

type HighValue struct {
	fx        Converter
	threshold int64
	riskScore int
}

func NewHighValue(fx Converter, thresholdGBPMinor int64, riskScore int) *HighValue {
	return &HighValue{fx: fx, threshold: thresholdGBPMinor, riskScore: riskScore}
}

func (r *HighValue) Name() string { return "high_value" }

func (high_value *HighValue) Evaluate(_ context.Context, txn contract.Transaction) (*Hit, error) {
	gbp, err := high_value.fx.ToGBPMinor(txn.AmountMinor, txn.Currency)
	if err != nil {
		return nil, err
	}

	if gbp <= high_value.threshold {
		return nil, nil
	}

	formattedThreshold := float64(high_value.threshold) / 100.0

	return &Hit{
		Score:  high_value.riskScore,
		Reason: fmt.Sprintf("High value transaction (over £%.2f)", formattedThreshold),
	}, nil
}
