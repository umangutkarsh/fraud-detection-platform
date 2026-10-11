package rules

import (
	"context"
	"fmt"
	"fraud-detection-platform/internal/contract"

	"github.com/redis/go-redis/v9"
)

type CrossBorder struct {
	rdb    redis.Cmdable
	fx     Converter
	config Config
}

func NewCrossBorder(rdb redis.Cmdable, fx Converter, cfg Config) *CrossBorder {
	return &CrossBorder{rdb: rdb, fx: fx, config: cfg}
}

func (cross_border *CrossBorder) Name() string { return "cross_border" }

func (cross_border *CrossBorder) Evaluate(ctx context.Context, txn contract.Transaction) (*Hit, error) {
	if txn.Country == "" || txn.Country == cross_border.config.HomeCountry {
		return nil, nil
	}

	gbp, err := cross_border.fx.ToGBPMinor(txn.AmountMinor, txn.Currency)
	if err != nil {
		return nil, err
	}

	if gbp < cross_border.config.CrossBorderMinGBPMinor {
		return nil, nil
	}

	key := "window:" + txn.UserID
	cutoff := txn.Timestamp.Add(-cross_border.config.AMLWindow).UnixMilli()

	pipe := cross_border.rdb.TxPipeline()
	pipe.ZRemRangeByScore(ctx, key, "-inf", fmt.Sprintf("%d", cutoff))
	pipe.ZAdd(ctx, key, redis.Z{Score: float64(txn.Timestamp.UnixMilli()), Member: txn.ScopedIDValue()})
	pipe.Expire(ctx, key, cross_border.config.AMLWindow+cross_border.config.AMLWindow/2)
	count := pipe.ZCard(ctx, key)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("cross-border window: %w", err)
	}

	if count.Val() <= int64(cross_border.config.CrossBorderMaxTxns) {
		return nil, nil
	}

	return &Hit{
		Score: cross_border.config.CrossBorderRiskScore,
		Reason: fmt.Sprintf(
			"Cross-border velocity: more than %d transactions over £%.2f within %s",
			cross_border.config.CrossBorderMaxTxns,
			float64(cross_border.config.CrossBorderMinGBPMinor)/100.0, // Using float formatting so decimals display correctly
			cross_border.config.AMLWindow,
		),
	}, nil

}
