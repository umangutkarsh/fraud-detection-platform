// Package store is the Redis adapter for port.DecisionStore: the gateway's
// read-only view of decisions the processor wrote, plus its own receipts.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"fraud-detection-platform/internal/contract"
	"fraud-detection-platform/internal/gateway/domain/port"
)

const receiptTTL = 7 * 24 * time.Hour

type RedisDecisions struct{ rdb redis.Cmdable }

func NewRedisDecisions(rdb redis.Cmdable) port.DecisionStore { return &RedisDecisions{rdb: rdb} }

func (r *RedisDecisions) GetDecision(ctx context.Context, merchantID, id string) (*contract.FraudResult, error) {
	raw, err := r.rdb.Get(ctx, contract.DecisionKey(merchantID, id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get decision: %w", err)
	}
	var res contract.FraudResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return nil, fmt.Errorf("decode decision: %w", err)
	}
	return &res, nil
}

func (r *RedisDecisions) PutReceipt(ctx context.Context, merchantID, id string) error {
	return r.rdb.SetNX(ctx, contract.ReceiptKey(merchantID, id), "1", receiptTTL).Err()
}

func (r *RedisDecisions) HasReceipt(ctx context.Context, merchantID, id string) (bool, error) {
	n, err := r.rdb.Exists(ctx, contract.ReceiptKey(merchantID, id)).Result()
	if err != nil {
		return false, fmt.Errorf("check receipt: %w", err)
	}
	return n > 0, nil
}
