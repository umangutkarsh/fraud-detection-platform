package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"fraud-detection-platform/internal/contract"
	"time"

	"github.com/redis/go-redis/v9"
)

type DecisionStore interface {
	Save(ctx context.Context, result contract.FraudResult) error
}

type RedisDecisionStore struct {
	redisDB redis.Cmdable
	ttl     time.Duration
}

func NewRedisDecisionStore(redisDB redis.Cmdable, ttl time.Duration) *RedisDecisionStore {
	return &RedisDecisionStore{redisDB: redisDB, ttl: ttl}
}

func (redisStore *RedisDecisionStore) Save(ctx context.Context, result contract.FraudResult) error {
	payload, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal decision: %w", err)
	}

	key := contract.DecisionKey(result.Transaction.MerchantID, result.Transaction.ID)
	if err := redisStore.redisDB.Set(ctx, key, payload, redisStore.ttl).Err(); err != nil {
		return fmt.Errorf("store decision: %w", err)
	}

	return nil
}
