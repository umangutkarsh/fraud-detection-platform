package pipeline

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type claimState int

const (
	claimAcquired claimState = iota // this worker owns the transaction now
	claimDone                       // a previous attempt fully completed it
	claimBusy                       // another worker holds an unexpired claim
)

const (
	stateProcessing = "processing"
	stateDone       = "done"
)

type Idempotency struct {
	redisDB       redis.Cmdable
	processingTTL time.Duration
	doneTTL       time.Duration
}

func NewIdempotency(rdb redis.Cmdable) *Idempotency {
	return &Idempotency{redisDB: rdb, processingTTL: 30 * time.Second, doneTTL: 24 * time.Hour}
}

func (idempotency *Idempotency) Claim(ctx context.Context, key string) (claimState, error) {
	ok, err := idempotency.redisDB.SetNX(ctx, key, stateProcessing, idempotency.processingTTL).Result()
	if err != nil {
		return 0, fmt.Errorf("claim %s: %w", key, err)
	}

	if ok {
		return claimAcquired, nil
	}

	val, err := idempotency.redisDB.Get(ctx, key).Result()
	switch {
	case errors.Is(err, redis.Nil):
		return claimBusy, nil

	case err != nil:
		return 0, fmt.Errorf("read claim %s: %w", key, err)

	case val == stateDone:
		return claimDone, nil

	default:
		return claimBusy, nil
	}
}

func (idempotency *Idempotency) MarkDone(ctx context.Context, key string) error {
	return idempotency.redisDB.Set(ctx, key, stateDone, idempotency.doneTTL).Err()
}

var releaseScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
`)

func (idempotency *Idempotency) Release(ctx context.Context, key string) {
	_ = releaseScript.Run(ctx, idempotency.redisDB, []string{key}, stateProcessing).Err()
}
