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

// idempotency is a two-phase guard. The original single SETNX marked a
// transaction "seen" BEFORE the decision was published, so a crash between the
// two silently dropped the transaction on redelivery. Here a short-lived
// "processing" claim is upgraded to "done" only after the decision is
// published; a crashed claim simply expires and the message is reprocessed.
type idempotency struct {
	rdb           redis.Cmdable
	processingTTL time.Duration
	doneTTL       time.Duration
}

func newIdempotency(rdb redis.Cmdable) *idempotency {
	return &idempotency{rdb: rdb, processingTTL: 30 * time.Second, doneTTL: 24 * time.Hour}
}

// Keys come from contract.IdempotencyKey(merchant, id), so ids are unique per
// merchant rather than globally.
func (i *idempotency) Claim(ctx context.Context, key string) (claimState, error) {
	ok, err := i.rdb.SetNX(ctx, key, stateProcessing, i.processingTTL).Result()
	if err != nil {
		return 0, fmt.Errorf("claim %s: %w", key, err)
	}
	if ok {
		return claimAcquired, nil
	}

	val, err := i.rdb.Get(ctx, key).Result()
	switch {
	case errors.Is(err, redis.Nil):
		return claimBusy, nil // expired between the two calls; the caller retries
	case err != nil:
		return 0, fmt.Errorf("read claim %s: %w", key, err)
	case val == stateDone:
		return claimDone, nil
	default:
		return claimBusy, nil
	}
}

func (i *idempotency) MarkDone(ctx context.Context, key string) error {
	return i.rdb.Set(ctx, key, stateDone, i.doneTTL).Err()
}

// releaseScript deletes the claim only while it is still "processing", so a
// late Release can never erase a "done" marker.
var releaseScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
`)

func (i *idempotency) Release(ctx context.Context, key string) {
	// Best effort: if this fails the claim expires on its own.
	_ = releaseScript.Run(ctx, i.rdb, []string{key}, stateProcessing).Err()
}
