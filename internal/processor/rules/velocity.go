package rules

import (
	"context"
	"fmt"
	"fraud-detection-platform/internal/contract"
	"time"

	"github.com/redis/go-redis/v9"
)

var velocityScript = redis.NewScript(`
local cur = redis.call('HMGET', KEYS[1], 'id', 'ts', 'pts')
local ts = tonumber(ARGV[1])
if cur[1] == ARGV[3] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  if cur[3] then return {1, ts - tonumber(cur[3])} end
  return {0, 0}
end
if cur[2] then
  redis.call('HSET', KEYS[1], 'id', ARGV[3], 'ts', ARGV[1], 'pts', cur[2])
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return {1, ts - tonumber(cur[2])}
end
redis.call('HSET', KEYS[1], 'id', ARGV[3], 'ts', ARGV[1])
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return {0, 0}
`)

type Velocity struct {
	rdb       redis.Scripter
	window    time.Duration
	baseScore int
}

func NewVelocity(rdb redis.Scripter, window time.Duration, baseScore int) *Velocity {
	return &Velocity{rdb: rdb, window: window, baseScore: baseScore}
}

func (velocity *Velocity) Name() string { return "velocity" }

func (velocity *Velocity) Evaluate(ctx context.Context, txn contract.Transaction) (*Hit, error) {
	res, err := velocityScript.Run(ctx, velocity.rdb,
		[]string{"velocity:" + txn.UserID},
		txn.Timestamp.UnixMilli(), (velocity.window * 5).Milliseconds(), txn.ScopedIDValue(),
	).Int64Slice()
	if err != nil {
		return nil, fmt.Errorf("velocity script: %w", err)
	}

	hasPrev, gapMs := res[0] == 1, res[1]
	if gapMs < 0 { // out-of-order delivery: use the absolute gap
		gapMs = -gapMs
	}
	if !hasPrev || gapMs >= velocity.window.Milliseconds() {
		return nil, nil
	}

	finalScore := velocity.baseScore
	if gapMs < 500 {
		finalScore = velocity.baseScore * 2
	}

	return &Hit{
		Score:  finalScore,
		Reason: fmt.Sprintf("Velocity limit breached: multiple transactions within %s (Gap: %dms)", velocity.window, gapMs),
	}, nil
}
