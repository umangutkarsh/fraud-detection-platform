package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"fraud-detection-platform/internal/gateway/domain/port"
)

// tokenBucketScript is an atomic check-and-take. Redis runs a script as one
// uninterrupted unit, so two gateway replicas can never both read the same
// balance and both spend it (the race a GET-then-SET approach has). Time comes
// from the Redis server (TIME), so clock skew between gateway nodes is
// irrelevant.
//
//	KEYS[1] bucket   ARGV[1] tokens/second   ARGV[2] burst capacity
//	returns {allowed(0|1), remainingTokens, retryAfterMillis}
var tokenBucketScript = redis.NewScript(`
local rate  = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then
  tokens = burst
  ts = now
end

tokens = math.min(burst, tokens + math.max(0, now - ts) * rate / 1000)

local allowed = 0
local retry = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  retry = math.ceil((1 - tokens) * 1000 / rate)
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(burst / rate * 1000) + 1000)
return {allowed, math.floor(tokens), retry}
`)

// Redis is a cluster-wide token bucket shared by every gateway replica.
type Redis struct {
	rdb    redis.Scripter
	prefix string
	rate   float64
	burst  int
}

func NewRedis(rdb redis.Scripter, prefix string, rps float64, burst int) *Redis {
	return &Redis{rdb: rdb, prefix: prefix, rate: rps, burst: burst}
}

func (r *Redis) Allow(ctx context.Context, key string) (port.Decision, error) {
	res, err := tokenBucketScript.Run(ctx, r.rdb, []string{r.prefix + key}, r.rate, r.burst).Int64Slice()
	if err != nil {
		return port.Decision{}, fmt.Errorf("rate limit script: %w", err)
	}
	if len(res) != 3 {
		return port.Decision{}, fmt.Errorf("rate limit script: unexpected reply %v", res)
	}
	return port.Decision{
		Allowed:    res[0] == 1,
		Remaining:  int(res[1]),
		RetryAfter: time.Duration(res[2]) * time.Millisecond,
	}, nil
}
