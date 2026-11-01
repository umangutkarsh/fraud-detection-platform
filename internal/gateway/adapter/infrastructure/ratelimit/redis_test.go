package ratelimit

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func NewTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestRedisBucketIsSharedAcrossGatewayReplicas(t *testing.T) {
	rdb := NewTestRedis(t)
	// Two "replicas" with their own limiter objects but one Redis.
	a := NewRedis(rdb, "rl:", 0.001, 5)
	b := NewRedis(rdb, "rl:", 0.001, 5)

	allowed := 0
	for i := 0; i < 10; i++ {
		lim := a
		if i%2 == 1 {
			lim = b
		}
		d, err := lim.Allow(ctx, "client-1")
		if err != nil {
			t.Fatal(err)
		}
		if d.Allowed {
			allowed++
		}
	}
	if allowed != 5 {
		t.Fatalf("burst of 5 must be enforced across replicas, %d allowed", allowed)
	}
	d, _ := a.Allow(ctx, "client-1")
	if d.Allowed || d.RetryAfter <= 0 {
		t.Fatalf("exhausted bucket must give a retry hint: %+v", d)
	}
	if d, _ := b.Allow(ctx, "client-2"); !d.Allowed {
		t.Fatal("other keys are unaffected")
	}
}
