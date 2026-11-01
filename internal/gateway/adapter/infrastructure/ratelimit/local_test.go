package ratelimit

import (
	"context"
	"testing"
	"time"
)

var ctx = context.Background()

func TestLocalBurstThenReject(t *testing.T) {
	l := NewLocal(1, 3)
	defer l.Close()
	for i := 0; i < 3; i++ {
		if d, _ := l.Allow(ctx, "k"); !d.Allowed {
			t.Fatalf("request %d within burst must pass", i+1)
		}
	}
	d, _ := l.Allow(ctx, "k")
	if d.Allowed || d.RetryAfter <= 0 {
		t.Fatalf("4th request must be rejected with a retry hint: %+v", d)
	}
	if d, _ := l.Allow(ctx, "other"); !d.Allowed {
		t.Fatal("keys are independent")
	}
}

func TestLocalEvictsIdleKeys(t *testing.T) {
	l := NewLocal(1, 1)
	defer l.Close()
	_, _ = l.Allow(ctx, "stale")
	time.Sleep(5 * time.Millisecond)
	_, _ = l.Allow(ctx, "fresh")

	l.evictIdle(time.Now().Add(-2 * time.Millisecond)) // "stale" is older than the cutoff
	if l.Size() != 1 {
		t.Fatalf("only the idle key must be evicted, %d keys remain", l.Size())
	}
	l.evictIdle(time.Now().Add(time.Hour))
	if l.Size() != 0 {
		t.Fatal("everything older than the cutoff must go")
	}
}
