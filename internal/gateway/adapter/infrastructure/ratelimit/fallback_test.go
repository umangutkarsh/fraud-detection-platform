package ratelimit

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"fraud-detection-platform/internal/gateway/domain/port"
)

type failing struct{ calls int }

func (f *failing) Allow(context.Context, string) (port.Decision, error) {
	f.calls++
	return port.Decision{}, errors.New("redis down")
}

func TestFallbackToLocalWhenRedisFails(t *testing.T) {
	primary := &failing{}
	local := NewLocal(100, 2)
	defer local.Close()
	fallbacks := 0
	f := NewFallback(primary, local, slog.New(slog.NewTextHandler(io.Discard, nil)), func() { fallbacks++ })

	var results []bool
	for i := 0; i < 4; i++ {
		d, err := f.Allow(ctx, "k")
		if err != nil {
			t.Fatalf("fallback must absorb the error: %v", err)
		}
		results = append(results, d.Allowed)
	}
	if !results[0] || !results[1] || results[2] {
		t.Fatalf("local limiter (burst 2) must take over: %v", results)
	}
	if primary.calls != 1 {
		t.Fatalf("cooldown must stop hammering a dead Redis, primary called %d times", primary.calls)
	}
	if fallbacks != 4 {
		t.Fatalf("fallback metric = %d, want 4", fallbacks)
	}
}
