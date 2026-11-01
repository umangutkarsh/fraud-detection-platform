package resilience

import (
	"context"
	"errors"
	"testing"

	"github.com/sony/gobreaker"

	"fraud-detection-platform/internal/gateway/domain/port"
)

var ctx = context.Background()

type stub struct {
	calls int
	err   error
}

func (s *stub) Publish(context.Context, port.Event) error { s.calls++; return s.err }
func (s *stub) Close() error                              { return nil }

func TestBreakerOpensAfterConsecutiveFailuresAndFailsFast(t *testing.T) {
	inner := &stub{err: errors.New("broker down")}
	b := NewBreakerPublisher(inner, nil)

	for i := 0; i < 5; i++ {
		if err := b.Publish(ctx, port.Event{}); err == nil {
			t.Fatal("expected the broker error")
		}
	}
	err := b.Publish(ctx, port.Event{})
	if !errors.Is(err, gobreaker.ErrOpenState) {
		t.Fatalf("want an open circuit, got %v", err)
	}
	if inner.calls != 5 {
		t.Fatalf("an open circuit must not call the broker, calls=%d", inner.calls)
	}
}

func TestCancelledRequestsDoNotTripTheBreaker(t *testing.T) {
	inner := &stub{err: context.Canceled}
	b := NewBreakerPublisher(inner, nil)
	for i := 0; i < 20; i++ {
		_ = b.Publish(ctx, port.Event{})
	}
	inner.err = nil
	if err := b.Publish(ctx, port.Event{}); err != nil {
		t.Fatalf("client cancellations must not open the circuit: %v", err)
	}
}

func TestStateChangeIsReported(t *testing.T) {
	var from, to string
	b := NewBreakerPublisher(&stub{err: errors.New("down")}, func(_, f, t string) { from, to = f, t })
	for i := 0; i < 5; i++ {
		_ = b.Publish(ctx, port.Event{})
	}
	if from != "closed" || to != "open" {
		t.Fatalf("transition = %s -> %s", from, to)
	}
}
