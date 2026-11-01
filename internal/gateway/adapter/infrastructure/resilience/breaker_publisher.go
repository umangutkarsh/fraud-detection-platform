package resilience

import (
	"context"
	"errors"
	"time"

	"github.com/sony/gobreaker"

	"fraud-detection-platform/internal/gateway/domain/port"
)

// BreakerPublisher wraps the Kafka publisher so that when the broker is
// unreachable the gateway fails fast with 503 instead of letting every
// request hang for the full write timeout and pile up goroutines.
type BreakerPublisher struct {
	inner port.EventPublisher
	cb    *gobreaker.CircuitBreaker
}

func NewBreakerPublisher(inner port.EventPublisher, onChange func(name, from, to string)) *BreakerPublisher {
	return &BreakerPublisher{
		inner: inner,
		cb: gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:        "kafka-publisher",
			MaxRequests: 2,
			Interval:    30 * time.Second,
			Timeout:     15 * time.Second,
			ReadyToTrip: func(c gobreaker.Counts) bool { return c.ConsecutiveFailures >= 5 },
			IsSuccessful: func(err error) bool {
				return err == nil || errors.Is(err, context.Canceled)
			},
			OnStateChange: func(n string, from, to gobreaker.State) {
				if onChange != nil {
					onChange(n, from.String(), to.String())
				}
			},
		}),
	}
}

func (b *BreakerPublisher) Publish(ctx context.Context, e port.Event) error {
	_, err := b.cb.Execute(func() (any, error) { return nil, b.inner.Publish(ctx, e) })
	return err
}

func (b *BreakerPublisher) Close() error { return b.inner.Close() }
