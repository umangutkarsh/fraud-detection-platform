package port

import "context"

type Event struct {
	Key     string
	Value   []byte
	Headers map[string]string
}

// EventPublisher publishes to the event log. Publish returns only after the
// broker has acknowledged the write.
type EventPublisher interface {
	Publish(ctx context.Context, e Event) error
	Close() error
}
