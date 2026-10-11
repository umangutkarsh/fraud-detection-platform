package pipeline

import (
	"sync"

	"github.com/segmentio/kafka-go"
)

type partitionOffsets struct {
	order      []int64                 // fetched offsets not yet committable, ascending
	done       map[int64]kafka.Message // completed out of order, waiting for the head
	ready      *kafka.Message          // highest contiguous completed message, awaiting commit
	maxTracked int64
}

type offsetTracker struct {
	mu      sync.Mutex
	parts   map[int]*partitionOffsets
	pending int
}

func newOffsetTracker() *offsetTracker {
	return &offsetTracker{parts: make(map[int]*partitionOffsets)}
}

// Track registers a fetched message. It must be called before the message is
// handed to a worker.
func (t *offsetTracker) Track(m kafka.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()

	ps := t.parts[m.Partition]
	if ps == nil {
		ps = &partitionOffsets{done: make(map[int64]kafka.Message), maxTracked: -1}
		t.parts[m.Partition] = ps
	}
	// An offset at or below one we have already seen means the partition was
	// revoked and re-assigned and the reader rewound to the committed position.
	// Drop stale state; redelivered messages are covered by the idempotency guard.
	if m.Offset <= ps.maxTracked {
		t.pending -= len(ps.order)
		ps.order = nil
		ps.done = make(map[int64]kafka.Message)
	}
	ps.maxTracked = m.Offset
	ps.order = append(ps.order, m.Offset)
	t.pending++
}

// Complete marks a message fully handled and advances the commit point if it
// unblocks a contiguous run.
func (t *offsetTracker) Complete(m kafka.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()

	ps := t.parts[m.Partition]
	if ps == nil || len(ps.order) == 0 || m.Offset < ps.order[0] {
		return // unknown or already committed
	}
	ps.done[m.Offset] = m

	for len(ps.order) > 0 {
		head := ps.order[0]
		msg, ok := ps.done[head]
		if !ok {
			break
		}
		delete(ps.done, head)
		ps.order = ps.order[1:]
		t.pending--
		cp := msg
		ps.ready = &cp
	}
}

// Drain returns one message per partition whose offset is safe to commit.
func (t *offsetTracker) Drain() []kafka.Message {
	t.mu.Lock()
	defer t.mu.Unlock()

	var out []kafka.Message
	for _, ps := range t.parts {
		if ps.ready != nil {
			out = append(out, *ps.ready)
			ps.ready = nil
		}
	}
	return out
}

// Restore puts back commits that failed so the next tick retries them.
func (t *offsetTracker) Restore(msgs []kafka.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, m := range msgs {
		ps := t.parts[m.Partition]
		if ps == nil {
			continue
		}
		if ps.ready == nil || m.Offset > ps.ready.Offset {
			cp := m
			ps.ready = &cp
		}
	}
}

// Pending is the number of fetched messages not yet committable.
func (t *offsetTracker) Pending() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.pending
}
