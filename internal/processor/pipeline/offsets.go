package pipeline

import (
	"sync"

	"github.com/segmentio/kafka-go"
)

type PartitionOffsets struct {
	order      []int64
	done       map[int64]kafka.Message
	ready      *kafka.Message
	maxTracked int64
}

type OffsetTracker struct {
	mu      sync.Mutex
	parts   map[int]*PartitionOffsets
	pending int
}

func NewOffsetTracker() *OffsetTracker {
	return &OffsetTracker{parts: make(map[int]*PartitionOffsets)}
}

func (tracker *OffsetTracker) Track(message *kafka.Message) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	partitionOffset := tracker.parts[message.Partition]
	if partitionOffset == nil {
		partitionOffset = &PartitionOffsets{done: make(map[int64]kafka.Message), maxTracked: -1}
		tracker.parts[message.Partition] = partitionOffset
	}

	if message.Offset <= partitionOffset.maxTracked {
		tracker.pending -= len(partitionOffset.order)
		partitionOffset.order = nil
		partitionOffset.done = make(map[int64]kafka.Message)
	}
	partitionOffset.maxTracked = message.Offset
	partitionOffset.order = append(partitionOffset.order, partitionOffset.maxTracked)
	tracker.pending++
}

func (tracker *OffsetTracker) Complete(message *kafka.Message) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	partitionOffset := tracker.parts[message.Partition]
	if partitionOffset == nil || len(partitionOffset.order) == 0 || message.Offset < partitionOffset.order[0] {
		return
	}

	partitionOffset.done[message.Offset] = *message

	for len(partitionOffset.order) > 0 {
		head := partitionOffset.order[0]
		message, ok := partitionOffset.done[head]
		if !ok {
			break
		}
		delete(partitionOffset.done, head)
		partitionOffset.order = partitionOffset.order[1:]
		tracker.pending--
		cp := message
		partitionOffset.ready = &cp
	}
}

func (tracker *OffsetTracker) Drain() []kafka.Message {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	var out []kafka.Message
	for _, partitionOffset := range tracker.parts {
		if partitionOffset.ready != nil {
			out = append(out, *partitionOffset.ready)
			partitionOffset.ready = nil
		}
	}
	return out
}

func (tracker *OffsetTracker) Restore(messages []kafka.Message) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	for _, message := range messages {
		partitionOffset := tracker.parts[message.Partition]
		if partitionOffset == nil {
			continue
		}
		if partitionOffset.ready == nil || message.Offset > partitionOffset.ready.Offset {
			cp := message
			partitionOffset.ready = &cp
		}
	}
}

func (tracker *OffsetTracker) Pending() int {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	return tracker.pending
}
