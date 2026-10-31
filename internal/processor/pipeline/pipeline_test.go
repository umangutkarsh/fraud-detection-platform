package pipeline

import (
	"context"
	"sync"
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestShardingKeepsUsersTogether(t *testing.T) {
	const shards = 8
	m := kafka.Message{Key: []byte("user-42")}
	first := shardFor(m, shards)
	for i := 0; i < 100; i++ {
		if shardFor(m, shards) != first {
			t.Fatal("same key must always map to the same shard")
		}
	}
	seen := map[int]bool{}
	for i := 0; i < 200; i++ {
		seen[shardFor(kafka.Message{Key: []byte{byte(i), byte(i >> 3), 'x'}}, shards)] = true
	}
	if len(seen) < shards-1 {
		t.Fatalf("keys should spread across shards, only hit %d of %d", len(seen), shards)
	}
}

// FakeSource feeds a fixed list of messages, then blocks like a real reader.
type FakeSource struct {
	msgs      chan kafka.Message
	mu        sync.Mutex
	committed map[int]int64
	closed    bool
}

func NewFakeSource(msgs []kafka.Message) *FakeSource {
	ch := make(chan kafka.Message, len(msgs))
	for _, m := range msgs {
		ch <- m
	}
	return &FakeSource{msgs: ch, committed: map[int]int64{}}
}

func (f *FakeSource) FetchMessage(ctx context.Context) (kafka.Message, error) {
	select {
	case m := <-f.msgs:
		return m, nil
	case <-ctx.Done():
		return kafka.Message{}, ctx.Err()
	}
}

func (f *FakeSource) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, m := range msgs {
		if m.Offset > f.committed[m.Partition] {
			f.committed[m.Partition] = m.Offset
		}
	}
	return nil
}

func (f *FakeSource) Stats() kafka.ReaderStats { return kafka.ReaderStats{} }
func (f *FakeSource) Close() error             { f.closed = true; return nil }

func (f *FakeSource) offsets() map[int]int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := map[int]int64{}
	for k, v := range f.committed {
		cp[k] = v
	}
	return cp
}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }
