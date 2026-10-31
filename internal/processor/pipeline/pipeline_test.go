package pipeline

import (
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
