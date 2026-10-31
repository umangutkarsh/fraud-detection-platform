package pipeline

import (
	"context"
	"fraud-detection-platform/internal/processor/config"
	"fraud-detection-platform/internal/processor/rules"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
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

func TestEndToEndCommitsOnlyAfterProcessingAndDrainsOnStop(t *testing.T) {
	const perPartition = 25
	var msgs []kafka.Message
	for part := 0; part < 2; part++ {
		for off := 0; off < perPartition; off++ {
			tx := goodTx(string(rune('a'+part)) + "-" + time.Duration(off).String())
			m := kmsg(t, tx)
			m.Partition, m.Offset = part, int64(off)
			msgs = append(msgs, m)
		}
	}

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	pub := &FakePublisher{}
	src := NewFakeSource(msgs)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	fetchCtx, fetchCancel := context.WithCancel(context.Background())
	workCtx, workCancel := context.WithCancel(context.Background())
	const workers = 4
	shards := make([]chan kafka.Message, workers)
	for i := range shards {
		shards[i] = make(chan kafka.Message, 8)
	}

	p := &Pipeline{
		cfg:     config.Config{CommitInterval: 10 * time.Millisecond},
		log:     log,
		reader:  src,
		pub:     nopCloser{},
		rdb:     rdb,
		tracker: NewOffsetTracker(),
		proc: &Processor{
			Eval:      rules.NewDefaultEngine(rdb, rules.DefaultFX(), rules.DefaultConfig()),
			Idemp:     NewIdempotency(rdb),
			Publisher: pub,
			Store:     NewRedisDecisionStore(rdb, time.Hour),
			Retry:     RetryPolicy{MaxAttempts: 3, Base: time.Millisecond, Max: 5 * time.Millisecond},
			Log:       log,
		},
		shards:   shards,
		fetchCtx: fetchCtx, fetchCancel: fetchCancel,
		workCtx: workCtx, workCancel: workCancel,
		auxStop: make(chan struct{}),
	}

	p.Start()

	deadline := time.Now().Add(5 * time.Second)
	for {
		if d, _ := pub.counts(); d == 2*perPartition {
			break
		}
		if time.Now().After(deadline) {
			d, q := pub.counts()
			t.Fatalf("timed out: decisions=%d dlq=%d", d, q)
		}
		time.Sleep(5 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}

	// Stop performs a final flush, so every partition is committed up to its last offset.
	got := src.offsets()
	for part := 0; part < 2; part++ {
		if got[part] != perPartition-1 {
			t.Fatalf("partition %d committed offset %d, want %d", part, got[part], perPartition-1)
		}
	}
	if !src.closed {
		t.Fatal("reader must be closed on shutdown")
	}
	if d, q := pub.counts(); d != 2*perPartition || q != 0 {
		t.Fatalf("decisions=%d dlq=%d", d, q)
	}
	if err := p.Ready(context.Background()); err == nil {
		t.Fatal("a stopped pipeline must report not-ready")
	}
}

func TestPoisonMessageDoesNotBlockCommitsBehindIt(t *testing.T) {
	good := kmsg(t, goodTx("after-poison"))
	good.Offset = 1
	bad := kafka.Message{Key: []byte("u"), Value: []byte("%%%"), Offset: 0}

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	pub := &FakePublisher{}
	src := NewFakeSource([]kafka.Message{bad, good})
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	fetchCtx, fetchCancel := context.WithCancel(context.Background())
	workCtx, workCancel := context.WithCancel(context.Background())
	p := &Pipeline{
		cfg: config.Config{CommitInterval: 10 * time.Millisecond}, log: log, reader: src, pub: nopCloser{}, rdb: rdb,
		tracker: NewOffsetTracker(),
		proc: &Processor{
			Eval:      rules.NewDefaultEngine(rdb, rules.DefaultFX(), rules.DefaultConfig()),
			Idemp:     NewIdempotency(rdb),
			Publisher: pub,
			Store:     NewRedisDecisionStore(rdb, time.Hour),
			Retry:     RetryPolicy{MaxAttempts: 2, Base: time.Millisecond, Max: time.Millisecond},
			Log:       log,
		},
		shards:   []chan kafka.Message{make(chan kafka.Message, 4)},
		fetchCtx: fetchCtx, fetchCancel: fetchCancel, workCtx: workCtx, workCancel: workCancel,
		auxStop: make(chan struct{}),
	}
	p.Start()
	time.Sleep(200 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = p.Stop(ctx)

	if d, q := pub.counts(); d != 1 || q != 1 {
		t.Fatalf("want 1 decision + 1 DLQ record, got %d / %d", d, q)
	}
	if got := src.offsets()[0]; got != 1 {
		t.Fatalf("both messages are resolved so offset 1 must be committed, got %d", got)
	}
}
