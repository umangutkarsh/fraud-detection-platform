package pipeline

import (
	"context"
	"fraud-detection-platform/internal/processor/config"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

// source is the part of *kafka.Reader the pipeline uses, so fetch, commit
// and shutdown logic can be tested without a broker.
type source interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Stats() kafka.ReaderStats
	Close() error
}

type Pipeline struct {
	cfg config.Config
	log *slog.Logger

	reader  source
	pub     io.Closer
	rdb     *redis.Client
	tracker *OffsetTracker
	proc    *Processor
	shards  []chan kafka.Message

	// Two contexts on purpose. fetchCtx stops intake at the start of shutdown.
	// workCtx stays alive while workers drain, so in-flight transactions can
	// still reach Redis and Kafka. (The original used a single context, so
	// cancelling it also broke the very work it was trying to finish.)
	fetchCtx    context.Context
	fetchCancel context.CancelFunc
	workCtx     context.Context
	workCancel  context.CancelFunc

	fetchWG  sync.WaitGroup
	workerWG sync.WaitGroup
	auxWG    sync.WaitGroup
	auxStop  chan struct{}

	ready atomic.Bool
}
