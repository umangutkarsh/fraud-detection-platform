package pipeline

import (
	"context"
	"crypto/tls"
	"fmt"
	"fraud-detection-platform/internal/processor/config"
	"fraud-detection-platform/internal/processor/rules"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

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

func New(cfg config.Config, log *slog.Logger) (*Pipeline, error) {
	dialer, transport, err := KafkaSecurity(cfg)
	if err != nil {
		return nil, err
	}

	redisOpts := &redis.Options{
		Addr:         cfg.RedisAddress,
		Password:     cfg.RedisPassword,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
		PoolSize:     cfg.WorkerCount * 2,
		MinIdleConns: 2,
	}
	if cfg.RedisTLS {
		redisOpts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	rdb := redis.NewClient(redisOpts)

	// Fail fast at startup rather than on the first message.
	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("connect to redis at %s: %w", cfg.RedisAddress, err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.Brokers,
		Topic:       cfg.InputTopic,
		GroupID:     cfg.ConsumerGroupID,
		Dialer:      dialer,
		StartOffset: kafka.FirstOffset, // only used when the group has no committed offset
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     250 * time.Millisecond,
		// We commit explicitly through the offset tracker.
		CommitInterval: 0,
		ErrorLogger: kafka.LoggerFunc(func(format string, args ...any) {
			log.Error("kafka reader", "detail", fmt.Sprintf(format, args...))
		}),
	})

	pub := NewKafkaPublisher(cfg.Brokers, transport, cfg.DecisionsTopic, cfg.QuarantineTopic, cfg.DLQTopic)

	rcfg := rules.DefaultConfig()
	rcfg.HomeCountry = cfg.HomeCountry
	engine := rules.NewDefaultEngine(rdb, rules.DefaultFX(), rcfg)

	fetchCtx, fetchCancel := context.WithCancel(context.Background())
	workCtx, workCancel := context.WithCancel(context.Background())

	perShard := max(1, cfg.BufferSize/cfg.WorkerCount)
	shards := make([]chan kafka.Message, cfg.WorkerCount)
	for i := range shards {
		shards[i] = make(chan kafka.Message, perShard)
	}

	return &Pipeline{
		cfg:     cfg,
		log:     log,
		reader:  reader,
		pub:     pub,
		rdb:     rdb,
		tracker: NewOffsetTracker(),
		proc: &Processor{
			Eval:      engine,
			Idemp:     NewIdempotency(rdb),
			Publisher: pub,
			Store:     NewRedisDecisionStore(rdb, cfg.DecisionTTL),
			Retry:     RetryPolicy{MaxAttempts: cfg.MaxAttempts, Base: 100 * time.Millisecond, Max: 2 * time.Second},
			Log:       log,
		},
		shards:      shards,
		fetchCtx:    fetchCtx,
		fetchCancel: fetchCancel,
		workCtx:     workCtx,
		workCancel:  workCancel,
		auxStop:     make(chan struct{}),
	}, nil
}
