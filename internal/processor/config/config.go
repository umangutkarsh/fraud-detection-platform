package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Brokers         []string
	InputTopic      string
	DecisionsTopic  string // every decision (approved + quarantined) for downstream systems
	QuarantineTopic string // flagged transactions only (picked up by the case/saga service)
	DLQTopic        string
	ConsumerGroupID string

	RedisAddress  string
	RedisPassword string
	RedisTLS      bool

	KafkaTLS           bool
	KafkaSASLMechanism string // "", "plain", "scram-sha-256", "scram-sha-512"
	KafkaSASLUsername  string
	KafkaSASLPassword  string

	WorkerCount     int
	BufferSize      int
	MaxAttempts     int
	DecisionTTL     time.Duration // how long decisions stay queryable via the gateway
	CommitInterval  time.Duration
	ShutdownTimeout time.Duration

	MetricsAddr string
	LogLevel    string
	HomeCountry string
}

func Load() (Config, error) {
	return LoadFrom(flag.CommandLine, os.Args[1:], os.Getenv)
}

func LoadFrom(fs *flag.FlagSet, args []string, getenv func(string) string) (Config, error) {
	str := func(env, def string) string {
		if v := getenv(env); v != "" {
			return v
		}
		return def
	}
	integer := func(env string, def int) int {
		if v, err := strconv.Atoi(getenv(env)); err == nil {
			return v
		}
		return def
	}
	boolean := func(env string) bool {
		v, _ := strconv.ParseBool(getenv(env))
		return v
	}

	brokers := fs.String("brokers", str("KAFKA_BROKERS", "localhost:9092"), "comma-separated Kafka brokers")
	in := fs.String("in-topic", str("KAFKA_TOPIC_TRANSACTIONS", "transactions"), "input topic")
	decisions := fs.String("decisions-topic", str("KAFKA_TOPIC_DECISIONS", "decisions"), "topic for approved decisions")
	quarantine := fs.String("quarantine-topic", str("KAFKA_TOPIC_QUARANTINE", "quarantine"), "topic for flagged transactions")
	dlq := fs.String("dlq-topic", str("KAFKA_TOPIC_DLQ", "transactions.dlq"), "dead-letter topic")
	group := fs.String("group", str("KAFKA_GROUP_ID", "fraud-processor"), "consumer group id")
	redisAddr := fs.String("redis", str("REDIS_ADDR", "localhost:6379"), "Redis address")
	workers := fs.Int("workers", integer("WORKERS", 8), "worker goroutines (also the per-user ordering shards)")
	buffer := fs.Int("buffer", integer("BUFFER_SIZE", 1000), "total buffered messages across workers")
	attempts := fs.Int("max-attempts", integer("MAX_ATTEMPTS", 5), "processing attempts before dead-lettering")
	decisionTTL := fs.Duration("decision-ttl", dur(getenv, "DECISION_TTL", 168*time.Hour), "how long decisions remain queryable")
	commit := fs.Duration("commit-interval", 1*time.Second, "offset commit interval")
	shutdown := fs.Duration("shutdown-timeout", 20*time.Second, "max time to drain in-flight work on SIGTERM")
	metrics := fs.String("metrics-addr", str("METRICS_ADDR", ":2112"), "metrics/health listen address")
	logLevel := fs.String("log-level", str("LOG_LEVEL", "info"), "debug|info|warn|error")
	home := fs.String("home-country", str("HOME_COUNTRY", "GB"), "ISO country treated as domestic")

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	cfg := Config{
		Brokers:            splitList(*brokers),
		InputTopic:         *in,
		DecisionsTopic:     *decisions,
		QuarantineTopic:    *quarantine,
		DLQTopic:           *dlq,
		ConsumerGroupID:    *group,
		RedisAddress:       *redisAddr,
		RedisPassword:      getenv("REDIS_PASSWORD"),
		RedisTLS:           boolean("REDIS_TLS"),
		KafkaTLS:           boolean("KAFKA_TLS"),
		KafkaSASLMechanism: strings.ToLower(getenv("KAFKA_SASL_MECHANISM")),
		KafkaSASLUsername:  getenv("KAFKA_SASL_USERNAME"),
		KafkaSASLPassword:  getenv("KAFKA_SASL_PASSWORD"),
		WorkerCount:        *workers,
		BufferSize:         *buffer,
		MaxAttempts:        *attempts,
		DecisionTTL:        *decisionTTL,
		CommitInterval:     *commit,
		ShutdownTimeout:    *shutdown,
		MetricsAddr:        *metrics,
		LogLevel:           *logLevel,
		HomeCountry:        strings.ToUpper(*home),
	}
	return cfg, cfg.Validate()
}

func splitList(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func dur(getenv func(string) string, key string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(getenv(key)); err == nil {
		return d
	}
	return def
}

func (c Config) Validate() error {
	var errs []error
	if len(c.Brokers) == 0 {
		errs = append(errs, errors.New("at least one Kafka broker is required"))
	}
	if c.WorkerCount < 1 {
		errs = append(errs, errors.New("workers must be >= 1"))
	}
	if c.BufferSize < c.WorkerCount {
		errs = append(errs, fmt.Errorf("buffer (%d) must be >= workers (%d)", c.BufferSize, c.WorkerCount))
	}
	if c.MaxAttempts < 1 {
		errs = append(errs, errors.New("max-attempts must be >= 1"))
	}
	if c.DecisionTTL <= 0 {
		errs = append(errs, errors.New("decision-ttl must be positive"))
	}
	if c.CommitInterval <= 0 {
		errs = append(errs, errors.New("commit-interval must be positive"))
	}
	for name, topic := range map[string]string{
		"in-topic": c.InputTopic, "decisions-topic": c.DecisionsTopic,
		"quarantine-topic": c.QuarantineTopic, "dlq-topic": c.DLQTopic,
	} {
		if topic == "" {
			errs = append(errs, fmt.Errorf("%s must not be empty", name))
		}
	}
	if c.KafkaSASLMechanism != "" && (c.KafkaSASLUsername == "" || c.KafkaSASLPassword == "") {
		errs = append(errs, errors.New("KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required when a SASL mechanism is set"))
	}
	return errors.Join(errs...)
}
