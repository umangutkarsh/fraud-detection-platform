package redisclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisOptions struct {
	Addr     string
	Password string
	TLS      bool
}

// NewClient builds the single shared Redis client (used by the decision store
// and the distributed rate limiter) with explicit timeouts, and fails fast if
// Redis is unreachable at startup.
func NewClient(opts RedisOptions) (*redis.Client, error) {
	o := &redis.Options{
		Addr:         opts.Addr,
		Password:     opts.Password,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  500 * time.Millisecond,
		WriteTimeout: 500 * time.Millisecond,
		PoolSize:     50,
		MinIdleConns: 5,
	}
	if opts.TLS {
		o.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	client := redis.NewClient(o)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("connect to redis at %s: %w", opts.Addr, err)
	}
	return client, nil
}
