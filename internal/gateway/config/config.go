// Package config loads gateway configuration from environment variables.
// Secrets have no defaults, so nobody can mint valid tokens from the repo.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Service is an optional reverse-proxied upstream (for example a future
// case-management service for quarantined transactions). None by default.
type Service struct {
	Prefix  string // e.g. "/cases"; callers need the scope "<name>:access"
	URL     string
	Timeout time.Duration
}

// Scope is the JWT scope required to reach this upstream.
func (s Service) Scope() string { return strings.TrimPrefix(s.Prefix, "/") + ":access" }

type Config struct {
	HTTPAddr        string
	AdminAddr       string // /metrics, /healthz, /readyz: never on the public listener
	ShutdownTimeout time.Duration
	LogLevel        string

	// TrustedProxies lists load balancers whose X-Forwarded-For we believe.
	// Empty (the default) means "trust nobody": the client IP is the TCP peer.
	TrustedProxies []string
	AllowedCIDRs   []string // empty = allow all (logged as a warning at startup)

	JWTSecret   []byte
	JWTIssuer   string
	JWTAudience string

	RedisAddr     string
	RedisPassword string
	RedisTLS      bool

	KafkaBrokers       []string
	TransactionsTopic  string
	KafkaTLS           bool
	KafkaSASLMechanism string
	KafkaSASLUsername  string
	KafkaSASLPassword  string

	IPRate      float64
	IPBurst     int
	ClientRate  float64
	ClientBurst int

	Services []Service
}

func Load(getenv func(string) string) (Config, error) {
	str := func(k, def string) string {
		if v := getenv(k); v != "" {
			return v
		}
		return def
	}
	num := func(k string, def float64) float64 {
		if v, err := strconv.ParseFloat(getenv(k), 64); err == nil {
			return v
		}
		return def
	}
	dur := func(k string, def time.Duration) time.Duration {
		if v, err := time.ParseDuration(getenv(k)); err == nil {
			return v
		}
		return def
	}
	flag := func(k string) bool { v, _ := strconv.ParseBool(getenv(k)); return v }

	cfg := Config{
		HTTPAddr:           str("HTTP_ADDR", ":5000"),
		AdminAddr:          str("ADMIN_ADDR", ":9100"),
		ShutdownTimeout:    dur("SHUTDOWN_TIMEOUT", 20*time.Second),
		LogLevel:           str("LOG_LEVEL", "info"),
		TrustedProxies:     list(getenv("TRUSTED_PROXIES")),
		AllowedCIDRs:       list(getenv("ALLOWED_CIDRS")),
		JWTSecret:          []byte(getenv("JWT_SECRET")),
		JWTIssuer:          str("JWT_ISSUER", "api-gateway"),
		JWTAudience:        getenv("JWT_AUDIENCE"),
		RedisAddr:          str("REDIS_ADDR", "localhost:6379"),
		RedisPassword:      getenv("REDIS_PASSWORD"),
		RedisTLS:           flag("REDIS_TLS"),
		KafkaBrokers:       list(str("KAFKA_BROKERS", "localhost:9092")),
		TransactionsTopic:  str("KAFKA_TOPIC_TRANSACTIONS", "transactions"),
		KafkaTLS:           flag("KAFKA_TLS"),
		KafkaSASLMechanism: strings.ToLower(getenv("KAFKA_SASL_MECHANISM")),
		KafkaSASLUsername:  getenv("KAFKA_SASL_USERNAME"),
		KafkaSASLPassword:  getenv("KAFKA_SASL_PASSWORD"),
		IPRate:             num("RATE_LIMIT_IP_RPS", 50),
		IPBurst:            int(num("RATE_LIMIT_IP_BURST", 100)),
		ClientRate:         num("RATE_LIMIT_CLIENT_RPS", 100),
		ClientBurst:        int(num("RATE_LIMIT_CLIENT_BURST", 200)),
	}

	var err error
	cfg.Services, err = parseUpstreams(getenv("UPSTREAMS"), dur("UPSTREAM_TIMEOUT", 5*time.Second))
	if err != nil {
		return cfg, err
	}
	return cfg, cfg.Validate()
}

func (c Config) Validate() error {
	var errs []error
	if len(c.JWTSecret) < 32 {
		errs = append(errs, errors.New("JWT_SECRET is required and must be at least 32 bytes"))
	}
	if len(c.KafkaBrokers) == 0 {
		errs = append(errs, errors.New("KAFKA_BROKERS is required"))
	}
	if c.IPRate <= 0 || c.IPBurst < 1 || c.ClientRate <= 0 || c.ClientBurst < 1 {
		errs = append(errs, errors.New("rate limits must be positive"))
	}
	if c.KafkaSASLMechanism != "" && (c.KafkaSASLUsername == "" || c.KafkaSASLPassword == "") {
		errs = append(errs, errors.New("KAFKA_SASL_USERNAME/PASSWORD required when KAFKA_SASL_MECHANISM is set"))
	}
	for _, s := range c.Services {
		u, err := url.Parse(s.URL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			errs = append(errs, fmt.Errorf("upstream %s: %q is not an absolute http(s) URL", s.Prefix, s.URL))
		}
	}
	return errors.Join(errs...)
}

// parseUpstreams reads "UPSTREAMS=/cases=http://case-service:8081,/x=http://..".
func parseUpstreams(v string, timeout time.Duration) ([]Service, error) {
	var out []Service
	for _, entry := range list(v) {
		prefix, rawURL, ok := strings.Cut(entry, "=")
		if !ok || !strings.HasPrefix(prefix, "/") || len(prefix) < 2 || strings.HasSuffix(prefix, "/") || strings.ContainsAny(prefix, "*:") {
			return nil, fmt.Errorf("UPSTREAMS entry %q must look like /name=http://host:port", entry)
		}
		out = append(out, Service{Prefix: prefix, URL: rawURL, Timeout: timeout})
	}
	return out, nil
}

func list(v string) []string {
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
