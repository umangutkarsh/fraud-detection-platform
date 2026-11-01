package config

import (
	"flag"
	"testing"
	"time"
)

func load(t *testing.T, env map[string]string, args ...string) (Config, error) {
	t.Helper()
	return LoadFrom(flag.NewFlagSet("test", flag.ContinueOnError), args, func(k string) string { return env[k] })
}

func TestDefaults(t *testing.T) {
	cfg, err := load(t, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.InputTopic != "transactions" || cfg.WorkerCount != 8 || cfg.HomeCountry != "GB" || cfg.DecisionTTL != 168*time.Hour {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestEnvAndFlagPrecedence(t *testing.T) {
	cfg, err := load(t, map[string]string{"KAFKA_BROKERS": "a:1, b:2", "WORKERS": "4", "BUFFER_SIZE": "100"}, "-workers", "16")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Brokers) != 2 || cfg.Brokers[1] != "b:2" {
		t.Fatalf("brokers not parsed from env: %v", cfg.Brokers)
	}
	if cfg.WorkerCount != 16 {
		t.Fatalf("flag must override env, got %d", cfg.WorkerCount)
	}
}

func TestValidation(t *testing.T) {
	if _, err := load(t, nil, "-workers", "0"); err == nil {
		t.Fatal("zero workers must be rejected")
	}
	if _, err := load(t, map[string]string{"KAFKA_SASL_MECHANISM": "plain"}); err == nil {
		t.Fatal("SASL without credentials must be rejected")
	}
}
