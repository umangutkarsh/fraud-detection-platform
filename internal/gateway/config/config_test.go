package config

import (
	"strings"
	"testing"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestJWTSecretIsMandatory(t *testing.T) {
	if _, err := Load(env(nil)); err == nil || !strings.Contains(err.Error(), "JWT_SECRET") {
		t.Fatalf("missing secret must fail startup, got %v", err)
	}
	if _, err := Load(env(map[string]string{"JWT_SECRET": "short"})); err == nil {
		t.Fatal("short secret must fail startup")
	}
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(env(map[string]string{"JWT_SECRET": strings.Repeat("s", 32)}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != ":5000" || len(cfg.Services) != 0 || len(cfg.TrustedProxies) != 0 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestUpstreamsParsing(t *testing.T) {
	base := map[string]string{"JWT_SECRET": strings.Repeat("s", 32)}
	with := func(v string) (Config, error) {
		e := map[string]string{"UPSTREAMS": v}
		for k, x := range base {
			e[k] = x
		}
		return Load(env(e))
	}
	cfg, err := with("/cases=http://case-service:8081")
	if err != nil || len(cfg.Services) != 1 || cfg.Services[0].Scope() != "cases:access" {
		t.Fatalf("got %+v %v", cfg.Services, err)
	}
	for _, bad := range []string{"cases=http://x", "/cases", "/cases/=http://x", "/ca*=http://x", "/cases=not a url"} {
		if _, err := with(bad); err == nil {
			t.Errorf("%q must be rejected", bad)
		}
	}
}
