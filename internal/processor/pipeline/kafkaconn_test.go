package pipeline

import (
	"crypto/tls"
	"testing"

	"fraud-detection-platform/internal/processor/config"
)

func TestKafkaSecurityMechanisms(t *testing.T) {
	cases := []struct {
		mech     string
		wantName string // "" means no SASL
		wantErr  bool
	}{
		{"", "", false},
		{"plain", "PLAIN", false},
		{"scram-sha-256", "SCRAM-SHA-256", false},
		{"scram-sha-512", "SCRAM-SHA-512", false},
		{"bogus", "", true},
	}
	for _, c := range cases {
		t.Run(c.mech, func(t *testing.T) {
			cfg := config.Config{KafkaSASLMechanism: c.mech, KafkaSASLUsername: "u", KafkaSASLPassword: "p"}
			d, tr, err := KafkaSecurity(cfg)
			if (err != nil) != c.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, c.wantErr)
			}
			if err != nil {
				return
			}
			if c.wantName == "" {
				if d.SASLMechanism != nil || tr.SASL != nil {
					t.Fatal("no SASL expected")
				}
				return
			}
			if d.SASLMechanism == nil || tr.SASL == nil {
				t.Fatal("reader and writer must both use SASL")
			}
			if got := d.SASLMechanism.Name(); got != c.wantName {
				t.Fatalf("mechanism = %q, want %q", got, c.wantName)
			}
		})
	}
}

func TestKafkaSecurityTLS(t *testing.T) {
	d, tr, err := KafkaSecurity(config.Config{KafkaTLS: true})
	if err != nil {
		t.Fatal(err)
	}
	if d.TLS == nil || tr.TLS == nil || d.TLS.MinVersion != tls.VersionTLS12 {
		t.Fatal("TLS 1.2 minimum expected on both reader and writer")
	}

	d, tr, _ = KafkaSecurity(config.Config{})
	if d.TLS != nil || tr.TLS != nil {
		t.Fatal("TLS must be off by default")
	}
}
