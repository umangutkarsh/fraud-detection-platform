package pipeline

import (
	"crypto/tls"
	"fmt"
	"fraud-detection-platform/internal/processor/config"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

func KafkaSecurity(config config.Config) (*kafka.Dialer, *kafka.Transport, error) {
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	transport := &kafka.Transport{
		DialTimeout: 10 * time.Second,
	}

	if config.KafkaTLS {
		tlsCfg := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
		dialer.TLS = tlsCfg
		transport.TLS = tlsCfg
	}

	var mech sasl.Mechanism
	mechanism := strings.ToLower(config.KafkaSASLMechanism)
	switch mechanism {
	case "":
	case "plain":
		mech = plain.Mechanism{
			Username: config.KafkaSASLUsername,
			Password: config.KafkaSASLPassword,
		}
	case "scram-sha-256", "scram-sha-512":
		algo := scram.SHA256
		if mechanism == "scram-sha-512" {
			algo = scram.SHA512
		}
		m, err := scram.Mechanism(algo, config.KafkaSASLUsername, config.KafkaSASLPassword)
		if err != nil {
			return nil, nil, fmt.Errorf("configure SCRAM: %w", err)
		}
		mech = m
	default:
		return nil, nil, fmt.Errorf("unsupported SASL mechanism: %s", mechanism)
	}
	dialer.SASLMechanism = mech
	transport.SASL = mech

	return dialer, transport, nil
}
