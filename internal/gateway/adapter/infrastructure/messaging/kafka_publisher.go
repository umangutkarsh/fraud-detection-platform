// Package messaging is the Kafka adapter for port.EventPublisher.
package messaging

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"

	"fraud-detection-platform/internal/gateway/domain/port"
)

type KafkaOptions struct {
	Brokers       []string
	Topic         string
	TLS           bool
	SASLMechanism string // "", plain, scram-sha-256, scram-sha-512 (lowercase)
	SASLUsername  string
	SASLPassword  string
}

type KafkaPublisher struct{ w *kafka.Writer }

func NewKafkaPublisher(o KafkaOptions) (*KafkaPublisher, error) {
	transport := &kafka.Transport{DialTimeout: 5 * time.Second}
	if o.TLS {
		transport.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}

	var mech sasl.Mechanism
	switch o.SASLMechanism {
	case "":
	case "plain":
		mech = plain.Mechanism{Username: o.SASLUsername, Password: o.SASLPassword}
	case "scram-sha-256", "scram-sha-512":
		algo := scram.SHA256
		if o.SASLMechanism == "scram-sha-512" {
			algo = scram.SHA512
		}
		m, err := scram.Mechanism(algo, o.SASLUsername, o.SASLPassword)
		if err != nil {
			return nil, fmt.Errorf("configure SCRAM: %w", err)
		}
		mech = m
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism %q", o.SASLMechanism)
	}
	transport.SASL = mech

	return &KafkaPublisher{w: &kafka.Writer{
		Addr:     kafka.TCP(o.Brokers...),
		Topic:    o.Topic,
		Balancer: &kafka.Murmur2Balancer{},
		// All in-sync replicas must ack before we tell a client "accepted".
		RequiredAcks: kafka.RequireAll,
		// The default BatchTimeout is 1s, which would add up to a second to
		// every synchronous request.
		BatchTimeout:           5 * time.Millisecond,
		WriteTimeout:           5 * time.Second,
		MaxAttempts:            3,
		AllowAutoTopicCreation: false,
		Transport:              transport,
	}}, nil
}

func (p *KafkaPublisher) Publish(ctx context.Context, e port.Event) error {
	headers := make([]kafka.Header, 0, len(e.Headers))
	for k, v := range e.Headers {
		headers = append(headers, kafka.Header{Key: k, Value: []byte(v)})
	}
	return p.w.WriteMessages(ctx, kafka.Message{Key: []byte(e.Key), Value: e.Value, Headers: headers})
}

func (p *KafkaPublisher) Close() error { return p.w.Close() }
