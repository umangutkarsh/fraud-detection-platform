package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"fraud-detection-platform/internal/contract"
	"fraud-detection-platform/internal/processor/metrics"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

type PublisherInterface interface {
	PublishDecision(ctx context.Context, result contract.FraudResult) error
	PublishDLQ(ctx context.Context, original kafka.Message, reason string, cause error) error
}

type KafkaPublisher struct {
	writer     *kafka.Writer
	decisions  string
	quarantine string
	dlq        string
}

func NewKafkaPublisher(brokers []string, transport *kafka.Transport, decisions string, quarantine, dlq string) *KafkaPublisher {
	return &KafkaPublisher{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Balancer:               &kafka.Murmur2Balancer{},
			RequiredAcks:           kafka.RequireAll,
			BatchTimeout:           5 * time.Millisecond,
			BatchSize:              100,
			WriteTimeout:           10 * time.Second,
			MaxAttempts:            5,
			AllowAutoTopicCreation: false,
			Transport:              transport,
		},
		decisions:  decisions,
		quarantine: quarantine,
		dlq:        dlq,
	}
}

func (publisher *KafkaPublisher) PublishDecision(ctx context.Context, result contract.FraudResult) error {
	payload, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal decision: %w", err)
	}
	topic, eventType := publisher.decisions, "transactions.approved"
	if result.IsFraudulent {
		topic, eventType = publisher.quarantine, "transactions.quarantined"
	}

	err = publisher.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(result.Transaction.ID),
		Value: payload,
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(eventType)},
			{Key: "transaction_id", Value: []byte(result.Transaction.ID)},
			{Key: "schema_version", Value: []byte(contract.SchemaVersion)},
		},
	})
	if err != nil {
		metrics.PublishFailures.Inc()
		return fmt.Errorf("write to %s: %w", topic, err)
	}
	return nil
}

func (publisher *KafkaPublisher) PublishDLQ(ctx context.Context, original kafka.Message, reason string, cause error) error {
	errText := ""
	if cause != nil {
		errText = cause.Error()
	}
	err := publisher.writer.WriteMessages(ctx, kafka.Message{
		Topic: publisher.dlq,
		Key:   original.Key,
		Value: original.Value,
		Headers: []kafka.Header{
			{Key: "dlq_reason", Value: []byte(reason)},
			{Key: "dlq_error", Value: []byte(errText)},
			{Key: "source_topic", Value: []byte(original.Topic)},
			{Key: "source_partition", Value: []byte(strconv.Itoa(original.Partition))},
			{Key: "source_offset", Value: []byte(strconv.FormatInt(original.Offset, 10))},
			{Key: "failed_at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
		},
	})
	if err != nil {
		return fmt.Errorf("write to %s: %w", publisher.dlq, err)
	}
	return nil
}

func (publisher *KafkaPublisher) Close() error {
	return publisher.writer.Close()
}
