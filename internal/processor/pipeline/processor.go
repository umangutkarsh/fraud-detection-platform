package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"fraud-detection-platform/internal/contract"
	"fraud-detection-platform/internal/processor/metrics"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/segmentio/kafka-go"
)

type Evaluator interface {
	Evaluate(ctx context.Context, transaction contract.Transaction) (contract.FraudResult, error)
}

var errBusy = errors.New("transaction is already being processed by another worker")

type RetryPolicy struct {
	MaxAttempts int
	Base, Max   time.Duration
}

func (policy RetryPolicy) Backoff(attempts int) time.Duration {
	duration := policy.Base << min(attempts-1, 16)
	if duration > policy.Max || duration <= 0 {
		duration = policy.Max
	}
	return time.Duration(rand.Int64N(int64(duration)) + 1)
}

func Sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type Processor struct {
	Eval  Evaluator
	Store DecisionStore

	Retry RetryPolicy
	Log   *slog.Logger

	Idemp     *Idempotency
	Publisher PublisherInterface
}

func (processor *Processor) Process(ctx context.Context, message kafka.Message) (err error) {
	defer func() {
		if recoverErr := recover(); recoverErr != nil {
			processor.Log.Error("panic while processing message", "panic", fmt.Sprint(recoverErr),
				"partition", message.Partition, "offset", message.Offset)
			err = processor.deadLetter(ctx, message, "panic", fmt.Errorf("panic: %v", recoverErr))
		}
	}()

	start := time.Now()

	var txn contract.Transaction
	if err := json.Unmarshal(message.Value, &txn); err != nil {
		processor.Log.Error("failed to unmarshal transaction", "error", err, "partition", message.Partition, "offset", message.Offset)
		return processor.deadLetter(ctx, message, "invalid_json", fmt.Errorf("invalid_json: %w", err))
	}
	if err := txn.Validate(time.Now()); err != nil {
		processor.Log.Error("invalid transaction", "error", err, "partition", message.Partition, "offset", message.Offset)
		return processor.deadLetter(ctx, message, "validation_failed", fmt.Errorf("validation_failed: %w", err))
	}

	var lastErr error
	for attempt := 1; attempt <= processor.Retry.MaxAttempts; attempt++ {
		err := processor.attempt(ctx, txn)
		if err == nil {
			metrics.ProcessingLatency.Observe(float64(time.Since(start).Seconds()))
			if !message.Time.IsZero() {
				metrics.EndToEndLatency.Observe(float64(time.Since(message.Time).Seconds()))
			}
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		if errors.Is(err, contract.ErrInvalidTransaction) {
			return processor.deadLetter(ctx, message, "validation_failed", err)
		}

		lastErr = err
		metrics.Retries.Inc()
		processor.Log.Warn("transient failure, will retry", "txn_id", txn.ID, "attempt", attempt,
			"max_attempts", processor.Retry.MaxAttempts, "error", err)
		if attempt < processor.Retry.MaxAttempts {
			if err := Sleep(ctx, processor.Retry.Backoff(attempt)); err != nil {
				return err
			}
		}
	}
	return processor.deadLetter(ctx, message, "retries_exhausted", lastErr)
}

func (processor *Processor) attempt(ctx context.Context, txn contract.Transaction) error {
	idempotencyKey := contract.IdempotencyKey(txn.MerchantID, txn.ID)
	state, err := processor.Idemp.Claim(ctx, idempotencyKey)
	if err != nil {
		return err
	}

	switch state {
	case claimDone:
		metrics.Duplicates.Inc()
		processor.Log.Debug("duplicate skipped", "txn_id", txn.ID)
		return nil

	case claimBusy:
		return errBusy
	}

	result, err := processor.Eval.Evaluate(ctx, txn)
	if err != nil {
		processor.release(ctx, idempotencyKey)
		return err
	}

	if err := processor.Publisher.PublishDecision(ctx, result); err != nil {
		processor.release(ctx, idempotencyKey)
		return err
	}

	if err := processor.Store.Save(ctx, result); err != nil {
		processor.release(ctx, idempotencyKey)
		return err
	}

	if err := processor.Idemp.MarkDone(ctx, idempotencyKey); err != nil {
		processor.Log.Error("failed to mark transaction done", "txn_id", txn.ID, "error", err)
	}

	metrics.Processed.WithLabelValues(result.Decision).Inc()
	processor.Log.Info("transaction decided", "txn_id", txn.ID, "decision", result.Decision,
		"risk_score", result.RiskScore, "results", len(result.Reasons))

	return nil

}

func (processor *Processor) release(ctx context.Context, key string) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	processor.Idemp.Release(releaseCtx, key)
}

func (processor *Processor) deadLetter(ctx context.Context, message kafka.Message, reason string, cause error) error {
	for attempt := 1; ; attempt++ {
		err := processor.Publisher.PublishDLQ(ctx, message, reason, cause)
		if err == nil {
			metrics.DeadLettered.WithLabelValues(reason).Inc()
			processor.Log.Error("message dead-lettered", "reason", reason, "partition", message.Partition,
				"offset", message.Offset, "cause", cause)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		processor.Log.Error("dead-letter write failed", "attempt", attempt, "error", err)
		if err := Sleep(ctx, processor.Retry.Backoff(attempt)); err != nil {
			return err
		}
	}
}
