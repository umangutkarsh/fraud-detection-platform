package port

import (
	"context"

	"fraud-detection-platform/internal/contract"
)

// DecisionStore is the gateway's read model of the pipeline's output plus the
// "receipt" it writes when it accepts a transaction, so a lookup can tell
// "still processing" from "never seen". All keys are merchant-scoped.
type DecisionStore interface {
	// GetDecision returns (nil, nil) when no decision exists yet.
	GetDecision(ctx context.Context, merchantID, id string) (*contract.FraudResult, error)
	PutReceipt(ctx context.Context, merchantID, id string) error
	HasReceipt(ctx context.Context, merchantID, id string) (bool, error)
}
