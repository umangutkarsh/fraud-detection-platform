package pipeline

import (
	"fraud-detection-platform/internal/contract"
	"sync"
)

type FakePublisher struct {
	mu            sync.Mutex
	decisions     []contract.FraudResult
	dlq           []string
	failDecisions int
	failDLQ       int
}
