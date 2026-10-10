package contract

import "time"

type Transaction struct {
	ID          string    `json:"id"`
	UserID      string    `json:"user_id"`
	MerchantID  string    `json:"merchant_id,omitempty"` // stamped by the gateway from the JWT, never client-supplied
	AmountMinor int64     `json:"amount_minor"`
	Currency    string    `json:"currency"` // ISO 4217
	Location    string    `json:"location,omitempty"`
	Country     string    `json:"country"` // ISO 3166-1 alpha-2
	Timestamp   time.Time `json:"timestamp"`
}

type FraudResult struct {
	Transaction  Transaction `json:"transaction"`
	Decision     string      `json:"decision"`
	IsFraudulent bool        `json:"is_fraudulent"`
	RiskScore    int         `json:"risk_score"`
	Reasons      []string    `json:"reasons"`
	EvaluatedAt  time.Time   `json:"evaluated_at"`
}
