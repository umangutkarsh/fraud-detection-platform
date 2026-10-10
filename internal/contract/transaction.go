package contract

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	maxIDLength   = 128
	maxFutureSkew = 5 * time.Minute
)

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

// ErrInvalidTransaction marks a permanently bad event. Retrying it can never
// succeed, so the pipeline dead-letters it and the gateway answers 422.
var ErrInvalidTransaction = errors.New("invalid transaction")

var supportedCurrencies = map[string]struct{}{"GBP": {}, "EUR": {}, "USD": {}}

func IsSupportedCurrency(currency string) bool {
	_, found := supportedCurrencies[currency]
	return found
}

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidTransaction, fmt.Sprintf(format, args...))
}

// Normalise tidies harmless client variations (case, whitespace) and defaults
// the timestamp.
func (t *Transaction) Normalise(now time.Time) {
	t.ID = strings.TrimSpace(t.ID)
	t.UserID = strings.TrimSpace(t.UserID)
	t.Currency = strings.ToUpper(strings.TrimSpace(t.Currency))
	t.Country = strings.ToUpper(strings.TrimSpace(t.Country))
	if t.Timestamp.IsZero() {
		t.Timestamp = now
	}
	t.Timestamp = t.Timestamp.UTC()
}

// To enforce event contract at both boundaries
func (t Transaction) Validate(now time.Time) error {
	switch {
	case t.ID == "" || len(t.ID) > maxIDLength:
		return invalid("id must be 1-%d characters", maxIDLength)
	case t.UserID == "" || len(t.UserID) > maxIDLength:
		return invalid("user_id must be 1-%d characters", maxIDLength)
	case t.MerchantID == "" || len(t.MerchantID) > maxIDLength:
		return invalid("merchant_id must be 1-%d characters", maxIDLength)
	case t.AmountMinor <= 0:
		return invalid("amount_minor must be a positive integer (pence/cents)")
	case !IsSupportedCurrency(t.Currency):
		return invalid("currency must be one of GBP, EUR, USD")
	case len(t.Country) != 2 || t.Country[0] < 'A' || t.Country[0] > 'Z' || t.Country[1] < 'A' || t.Country[1] > 'Z':
		return invalid("country must be an uppercase ISO 3166-1 alpha-2 code")
	case t.Timestamp.IsZero():
		return invalid("timestamp is required")
	case t.Timestamp.After(now.Add(maxFutureSkew)):
		return invalid("timestamp is in the future")
	}
	return nil
}

// ScopedID identifies a transaction within a merchant.
func ScopedID(merchantID, id string) string {
	return strconv.Itoa(len(merchantID)) + ":" + merchantID + ":" + id
}

func (t Transaction) ScopedIDValue() string { return ScopedID(t.MerchantID, t.ID) }

// Redis key layout shared by both services (gateway and processor).
// "decision": is the contract between gateway and processor.”
func IdempotencyKey(merchantID, id string) string { return "idem:" + ScopedID(merchantID, id) }
func DecisionKey(merchantID, id string) string    { return "decision:" + ScopedID(merchantID, id) }
func ReceiptKey(merchantID, id string) string     { return "receipt:" + ScopedID(merchantID, id) }
