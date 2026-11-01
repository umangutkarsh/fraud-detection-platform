package contract

import (
	"errors"
	"testing"
	"time"
)

func valid() Transaction {
	return Transaction{ID: "t1", UserID: "u1", MerchantID: "m1", AmountMinor: 100,
		Currency: "GBP", Country: "GB", Timestamp: time.Now()}
}

func TestValidateAcceptsAGoodTransaction(t *testing.T) {
	if err := valid().Validate(time.Now()); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRejectsBadFields(t *testing.T) {
	cases := map[string]func(*Transaction){
		"empty id":         func(x *Transaction) { x.ID = "" },
		"missing user":     func(x *Transaction) { x.UserID = "" },
		"missing merchant": func(x *Transaction) { x.MerchantID = "" },
		"zero amount":      func(x *Transaction) { x.AmountMinor = 0 },
		"negative amount":  func(x *Transaction) { x.AmountMinor = -5 },
		"bad currency":     func(x *Transaction) { x.Currency = "XYZ" },
		"bad country":      func(x *Transaction) { x.Country = "gb" },
		"no timestamp":     func(x *Transaction) { x.Timestamp = time.Time{} },
		"future":           func(x *Transaction) { x.Timestamp = time.Now().Add(time.Hour) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			tx := valid()
			mutate(&tx)
			if err := tx.Validate(time.Now()); !errors.Is(err, ErrInvalidTransaction) {
				t.Fatalf("want ErrInvalidTransaction, got %v", err)
			}
		})
	}
}

func TestScopedIDCannotCollide(t *testing.T) {
	if ScopedID("a:b", "c") == ScopedID("a", "b:c") {
		t.Fatal("the length prefix must keep these two apart")
	}
	if IdempotencyKey("m1", "t1") == DecisionKey("m1", "t1") {
		t.Fatal("different key kinds must never share a key")
	}
}
