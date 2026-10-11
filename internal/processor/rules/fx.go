package rules

import (
	"fmt"
	"fraud-detection-platform/internal/contract"
	"math"
	"strings"
)

type Converter interface {
	ToGBPMinor(amountMinor int64, currency string) (int64, error)
}

type StaticFX struct{ toGBP map[string]float64 }

func NewStaticFX(rates map[string]float64) *StaticFX {
	cp := make(map[string]float64, len(rates))
	for key, value := range rates {
		cp[strings.ToUpper(key)] = value
	}
	return &StaticFX{toGBP: cp}
}

func DefaultFX() *StaticFX {
	return NewStaticFX(map[string]float64{
		"GBP": 1.0,
		"EUR": 0.85, // 1 EUR = ~0.85 GBP
		"USD": 0.76, // 1 USD = ~0.76 GBP
	})
}

func (fx *StaticFX) ToGBPMinor(amountMinor int64, currency string) (int64, error) {
	rate, ok := fx.toGBP[currency]
	if !ok {
		return 0, fmt.Errorf("%w: no FX rate for %q", contract.ErrInvalidTransaction, currency)
	}
	return int64(math.Round(float64(amountMinor) * rate)), nil
}
