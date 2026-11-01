package observability

import "testing"

func TestBreakerStateValue(t *testing.T) {
	for state, want := range map[string]float64{"closed": 0, "half-open": 1, "open": 2, "unknown": 0} {
		if got := BreakerStateValue(state); got != want {
			t.Errorf("%s = %v, want %v", state, got, want)
		}
	}
}
