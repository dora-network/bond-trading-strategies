package testutils

import (
	"testing"

	"github.com/govalues/decimal"
)

func InDelta(t *testing.T, a, b, delta decimal.Decimal) bool {
	t.Helper()
	if a.Equal(b) {
		return true
	}
	min, err := a.Sub(delta)
	if err != nil {
		panic(err)
	}
	max, err := a.Add(delta)
	if err != nil {
		panic(err)
	}
	return b.Cmp(min) >= 0 && b.Cmp(max) <= 0
}
