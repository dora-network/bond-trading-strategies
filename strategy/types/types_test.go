package types_test

import (
	"testing"

	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/strategy/meanreversion"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
)

// TestMeanReversionDecision_SatisfiesDecision is a compile-time guard
// that meanreversion.Decision still satisfies types.Decision. Breaks
// the build if either interface changes incompatibly.
func TestMeanReversionDecision_SatisfiesDecision(t *testing.T) {
	t.Parallel()
	var _ types.Decision = meanreversion.Decision{}
	assert.True(t, true, "compile-time conformance")
}

func TestBarTrueRange(t *testing.T) {
	t.Parallel()
	bar := types.Bar{
		High:  decimal.MustNew(10150, 2), // 101.50
		Low:   decimal.MustNew(10000, 2), // 100.00
		Close: decimal.MustNew(10050, 2), // 100.50
	}

	// No prev close: range = H - L = 1.50.
	tr, err := bar.TrueRange(decimal.Zero)
	require.NoError(t, err)
	require.True(t, tr.Equal(decimal.MustNew(150, 2)))

	// prevClose 99.00: max(1.50, |101.50-99.00|=2.50, |100.00-99.00|=1.00) = 2.50.
	tr, err = bar.TrueRange(decimal.MustNew(9900, 2))
	require.NoError(t, err)
	require.True(t, tr.Equal(decimal.MustNew(250, 2)))
}
