package stats_test

import (
	"testing"

	"github.com/dora-network/bond-trading-strategies/strategy/stats"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSharpe_EmptyAndSingle(t *testing.T) {
	t.Parallel()
	for _, pnls := range [][]decimal.Decimal{nil, {}, {decimal.MustNew(1, 0)}} {
		got, err := stats.Sharpe(pnls)
		require.NoError(t, err)
		assert.True(t, got.IsZero(), "Sharpe(%v) should be zero", pnls)
	}
}

func TestSharpe_ConstantPnLReturnsZero(t *testing.T) {
	t.Parallel()
	pnls := []decimal.Decimal{
		decimal.MustNew(1, 0),
		decimal.MustNew(1, 0),
		decimal.MustNew(1, 0),
		decimal.MustNew(1, 0),
	}
	got, err := stats.Sharpe(pnls)
	require.NoError(t, err)
	assert.True(t, got.IsZero(), "Sharpe of constant pnl should be zero (sd=0), got %s", got.String())
}

func TestSharpe_KnownValue(t *testing.T) {
	t.Parallel()
	pnls := []decimal.Decimal{
		decimal.MustParse("0.01"),
		decimal.MustParse("0.02"),
		decimal.MustParse("-0.01"),
		decimal.MustParse("0.03"),
	}
	got, err := stats.Sharpe(pnls)
	require.NoError(t, err)
	assert.False(t, got.IsZero(), "Sharpe of varying pnl should be non-zero")
	assert.True(t, got.IsPos(), "Sharpe should be positive (mean > 0), got %s", got.String())
}

func TestAnnualTradingDays(t *testing.T) {
	t.Parallel()
	assert.InDelta(t, 365.25, stats.AnnualTradingDays, 1e-9)
}
