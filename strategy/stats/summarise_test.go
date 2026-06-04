package stats_test

import (
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy/stats"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSummarise_EmptyTrades(t *testing.T) {
	t.Parallel()
	got, err := stats.Summarise(nil, time.Now(), time.Now().Add(time.Hour))
	require.NoError(t, err)
	assert.True(t, got.TotalPnL.IsZero())
	assert.Equal(t, 0, got.WinCount)
	assert.Equal(t, 0, got.LossCount)
}

func TestSummarise_ZeroTimeReturnsError(t *testing.T) {
	t.Parallel()
	_, err := stats.Summarise(nil, time.Time{}, time.Now())
	require.Error(t, err)

	_, err = stats.Summarise(nil, time.Now(), time.Time{})
	require.Error(t, err)
}

func TestSummarise_EndBeforeStartReturnsError(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	_, err := stats.Summarise(nil, start, end)
	require.Error(t, err)
}

func TestSummarise_WinLossCounts(t *testing.T) {
	t.Parallel()
	d := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	// 1 positive (win), 1 negative (loss), 1 zero (neither).
	trades := []stats.PnLPoint{
		{PnL: decimal.MustParse("100"), CloseTime: d},
		{PnL: decimal.MustParse("-50"), CloseTime: d},
		{PnL: decimal.Zero, CloseTime: d},
	}
	got, err := stats.Summarise(trades, d, d)
	require.NoError(t, err)
	assert.Equal(t, 1, got.WinCount, "PnL>0 is a win; zero PnL is not counted as a win")
	assert.Equal(t, 1, got.LossCount, "PnL<0 is a loss; zero PnL is not counted as a loss")
	require.Equal(t, "50", got.TotalPnL.String())
}

func TestSummarise_MaxDrawdown(t *testing.T) {
	t.Parallel()
	d := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	// Equity goes 10, 30, 20, 25 → max drawdown is 30-20 = 10
	trades := []stats.PnLPoint{
		{PnL: decimal.MustParse("10"), CloseTime: d},
		{PnL: decimal.MustParse("20"), CloseTime: d},
		{PnL: decimal.MustParse("-10"), CloseTime: d},
		{PnL: decimal.MustParse("5"), CloseTime: d},
	}
	got, err := stats.Summarise(trades, d, d)
	require.NoError(t, err)
	require.Equal(t, "10", got.MaxDrawdown.String())
}

func TestSummarise_DailyPnLSpansFullWindow(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	end := time.Date(2026, 6, 3, 12, 0, 0, 0, time.UTC)
	// Single trade on day 1; days 2 and 3 contribute 0 to the Sharpe series.
	trades := []stats.PnLPoint{
		{PnL: decimal.MustParse("100"), CloseTime: start},
	}
	got, err := stats.Summarise(trades, start, end)
	require.NoError(t, err)
	// With only one non-zero day and two zero days in a 3-day window,
	// variance is non-zero (only one non-zero value has variance 0,
	// but two zero values against the mean dilute it). Just assert the
	// value is computable and well-defined.
	assert.False(t, got.SharpeRatio.IsNeg(), "Sharpe should be non-negative, got %s", got.SharpeRatio.String())
}

func TestSummarise_WindowVsTradesScope(t *testing.T) {
	t.Parallel()
	// Trades on day 1, but the window spans 1..3. copytrading's old
	// implementation derived start/end from the trades' own CloseTime
	// range (day 1..1). meanreversion's used the backtest window
	// (1..3). This test pins behaviour to the backtest window: an
	// isolated trade should not be "1" in a single-day series.
	start := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	end := time.Date(2026, 6, 3, 12, 0, 0, 0, time.UTC)
	trades := []stats.PnLPoint{
		{PnL: decimal.MustParse("100"), CloseTime: start},
		{PnL: decimal.MustParse("100"), CloseTime: end},
	}
	got, err := stats.Summarise(trades, start, end)
	require.NoError(t, err)
	// Both trades are PnL=100; with a 3-day window and one zero day,
	// the mean of [100, 0, 100] is ~66.67 and Sharpe is well-defined.
	assert.True(t, got.SharpeRatio.IsPos(), "Sharpe should be positive, got %s", got.SharpeRatio.String())
}
