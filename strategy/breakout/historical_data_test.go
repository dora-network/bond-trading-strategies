package breakout_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/candles"
	"github.com/dora-network/bond-trading-strategies/strategy/breakout"
	"github.com/dora-network/bond-trading-strategies/strategy/breakout/breakoutfakes"
)

var hEpoch = time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)

// TestGetBars_WarmupAndPreflight: getBars requests warmup of
// LongVolWindow+1 bars before start, projects candles to bars
// oldest-first, and PreflightBacktest passes with full coverage.
func TestGetBars_WarmupAndPreflight(t *testing.T) {
	t.Parallel()

	ob := uuid.Must(uuid.NewV7())
	res := 5 * time.Minute
	start := hEpoch
	end := hEpoch.Add(10 * res)

	cfg := defaultCfg()
	cfg.Resolution = "5m"
	cfg.LongVolWindow = 96
	cfg.OrderBookID = ob
	s := breakout.New(cfg, nil)

	cs := []candles.Candle{
		{OrderBookID: ob.String(), StartTimestamp: hEpoch.Add(-97 * res), Open: decimal.One, High: decimal.One, Low: decimal.One, Close: decimal.One},
		{OrderBookID: ob.String(), StartTimestamp: hEpoch.Add(-96 * res), Open: decimal.One, High: decimal.One, Low: decimal.One, Close: decimal.One},
		{OrderBookID: ob.String(), StartTimestamp: hEpoch.Add(9 * res), Open: decimal.One, High: decimal.One, Low: decimal.One, Close: decimal.One},
	}
	// Coverage must include the warmup start: start − 97×res.
	store := &breakoutfakes.FakeCandleHistoryStore{}
	lo, hi := hEpoch.Add(-97*res), hEpoch.Add(9*res)
	store.CandleRangeReturns(&lo, &hi, nil)
	store.LoadCandlesBucketedReturns(cs, nil)
	breakout.SetCandleHistoryStore(s, store)

	bars, err := breakout.GetBars(context.Background(), s, start, end)
	require.NoError(t, err)
	require.Len(t, bars, len(cs))
	assert.Equal(t, cs[0].StartTimestamp.UTC(), bars[0].Time, "bars must be oldest-first")
	assert.True(t, bars[0].Close.Equal(decimal.One), "candle close must project into the bar")

	// LoadCandlesBucketed must have been called with the warmup-inclusive window.
	ctxArg, obID, resolution, since, until := store.LoadCandlesBucketedArgsForCall(0)
	_ = ctxArg
	assert.Equal(t, ob.String(), obID)
	assert.Equal(t, candles.Resolution5m, resolution)
	assert.Equal(t, start.Add(-97*res).UTC(), since.UTC(), "warmup must be LongVolWindow+1 bars")
	assert.Equal(t, end.UTC(), until.UTC())

	// Preflight passes with full coverage.
	require.NoError(t, breakout.PreflightBacktest(context.Background(), s, start, end))
}

// TestPreflightBacktest_CoverageGap: a window the store does not cover
// must fail with ErrNoCandleCoverage.
func TestPreflightBacktest_CoverageGap(t *testing.T) {
	t.Parallel()

	ob := uuid.Must(uuid.NewV7())
	start := hEpoch
	end := hEpoch.Add(time.Hour)

	cfg := defaultCfg()
	cfg.Resolution = "5m"
	cfg.OrderBookID = ob
	s := breakout.New(cfg, nil)

	// Coverage ends an hour before end → gap.
	lo, hi := hEpoch.Add(-time.Hour), end.Add(-time.Hour)
	store := &breakoutfakes.FakeCandleHistoryStore{}
	store.CandleRangeReturns(&lo, &hi, nil)
	breakout.SetCandleHistoryStore(s, store)

	err := breakout.PreflightBacktest(context.Background(), s, start, end)
	require.Error(t, err)
	var cov *candles.ErrNoCandleCoverage
	require.ErrorAs(t, err, &cov)
	assert.Equal(t, ob.String(), cov.OrderBookID)
}
