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
		{OrderBookID: ob.String(), StartTimestamp: end.Add(-time.Minute), Open: decimal.One, High: decimal.One, Low: decimal.One, Close: decimal.One},
	}
	// Coverage must include the warmup start: start − 97×res, and the
	// last raw 1m candle must be at/after end−1m so the source-level
	// guard is satisfied.
	store := &breakoutfakes.FakeCandleHistoryStore{}
	lo, hi := hEpoch.Add(-97*res), end.Add(-time.Minute)
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

// TestPreflightBacktest_SourceResolutionEnforced: CandleRange reports
// raw 1m rows. At a strategy resolution above 1m, the source-level
// guard must reject when the last raw 1m candle is more than 1m short
// of end (i.e., the strategy would fold a partial final bucket from a
// real coverage gap). Pre-fix this wrongly passed because the guard
// added the strategy resolution to hi instead of 1m; this test pins
// the corrected behavior.
func TestPreflightBacktest_SourceResolutionEnforced(t *testing.T) {
	t.Parallel()

	ob := uuid.Must(uuid.NewV7())
	start := hEpoch
	end := hEpoch.Add(time.Hour)

	cfg := defaultCfg()
	cfg.Resolution = "1h"
	cfg.OrderBookID = ob
	s := breakout.New(cfg, nil)

	// defaultCfg sets LongVolWindow=30; res=1h → dataStart = hEpoch−31h.
	// Last raw 1m candle is 2m before end → 1m of source coverage is
	// missing. Pre-fix guard computed hi.Add(1h) = end+58m, which is
	// not before end, and wrongly passed.
	lo, hi := hEpoch.Add(-31*time.Hour), end.Add(-2*time.Minute)
	store := &breakoutfakes.FakeCandleHistoryStore{}
	store.CandleRangeReturns(&lo, &hi, nil)
	breakout.SetCandleHistoryStore(s, store)

	err := breakout.PreflightBacktest(context.Background(), s, start, end)
	require.Error(t, err)
	var cov *candles.ErrNoCandleCoverage
	require.ErrorAs(t, err, &cov)
	assert.Equal(t, ob.String(), cov.OrderBookID)
	assert.True(t, cov.Until.Equal(hi))
}

// TestPreflightBacktest_SourceEdgePasses: the last raw 1m candle at
// end−1m covers [end−1m, end), so the source-level guard must accept.
// This is the regression counterpart to
// TestPreflightBacktest_SourceResolutionEnforced: it pins that the
// corrected guard does not over-reject at the source edge.
func TestPreflightBacktest_SourceEdgePasses(t *testing.T) {
	t.Parallel()

	ob := uuid.Must(uuid.NewV7())
	start := hEpoch
	end := hEpoch.Add(time.Hour)

	cfg := defaultCfg()
	cfg.Resolution = "1h"
	cfg.OrderBookID = ob
	s := breakout.New(cfg, nil)

	// defaultCfg sets LongVolWindow=30; res=1h → dataStart = hEpoch−31h.
	lo, hi := hEpoch.Add(-31*time.Hour), end.Add(-time.Minute)
	store := &breakoutfakes.FakeCandleHistoryStore{}
	store.CandleRangeReturns(&lo, &hi, nil)
	breakout.SetCandleHistoryStore(s, store)

	require.NoError(t, breakout.PreflightBacktest(context.Background(), s, start, end))
}

// TestGetBars_UnalignedEndTruncatesToGrid: getBars must floor end to the
// resolution grid so a partial trailing bucket is never loaded. At 15m
// resolution with end = start + 4*15m + 7m30s, the pre-fix code passed
// the raw end to LoadCandlesBucketed (including the 7m30s partial
// bucket). Post-fix the call's `until` arg equals the truncated loadEnd
// and coverage is checked against the same value, so a store that only
// covers up to the truncated end passes.
func TestGetBars_UnalignedEndTruncatesToGrid(t *testing.T) {
	t.Parallel()

	ob := uuid.Must(uuid.NewV7())
	res := 15 * time.Minute
	start := hEpoch
	end := start.Add(4*res + 7*time.Minute + 30*time.Second)
	loadEnd := end.UTC().Truncate(res) // start + 4*res

	cfg := defaultCfg()
	cfg.Resolution = "15m"
	cfg.LongVolWindow = 8
	cfg.OrderBookID = ob
	s := breakout.New(cfg, nil)

	// Coverage must extend up to loadEnd (NOT raw end). Last raw 1m
	// candle at loadEnd−1m satisfies the source-level guard for the
	// truncated window.
	store := &breakoutfakes.FakeCandleHistoryStore{}
	lo := start.Add(-9 * res)
	hi := loadEnd.Add(-time.Minute)
	store.CandleRangeReturns(&lo, &hi, nil)
	store.LoadCandlesBucketedReturns(nil, nil)
	breakout.SetCandleHistoryStore(s, store)

	_, err := breakout.GetBars(context.Background(), s, start, end)
	require.NoError(t, err)

	// LoadCandlesBucketed must have been called with loadEnd, not raw end.
	ctxArg, obID, resolution, since, until := store.LoadCandlesBucketedArgsForCall(0)
	_ = ctxArg
	_ = obID
	_ = resolution
	assert.Equal(t, start.Add(-9*res).UTC(), since.UTC(), "since must remain warmup-inclusive")
	assert.Equal(t, loadEnd, until.UTC(),
		"until must be the resolution-truncated loadEnd, dropping the partial trailing bucket")

	// Preflight covers the same window: with raw end unaligned, the
	// store's hi (loadEnd−1m) must be sufficient; if getBars still
	// checked against raw end the preflight would fail because the
	// store does not cover the 7m30s past loadEnd.
	require.NoError(t, breakout.PreflightBacktest(context.Background(), s, start, end))
}
