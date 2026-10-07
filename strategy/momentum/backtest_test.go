package momentum_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/candles"
	"github.com/dora-network/bond-trading-strategies/prices"
	"github.com/dora-network/bond-trading-strategies/strategy/momentum"
	"github.com/dora-network/bond-trading-strategies/strategy/momentum/momentumfakes"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
)

var barBase = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

// flatBar builds a degenerate bar (H=L=C=price), whose true range
// reduces to |Δclose|.
func flatBar(i int, price int64) types.Bar {
	p := decimal.MustNew(price, 0)
	return types.Bar{
		Time: barBase.Add(time.Duration(i) * time.Minute),
		Open: p, High: p, Low: p, Close: p,
	}
}

// halfBar prices: decimal.MustNew(995, 1) = 99.5.
func half(value int64) decimal.Decimal { return decimal.MustNew(value, 1) }

// hourlyBar builds a flat bar at the given hour offset from barBase.
// 1h resolution fixture helper for the multi-day bounds test.
func hourlyBar(i int, price int64) types.Bar {
	p := decimal.MustNew(price, 0)
	return types.Bar{
		Time: barBase.Add(time.Duration(i) * time.Hour),
		Open: p, High: p, Low: p, Close: p,
	}
}

// risingHourlyBar builds a bar with a non-degenerate range so the
// reversal-exit path uses the adverse-extreme fill (matches the
// F11 wiring for bar-driven exits). Price is the close.
func risingHourlyBar(i int, price int64) types.Bar {
	p := decimal.MustNew(price, 0)
	open := decimal.MustNew(price-1, 0)
	return types.Bar{
		Time:  barBase.Add(time.Duration(i) * time.Hour),
		Open:  open,
		High:  p,
		Low:   open,
		Close: p,
	}
}

// uptrendThenReversal: prices rise (fast>slow -> Buy), then fall so the
// MAs cross back (-> reversal exit).
func uptrendThenReversal() []types.Bar {
	bars := make([]types.Bar, 0, 14)
	for i := range 7 {
		bars = append(bars, flatBar(i, int64(100+i)))
	}
	for i := range 7 {
		bars = append(bars, flatBar(7+i, int64(106-i)))
	}
	return bars
}

// testCfg builds a momentum config with small windows (defaults for
// price source, ATR window, $1000 seed capital, no stops) and applies
// the per-test overrides the caller passes.
func testCfg(fast, slow int, stopLossATR, takeProfitATR decimal.Decimal) momentum.Config {
	cfg := momentum.DefaultConfig()
	cfg.SignalSource = momentum.SignalSourcePrice
	cfg.FastWindow = fast
	cfg.SlowWindow = slow
	cfg.StopLossATR = stopLossATR
	cfg.TakeProfitATR = takeProfitATR
	cfg.InitialBalance = decimal.MustNew(1000, 0)
	cfg.MinOrderSize = decimal.Zero
	cfg.MaxOrderSize = decimal.Zero
	return cfg
}

// closedOf extracts the closed-trades slice from a BacktestResult.
func closedOf(t *testing.T, res types.BacktestResult) []momentum.ClosedTrade {
	t.Helper()
	closed, ok := res.GetClosedTrades().([]momentum.ClosedTrade)
	require.True(t, ok, "closedTrades type mismatch: %T", res.GetClosedTrades())
	return closed
}

func recordsOf(t *testing.T, res types.BacktestResult) []momentum.TradeRecord {
	t.Helper()
	records, ok := res.GetTradeRecords().([]momentum.TradeRecord)
	require.True(t, ok, "tradeRecords type mismatch: %T", res.GetTradeRecords())
	return records
}

func TestBacktest_OpensAndExits(t *testing.T) {
	cfg := testCfg(3, 5, decimal.Zero, decimal.Zero)
	s := momentum.New(cfg, nil)
	bt := momentum.NewBacktester(s, nil)

	res, err := bt.Run(context.Background(), uptrendThenReversal())
	require.NoError(t, err)
	closedTrades := closedOf(t, res)
	tradeRecords := recordsOf(t, res)

	// Pinning assertions. The fixture (rising prices then falling prices)
	// produces multiple round-trips as the strategy flips direction with
	// each MA crossover. The assertions catch silent mutations in:
	//   - exit reason (reversal must fire when fast/slow MA cross)
	//   - matched trade-record pairs (no dangling open entry)
	//   - PnL sign (sign-flip in computePnL is silent mutation)
	require.NotEmpty(t, closedTrades)
	require.NotEmpty(t, tradeRecords)
	sawReversal := false
	for _, ct := range closedTrades {
		if ct.ExitReason == momentum.ExitReasonReversal {
			sawReversal = true
			break
		}
	}
	require.True(t, sawReversal,
		"reversal exit must fire when MA crossover flips signal against open position")
	require.Equal(t, 0, len(tradeRecords)%2,
		"trade records must come in matched entry/exit pairs, got %d", len(tradeRecords))
	sum := decimal.Zero
	for _, ct := range closedTrades {
		var err error
		sum, err = sum.Add(ct.PnL)
		require.NoError(t, err)
	}
	require.True(t, res.GetTotalPnL().Cmp(sum) == 0,
		"TotalPnL must equal Σ closed-trade PnL: want %s, got %s", sum, res.GetTotalPnL())
}

func TestBacktest_StopLossExits(t *testing.T) {
	cfg := testCfg(2, 3, decimal.MustNew(2, 0), decimal.Zero)
	s := momentum.New(cfg, nil)
	bt := momentum.NewBacktester(s, nil)
	// Up then sharp drop — fastMA still > slowMA, but stop-loss fires on the drop.
	bars := make([]types.Bar, 0, 9)
	for i := range 5 {
		bars = append(bars, flatBar(i, int64(100+i)))
	}
	bars = append(bars, flatBar(5, 120), flatBar(6, 80), flatBar(7, 75), flatBar(8, 70))

	res, err := bt.Run(context.Background(), bars)
	require.NoError(t, err)
	closedTrades := closedOf(t, res)

	require.NotEmpty(t, closedTrades)
	for _, ct := range closedTrades {
		require.NotEqual(t, momentum.ExitReasonReversal, ct.ExitReason,
			"priority stop > reversal must not tag a stop-loss exit as reversal")
	}
	require.Equal(t, momentum.ExitReasonStopLoss, closedTrades[0].ExitReason,
		"crash from 120 to 80 must fire the stop-loss exit")
}

func TestBacktest_TakeProfitExits(t *testing.T) {
	cfg := testCfg(2, 3, decimal.Zero, decimal.MustNew(2, 0))
	s := momentum.New(cfg, nil)
	bt := momentum.NewBacktester(s, nil)
	// Rising prices into a long entry (price 102 at bar 2 — fastMA
	// 101.5 > slowMA 101), entryATR = mean of true ranges across the
	// first three bars (0, 1, 1) = 0.667. Take-profit threshold =
	// 102 + 2×0.667 = 103.33, so bar 3 (price 104) fires take_profit.
	// StopLossATR=0 disables the stop; the series never falls, so no
	// reversal; only take_profit or strategy_exit can be the first
	// close — a broken take-profit branch surfaces as strategy_exit.
	bars := []types.Bar{flatBar(0, 100), flatBar(1, 101), flatBar(2, 102), flatBar(3, 104), flatBar(4, 111)}

	res, err := bt.Run(context.Background(), bars)
	require.NoError(t, err)
	closedTrades := closedOf(t, res)

	require.NotEmpty(t, closedTrades)
	require.Equal(t, momentum.ExitReasonTakeProfit, closedTrades[0].ExitReason,
		"spike from ~102 to 104 must fire the take-profit exit (threshold ≈ 103.33)")
	require.True(t, closedTrades[0].PnL.IsPos(),
		"take-profit close at the spike price must be profitable for the long, got PnL %s", closedTrades[0].PnL)
}

// TestBacktest_StopFiresAtBarLowNotClose pins the adverse-extreme stop
// evaluation: a bar whose LOW crosses the stop band must close the
// position even when its CLOSE is back above the threshold. With a
// close-only evaluation this fixture leaves the position open to the
// force-close (strategy_exit), so deleting the extreme check fails it.
func TestBacktest_StopFiresAtBarLowNotClose(t *testing.T) {
	cfg := testCfg(2, 3, decimal.MustNew(2, 0), decimal.Zero)
	s := momentum.New(cfg, nil)
	bt := momentum.NewBacktester(s, nil)

	// Entry at bar 2 (close 102, fastMA 101.5 > slowMA 101), entryATR
	// 0.667 → stop threshold ≈ 100.67. Bar 3 dips to 99.5 (below the
	// stop) but closes at 102.5 — above the threshold and still fast >
	// slow, so ONLY the adverse-extreme check can fire here.
	bars := []types.Bar{
		flatBar(0, 100), flatBar(1, 101), flatBar(2, 102),
		{Time: barBase.Add(3 * time.Minute),
			Open: decimal.MustNew(102, 0), High: decimal.MustNew(103, 0),
			Low: half(995), Close: half(1025)},
		flatBar(4, 103), flatBar(5, 104),
	}

	res, err := bt.Run(context.Background(), bars)
	require.NoError(t, err)
	closedTrades := closedOf(t, res)

	require.NotEmpty(t, closedTrades)
	ct := closedTrades[0]
	require.Equal(t, momentum.ExitReasonStopLoss, ct.ExitReason,
		"stop must fire on the bar LOW (99.5 < ~100.67) even though the close (102.5) is above the threshold")
	// Band-level fill: exit at the stop threshold (~100.667), not the
	// bar close (102.5). Bar.Open=102 sits above the level so the gap
	// cap is the level itself.
	wantLevel := stopLevel(102, cfg.StopLossATR)
	assert.True(t, ct.ExitPrice.Equal(wantLevel),
		"stop fill must be the band level (entry − SLmult×ATR), got %s want %s", ct.ExitPrice, wantLevel)
	assert.True(t, ct.ExitPrice.Cmp(decimal.MustNew(102, 0)) < 0,
		"stop fill must sit below entry 102, got %s", ct.ExitPrice)
	assert.True(t, ct.PnL.IsNeg(),
		"stop fill below entry must record a loss, got PnL %s", ct.PnL)
}

// stopLevel replicates the strategy's SL math (entry − SLmult×ATR) so
// tests can compare against the actual fill without depending on the
// internal decimal precision of 2/3 + 102.
func stopLevel(entry int64, slMult decimal.Decimal) decimal.Decimal {
	atr, _ := decimal.MustNew(2, 0).Quo(decimal.MustNew(3, 0))
	dist, _ := slMult.Mul(atr)
	level, _ := decimal.MustNew(entry, 0).Sub(dist)
	return level
}

// Tick-faithful replay: with ticks covering the bar window, the intrabar
// stop fires on the first crossing TICK's price and timestamp, not on
// the bar's close and not via the extreme approximation. Mirrors the
// live run loop, where ticks between two bar closes drive bandExit.
func TestBacktest_TickReplayExitsAtTickPrice(t *testing.T) {
	cfg := testCfg(2, 3, decimal.MustNew(2, 0), decimal.Zero)
	cfg.Resolution = candles.Resolution1m
	s := momentum.New(cfg, nil)
	bt := momentum.NewBacktester(s, nil)

	// Same entry as TestBacktest_StopFiresAtBarLowNotClose: entry at bar 2
	// (close 102, stop threshold ≈ 100.67). Bar 3 has NO adverse low and
	// closes at 102.5 (above the stop, fast > slow), so only a tick can
	// fire: one mid-window tick at 99.5 crosses the stop band.
	bars := []types.Bar{
		flatBar(0, 100), flatBar(1, 101), flatBar(2, 102),
		{Time: barBase.Add(3 * time.Minute),
			Open: decimal.MustNew(102, 0), High: decimal.MustNew(103, 0),
			Low: decimal.MustNew(102, 0), Close: half(1025)},
		flatBar(4, 103), flatBar(5, 104),
	}
	tickTime := barBase.Add(3*time.Minute + 30*time.Second)
	momentum.SetBacktestTicks(bt, []prices.AssetPrice{
		{AssetID: "asset", Price: half(995), YTM: ptrYTM(), Time: tickTime},
	})

	res, err := bt.Run(context.Background(), bars)
	require.NoError(t, err)
	closedTrades := closedOf(t, res)

	require.NotEmpty(t, closedTrades)
	ct := closedTrades[0]
	assert.Equal(t, momentum.ExitReasonStopLoss, ct.ExitReason)
	assert.True(t, ct.ExitPrice.Equal(half(995)), "exit fills at the tick price 99.5, got %s", ct.ExitPrice)
	assert.True(t, ct.CloseTime.Equal(tickTime), "exit is timestamped at the tick, not the bar close")
}

// TestBacktest_TickExitThenBarEntryTimestampsAreMonotonic pins the
// F11 ordering invariant: a tick-driven exit (recorded at the tick's
// timestamp) must precede the next bar-driven entry (recorded at the
// bar CLOSE). Pre-fix, the bar-driven entry was stamped at the bar
// START, which sits before the tick window opens, so a tick that fires
// after the next bar's START produced a non-monotonic trade-records
// timeline (entry_2 < tick_exit). Bar-driven entries must carry the
// CLOSE time so they sort AFTER any tick exit that fired inside the
// preceding bar's window.
func TestBacktest_TickExitThenBarEntryTimestampsAreMonotonic(t *testing.T) {
	cfg := testCfg(2, 3, decimal.MustNew(2, 0), decimal.Zero)
	cfg.Resolution = candles.Resolution1m
	s := momentum.New(cfg, nil)
	bt := momentum.NewBacktester(s, nil)

	// Same entry series as TestBacktest_TickReplayExitsAtTickPrice:
	// rising prices through 6 bars, BUY at bar 2. Bar 3 has no
	// bar-level stop (close 102.5 > stop ≈ 100.67) and fast>slow
	// still holds, so the only thing that can close the position in
	// bar 3's formation window is a crossing tick. Bar 4 still has
	// fast>slow → BUY re-entry.
	bars := []types.Bar{
		flatBar(0, 100), flatBar(1, 101), flatBar(2, 102),
		{Time: barBase.Add(3 * time.Minute),
			Open: decimal.MustNew(102, 0), High: decimal.MustNew(103, 0),
			Low: decimal.MustNew(102, 0), Close: half(1025)},
		flatBar(4, 103), flatBar(5, 104),
	}
	// Tick at bar 4 START + 30s — inside the window
	// (bar 3 CLOSE, bar 4 CLOSE] = (barBase+4min, barBase+5min]. This
	// is AFTER bar 4 START (barBase+4min) but BEFORE bar 4 CLOSE
	// (barBase+5min), so pre-fix the re-entry recorded at bar 4 START
	// precedes the tick exit and the trade-records timeline goes
	// non-monotonic.
	tickTime := barBase.Add(4*time.Minute + 30*time.Second)
	momentum.SetBacktestTicks(bt, []prices.AssetPrice{
		{AssetID: "asset", Price: half(995), YTM: ptrYTM(), Time: tickTime},
	})

	res, err := bt.Run(context.Background(), bars)
	require.NoError(t, err)
	records := recordsOf(t, res)
	closedTrades := closedOf(t, res)

	require.Len(t, closedTrades, 2, "tick exit + force-close after re-entry = 2 trades")
	require.Len(t, records, 4,
		"expected entry_1 + tick-exit_1 + re-entry_2 + force-close_2 "+
			"trade records, got %d", len(records))

	entry, tickExit, reEntry, forceExit := records[0], records[1], records[2], records[3]
	require.True(t, tickExit.Time.After(entry.Time),
		"tick exit must follow the entry, got entry=%s tick_exit=%s",
		entry.Time, tickExit.Time)
	require.True(t, reEntry.Time.After(tickExit.Time),
		"re-entry must be timestamped AFTER the tick exit "+
			"(otherwise the persisted trade-records timeline goes "+
			"non-monotonic: re-entry=%s, tick_exit=%s)",
		reEntry.Time, tickExit.Time)
	require.True(t, forceExit.Time.After(reEntry.Time),
		"force-close must follow the re-entry, got re-entry=%s force-close=%s",
		reEntry.Time, forceExit.Time)
}

// TestBacktest_TakeProfitFiresAtBarHigh pins the favorable-extreme TP
// evaluation: a bar whose HIGH crosses the take-profit band must close
// the position even when its CLOSE is back below the threshold.
func TestBacktest_TakeProfitFiresAtBarHigh(t *testing.T) {
	cfg := testCfg(2, 3, decimal.Zero, decimal.MustNew(2, 0))
	s := momentum.New(cfg, nil)
	bt := momentum.NewBacktester(s, nil)

	// Entry at bar 2 (close 102), entryATR 0.667 → TP threshold ≈
	// 103.33. Bar 3 spikes to 104 (above TP) but closes at 102.5 —
	// below the threshold, so only the favorable extreme fires.
	bars := []types.Bar{
		flatBar(0, 100), flatBar(1, 101), flatBar(2, 102),
		{Time: barBase.Add(3 * time.Minute),
			Open: decimal.MustNew(102, 0), High: decimal.MustNew(104, 0),
			Low: decimal.MustNew(102, 0), Close: half(1025)},
		flatBar(4, 103), flatBar(5, 104),
	}

	res, err := bt.Run(context.Background(), bars)
	require.NoError(t, err)
	closedTrades := closedOf(t, res)

	require.NotEmpty(t, closedTrades)
	ct := closedTrades[0]
	require.Equal(t, momentum.ExitReasonTakeProfit, ct.ExitReason,
		"take-profit must fire on the bar HIGH (104 > ~103.33) even though the close (102.5) is below the threshold")
	// Band-level fill: exit at the TP threshold (~103.333), not the
	// bar close (102.5). Bar.Open=102 sits below the level so the gap
	// cap is the level itself.
	wantLevel := tpLevel(102, cfg.TakeProfitATR)
	assert.True(t, ct.ExitPrice.Equal(wantLevel),
		"TP fill must be the band level (entry + TPmult×ATR), got %s want %s", ct.ExitPrice, wantLevel)
	assert.True(t, ct.ExitPrice.Cmp(decimal.MustNew(102, 0)) > 0,
		"TP fill must sit above entry 102, got %s", ct.ExitPrice)
	assert.True(t, ct.PnL.IsPos(),
		"TP fill above entry must record a profit, got PnL %s", ct.PnL)
}

// tpLevel mirrors stopLevel for the take-profit band (entry + TPmult×ATR).
func tpLevel(entry int64, tpMult decimal.Decimal) decimal.Decimal {
	atr, _ := decimal.MustNew(2, 0).Quo(decimal.MustNew(3, 0))
	dist, _ := tpMult.Mul(atr)
	level, _ := decimal.MustNew(entry, 0).Add(dist)
	return level
}

// TestBacktest_StopGappedDownFillsAtOpen pins the gap-capped fill rule
// for a long stop: when the bar OPENS below the stop level, the order
// can't fill better than the market's first price, so the fill is the
// open (worse than the level).
func TestBacktest_StopGappedDownFillsAtOpen(t *testing.T) {
	cfg := testCfg(2, 3, decimal.MustNew(2, 0), decimal.Zero)
	s := momentum.New(cfg, nil)
	bt := momentum.NewBacktester(s, nil)

	// Entry at bar 2 (close 102, stop ≈ 100.667). Bar 3 opens at 99
	// (below the stop) and stays there — open gapped through the
	// level, so fill must be 99, not 100.667.
	bars := []types.Bar{
		flatBar(0, 100), flatBar(1, 101), flatBar(2, 102),
		{Time: barBase.Add(3 * time.Minute),
			Open: decimal.MustNew(99, 0), High: decimal.MustNew(99, 0),
			Low: decimal.MustNew(98, 0), Close: decimal.MustNew(99, 0)},
	}

	res, err := bt.Run(context.Background(), bars)
	require.NoError(t, err)
	closedTrades := closedOf(t, res)
	require.NotEmpty(t, closedTrades)
	ct := closedTrades[0]
	require.Equal(t, momentum.ExitReasonStopLoss, ct.ExitReason)
	assert.True(t, ct.ExitPrice.Equal(decimal.MustNew(99, 0)),
		"long stop gapped down: fill must be bar.Open (99), got %s", ct.ExitPrice)
}

// TestBacktest_TakeProfitGappedUpFillsAtOpen pins the gap-capped fill
// rule for a long take-profit: when the bar OPENS above the TP level,
// the limit order fills at the open (better than the level).
func TestBacktest_TakeProfitGappedUpFillsAtOpen(t *testing.T) {
	cfg := testCfg(2, 3, decimal.Zero, decimal.MustNew(2, 0))
	s := momentum.New(cfg, nil)
	bt := momentum.NewBacktester(s, nil)

	// Entry at bar 2 (close 102, TP ≈ 103.333). Bar 3 opens at 105
	// (above the TP) and stays there — open gapped through the
	// level, so fill must be 105, not 103.333.
	bars := []types.Bar{
		flatBar(0, 100), flatBar(1, 101), flatBar(2, 102),
		{Time: barBase.Add(3 * time.Minute),
			Open: decimal.MustNew(105, 0), High: decimal.MustNew(106, 0),
			Low: decimal.MustNew(105, 0), Close: decimal.MustNew(105, 0)},
	}

	res, err := bt.Run(context.Background(), bars)
	require.NoError(t, err)
	closedTrades := closedOf(t, res)
	require.NotEmpty(t, closedTrades)
	ct := closedTrades[0]
	require.Equal(t, momentum.ExitReasonTakeProfit, ct.ExitReason)
	assert.True(t, ct.ExitPrice.Equal(decimal.MustNew(105, 0)),
		"long TP gapped up: fill must be bar.Open (105), got %s", ct.ExitPrice)
}

// TestBacktest_VolumeGateSuppressesEntry verifies the backtest replay
// runs Update with the volume gate active: a rising series with every
// post-warmup bar's volume below the gate threshold produces no trades
// at all. Making the gate always-allow fails this test.
func TestBacktest_VolumeGateSuppressesEntry(t *testing.T) {
	cfg := testCfg(2, 3, decimal.Zero, decimal.Zero)
	cfg.VolumeAvgWindow = 3
	cfg.VolumeRatioThreshold = decimal.MustNew(2, 0)
	s := momentum.New(cfg, nil)
	bt := momentum.NewBacktester(s, nil)

	bars := make([]types.Bar, 0, 8)
	for i := range 8 {
		b := flatBar(i, int64(100+i))
		if i < 2 {
			// Warm the volume window high so its mean stays above
			// half of every later bar's volume: mean of any 3-window
			// that includes a 100 is ≥ 40 > 2×10.
			b.Volume = decimal.MustNew(100, 0)
		} else {
			b.Volume = decimal.MustNew(10, 0)
		}
		bars = append(bars, b)
	}

	res, err := bt.Run(context.Background(), bars)
	require.NoError(t, err)

	require.Empty(t, closedOf(t, res), "volume gate must suppress the entry")
	require.Empty(t, recordsOf(t, res), "no trade records without an entry")
}

// TestBacktest_CoverageErrorPropagates: Backtest surfaces getBars'
// coverage errors (the handler maps them to 400).
func TestBacktest_CoverageErrorPropagates(t *testing.T) {
	cfg := testCfg(2, 3, decimal.Zero, decimal.Zero)
	cfg.OrderBookID = uuid.Must(uuid.NewV7())
	s := momentum.New(cfg, nil)
	store := &momentumfakes.FakeCandleHistoryStore{}
	store.CandleRangeReturns(nil, nil, nil)
	momentum.SetCandleHistoryStore(s, store)

	_, err := s.Backtest(context.Background(),
		time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC))

	var cov *candles.ErrNoCandleCoverage
	require.ErrorAs(t, err, &cov)
}

// TestBacktest_ForceClosePersistsExitAndRecordsExitSignal exercises
// the force-close path: the strategy hits Buy on bar 2 and never
// reverses. No stop-loss / take-profit. End of history triggers the
// force-close.
func TestBacktest_ForceClosePersistsExitAndRecordsExitSignal(t *testing.T) {
	cfg := testCfg(2, 3, decimal.Zero, decimal.Zero)
	s := momentum.New(cfg, nil)
	bt := momentum.NewBacktester(s, nil)

	bars := make([]types.Bar, 0, 8)
	for i := range 8 {
		bars = append(bars, flatBar(i, int64(100+i)))
	}

	res, err := bt.Run(context.Background(), bars)
	require.NoError(t, err)

	closedTrades := closedOf(t, res)
	tradeRecords := recordsOf(t, res)

	// Force-close produces exactly 1 closed trade (round-trip).
	require.Len(t, closedTrades, 1, "force-close must close exactly 1 position")
	// And exactly 2 trade_records: entry + exit, so /trades shows a
	// matched pair, not a dangling open entry.
	require.Len(t, tradeRecords, 2,
		"force-close must append an exit TradeRecord so /trades shows "+
			"a matched pair, not a dangling open entry")

	ct := closedTrades[0]
	require.Equal(t, types.SignalBuy, ct.Signal, "open direction is Buy")
	require.Equal(t, momentum.ExitReasonStrategyExit, ct.ExitReason,
		"force-close must tag the close with ExitReasonStrategyExit")

	// PnL integrity: for a Buy, PnL = Exit×Qty − Entry×Qty, recomputed
	// from the trade's own recorded fields in the same op order as
	// computePnL (decimal arithmetic is not distributive).
	cost, err := ct.EntryPrice.Mul(ct.Quantity)
	require.NoError(t, err)
	proceeds, err := ct.ExitPrice.Mul(ct.Quantity)
	require.NoError(t, err)
	wantPnL, err := proceeds.Sub(cost)
	require.NoError(t, err)
	require.True(t, ct.PnL.Cmp(wantPnL) == 0,
		"PnL must be Exit×Qty−Entry×Qty for a long: want %s, got %s", wantPnL, ct.PnL)
	require.True(t, ct.PnL.IsPos(), "rising fixture must close a long at a profit, got %s", ct.PnL)

	sum := decimal.Zero
	for _, c := range closedTrades {
		sum, err = sum.Add(c.PnL)
		require.NoError(t, err)
	}
	require.True(t, res.GetTotalPnL().Cmp(sum) == 0,
		"TotalPnL must equal Σ closed-trade PnL: want %s, got %s", sum, res.GetTotalPnL())
	require.Equal(t, 1, res.GetWinCount(), "single profitable trade must be one win")
	require.Equal(t, 0, res.GetLossCount())

	require.Equal(t, tradeRecords[0].Time, ct.OpenTime)
	require.Equal(t, tradeRecords[1].Time, ct.CloseTime)

	// The force-close exit TradeRecord must carry the same MA / ATR
	// state as the in-loop exit rows (inherited from lastDecision).
	exitRec := tradeRecords[1]
	require.True(t, exitRec.FastMA.IsPos(),
		"force-close exit TradeRecord FastMA must match lastDecision, got %s", exitRec.FastMA)
	require.True(t, exitRec.SlowMA.IsPos(),
		"force-close exit TradeRecord SlowMA must match lastDecision, got %s", exitRec.SlowMA)
}

// Regression (identity bug): Decision.BondID must be the order book's
// BASE ASSET UUID — a different ID from the order book — resolved once
// per backtest via the market API client and stamped on every record.
func TestBacktest_BondIDIsBaseAssetNotOrderBook(t *testing.T) {
	const assetID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	cfg := barTestConfig(momentum.SignalSourcePrice)
	cfg.FastWindow = 2
	cfg.SlowWindow = 3
	cfg.InitialBalance = decimal.MustNew(1000, 0)
	cfg.MinOrderSize = decimal.Zero
	cfg.MaxOrderSize = decimal.Zero
	s := momentum.New(cfg, nil)

	// Hourly rising closes across the warmup-inclusive window: the MA
	// crossover fires a BUY and the force-close at end-of-history books
	// the matched record pair.
	store := coveredStore(dataStart(), btEnd)
	cs := make([]candles.Candle, 0, 28)
	for i := range 28 {
		cs = append(cs, candles.Candle{
			OrderBookID:    cfg.OrderBookID.String(),
			StartTimestamp: dataStart().Add(time.Duration(i) * time.Hour),
			Close:          decimal.MustNew(int64(100+i), 0),
		})
	}
	store.LoadCandlesBucketedReturns(cs, nil)
	momentum.SetCandleHistoryStore(s, store)

	client := &momentumfakes.FakeMarketAPIClient{}
	client.BaseAssetIDReturns(assetID, nil)
	momentum.SetLookupClient(s, client)

	res, err := s.Backtest(context.Background(), btStart, btEnd)
	require.NoError(t, err)
	records := recordsOf(t, res)
	require.NotEmpty(t, records, "rising series must produce trade records")
	for _, r := range records {
		assert.Equal(t, assetID, r.BondID,
			"BondID must be the resolved base asset, not the order book")
	}
	assert.NotEqual(t, cfg.OrderBookID.String(), records[0].BondID)
	assert.Equal(t, 1, client.BaseAssetIDCallCount(), "asset must be resolved exactly once")
}

// Backtest fails fast with a wrapped error when the order book's base
// asset cannot be resolved.
func TestBacktest_RequiresBaseAssetLookup(t *testing.T) {
	cfg := barTestConfig(momentum.SignalSourcePrice)
	s := momentum.New(cfg, nil)
	momentum.SetCandleHistoryStore(s, coveredStore(dataStart(), btEnd))
	client := &momentumfakes.FakeMarketAPIClient{}
	client.BaseAssetIDReturns("", errors.New("dora down"))
	momentum.SetLookupClient(s, client)

	_, err := s.Backtest(context.Background(), btStart, btEnd)
	require.ErrorContains(t, err, "backtest requires the order book's base asset")
}

// TestBacktest_TradeBoundsSuppressWarmupEntry pins the F10 fix: when the
// runner supplies TradeFrom/TradeTo bounds, bars before TradeFrom must
// only warm indicators — they MUST NOT open a position. Fixture: fast=3,
// slow=5, prices 100..108. Slow window fills at bar 4, fast crosses
// above slow at bar 4 → BUY signal on the LAST warmup bar. TradeFrom
// is set to bar 5's start; the in-bounds bars continue the uptrend so
// the cross persists and the next in-bounds bar enters. Pre-fix the
// warmup BUY leaks into the result (entry bar before TradeFrom); post-
// fix the warmup signal is dropped and the in-bounds bar enters. The
// trade record's stamped Time = bar.Time + res, so the in-bounds check
// uses rec.Time - res (the bar START) ≥ TradeFrom.
func TestBacktest_TradeBoundsSuppressWarmupEntry(t *testing.T) {
	cfg := testCfg(3, 5, decimal.Zero, decimal.Zero)
	cfg.Resolution = candles.Resolution1m
	s := momentum.New(cfg, nil)
	bt := momentum.NewBacktester(s, nil)

	// 5 warmup rising bars (fills slow window, MA cross fires at bar 4
	// — the last warmup bar). 4 in-bounds rising bars: the cross
	// persists, so a fresh BUY fires on bar 5 in-bounds. No opposite
	// signal ever appears → position force-closed at the end.
	obs := make([]types.Bar, 0, 9)
	for i := range 5 {
		obs = append(obs, flatBar(i, int64(100+i)))
	}
	for i := range 4 {
		obs = append(obs, flatBar(5+i, int64(105+i)))
	}

	tradeFrom := obs[5].Time
	tradeTo := obs[len(obs)-1].Time
	bt.TradeFrom = tradeFrom
	bt.TradeTo = tradeTo

	res, err := bt.Run(context.Background(), obs)
	require.NoError(t, err)

	closedTrades := closedOf(t, res)
	tradeRecords := recordsOf(t, res)

	// (b) Exactly one closed trade: the warmup signal was suppressed
	// and the in-bounds cross entered.
	require.Len(t, closedTrades, 1, "expected one in-bounds closed trade from the persisted cross")
	require.NotEmpty(t, tradeRecords, "expected trade records for the in-bounds entry")

	// (a) Every trade record must come from a bar in [TradeFrom, TradeTo].
	// rec.Time = bar.Time + res (F11), so rec.Time - res is the bar START.
	for _, rec := range tradeRecords {
		barStart := rec.Time.Add(-time.Minute)
		require.False(t, barStart.Before(tradeFrom),
			"entry bar %s must be >= TradeFrom %s (rec.Time=%s)",
			barStart, tradeFrom, rec.Time)
	}

	// (c) The closed trade's OpenTime must be at the in-bounds bar CLOSE
	// (or later), and the CloseTime must sit inside the requested period.
	ct := closedTrades[0]
	openBarStart := ct.OpenTime.Add(-time.Minute)
	require.False(t, openBarStart.Before(tradeFrom),
		"OpenTime bar %s must be >= TradeFrom %s", openBarStart, tradeFrom)
	require.False(t, ct.CloseTime.After(tradeTo.Add(time.Minute)),
		"CloseTime %s must be <= TradeTo+res %s", ct.CloseTime, tradeTo.Add(time.Minute))
}

// TestBacktest_TradeBoundsRestrictSummaryBounds pins the F10 summary
// override: when the runner pins TradeFrom/TradeTo, the stats.Summarise
// call must use those bounds (not the warmup-inclusive bar slice). The
// reporting restriction is half the F10 finding — stats.Summarise uses
// the period bounds as the Sharpe daily-PnL denominator, so a wider
// span (warmup included) would silently distort Sharpe. Fixture: 1h
// resolution, 72 bars = 3 days. Day 1 (bars 0-23) and day 2 first
// half (bars 24-35) are flat at 100 (warmup). Day 2 second half
// (bars 36-47) rises 101..112 → MA cross → BUY at bar 36 (in-bounds).
// Day 3 first half (bars 48-59) rises 113..124 (position held). Day 3
// second half (bars 60-71) falls 124..102 (steep enough to force the
// reversal exit). The closed trade's PnL lands on day 3.
//
// The test runs the same fixture twice: Run A pins TradeFrom=bar 0
// (so the override is a no-op — slice bounds == requested bounds);
// Run B pins TradeFrom=bar 36 (mid-day 2) so the override must
// shorten the period. The Sharpe ratio depends on the daily-PnL
// series length, so the two Sharpes MUST differ when the override
// is wired; if the override is removed (mutation), both runs use
// slice bounds and the Sharpes are equal.
func TestBacktest_TradeBoundsRestrictSummaryBounds(t *testing.T) {
	const barsPerDay = 24
	const day2Mid = barsPerDay + 12   // bar 36 = day 2 12:00
	const day3Start = 2 * barsPerDay  // bar 48 = day 3 00:00
	const day3Mid = 2*barsPerDay + 12 // bar 60 = day 3 12:00
	const total = 3 * barsPerDay      // 72

	obs := make([]types.Bar, 0, total)
	// Day 1: flat at 100.
	for i := range barsPerDay {
		obs = append(obs, hourlyBar(i, 100))
	}
	// Day 2 first half: flat at 100 (continues warmup).
	for i := barsPerDay; i < day2Mid; i++ {
		obs = append(obs, hourlyBar(i, 100))
	}
	// Day 2 second half: rising 101..112 (12 bars, +1/bar).
	for i := day2Mid; i < day3Start; i++ {
		obs = append(obs, risingHourlyBar(i, int64(101+(i-day2Mid))))
	}
	// Day 3 first half: rising 113..124.
	for i := day3Start; i < day3Mid; i++ {
		obs = append(obs, risingHourlyBar(i, int64(113+(i-day3Start))))
	}
	// Day 3 second half: falling 124..102 (−2/bar, steep enough for
	// fast < slow reversal exit). Once the position closes, the
	// remaining flat bars carry no signal, so the fixture records
	// exactly one closed trade.
	price := int64(124)
	for i := day3Mid; i < total; i++ {
		obs = append(obs, risingHourlyBar(i, price))
		price -= 2
	}

	runOnce := func(tradeFrom time.Time) momentum.BacktestResult {
		cfg := testCfg(3, 5, decimal.Zero, decimal.Zero)
		cfg.Resolution = candles.Resolution1h
		s := momentum.New(cfg, nil)
		bt := momentum.NewBacktester(s, nil)
		bt.TradeFrom = tradeFrom
		bt.TradeTo = obs[len(obs)-1].Time
		res, err := bt.Run(context.Background(), obs)
		require.NoError(t, err)
		return res
	}

	// Run A: TradeFrom pinned to the very first bar (no warmup). The
	// override, if active, is a no-op for this run (slice bounds ==
	// requested bounds). Captures the slice-bounds Sharpe as a
	// baseline so the test fails when the override is removed: with
	// no override, Run B falls back to the slice bounds and matches
	// Run A's Sharpe.
	resA := runOnce(obs[0].Time)
	require.False(t, resA.SharpeRatio.IsZero(),
		"slice-bounds Sharpe must be non-zero for the override assertion to bite")

	// Run B: TradeFrom pinned to mid-day 2. The override, if active,
	// shortens the period to [day 2 mid, end of day 3] — a 1-day
	// daily-PnL series instead of the 3-day series Run A used.
	resB := runOnce(obs[day2Mid].Time)
	require.NotEqual(t, 0, resA.SharpeRatio.Cmp(resB.SharpeRatio),
		"summary-bounds override must shorten the Sharpe window: "+
			"with TradeFrom=mid-day 2 the Sharpe (%s) must differ "+
			"from the slice-bounds Sharpe (%s); equality means the "+
			"override is bypassed", resB.SharpeRatio, resA.SharpeRatio)

	// Sanity: the in-bounds cross must still produce at least one
	// closed trade when the override is active. The override changes
	// reporting bounds, not the entry guard.
	require.NotEmpty(t, resB.ClosedTrades,
		"in-bounds cross must still produce a closed trade when the override is active")
}

// TestBacktest_TradeToExactBoundaryIsExclusive pins the F10 entry-guard
// strictness: bar.Time == TradeTo MUST NOT open a position. The guard
// is `bar.Time.Before(b.TradeTo)` (exclusive upper bound), so a fresh
// entry signal on the last bar — whose Time equals TradeTo — is
// suppressed. Fixture: 5 flat bars at 100 (fast=slow=100, no cross)
// followed by 1 rising bar at 101 (cross fires at bar 5, the last
// bar). TradeFrom=bar 0 (no warmup gate), TradeTo=bar 5's Time.
// Pre-mutation: 0 closed trades (entry at bar 5 suppressed).
// Mutation `!bar.Time.After` → entry allowed at bar 5 → 1 closed
// trade.
func TestBacktest_TradeToExactBoundaryIsExclusive(t *testing.T) {
	cfg := testCfg(3, 5, decimal.Zero, decimal.Zero)
	cfg.Resolution = candles.Resolution1h
	s := momentum.New(cfg, nil)
	bt := momentum.NewBacktester(s, nil)

	// 5 flat bars at 100 keep fast=slow=100 (no cross) until bar 5's
	// spike to 101 (Close=101) crosses fast above slow — the only
	// BUY in the series, sitting on the last bar.
	obs := make([]types.Bar, 0, 6)
	for i := range 5 {
		obs = append(obs, hourlyBar(i, 100))
	}
	obs = append(obs, risingHourlyBar(5, 101))

	bt.TradeFrom = obs[0].Time
	bt.TradeTo = obs[len(obs)-1].Time

	res, err := bt.Run(context.Background(), obs)
	require.NoError(t, err)

	require.Empty(t, res.ClosedTrades,
		"entry on bar.Time == TradeTo must be suppressed; got %d closed trades",
		len(res.ClosedTrades))
	require.Empty(t, res.TradeRecords,
		"entry on bar.Time == TradeTo must be suppressed; got %d trade records",
		len(res.TradeRecords))
}
