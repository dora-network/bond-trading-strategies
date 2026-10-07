package meanreversion_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/prices"
	"github.com/dora-network/bond-trading-strategies/strategy/meanreversion"
	"github.com/dora-network/bond-trading-strategies/strategy/meanreversion/meanreversionfakes"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/bond-trading-strategies/trades"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBacktester_NoTradesBeforeWindowFull(t *testing.T) {
	s := meanreversion.New(defaultConfig(), nil)
	bt := meanreversion.NewBacktester(s, nil)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	result, err := bt.Run(ctx, makeBars(19, decimal.MustNew(1, 2)))
	require.NoError(t, err)
	assert.Empty(t, result.ClosedTrades)
	assert.True(t, result.TotalPnL.IsZero())
}

func TestBacktester_ProfitableReversion(t *testing.T) {
	cfg := defaultConfig()
	cfg.LookbackWindow = 20
	cfg.MinStdDev = decimal.MustNew(1, 4)
	cfg.InitialBalance = decimal.MustNew(10000, 0)
	s := meanreversion.New(cfg, nil)
	bt := meanreversion.NewBacktester(s, nil)

	base := decimal.MustNew(5, 2)
	bars := make([]types.Bar, 0, 31)
	for i := range 20 {
		var sp decimal.Decimal
		switch i % 3 {
		case 0:
			sp = decimal.MustNew(9, 3)
		case 1:
			sp = decimal.MustNew(10, 3)
		case 2:
			sp = decimal.MustNew(11, 3)
		}
		ytm, _ := base.Add(sp)
		bars = append(bars, bar(i, ytm, base))
	}

	// Wide spread → BUY entry on the z-cross.
	obs20YTM := decimal.MustNew(13, 2)
	bars = append(bars, bar(20, obs20YTM, decimal.MustNew(5, 2)))

	// Spread reverts → take-profit exit.
	for i := range 10 {
		bars = append(bars, bar(21+i, decimal.MustNew(6, 2), decimal.MustNew(5, 2)))
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	result, err := bt.Run(ctx, bars)
	require.NoError(t, err)
	require.NotEmpty(t, result.ClosedTrades, "should have at least one closed trade")
	assert.Equal(t, types.SignalBuy, result.ClosedTrades[0].Signal, "entry fires on the z-cross")
	assert.True(t, result.TotalPnL.IsPos(), "reversion trade should be profitable")
	assert.Greater(t, result.WinCount, 0)
}

func TestBacktester_AdverseExtremeStopFiresOnHighYTM(t *testing.T) {
	cfg := defaultConfig()
	cfg.LookbackWindow = 20
	cfg.MinStdDev = decimal.MustNew(1, 4)
	cfg.InitialBalance = decimal.MustNew(10000, 0)
	s := meanreversion.New(cfg, nil)
	bt := meanreversion.NewBacktester(s, nil)

	base := decimal.MustNew(5, 2)
	bars := make([]types.Bar, 0, 22)
	for i := range 20 {
		var sp decimal.Decimal
		switch i % 3 {
		case 0:
			sp = decimal.MustNew(9, 3)
		case 1:
			sp = decimal.MustNew(10, 3)
		case 2:
			sp = decimal.MustNew(11, 3)
		}
		ytm, _ := base.Add(sp)
		bars = append(bars, bar(i, ytm, base))
	}
	// Entry: wide close spread → long.
	entry := bar(20, decimal.MustNew(13, 2), decimal.MustNew(5, 2))
	bars = append(bars, entry)

	// Next bar: the close spread alone stays below the stop z (close
	// z ≈ 3.09 < 3.5 and above the 0.5 take-profit floor), but the
	// intra-bar high is far wider than the stop z — the stop must fire
	// on the high, not the close.
	next := bar(21, decimal.MustNew(8, 2), decimal.MustNew(5, 2))
	next.HighYTM = decimal.MustNew(16, 2)
	// Pin a Low below Close so the test can verify the
	// adverse-price-extreme fill convention.
	next.Low, _ = next.Close.Sub(decimal.MustNew(5, 0))
	bars = append(bars, next)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	result, err := bt.Run(ctx, bars)
	require.NoError(t, err)
	require.Len(t, result.ClosedTrades, 1)
	ct := result.ClosedTrades[0]
	assert.Equal(t, meanreversion.ExitReasonStopLoss, ct.ExitReason)
	assert.True(t, ct.CloseTime.Equal(next.Time.Add(time.Hour)),
		"stop closes on the adverse-extreme bar at the bar CLOSE "+
			"(= next.Time + res), got %s want %s", ct.CloseTime, next.Time.Add(time.Hour))
	assert.True(t, ct.ExitZScore.Cmp(decimal.MustNew(35, 1)) >= 0, "exit z is the adverse extreme z")
	// YTM extremes have no direct price mapping; the backtest records
	// the bar's adverse PRICE extreme (long → Low, short → High) as
	// the "worse outcome" fill. The bar's Low=5.05 sits below the
	// close 5.08, so a long records 5.05.
	assert.True(t, ct.ExitPrice.Equal(next.Low),
		"adverse-extreme fill must be the bar's adverse price extreme (Low for a long), got %s want %s",
		ct.ExitPrice, next.Low)
	assert.True(t, ct.ExitPrice.Cmp(next.Close) < 0,
		"adverse-extreme fill must be worse than the close, got %s close %s",
		ct.ExitPrice, next.Close)
}

// Tick-faithful replay: with ticks covering the bar windows, the intrabar
// stop fires on the first crossing TICK (its price and timestamp), not on
// the bar close and not on the extreme approximation. Mirrors the live run
// loop, where ticks between two bar closes drive intrabar exits.
func TestBacktester_TickReplayExitsAtTickPrice(t *testing.T) {
	cfg := defaultConfig()
	cfg.LookbackWindow = 20
	cfg.MinStdDev = decimal.MustNew(1, 4)
	cfg.InitialBalance = decimal.MustNew(10000, 0)
	s := meanreversion.New(cfg, nil)
	bt := meanreversion.NewBacktester(s, nil)

	base := decimal.MustNew(5, 2)
	bars := make([]types.Bar, 0, 22)
	for i := range 20 {
		var sp decimal.Decimal
		switch i % 3 {
		case 0:
			sp = decimal.MustNew(9, 3)
		case 1:
			sp = decimal.MustNew(10, 3)
		case 2:
			sp = decimal.MustNew(11, 3)
		}
		ytm, _ := base.Add(sp)
		bars = append(bars, bar(i, ytm, base))
	}
	// Entry at bar 20's close (spread 0.08 → long).
	bars = append(bars, bar(20, decimal.MustNew(13, 2), decimal.MustNew(5, 2)))

	// Bar 21 closes at a mid-band spread (no close-decision exit) but its
	// intra-bar high would cross the stop — the extreme approximation would
	// exit at the CLOSE price. A mid-bar tick at the same adverse YTM must
	// instead exit at the TICK's price.
	next := bar(21, decimal.MustNew(8, 2), decimal.MustNew(5, 2))
	next.HighYTM = decimal.MustNew(16, 2)
	bars = append(bars, next)

	benign := decimal.MustNew(8, 2)   // z ≈ 1: between take-profit and stop bands
	adverse := decimal.MustNew(16, 2) // same as the extreme: z above the stop
	tickPrice := bondPriceFromYTM(adverse)
	meanreversion.SetBacktestTicks(bt, []prices.AssetPrice{
		{
			AssetID: "asset", Price: bondPriceFromYTM(benign), YTM: ptrYTM(benign),
			Time: epoch.Add(21*time.Hour + 30*time.Minute),
		},
		{
			AssetID: "asset", Price: tickPrice, YTM: ptrYTM(adverse),
			Time: epoch.Add(22*time.Hour + 30*time.Minute),
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	result, err := bt.Run(ctx, bars)
	require.NoError(t, err)
	require.Len(t, result.ClosedTrades, 1)
	ct := result.ClosedTrades[0]
	assert.Equal(t, meanreversion.ExitReasonStopLoss, ct.ExitReason)
	assert.True(t, ct.ExitPrice.Equal(tickPrice), "exit fills at the tick price %s, got %s", tickPrice, ct.ExitPrice)
	assert.False(t, ct.ExitPrice.Equal(next.Close), "tick exit must not fill at the bar close")
	assert.True(t, ct.CloseTime.Equal(epoch.Add(22*time.Hour+30*time.Minute)),
		"exit is timestamped at the tick, not the bar close")
	assert.True(t, ct.ExitZScore.Cmp(decimal.MustNew(35, 1)) >= 0, "exit z is the tick's z (tick YTM, bar benchmark)")
}

// TestBacktester_TickExitThenBarEntryTimestampsAreMonotonic pins the
// F11 ordering invariant for meanreversion: a tick-driven exit
// (recorded at the tick's timestamp) must precede the next bar-driven
// entry (recorded at the bar CLOSE). Pre-fix, the bar-driven entry was
// stamped at the bar START, which sits before the tick window opens,
// so a tick that fires after the next bar's START produced a
// non-monotonic trade-records timeline (entry_2 < tick_exit).
// Bar-driven entries must carry the CLOSE time so they sort AFTER any
// tick exit that fired inside the preceding bar's window.
func TestBacktester_TickExitThenBarEntryTimestampsAreMonotonic(t *testing.T) {
	cfg := defaultConfig()
	cfg.LookbackWindow = 20
	cfg.MinStdDev = decimal.MustNew(1, 4)
	cfg.InitialBalance = decimal.MustNew(10000, 0)
	s := meanreversion.New(cfg, nil)
	bt := meanreversion.NewBacktester(s, nil)

	base := decimal.MustNew(5, 2)
	bars := make([]types.Bar, 0, 24)
	for i := range 20 {
		var sp decimal.Decimal
		switch i % 3 {
		case 0:
			sp = decimal.MustNew(9, 3)
		case 1:
			sp = decimal.MustNew(10, 3)
		case 2:
			sp = decimal.MustNew(11, 3)
		}
		ytm, _ := base.Add(sp)
		bars = append(bars, bar(i, ytm, base))
	}
	// Entry at bar 20 (wide spread → long).
	bars = append(bars, bar(20, decimal.MustNew(13, 2), decimal.MustNew(5, 2)))
	// Bar 21 stays at the mid-band spread (no close-decision exit).
	bars = append(bars, bar(21, decimal.MustNew(8, 2), decimal.MustNew(5, 2)))
	// Bar 22 returns to a wide spread → fresh BUY re-entry.
	bars = append(bars, bar(22, decimal.MustNew(13, 2), decimal.MustNew(5, 2)))

	// Tick at bar 22 START + 30m = epoch+22h+30m. meanreversion
	// runs on 1h bars, so the tick window for bar 21 is
	// (bar 21 CLOSE, bar 22 CLOSE] = (epoch+22h, epoch+23h]. The
	// tick lands AFTER bar 22 START (epoch+22h) but BEFORE bar 22
	// CLOSE (epoch+23h) — pre-fix the re-entry is recorded at
	// epoch+22h < tick time epoch+22h30m, so the trade-records
	// timeline goes non-monotonic.
	adverse := decimal.MustNew(16, 2)
	tickTime := epoch.Add(22*time.Hour + 30*time.Minute)
	meanreversion.SetBacktestTicks(bt, []prices.AssetPrice{
		{
			AssetID: "asset", Price: bondPriceFromYTM(adverse), YTM: ptrYTM(adverse),
			Time: tickTime,
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	result, err := bt.Run(ctx, bars)
	require.NoError(t, err)
	records := result.TradeRecords
	closedTrades := result.ClosedTrades

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
	require.False(t, forceExit.Time.Before(reEntry.Time),
		"force-close must not precede the re-entry, got re-entry=%s force-close=%s",
		reEntry.Time, forceExit.Time)
}

// With no ticks seeded, the same fixture falls back to the bar-extreme
// approximation: the stop still fires but fills at the bar's close price.
func TestBacktester_NoTicksFallsBackToExtreme(t *testing.T) {
	cfg := defaultConfig()
	cfg.LookbackWindow = 20
	cfg.MinStdDev = decimal.MustNew(1, 4)
	cfg.InitialBalance = decimal.MustNew(10000, 0)
	s := meanreversion.New(cfg, nil)
	bt := meanreversion.NewBacktester(s, nil)

	base := decimal.MustNew(5, 2)
	bars := make([]types.Bar, 0, 22)
	for i := range 20 {
		var sp decimal.Decimal
		switch i % 3 {
		case 0:
			sp = decimal.MustNew(9, 3)
		case 1:
			sp = decimal.MustNew(10, 3)
		case 2:
			sp = decimal.MustNew(11, 3)
		}
		ytm, _ := base.Add(sp)
		bars = append(bars, bar(i, ytm, base))
	}
	bars = append(bars, bar(20, decimal.MustNew(13, 2), decimal.MustNew(5, 2)))
	next := bar(21, decimal.MustNew(8, 2), decimal.MustNew(5, 2))
	next.HighYTM = decimal.MustNew(16, 2)
	// Pin a Low below Close so the test can verify the
	// adverse-price-extreme fill convention.
	next.Low, _ = next.Close.Sub(decimal.MustNew(5, 0))
	bars = append(bars, next)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	result, err := bt.Run(ctx, bars)
	require.NoError(t, err)
	require.Len(t, result.ClosedTrades, 1)
	ct := result.ClosedTrades[0]
	assert.Equal(t, meanreversion.ExitReasonStopLoss, ct.ExitReason)
	// YTM extremes have no direct price mapping; the fallback fills
	// at the bar's adverse PRICE extreme (long → Low).
	assert.True(t, ct.ExitPrice.Equal(next.Low),
		"fallback fills at the adverse price extreme (Low for a long), got %s want %s",
		ct.ExitPrice, next.Low)
}

func ptrYTM(d decimal.Decimal) *decimal.Decimal {
	c := d
	return &c
}

func TestBacktester_LosingTradeForceClosedAtEnd(t *testing.T) {
	cfg := defaultConfig()
	cfg.LookbackWindow = 10
	cfg.StopLossZScore = decimal.Zero
	cfg.MinStdDev = decimal.MustNew(1, 4)
	cfg.InitialBalance = decimal.MustNew(10000, 0)
	s := meanreversion.New(cfg, nil)
	bt := meanreversion.NewBacktester(s, nil)

	base := decimal.MustNew(5, 2)
	bars := make([]types.Bar, 0, 15)
	for i := range 10 {
		var sp decimal.Decimal
		switch i % 3 {
		case 0:
			sp = decimal.MustNew(9, 3)
		case 1:
			sp = decimal.MustNew(10, 3)
		case 2:
			sp = decimal.MustNew(11, 3)
		}
		ytm, _ := base.Add(sp)
		bars = append(bars, bar(i, ytm, base))
	}
	for i := range 5 {
		bars = append(bars, bar(10+i, decimal.MustNew(13, 2), decimal.MustNew(5, 2)))
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	result, err := bt.Run(ctx, bars)
	require.NoError(t, err)
	require.NotEmpty(t, result.ClosedTrades)
	assert.Equal(t, 1, len(result.ClosedTrades))
}

func TestBacktestResult_MaxDrawdown(t *testing.T) {
	cfg := defaultConfig()
	cfg.InitialBalance = decimal.MustNew(10000, 0)
	s := meanreversion.New(cfg, nil)
	bt := meanreversion.NewBacktester(s, nil)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	result, err := bt.Run(ctx, makeBars(50, decimal.MustNew(1, 2)))
	require.NoError(t, err)
	assert.True(t, !result.MaxDrawdown.IsNeg())
}

// tapeStore is a minimal in-memory trades.TradeStore streaming the
// given trades.
type tapeStore struct {
	trades []trades.Trade
}

func (s *tapeStore) StreamTrades(
	_ context.Context, _ uuid.UUID, _, _ time.Time,
) (<-chan trades.Trade, <-chan error) {
	ch := make(chan trades.Trade, len(s.trades))
	for _, tr := range s.trades {
		ch <- tr
	}
	close(ch)
	errCh := make(chan error)
	close(errCh)
	return ch, errCh
}

// tapeStoreOf returns a fake trades.TradeStore streaming the given trades.
func tapeStoreOf(ts ...trades.Trade) *tapeStore { return &tapeStore{trades: ts} }

// imbalanceBars mirrors TestBacktester_ProfitableReversion's bar set: 20
// warm bars, a wide-spread entry bar at index 20 (BUY signal), then flat
// bars.
func imbalanceBars() []types.Bar {
	base := decimal.MustNew(5, 2)
	bars := make([]types.Bar, 0, 31)
	for i := range 20 {
		var sp decimal.Decimal
		switch i % 3 {
		case 0:
			sp = decimal.MustNew(9, 3)
		case 1:
			sp = decimal.MustNew(10, 3)
		case 2:
			sp = decimal.MustNew(11, 3)
		}
		ytm, _ := base.Add(sp)
		bars = append(bars, bar(i, ytm, base))
	}
	bars = append(bars, bar(20, decimal.MustNew(13, 2), decimal.MustNew(5, 2)))
	for i := range 10 {
		bars = append(bars, bar(21+i, decimal.MustNew(6, 2), decimal.MustNew(5, 2)))
	}
	return bars
}

// TestBacktester_ImbalanceGateBlocksOpposingEntry verifies that historical
// SELL trades interleaved before the entry bar suppress the BUY entry.
func TestBacktester_ImbalanceGateBlocksOpposingEntry(t *testing.T) {
	cfg := defaultConfig()
	cfg.LookbackWindow = 20
	cfg.MinStdDev = decimal.MustNew(1, 4)
	cfg.InitialBalance = decimal.MustNew(10000, 0)
	cfg.ImbalanceWindow = 3
	s := meanreversion.New(cfg, nil)
	meanreversion.SetTradeHistoryStore(s, tapeStoreOf(
		trades.Trade{Time: epoch.Add(15 * time.Hour), Price: decimal.MustNew(100, 0), Quantity: decimal.One, Side: "SELL"},
		trades.Trade{Time: epoch.Add(16 * time.Hour), Price: decimal.MustNew(100, 0), Quantity: decimal.One, Side: "SELL"},
		trades.Trade{Time: epoch.Add(17 * time.Hour), Price: decimal.MustNew(100, 0), Quantity: decimal.One, Side: "SELL"},
	))
	bt := meanreversion.NewBacktester(s, nil)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	result, err := bt.Run(ctx, imbalanceBars())
	require.NoError(t, err)
	assert.Empty(t, result.TradeRecords, "net-selling tape must block the BUY entry")
	assert.Empty(t, result.ClosedTrades)
}

// TestBacktester_ImbalanceGateNoSourcePassesThrough verifies the entry fires
// when the gate is enabled but no trade source is wired (passthrough).
func TestBacktester_ImbalanceGateNoSourcePassesThrough(t *testing.T) {
	cfg := defaultConfig()
	cfg.LookbackWindow = 20
	cfg.MinStdDev = decimal.MustNew(1, 4)
	cfg.InitialBalance = decimal.MustNew(10000, 0)
	cfg.ImbalanceWindow = 3
	s := meanreversion.New(cfg, nil)
	bt := meanreversion.NewBacktester(s, nil)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	result, err := bt.Run(ctx, imbalanceBars())
	require.NoError(t, err)
	assert.NotEmpty(t, result.ClosedTrades, "no trade source means the gate passes through")
	assert.Equal(t, types.SignalBuy, result.ClosedTrades[0].Signal)
}

// TestBacktester_ImbalanceMidBarTradeGatesThatBarsDecision verifies the
// trade-interleave cutoff is the bar CLOSE (bar.Time+res), not the bar
// start: the entry bar (index 20) starts at 20:00 and its decision
// happens at 21:00 (1h bars). A SELL trade at 20:30 — during the entry
// bar — must fold into the imbalance window before that bar's gate and
// block the BUY. With a start-anchored cutoff the trade would lag one
// bar and the entry would fire.
func TestBacktester_ImbalanceMidBarTradeGatesThatBarsDecision(t *testing.T) {
	cfg := defaultConfig()
	cfg.LookbackWindow = 20
	cfg.MinStdDev = decimal.MustNew(1, 4)
	cfg.InitialBalance = decimal.MustNew(10000, 0)
	cfg.ImbalanceWindow = 3
	s := meanreversion.New(cfg, nil)
	meanreversion.SetTradeHistoryStore(s, tapeStoreOf(
		trades.Trade{Time: epoch.Add(20*time.Hour + 30*time.Minute), Price: decimal.MustNew(100, 0), Quantity: decimal.One, Side: "SELL"},
		trades.Trade{Time: epoch.Add(20*time.Hour + 40*time.Minute), Price: decimal.MustNew(100, 0), Quantity: decimal.One, Side: "SELL"},
		trades.Trade{Time: epoch.Add(20*time.Hour + 50*time.Minute), Price: decimal.MustNew(100, 0), Quantity: decimal.One, Side: "SELL"},
	))
	bt := meanreversion.NewBacktester(s, nil)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	result, err := bt.Run(ctx, imbalanceBars())
	require.NoError(t, err)
	assert.Empty(t, result.TradeRecords,
		"mid-entry-bar net-selling tape must block the BUY on that same bar's decision")
	assert.Empty(t, result.ClosedTrades)
}

// Regression (identity bug): Decision.BondID must be the order book's
// BASE ASSET UUID — a different ID from the order book — carried from
// the strategy onto every trade record.
func TestBacktester_BondIDIsBaseAssetNotOrderBook(t *testing.T) {
	const assetID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	cfg := defaultConfig()
	cfg.MinStdDev = decimal.MustNew(1, 4)
	cfg.InitialBalance = decimal.MustNew(10000, 0)
	cfg.OrderBookID = uuid.Must(uuid.NewV7())
	s := meanreversion.New(cfg, nil)
	meanreversion.SetBaseAssetID(s, assetID)

	// Same profitable-reversion fixture as TestBacktester_ProfitableReversion.
	base := decimal.MustNew(5, 2)
	bars := make([]types.Bar, 0, 31)
	for i := range 20 {
		var sp decimal.Decimal
		switch i % 3 {
		case 0:
			sp = decimal.MustNew(9, 3)
		case 1:
			sp = decimal.MustNew(10, 3)
		case 2:
			sp = decimal.MustNew(11, 3)
		}
		ytm, _ := base.Add(sp)
		bars = append(bars, bar(i, ytm, base))
	}
	bars = append(bars, bar(20, decimal.MustNew(13, 2), decimal.MustNew(5, 2)))
	for i := range 10 {
		bars = append(bars, bar(21+i, decimal.MustNew(6, 2), decimal.MustNew(5, 2)))
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	result, err := meanreversion.NewBacktester(s, nil).Run(ctx, bars)
	require.NoError(t, err)
	require.NotEmpty(t, result.ClosedTrades, "reversion fixture must close at least one trade")
	for _, ct := range result.ClosedTrades {
		assert.Equal(t, assetID, ct.BondID,
			"BondID must be the resolved base asset, not the order book")
		assert.NotEqual(t, cfg.OrderBookID.String(), ct.BondID)
	}
}

// Backtest fails fast with a wrapped error when the order book's base
// asset cannot be resolved.
func TestBacktest_RequiresBaseAssetLookup(t *testing.T) {
	cfg := defaultConfig()
	cfg.OrderBookID = uuid.Must(uuid.NewV7())
	cfg.Resolution = "1h"
	cfg.Tenor = "10Y"
	s := meanreversion.New(cfg, nil)
	end := time.Now().UTC().Add(-24 * time.Hour)
	meanreversion.SetCandleHistoryStore(s, coveredStore(end.Add(-72*time.Hour), end))
	meanreversion.SetBenchmarkYieldClient(s, &meanreversionfakes.FakeBenchmarkYieldClient{})
	client := &meanreversionfakes.FakeMarketAPIClient{}
	client.BaseAssetIDReturns("", errors.New("dora down"))
	meanreversion.SetLookupClient(s, client)

	_, err := s.Backtest(context.Background(), end.Add(-24*time.Hour), end)
	require.ErrorContains(t, err, "backtest requires the order book's base asset")
}

// Backtest and live runs fail fast with one clear error when the config
// carries no usable tenor — instead of per-bar parse failures deep in
// getBars/getBenchmarkYield (the "unsupported tenor" bug).
func TestBacktest_RequiresTenor(t *testing.T) {
	cfg := defaultConfig()
	cfg.OrderBookID = uuid.Must(uuid.NewV7())
	cfg.Resolution = "1h"
	cfg.Tenor = "" // the broken default
	s := meanreversion.New(cfg, nil)
	end := time.Now().UTC().Add(-24 * time.Hour)

	_, err := s.Backtest(context.Background(), end.Add(-24*time.Hour), end)
	require.ErrorContains(t, err, "tenor is required for mean_reversion")

	cfg.Tenor = "13Y" // unsupported value surfaces the parse error too
	s2 := meanreversion.New(cfg, nil)
	_, err = s2.Backtest(context.Background(), end.Add(-24*time.Hour), end)
	require.ErrorContains(t, err, "tenor is required for mean_reversion")
}

// TestBacktester_TradeBoundsSuppressWarmupEntry pins the F10 fix: when
// the runner supplies TradeFrom/TradeTo bounds, bars before TradeFrom
// must only warm indicators — they MUST NOT open a position. Fixture:
// 20 warmup bars cycling spreads (0.009, 0.010, 0.011) → 1 warmup-tail
// wide-spread bar (0.08, z-cross fires BUY) → 3 in-bounds wide-spread
// bars (sustained BUY if flat) → 7 in-bounds reverting bars (spread
// 0.01, exit). Pre-fix the warmup BUY leaks into the result; post-fix
// the warmup signal is dropped and the in-bounds bar enters. Trade
// records are stamped at bar CLOSE (bar.Time + res, F11), so the
// in-bounds check uses rec.Time - res.
func TestBacktester_TradeBoundsSuppressWarmupEntry(t *testing.T) {
	cfg := defaultConfig()
	cfg.LookbackWindow = 20
	cfg.MinStdDev = decimal.MustNew(1, 4)
	cfg.InitialBalance = decimal.MustNew(10000, 0)
	s := meanreversion.New(cfg, nil)
	bt := meanreversion.NewBacktester(s, nil)

	base := decimal.MustNew(5, 2)
	spreads := []decimal.Decimal{
		decimal.MustNew(9, 3), decimal.MustNew(10, 3), decimal.MustNew(11, 3),
	}
	buildBar := func(i int, ytm decimal.Decimal) types.Bar {
		return types.Bar{
			Time:           epoch.Add(time.Duration(i) * time.Hour),
			Close:          bondPriceFromYTM(ytm),
			CloseYTM:       ytm,
			BenchmarkYield: base,
		}
	}

	// 20 warmup bars cycling spreads.
	bars := make([]types.Bar, 0, 31)
	for i := range 20 {
		sp := spreads[i%3]
		ytm, _ := base.Add(sp)
		bars = append(bars, buildBar(i, ytm))
	}
	// Warmup-tail wide spread (z-cross fires BUY here — suppressed post-fix).
	obs20YTM := decimal.MustNew(13, 2)
	bars = append(bars, buildBar(20, obs20YTM))
	// In-bounds sustained wide spread (BUY fires here if flat).
	for i := range 3 {
		bars = append(bars, buildBar(21+i, obs20YTM))
	}
	// In-bounds reverting spread (z exits → take-profit close).
	for i := range 7 {
		bars = append(bars, buildBar(24+i, decimal.MustNew(6, 2)))
	}

	tradeFrom := bars[21].Time
	tradeTo := bars[len(bars)-1].Time
	bt.TradeFrom = tradeFrom
	bt.TradeTo = tradeTo

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	res, err := bt.Run(ctx, bars)
	require.NoError(t, err)

	// (b) Exactly one closed trade: the warmup signal was suppressed
	// and the in-bounds wide spread entered.
	require.Len(t, res.ClosedTrades, 1,
		"expected one in-bounds closed trade from the z-cross series")

	// (a) Every trade record must come from a bar in [TradeFrom, TradeTo].
	// rec.Time = bar.Time + res (F11), so rec.Time - res is the bar START.
	for _, rec := range res.TradeRecords {
		barStart := rec.Time.Add(-time.Hour)
		require.False(t, barStart.Before(tradeFrom),
			"entry bar %s must be >= TradeFrom %s (rec.Time=%s)",
			barStart, tradeFrom, rec.Time)
	}

	// (c) The closed trade's OpenTime must be at the in-bounds bar CLOSE
	// (or later), and the CloseTime must sit inside the requested period.
	ct := res.ClosedTrades[0]
	openBarStart := ct.OpenTime.Add(-time.Hour)
	require.False(t, openBarStart.Before(tradeFrom),
		"OpenTime bar %s must be >= TradeFrom %s", openBarStart, tradeFrom)
	require.False(t, ct.CloseTime.After(tradeTo.Add(time.Hour)),
		"CloseTime %s must be <= TradeTo+res %s", ct.CloseTime, tradeTo.Add(time.Hour))
}
