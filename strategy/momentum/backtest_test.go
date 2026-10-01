package momentum_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/candles"
	"github.com/dora-network/bond-trading-strategies/prices"
	"github.com/dora-network/bond-trading-strategies/strategy/momentum"
	"github.com/dora-network/bond-trading-strategies/strategy/momentum/momentumfakes"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	require.Equal(t, momentum.ExitReasonStopLoss, closedTrades[0].ExitReason,
		"stop must fire on the bar LOW (99.5 < ~100.67) even though the close (102.5) is above the threshold")
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
	require.Equal(t, momentum.ExitReasonTakeProfit, closedTrades[0].ExitReason,
		"take-profit must fire on the bar HIGH (104 > ~103.33) even though the close (102.5) is below the threshold")
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
