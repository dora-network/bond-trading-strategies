package breakout_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/prices"
	"github.com/dora-network/bond-trading-strategies/strategy/breakout"
	"github.com/dora-network/bond-trading-strategies/strategy/breakout/breakoutfakes"
	"github.com/dora-network/bond-trading-strategies/strategy/stats"
	"github.com/dora-network/bond-trading-strategies/strategy/strategyfakes"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
)

// TestBacktest_SingleBreakoutTrade constructs a price series with a known
// compression followed by a sustained breakout, then a sustained run that
// holds the position open. The backtest must:
//   - open exactly one BUY at the breakout price (110)
//   - force-close it at the end of history (no opposite signal fires)
//   - record compression ratio at entry (compressed) and at exit
func TestBacktest_SingleBreakoutTrade(t *testing.T) {
	t.Parallel()
	cfg := defaultCfg()
	cfg.ConfirmationBars = 1 // single tick fires
	cfg.InitialBalance = decimal.MustNew(10000, 0)
	cfg.Leverage = decimal.One
	s := breakout.New(cfg, nil)

	// Synthetic price series:
	//   30 flat ticks at 100  → fills long window, arms compression (ratio = 0)
	//    1 jump to 110        → BUY at the breakout tick
	//   10 rising ticks 110→120 → hold the long; no opposite signal
	const flatAt100 = 30
	const risingTail = 10
	obs := make([]types.Bar, 0, flatAt100+1+risingTail)
	for i := range flatAt100 {
		obs = append(obs, flatBar(i, 100))
	}
	obs = append(obs, flatBar(flatAt100, 110))
	for i := range risingTail {
		// 110, 111, 112, ..., 119
		obs = append(obs, flatBar(flatAt100+1+i, 110+int64(i)+1))
	}

	bt := breakout.NewBacktester(s, nil)
	res, err := bt.Run(context.Background(), obs)
	require.NoError(t, err)

	// Exactly one closed trade.
	require.Len(t, res.ClosedTrades, 1, "expected exactly one closed trade from the breakout series")
	ct := res.ClosedTrades[0]

	assert.Equal(t, types.SignalBuy, ct.Signal, "open signal should be BUY")
	assert.Equal(t, decimal.MustNew(110, 0), ct.EntryPrice,
		"entry price should equal the breakout tick (110)")
	assert.Equal(t, decimal.MustNew(120, 0), ct.ExitPrice,
		"exit price should equal the force-close level (last rising tick = 120)")
	assert.True(t, ct.PnL.IsPos(), "long position 110→120 should be profitable; got %s", ct.PnL.String())
	assert.Equal(t, breakout.ExitReasonStrategyExit, ct.ExitReason,
		"position stays open through the rising tail, so the backtest records a strategy_exit")
	// Note: EntryCompressionRatio is the ratio AT the BUY tick, which has
	// already spiked (ShortVol/LongVol > 1 on a 100→110 breakout). The
	// fact that a BUY fired at all proves compression was armed during
	// the flat period; we don't assert the ratio here.
}

// TestBacktest_ForceCloseCarriesEntryArmedRatio verifies that the
// force-close exit TradeRecord carries the entry's armed compression
// ratio. Decision.ArmedCompressionRatio is only populated on signal
// ticks (and reset after firing), so the final HOLD decision has the
// zero value — the exit row must source the ratio from the open trade,
// not from the last decision. Unlike the flat-series tests, the quiet
// phase here oscillates so the armed ratio is non-zero and the
// equality assertion is non-vacuous.
func TestBacktest_ForceCloseCarriesEntryArmedRatio(t *testing.T) {
	t.Parallel()
	cfg := defaultCfg()
	cfg.ConfirmationBars = 2
	cfg.StopLossATR = decimal.MustNew(30, 1) // 3.0 — wide, no SL interference
	s := breakout.New(cfg, nil)

	// 26 ticks alternating 100/110 — violent, fills the long window
	// with |diffs| of 10. 6 ticks alternating 109/110 — quiet, arms
	// compression with a non-zero ShortVol/LongVol ratio (~0.11).
	// 140 then 155 — the trigger is recomputed as prevClose+1.5·ATR
	// each tick, so the second confirmation close must clear the
	// raised trigger; 155 > 140+1.5·ATR fires the BUY. 8 rising ticks
	// — hold, force-close at end of history.
	obs := make([]types.Bar, 0, 26+6+2+8)
	for i := range 26 {
		price := int64(100)
		if i%2 == 1 {
			price = 110
		}
		obs = append(obs, flatBar(i, price))
	}
	for i := range 6 {
		price := int64(110)
		if i%2 == 0 {
			price = 109
		}
		obs = append(obs, flatBar(26+i, price))
	}
	obs = append(obs, flatBar(32, 140), flatBar(33, 155))
	for i := range 8 {
		obs = append(obs, flatBar(34+i, 156+int64(i)))
	}

	bt := breakout.NewBacktester(s, nil)
	res, err := bt.Run(context.Background(), obs)
	require.NoError(t, err)

	require.Len(t, res.ClosedTrades, 1, "expected the breakout long to be force-closed")
	assert.Equal(t, breakout.ExitReasonStrategyExit, res.ClosedTrades[0].ExitReason,
		"rising tail should hold the position open to end of history")
	require.Len(t, res.TradeRecords, 2, "entry + force-close exit rows")
	entry, exit := res.TradeRecords[0], res.TradeRecords[1]
	assert.True(t, entry.CompressionRatio.IsPos(),
		"fixture must arm with a non-zero ratio for this test to discriminate; got %s",
		entry.CompressionRatio)
	assert.True(t, exit.CompressionRatio.Equal(entry.CompressionRatio),
		"exit row must carry the entry's armed ratio (last decision is HOLD → zero); got entry=%s exit=%s",
		entry.CompressionRatio, exit.CompressionRatio)
}

// TestBacktest_EmptyObservations verifies Run returns an empty result
// (no panic) when the historical store yields no observations.
func TestBacktest_EmptyObservations(t *testing.T) {
	t.Parallel()
	bt := breakout.NewBacktester(breakout.New(defaultCfg(), nil), nil)
	res, err := bt.Run(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, res.ClosedTrades)
	assert.Empty(t, res.TradeRecords)
}

// TestBacktest_NoTradesOnFlatSeries asserts that a perfectly flat price
// series produces no closed trades — the strategy arms compression but
// never sees a breakout.
func TestBacktest_NoTradesOnFlatSeries(t *testing.T) {
	t.Parallel()
	cfg := defaultCfg()
	s := breakout.New(cfg, nil)

	obs := make([]types.Bar, cfg.LongVolWindow+5)
	for i := range obs {
		obs[i] = flatBar(i, 100)
	}

	bt := breakout.NewBacktester(s, nil)
	res, err := bt.Run(context.Background(), obs)
	require.NoError(t, err)

	assert.Empty(t, res.ClosedTrades, "perfectly flat series should not produce any trades")
	assert.True(t, res.TotalPnL.IsZero(), "total PnL should be zero on a flat series; got %s", res.TotalPnL.String())
}

// TestBacktest_ReversalClosesOpenPosition constructs a BUY then a SELL
// signal and asserts that the backtest records the second signal as a
// reversal exit.
func TestBacktest_ReversalClosesOpenPosition(t *testing.T) {
	t.Parallel()
	cfg := defaultCfg()
	cfg.ConfirmationBars = 1
	cfg.StopLossATR = decimal.Zero   // disable SL so the test isolates reversal behaviour
	cfg.TakeProfitATR = decimal.Zero // disable TP for the same reason
	s := breakout.New(cfg, nil)

	// 30 flat at 100, 1 jump up at 110 (BUY), 30 flat at 110 (re-arm),
	// 1 jump down at 90 (SELL → reversal close).
	const flatTail = 30
	obs := make([]types.Bar, 0, cfg.LongVolWindow+1+flatTail+1)
	for i := range cfg.LongVolWindow {
		obs = append(obs, flatBar(i, 100))
	}
	obs = append(obs, flatBar(cfg.LongVolWindow, 110)) // BUY
	for i := range flatTail {
		obs = append(obs, flatBar(cfg.LongVolWindow+1+i, 110))
	}
	obs = append(obs, flatBar(cfg.LongVolWindow+1+flatTail, 90)) // SELL

	bt := breakout.NewBacktester(s, nil)
	res, err := bt.Run(context.Background(), obs)
	require.NoError(t, err)

	require.Len(t, res.ClosedTrades, 1, "reversal should close the open position")
	ct := res.ClosedTrades[0]
	assert.Equal(t, types.SignalBuy, ct.Signal, "open signal should be BUY")
	assert.Equal(t, decimal.MustNew(110, 0), ct.EntryPrice,
		"entry price should be the breakout tick (110)")
	assert.Equal(t, decimal.MustNew(90, 0), ct.ExitPrice,
		"exit price should be the SELL signal tick (90)")
	assert.Equal(t, breakout.ExitReasonReversal, ct.ExitReason,
		"SELL signal closes the long before end-of-history; this should be a reversal")
	// The exit TradeRecord must carry the entry's armed compression ratio:
	// ArmedCompressionRatio is only populated on signal ticks, so sourcing
	// it from the exit decision shows 0 on HOLD ticks.
	require.Len(t, res.TradeRecords, 2, "entry + exit trade records")
	assert.True(t, res.TradeRecords[0].CompressionRatio.Equal(res.TradeRecords[1].CompressionRatio),
		"exit row must carry the entry's armed compression ratio, got entry=%s exit=%s",
		res.TradeRecords[0].CompressionRatio, res.TradeRecords[1].CompressionRatio)
}

// TestBacktest_StopLossClosesAgainstMove constructs a BUY followed by a
// large drop. The strategy must close the long with ExitReasonStopLoss
// (priority: SL > reversal > hold), even if no opposite signal fires.
func TestBacktest_StopLossClosesAgainstMove(t *testing.T) {
	t.Parallel()
	cfg := defaultCfg()
	cfg.ConfirmationBars = 1
	cfg.StopLossATR = decimal.MustNew(2, 0) // 2.0; entry ATR ≈ 0.33, SL = 110 - 0.66 = 109.34
	cfg.TakeProfitATR = decimal.Zero        // disabled
	s := breakout.New(cfg, nil)

	// 30 flat at 100, 1 jump to 110 (BUY), 1 drop to 95 (well below SL).
	dropTick := cfg.LongVolWindow + 1
	obs := make([]types.Bar, 0, dropTick+1)
	for i := range cfg.LongVolWindow {
		obs = append(obs, flatBar(i, 100))
	}
	obs = append(obs, flatBar(cfg.LongVolWindow, 110)) // BUY
	obs = append(obs, flatBar(dropTick, 95))           // SL should fire here

	bt := breakout.NewBacktester(s, nil)
	res, err := bt.Run(context.Background(), obs)
	require.NoError(t, err)

	require.Len(t, res.ClosedTrades, 1, "stop-loss should close the open position")
	ct := res.ClosedTrades[0]
	assert.Equal(t, types.SignalBuy, ct.Signal, "open signal should be BUY")
	assert.Equal(t, decimal.MustNew(110, 0), ct.EntryPrice)
	assert.Equal(t, decimal.MustNew(95, 0), ct.ExitPrice,
		"exit price should be the SL trigger tick (95)")
	assert.Equal(t, breakout.ExitReasonStopLoss, ct.ExitReason,
		"95 < 110 - 2*ATR must trigger stop_loss")
	assert.True(t, ct.PnL.IsNeg(), "long 110→95 must be a loss; got %s", ct.PnL.String())
}

// TestBacktest_TakeProfitClosesFavourableMove constructs a BUY followed by
// a large rise. The strategy must close the long with ExitReasonTakeProfit.
func TestBacktest_TakeProfitClosesFavourableMove(t *testing.T) {
	t.Parallel()
	cfg := defaultCfg()
	cfg.ConfirmationBars = 1
	cfg.StopLossATR = decimal.Zero            // disabled
	cfg.TakeProfitATR = decimal.MustNew(2, 0) // 2.0; entry ATR ≈ 0.33, TP = 110 + 0.66 = 110.66
	s := breakout.New(cfg, nil)

	// 30 flat at 100, 1 jump to 110 (BUY), 1 rise to 115 (above TP).
	riseTick := cfg.LongVolWindow + 1
	obs := make([]types.Bar, 0, riseTick+1)
	for i := range cfg.LongVolWindow {
		obs = append(obs, flatBar(i, 100))
	}
	obs = append(obs, flatBar(cfg.LongVolWindow, 110)) // BUY
	obs = append(obs, flatBar(riseTick, 115))          // TP should fire here

	bt := breakout.NewBacktester(s, nil)
	res, err := bt.Run(context.Background(), obs)
	require.NoError(t, err)

	require.Len(t, res.ClosedTrades, 1, "take-profit should close the open position")
	ct := res.ClosedTrades[0]
	assert.Equal(t, types.SignalBuy, ct.Signal, "open signal should be BUY")
	assert.Equal(t, decimal.MustNew(110, 0), ct.EntryPrice)
	assert.Equal(t, decimal.MustNew(115, 0), ct.ExitPrice,
		"exit price should be the TP trigger tick (115)")
	assert.Equal(t, breakout.ExitReasonTakeProfit, ct.ExitReason,
		"115 > 110 + 2*ATR must trigger take_profit")
	assert.True(t, ct.PnL.IsPos(), "long 110→115 must be a profit; got %s", ct.PnL.String())
}

// TestBacktest_SLPriorityOverReversal asserts that on a tick where BOTH
// stop-loss and an opposite-signal reversal would fire, the backtest
// records the stop-loss (priority: SL > TP > reversal > hold).
func TestBacktest_SLPriorityOverReversal(t *testing.T) {
	t.Parallel()
	cfg := defaultCfg()
	cfg.ConfirmationBars = 1
	cfg.StopLossATR = decimal.MustNew(2, 0)
	cfg.TakeProfitATR = decimal.Zero
	s := breakout.New(cfg, nil)

	// 30 flat at 100, 1 jump to 110 (BUY), 1 drop to 90 (below SL AND
	// would also qualify as the start of a SELL reversal since ShortVol
	// is now greater than LongVol from the long drop).
	tick := cfg.LongVolWindow + 1
	obs := make([]types.Bar, 0, tick+1)
	for i := range cfg.LongVolWindow {
		obs = append(obs, flatBar(i, 100))
	}
	obs = append(obs, flatBar(cfg.LongVolWindow, 110)) // BUY
	obs = append(obs, flatBar(tick, 90))               // both SL and reversal would close

	bt := breakout.NewBacktester(s, nil)
	res, err := bt.Run(context.Background(), obs)
	require.NoError(t, err)

	require.Len(t, res.ClosedTrades, 1, "exactly one close should fire")
	ct := res.ClosedTrades[0]
	assert.Equal(t, breakout.ExitReasonStopLoss, ct.ExitReason,
		"SL has priority over reversal when both would fire on the same tick")
}

// wickBar builds a bar whose close stays inside the bands while one
// extreme (low or high) pierces a band, so only the bar-extreme exit
// path can fire.
func wickBar(minute int, open, high, low, close int64) types.Bar {
	return types.Bar{
		Time:  epoch.Add(time.Duration(minute) * time.Minute),
		Open:  decimal.MustNew(open, 0),
		High:  decimal.MustNew(high, 0),
		Low:   decimal.MustNew(low, 0),
		Close: decimal.MustNew(close, 0),
	}
}

func ptrYTM() *decimal.Decimal {
	v := decimal.MustNew(5, 2)
	return &v
}

// TestBacktest_StopLossFiresAtBarLowNotClose: long entered at 110 on a
// flat-then-jump series (entry ATR ≈ 0.71). The next bar closes at 102
// (above the stop band ≈ 99.3) but its Low=95 pierces the band — the
// exit must record stop_loss via the adverse extreme, not a close-based
// outcome, and must fill at the band level (not the bar close).
func TestBacktest_StopLossFiresAtBarLowNotClose(t *testing.T) {
	t.Parallel()
	cfg := defaultCfg()
	cfg.ConfirmationBars = 1
	cfg.StopLossATR = decimal.MustNew(15, 0) // stop = 110 − 15×ATR(≈0.71) ≈ 99.3
	cfg.TakeProfitATR = decimal.Zero
	s := breakout.New(cfg, nil)

	obs := make([]types.Bar, 0, cfg.LongVolWindow+2)
	for i := range cfg.LongVolWindow {
		obs = append(obs, flatBar(i, 100))
	}
	obs = append(obs, flatBar(cfg.LongVolWindow, 110)) // BUY entry
	obs = append(obs, wickBar(cfg.LongVolWindow+1, 103, 103, 95, 102))

	bt := breakout.NewBacktester(s, nil)
	res, err := bt.Run(context.Background(), obs)
	require.NoError(t, err)
	require.Len(t, res.ClosedTrades, 1, "wick bar must close the position")
	ct := res.ClosedTrades[0]
	assert.Equal(t, breakout.ExitReasonStopLoss, ct.ExitReason,
		"Low=95 < stop ≈ 99.3 must fire stop_loss even though Close=102 is inside the band")
	// Band-level fill: entry ATR = (bar-30 TR=10) / 14 ≈ 0.714, so
	// stop level = 110 − 15×0.714 ≈ 99.286. Bar.Open=103 sits above
	// the level so the gap cap is the level itself.
	wantLevel := breakoutStopLevel(110, cfg.StopLossATR)
	assert.True(t, ct.ExitPrice.Equal(wantLevel),
		"stop fill must be the band level, got %s want %s", ct.ExitPrice, wantLevel)
	assert.True(t, ct.ExitPrice.Cmp(decimal.MustNew(110, 0)) < 0,
		"stop fill must sit below entry 110, got %s", ct.ExitPrice)
	assert.True(t, ct.PnL.IsNeg(),
		"stop fill below entry must record a loss, got PnL %s", ct.PnL)
}

// Tick-faithful replay: with ticks covering the bar window, the intrabar
// stop fires on the first crossing TICK's price and timestamp, not via
// the extreme approximation. Mirrors the live run loop, where ticks
// between two bar closes drive liveCheckSLTP.
func TestBacktest_TickReplayExitsAtTickPrice(t *testing.T) {
	t.Parallel()
	cfg := defaultCfg()
	cfg.ConfirmationBars = 1
	cfg.StopLossATR = decimal.MustNew(15, 0) // stop ≈ 110 − 15×ATR(≈0.71) ≈ 99.3
	cfg.TakeProfitATR = decimal.Zero
	s := breakout.New(cfg, nil)

	obs := make([]types.Bar, 0, cfg.LongVolWindow+2)
	for i := range cfg.LongVolWindow {
		obs = append(obs, flatBar(i, 100))
	}
	obs = append(obs, flatBar(cfg.LongVolWindow, 110)) // BUY entry
	// No adverse wick: close 102 stays inside the band; only a tick can fire.
	obs = append(obs, wickBar(cfg.LongVolWindow+1, 103, 103, 102, 102))

	tickTime := epoch.Add(time.Duration(cfg.LongVolWindow+1)*time.Minute + 30*time.Second)
	bt := breakout.NewBacktester(s, nil)
	breakout.SetBacktestTicks(bt, []prices.AssetPrice{
		{AssetID: "asset", Price: decimal.MustNew(95, 0), YTM: ptrYTM(), Time: tickTime},
	})

	res, err := bt.Run(context.Background(), obs)
	require.NoError(t, err)
	require.Len(t, res.ClosedTrades, 1, "crossing tick must close the position")
	ct := res.ClosedTrades[0]
	assert.Equal(t, breakout.ExitReasonStopLoss, ct.ExitReason)
	assert.True(t, ct.ExitPrice.Equal(decimal.MustNew(95, 0)),
		"exit fills at the tick price 95, got %s", ct.ExitPrice)
	assert.True(t, ct.CloseTime.Equal(tickTime), "exit is timestamped at the tick, not the bar close")
}

// TestBacktest_TakeProfitFiresAtBarLowNotClose: short entered at 90,
// take-profit band ≈ 90 − 15×ATR(≈0.71) ≈ 79.3 (a short profits
// downward, so its favorable extreme is the Low). The next bar closes
// at 98 (inside the band) but its Low=75 pierces it — the exit must
// record take_profit via the favorable extreme and must fill at the
// band level (not the bar close).
func TestBacktest_TakeProfitFiresAtBarLowNotClose(t *testing.T) {
	t.Parallel()
	cfg := defaultCfg()
	cfg.ConfirmationBars = 1
	cfg.StopLossATR = decimal.Zero
	cfg.TakeProfitATR = decimal.MustNew(15, 0) // TP = 90 − 15×ATR(≈0.71) ≈ 79.3
	s := breakout.New(cfg, nil)

	obs := make([]types.Bar, 0, cfg.LongVolWindow+2)
	for i := range cfg.LongVolWindow {
		obs = append(obs, flatBar(i, 100))
	}
	obs = append(obs, flatBar(cfg.LongVolWindow, 90)) // SELL entry
	obs = append(obs, wickBar(cfg.LongVolWindow+1, 97, 99, 75, 98))

	bt := breakout.NewBacktester(s, nil)
	res, err := bt.Run(context.Background(), obs)
	require.NoError(t, err)
	require.Len(t, res.ClosedTrades, 1, "wick bar must close the position")
	ct := res.ClosedTrades[0]
	assert.Equal(t, types.SignalSell, ct.Signal, "short entry from the drop bar")
	assert.Equal(t, breakout.ExitReasonTakeProfit, ct.ExitReason,
		"Low=75 < TP ≈ 79.3 must fire take_profit even though Close=98 is inside the band")
	// Band-level fill: short TP is below entry, so level = 90 − 15×ATR
	// ≈ 79.286. Bar.Open=97 sits above the level so the gap cap is
	// the level itself.
	wantLevel := breakoutTpLevel(90, cfg.TakeProfitATR)
	assert.True(t, ct.ExitPrice.Equal(wantLevel),
		"TP fill must be the band level, got %s want %s", ct.ExitPrice, wantLevel)
	assert.True(t, ct.ExitPrice.Cmp(decimal.MustNew(90, 0)) < 0,
		"TP fill for a short must sit below entry 90, got %s", ct.ExitPrice)
	assert.True(t, ct.PnL.IsPos(),
		"short TP fill below entry must record a profit, got PnL %s", ct.PnL)
}

// breakoutStopLevel / breakoutTpLevel replicate the breakout band's
// math for the wick tests. The first ATRWindow bars are flat (TR=0),
// the entry bar produces a TR=10 (110-100 or 100-90), so entryATR =
// 10 / ATRWindow (=10/14).
func breakoutStopLevel(entry int64, slMult decimal.Decimal) decimal.Decimal {
	atr, _ := decimal.MustNew(10, 0).Quo(decimal.MustNew(14, 0))
	dist, _ := slMult.Mul(atr)
	level, _ := decimal.MustNew(entry, 0).Sub(dist)
	return level
}

func breakoutTpLevel(entry int64, tpMult decimal.Decimal) decimal.Decimal {
	atr, _ := decimal.MustNew(10, 0).Quo(decimal.MustNew(14, 0))
	dist, _ := tpMult.Mul(atr)
	level, _ := decimal.MustNew(entry, 0).Sub(dist)
	return level
}

// TestBacktest_StopGappedDownFillsAtOpen pins the gap-capped fill for
// the stop band: when the bar OPENS below the level the fill is the
// open (worse than the level).
func TestBacktest_StopGappedDownFillsAtOpen(t *testing.T) {
	t.Parallel()
	cfg := defaultCfg()
	cfg.ConfirmationBars = 1
	cfg.StopLossATR = decimal.MustNew(15, 0)
	cfg.TakeProfitATR = decimal.Zero
	s := breakout.New(cfg, nil)

	obs := make([]types.Bar, 0, cfg.LongVolWindow+2)
	for i := range cfg.LongVolWindow {
		obs = append(obs, flatBar(i, 100))
	}
	obs = append(obs, flatBar(cfg.LongVolWindow, 110)) // BUY entry
	// Open=99 sits below the stop level (~99.3), so the gap cap
	// kicks in and the fill is the open.
	obs = append(obs, wickBar(cfg.LongVolWindow+1, 99, 99, 98, 99))

	bt := breakout.NewBacktester(s, nil)
	res, err := bt.Run(context.Background(), obs)
	require.NoError(t, err)
	require.Len(t, res.ClosedTrades, 1)
	ct := res.ClosedTrades[0]
	assert.Equal(t, breakout.ExitReasonStopLoss, ct.ExitReason)
	assert.True(t, ct.ExitPrice.Equal(decimal.MustNew(99, 0)),
		"long stop gapped down: fill must be bar.Open (99), got %s", ct.ExitPrice)
}

// TestBacktest_TickExitThenBarEntryTimestampsAreMonotonic pins the
// F11 ordering invariant for breakout: a tick-driven exit (recorded
// at the tick's timestamp) must precede the next bar-driven event
// (entry, bar-driven exit, or force-close), all of which are
// recorded at the bar CLOSE. Pre-fix, the bar-driven events were
// stamped at the bar START, which sits before the tick window opens,
// so a tick that fires after the next bar's START produced a
// non-monotonic trade-records timeline. Bar-driven events must
// carry the CLOSE time so they sort AFTER any tick exit that fired
// inside the preceding bar's window.
//
// Breakout's compression mechanics make the canonical
// "fresh BUY right after a tick exit" scenario hard to construct in
// a single fixture (a fresh signal needs compression to re-arm,
// which takes at least one quiet bar). This test pins the invariant
// directly: the entry TradeRecord is stamped at the bar CLOSE
// (the moment the breakout fires), and the force-close at end of
// history is stamped at the LAST bar's CLOSE — together with
// TestBacktest_TickReplayExitsAtTickPrice (tick-exit pinned at
// tick time) this guarantees a non-monotonic timeline cannot
// appear.
func TestBacktest_TickExitThenBarEntryTimestampsAreMonotonic(t *testing.T) {
	t.Parallel()
	cfg := defaultCfg()
	cfg.ConfirmationBars = 1
	cfg.InitialBalance = decimal.MustNew(10000, 0)
	cfg.Leverage = decimal.One
	s := breakout.New(cfg, nil)

	// 30 flat at 100 fill the long window (and arm compression),
	// 1 jump to 110 fires the BUY. Rising tail keeps the position
	// open until force-close at end of history — same shape as
	// TestBacktest_SingleBreakoutTrade, used to pin the entry and
	// force-close timestamps.
	const flatAt100 = 30
	const risingTail = 5
	obs := make([]types.Bar, 0, flatAt100+1+risingTail)
	for i := range flatAt100 {
		obs = append(obs, flatBar(i, 100))
	}
	obs = append(obs, flatBar(flatAt100, 110))
	for i := range risingTail {
		obs = append(obs, flatBar(flatAt100+1+i, 110+int64(i)+1))
	}

	bt := breakout.NewBacktester(s, nil)
	res, err := bt.Run(context.Background(), obs)
	require.NoError(t, err)
	records := res.TradeRecords
	closedTrades := res.ClosedTrades

	require.Len(t, closedTrades, 1, "single breakout + force-close = 1 trade")
	require.Len(t, records, 2, "entry + force-close trade records")

	entry, forceExit := records[0], records[1]
	wantEntry := obs[flatAt100].Time.Add(time.Minute)
	require.True(t, entry.Time.Equal(wantEntry),
		"bar-driven entry must be timestamped at the bar CLOSE "+
			"(= bar.Time + res), got %s want %s (bar.Time %s)",
		entry.Time, wantEntry, obs[flatAt100].Time)

	wantForceExit := obs[len(obs)-1].Time.Add(time.Minute)
	require.True(t, forceExit.Time.Equal(wantForceExit),
		"force-close must be timestamped at the LAST bar's CLOSE "+
			"(= lastBar.Time + res), got %s want %s (lastBar.Time %s)",
		forceExit.Time, wantForceExit, obs[len(obs)-1].Time)
	require.True(t, forceExit.Time.After(entry.Time),
		"force-close must follow the entry, got entry=%s force-close=%s",
		entry.Time, forceExit.Time)

	require.True(t, closedTrades[0].OpenTime.Equal(entry.Time),
		"ClosedTrade.OpenTime must mirror the entry TradeRecord.Time, "+
			"got %s vs %s", closedTrades[0].OpenTime, entry.Time)
	require.True(t, closedTrades[0].CloseTime.Equal(forceExit.Time),
		"ClosedTrade.CloseTime must mirror the force-close TradeRecord.Time, "+
			"got %s vs %s", closedTrades[0].CloseTime, forceExit.Time)
}

// TestBacktest_TakeProfitGappedUpFillsAtOpen pins the gap-capped fill
// for the TP band on a long: when the bar OPENS above the level the
// fill is the open (better than the level).
func TestBacktest_TakeProfitGappedUpFillsAtOpen(t *testing.T) {
	t.Parallel()
	cfg := defaultCfg()
	cfg.ConfirmationBars = 1
	cfg.StopLossATR = decimal.Zero
	cfg.TakeProfitATR = decimal.MustNew(15, 0) // TP = 110 + 15×0.714 ≈ 120.71
	s := breakout.New(cfg, nil)

	obs := make([]types.Bar, 0, cfg.LongVolWindow+2)
	for i := range cfg.LongVolWindow {
		obs = append(obs, flatBar(i, 100))
	}
	obs = append(obs, flatBar(cfg.LongVolWindow, 110)) // BUY entry
	// Open=125 sits above the TP level (~120.71), so the gap cap
	// kicks in and the fill is the open.
	obs = append(obs, wickBar(cfg.LongVolWindow+1, 125, 126, 125, 125))

	bt := breakout.NewBacktester(s, nil)
	res, err := bt.Run(context.Background(), obs)
	require.NoError(t, err)
	require.Len(t, res.ClosedTrades, 1)
	ct := res.ClosedTrades[0]
	assert.Equal(t, breakout.ExitReasonTakeProfit, ct.ExitReason)
	assert.True(t, ct.ExitPrice.Equal(decimal.MustNew(125, 0)),
		"long TP gapped up: fill must be bar.Open (125), got %s", ct.ExitPrice)
}

// recordingWriter is a minimal in-memory stats.BacktestTradeWriter used to
// verify that Backtester.Run persists trade records and closed trades.

type recordingWriter struct {
	mu     sync.Mutex
	trades []stats.TradeRecordInsert
	closed []stats.ClosedTradeInsert
}

func (w *recordingWriter) WriteTradeRecord(_ context.Context, rec stats.TradeRecordInsert) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.trades = append(w.trades, rec)
	return nil
}

func (w *recordingWriter) WriteClosedTrade(_ context.Context, trade stats.ClosedTradeInsert) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = append(w.closed, trade)
	return nil
}

func (w *recordingWriter) Flush(_ context.Context) error { return nil }

// TestBacktest_PersistsTradesAndClosedTrades verifies that when a writer
// is provided, the backtester calls WriteTradeRecord and WriteClosedTrade
// for each simulated event, populating the breakout-specific signal
// fields (compression_ratio, entry_atr, entry/exit_compression_ratio)
// needed to verify the breakout signal.
func TestBacktest_PersistsTradesAndClosedTrades(t *testing.T) {
	t.Parallel()
	cfg := defaultCfg()
	cfg.ConfirmationBars = 1
	cfg.InitialBalance = decimal.MustNew(10000, 0)
	cfg.Leverage = decimal.One
	s := breakout.New(cfg, nil)

	const flatAt100 = 30
	const risingTail = 10
	obs := make([]types.Bar, 0, flatAt100+risingTail)
	for i := range flatAt100 {
		obs = append(obs, flatBar(i, 100))
	}
	obs = append(obs, flatBar(flatAt100, 110)) // BUY
	for i := range risingTail {
		obs = append(obs, flatBar(flatAt100+1+i, 110+int64(i)))
	}

	w := &recordingWriter{}
	bt := breakout.NewBacktester(s, w)
	_, err := bt.Run(context.Background(), obs)
	require.NoError(t, err)
	require.Len(t, w.trades, 2, "one entry + one exit trade record should be written")
	assert.Equal(t, "BUY", w.trades[0].Signal, "entry trade record signal should be BUY")
	assert.Equal(t, decimal.MustNew(110, 0), w.trades[0].Price,
		"entry trade record price should match the breakout tick (110)")
	assert.Equal(t, "BUY", w.trades[1].Signal,
		"exit trade record reuses the open signal (BUY), recording the close price")
	// For a flat series, ShortVol/LongVol = 0/0 → compressionRatio = 0
	// (maximum compression), which is the correct armed ratio. The
	// important thing is the field is populated (non-nil) and stable
	// between entry and exit.
	assert.True(t, w.trades[0].CompressionRatio.Sign() >= 0,
		"armed compression ratio must be recorded, got %s", w.trades[0].CompressionRatio)
	assert.True(t, w.trades[0].CompressionRatio.Equal(w.trades[1].CompressionRatio),
		"armed ratio must be stable between entry and exit, got entry=%s exit=%s",
		w.trades[0].CompressionRatio, w.trades[1].CompressionRatio)
	assert.True(t, w.trades[0].EntryATR.IsPos(),
		"breakout entry must carry a non-zero ATR for signal verification, got %s",
		w.trades[0].EntryATR)
	// PositionSize should now hold the computed bond quantity, not the fraction.
	expectedQty := w.trades[0].Quantity
	assert.True(t, w.trades[0].PositionSize.Equal(expectedQty),
		"position_size should equal the computed bond quantity (%s), got %s",
		expectedQty, w.trades[0].PositionSize)
}

// Regression (identity bug): Decision.BondID must be the order book's
// BASE ASSET UUID — a different ID from the order book — carried from
// the strategy onto every trade record.
func TestBacktest_BondIDIsBaseAssetNotOrderBook(t *testing.T) {
	t.Parallel()
	const assetID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	cfg := defaultCfg()
	cfg.ConfirmationBars = 1
	cfg.OrderBookID = uuid.Must(uuid.NewV7())
	s := breakout.New(cfg, nil)
	breakout.SetBaseAssetID(s, assetID)

	const flatAt100 = 30
	const risingTail = 10
	obs := make([]types.Bar, 0, flatAt100+1+risingTail)
	for i := range flatAt100 {
		obs = append(obs, flatBar(i, 100))
	}
	obs = append(obs, flatBar(flatAt100, 110))
	for i := range risingTail {
		obs = append(obs, flatBar(flatAt100+1+i, 110+int64(i)+1))
	}

	res, err := breakout.NewBacktester(s, nil).Run(context.Background(), obs)
	require.NoError(t, err)
	require.NotEmpty(t, res.ClosedTrades, "breakout series must close at least one trade")
	for _, ct := range res.ClosedTrades {
		assert.Equal(t, assetID, ct.BondID,
			"BondID must be the resolved base asset, not the order book")
		assert.NotEqual(t, cfg.OrderBookID.String(), ct.BondID)
	}
}

// Backtest fails fast with a wrapped error when the order book's base
// asset cannot be resolved.
func TestBacktest_RequiresBaseAssetLookup(t *testing.T) {
	t.Parallel()
	cfg := defaultCfg()
	cfg.OrderBookID = uuid.Must(uuid.NewV7())
	end := time.Now().UTC().Add(-24 * time.Hour)
	client := &strategyfakes.FakeMarketAPIClient{}
	client.BaseAssetIDReturns("", errors.New("dora down"))
	store := &breakoutfakes.FakeCandleHistoryStore{}
	lo := end.Add(-72 * time.Hour)
	store.CandleRangeReturns(&lo, &end, nil)
	s := breakout.New(cfg, nil, breakout.WithMarketAPIClient(client), breakout.WithCandleHistoryStore(store))

	_, err := s.Backtest(context.Background(), end.Add(-24*time.Hour), end)
	require.ErrorContains(t, err, "backtest requires the order book's base asset")
}

// TestBacktest_TradeBoundsSuppressWarmupEntry pins the F10 fix: when the
// runner supplies TradeFrom/TradeTo bounds, bars before TradeFrom must
// only warm indicators — they MUST NOT open a position. Fixture: 30
// warmup flat at 100 (fills long window, arms compression) → 1 warmup-
// tail breakout at 110 (BUY fires) → 30 in-bounds flat at 110
// (compression re-arms after the suppressed warmup signal) → 1 in-
// bounds breakout at 120 (BUY fires, in-bounds) → 5 in-bounds rising
// (position open, force-close at end). Pre-fix the warmup BUY leaks
// into the result; post-fix the warmup signal is dropped and the in-
// bounds breakout enters. Trade records are stamped at bar CLOSE
// (bar.Time + res, F11), so the in-bounds check uses rec.Time - res.
func TestBacktest_TradeBoundsSuppressWarmupEntry(t *testing.T) {
	t.Parallel()
	cfg := defaultCfg()
	cfg.ConfirmationBars = 1
	cfg.StopLossATR = decimal.Zero
	cfg.TakeProfitATR = decimal.Zero
	s := breakout.New(cfg, nil)

	const (
		warmup         = 30
		postWarmupFlat = 30
		tailLen        = 5
	)
	total := warmup + 1 + postWarmupFlat + 1 + tailLen
	obs := make([]types.Bar, 0, total)
	for i := range warmup {
		obs = append(obs, flatBar(i, 100))
	}
	// Warmup-tail breakout (suppressed post-fix, fires pre-fix).
	obs = append(obs, flatBar(warmup, 110))
	// In-bounds flat phase: lets compression re-arm after the warmup
	// BUY was suppressed (and the strategy's armed flag was reset).
	for i := range postWarmupFlat {
		obs = append(obs, flatBar(warmup+1+i, 110))
	}
	// In-bounds breakout: fires BUY on a flat strategy.
	obs = append(obs, flatBar(warmup+1+postWarmupFlat, 120))
	// In-bounds rising tail: position held, force-closed at end.
	for i := range tailLen {
		obs = append(obs, flatBar(warmup+1+postWarmupFlat+1+i, 121+int64(i)))
	}

	tradeFrom := obs[warmup+1].Time
	tradeTo := obs[len(obs)-1].Time
	bt := breakout.NewBacktester(s, nil)
	bt.TradeFrom = tradeFrom
	bt.TradeTo = tradeTo

	res, err := bt.Run(context.Background(), obs)
	require.NoError(t, err)

	// (b) Exactly one closed trade: the warmup signal was suppressed
	// and the in-bounds breakout entered.
	require.Len(t, res.ClosedTrades, 1,
		"expected one in-bounds closed trade from the breakout series")

	// (a) Every trade record must come from a bar in [TradeFrom, TradeTo].
	// rec.Time = bar.Time + res (F11), so rec.Time - res is the bar START.
	for _, rec := range res.TradeRecords {
		barStart := rec.Time.Add(-time.Minute)
		require.False(t, barStart.Before(tradeFrom),
			"entry bar %s must be >= TradeFrom %s (rec.Time=%s)",
			barStart, tradeFrom, rec.Time)
	}

	// (c) The closed trade's OpenTime must be at the in-bounds bar CLOSE
	// (or later), and the CloseTime must sit inside the requested period.
	ct := res.ClosedTrades[0]
	openBarStart := ct.OpenTime.Add(-time.Minute)
	require.False(t, openBarStart.Before(tradeFrom),
		"OpenTime bar %s must be >= TradeFrom %s", openBarStart, tradeFrom)
	require.False(t, ct.CloseTime.After(tradeTo.Add(time.Minute)),
		"CloseTime %s must be <= TradeTo+res %s", ct.CloseTime, tradeTo.Add(time.Minute))
}
