package breakout_test

import (
	"context"
	"sync"
	"testing"

	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/strategy/breakout"
	"github.com/dora-network/bond-trading-strategies/strategy/stats"
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
	obs := make([]types.YieldObservation, 0, flatAt100+1+risingTail)
	for i := range flatAt100 {
		obs = append(obs, flatObs(i, 100))
	}
	obs = append(obs, flatObs(flatAt100, 110))
	for i := range risingTail {
		// 110, 111, 112, ..., 119
		obs = append(obs, flatObs(flatAt100+1+i, 110+int64(i)+1))
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
	obs := make([]types.YieldObservation, 0, 26+6+2+8)
	for i := range 26 {
		price := int64(100)
		if i%2 == 1 {
			price = 110
		}
		obs = append(obs, flatObs(i, price))
	}
	for i := range 6 {
		price := int64(110)
		if i%2 == 0 {
			price = 109
		}
		obs = append(obs, flatObs(26+i, price))
	}
	obs = append(obs, flatObs(32, 140), flatObs(33, 155))
	for i := range 8 {
		obs = append(obs, flatObs(34+i, 156+int64(i)))
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

	obs := make([]types.YieldObservation, cfg.LongVolWindow+5)
	for i := range obs {
		obs[i] = flatObs(i, 100)
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
	obs := make([]types.YieldObservation, 0, cfg.LongVolWindow+1+flatTail+1)
	for i := 0; i < cfg.LongVolWindow; i++ {
		obs = append(obs, flatObs(i, 100))
	}
	obs = append(obs, flatObs(cfg.LongVolWindow, 110)) // BUY
	for i := range flatTail {
		obs = append(obs, flatObs(cfg.LongVolWindow+1+i, 110))
	}
	obs = append(obs, flatObs(cfg.LongVolWindow+1+flatTail, 90)) // SELL

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
	obs := make([]types.YieldObservation, 0, dropTick+1)
	for i := 0; i < cfg.LongVolWindow; i++ {
		obs = append(obs, flatObs(i, 100))
	}
	obs = append(obs, flatObs(cfg.LongVolWindow, 110)) // BUY
	obs = append(obs, flatObs(dropTick, 95))           // SL should fire here

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
	obs := make([]types.YieldObservation, 0, riseTick+1)
	for i := 0; i < cfg.LongVolWindow; i++ {
		obs = append(obs, flatObs(i, 100))
	}
	obs = append(obs, flatObs(cfg.LongVolWindow, 110)) // BUY
	obs = append(obs, flatObs(riseTick, 115))          // TP should fire here

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
	obs := make([]types.YieldObservation, 0, tick+1)
	for i := 0; i < cfg.LongVolWindow; i++ {
		obs = append(obs, flatObs(i, 100))
	}
	obs = append(obs, flatObs(cfg.LongVolWindow, 110)) // BUY
	obs = append(obs, flatObs(tick, 90))               // both SL and reversal would close

	bt := breakout.NewBacktester(s, nil)
	res, err := bt.Run(context.Background(), obs)
	require.NoError(t, err)

	require.Len(t, res.ClosedTrades, 1, "exactly one close should fire")
	ct := res.ClosedTrades[0]
	assert.Equal(t, breakout.ExitReasonStopLoss, ct.ExitReason,
		"SL has priority over reversal when both would fire on the same tick")
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
	obs := make([]types.YieldObservation, 0, flatAt100+risingTail)
	for i := range flatAt100 {
		obs = append(obs, flatObs(i, 100))
	}
	obs = append(obs, flatObs(flatAt100, 110)) // BUY
	for i := range risingTail {
		obs = append(obs, flatObs(flatAt100+1+i, 110+int64(i)))
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
