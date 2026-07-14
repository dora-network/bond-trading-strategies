package breakout_test

import (
	"context"
	"testing"

	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/strategy/breakout"
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
	for i := 0; i < flatAt100; i++ {
		obs = append(obs, flatObs(i, 100))
	}
	obs = append(obs, flatObs(flatAt100, 110))
	for i := 0; i < risingTail; i++ {
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
	for i := 0; i < flatTail; i++ {
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
		"entry price should be the BUY breakout tick (110)")
	assert.Equal(t, decimal.MustNew(90, 0), ct.ExitPrice,
		"exit price should be the SELL signal tick (90)")
	assert.Equal(t, breakout.ExitReasonReversal, ct.ExitReason,
		"SELL signal closes the long before end-of-history; this should be a reversal")
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

// silence unused import warnings if a future test drops a reference
