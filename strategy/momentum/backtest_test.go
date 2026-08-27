package momentum_test

import (
	"context"
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy/momentum"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/require"
)

// uptrendThenReversal: prices rise (fast>slow -> Buy), then fall so the
// MAs cross back (-> reversal exit).
func uptrendThenReversal() []types.YieldObservation {
	o := make([]types.YieldObservation, 0, 14)
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range 7 {
		o = append(o, types.YieldObservation{
			Time:   base.Add(time.Duration(i) * time.Minute),
			BondID: "b", Price: decimal.MustNew(int64(100+i), 0),
		})
	}
	for i := range 7 {
		o = append(o, types.YieldObservation{
			Time:   base.Add(time.Duration(7+i) * time.Minute),
			BondID: "b", Price: decimal.MustNew(int64(106-i), 0),
		})
	}
	return o
}

// defaults (price source, ATR window, $1000 seed capital, no stops)
// and applies the per-test overrides the caller passes. Centralises
// the 7-line boilerplate that every backtest test repeated.
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

func TestBacktest_OpensAndExits(t *testing.T) {
	cfg := testCfg(3, 5, decimal.Zero, decimal.Zero)
	s := momentum.New(cfg, nil)
	bt := momentum.NewBacktester(s, nil)

	res, err := bt.Run(context.Background(), uptrendThenReversal())
	require.NoError(t, err)
	closedTradesAny := res.GetClosedTrades()
	closedTrades, ok := closedTradesAny.([]momentum.ClosedTrade)
	require.True(t, ok, "closedTrades type mismatch: %T", closedTradesAny)
	tradesAny := res.GetTradeRecords()
	tradeRecords, ok := tradesAny.([]momentum.TradeRecord)
	require.True(t, ok, "tradeRecords type mismatch: %T", tradesAny)

	// Pinning assertions. The fixture (rising prices then falling prices)
	// produces multiple round-trips as the strategy flips direction with
	// each MA crossover. Pre-fix this test only asserted NotEmpty; the
	// new assertions catch silent mutations in:
	//   - exit reason (reversal must fire when fast/slow MA cross)
	//   - matched trade-record pairs (no dangling open entry)
	//   - PnL sign (sign-flip in computePnL is silent mutation)
	require.NotEmpty(t, closedTrades)
	require.NotEmpty(t, tradeRecords)
	// At least one trade must be a reversal close (the falling-leg
	// half of the fixture flips the signal against an open position).
	sawReversal := false
	for _, ct := range closedTrades {
		if ct.ExitReason == momentum.ExitReasonReversal {
			sawReversal = true
			break
		}
	}
	require.True(t, sawReversal,
		"reversal exit must fire when MA crossover flips signal against open position")
	// Trade records must come in matched entry/exit pairs.
	require.Equal(t, 0, len(tradeRecords)%2,
		"trade records must come in matched entry/exit pairs, got %d", len(tradeRecords))
	// Aggregate PnL consistency across multiple round-trips (round-4
	// review): TotalPnL must equal Σ closed-trade PnL — catches the
	// summariser dropping or double-counting a trade. (Win/Loss counts
	// are not asserted here: this fixture legitimately produces a
	// zero-PnL trade, which the classifier counts as neither.)
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
	obs := make([]types.YieldObservation, 0, 9)
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range 5 {
		obs = append(obs, types.YieldObservation{
			Time:   base.Add(time.Duration(i) * time.Minute),
			BondID: "b", Price: decimal.MustNew(int64(100+i), 0),
		})
	}
	obs = append(obs, types.YieldObservation{Time: base.Add(5 * time.Minute), BondID: "b", Price: decimal.MustNew(120, 0)})
	obs = append(obs, types.YieldObservation{Time: base.Add(6 * time.Minute), BondID: "b", Price: decimal.MustNew(80, 0)})
	obs = append(obs, types.YieldObservation{Time: base.Add(7 * time.Minute), BondID: "b", Price: decimal.MustNew(75, 0)})
	obs = append(obs, types.YieldObservation{Time: base.Add(8 * time.Minute), BondID: "b", Price: decimal.MustNew(70, 0)})

	res, err := bt.Run(context.Background(), obs)
	require.NoError(t, err)
	closedTradesAny := res.GetClosedTrades()
	closedTrades, ok := closedTradesAny.([]momentum.ClosedTrade)
	require.True(t, ok, "closedTrades type mismatch: %T", closedTradesAny)

	// Pinning: the sharp drop (120 -> 80 -> 70) must trigger a
	// close. Priority stop > reversal: if both branches fire on the
	// same tick, the exit reason must be stop_loss, not reversal.
	require.NotEmpty(t, closedTrades)
	for _, ct := range closedTrades {
		require.NotEqual(t, momentum.ExitReasonReversal, ct.ExitReason,
			"priority stop > reversal must not tag a stop-loss exit as reversal")
	}
	// The fixture is deterministic (entry ~102, EntryATR 2/3 → stop
	// ≈ 100.67, crash tick 80), so the first close must actually be the
	// stop, not merely "not a reversal" (round-4 review).
	require.Equal(t, momentum.ExitReasonStopLoss, closedTrades[0].ExitReason,
		"crash from 120 to 80 must fire the stop-loss exit")
}

func TestBacktest_TakeProfitExits(t *testing.T) {
	cfg := testCfg(2, 3, decimal.Zero, decimal.MustNew(2, 0))
	s := momentum.New(cfg, nil)
	bt := momentum.NewBacktester(s, nil)
	// Rising prices into a long entry (price 102 at tick 2 — fastMA
	// 101.5 > slowMA 101), entryATR = mean of |diff| across the
	// first three ticks (0, 1, 1) = 0.667. Take-profit threshold =
	// 102 + 2×0.667 = 103.33, so the very next tick (price 104)
	// fires take_profit — well before the 110 spike. StopLossATR=0
	// disables the stop; the series never falls, so no reversal;
	// only take_profit or strategy_exit can be the first close.
	// A broken take-profit branch therefore surfaces as
	// strategy_exit (force-close at end), not reversal — making
	// the first-close reason assertion mutation-sensitive.
	obs := make([]types.YieldObservation, 0, 7)
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range 5 {
		obs = append(obs, types.YieldObservation{
			Time:   base.Add(time.Duration(i) * time.Minute),
			BondID: "b", Price: decimal.MustNew(int64(100+i), 0),
		})
	}
	obs = append(obs, types.YieldObservation{Time: base.Add(5 * time.Minute), BondID: "b", Price: decimal.MustNew(110, 0)})
	obs = append(obs, types.YieldObservation{Time: base.Add(6 * time.Minute), BondID: "b", Price: decimal.MustNew(111, 0)})

	res, err := bt.Run(context.Background(), obs)
	require.NoError(t, err)
	closedTradesAny := res.GetClosedTrades()
	closedTrades, ok := closedTradesAny.([]momentum.ClosedTrade)
	require.True(t, ok, "closedTrades type mismatch: %T", closedTradesAny)

	require.NotEmpty(t, closedTrades)
	require.Equal(t, momentum.ExitReasonTakeProfit, closedTrades[0].ExitReason,
		"spike from ~102 to 110 must fire the take-profit exit (threshold ≈ 104)")
	require.True(t, closedTrades[0].PnL.IsPos(),
		"take-profit close at the spike price must be profitable for the long, got PnL %s", closedTrades[0].PnL)
}

// TestBacktest_ForceClosePersistsExitAndRecordsExitSignal exercises
// appended only to closedTrades, leaving tradeRecords with a dangling
// hits Buy on tick 3 and never reverses. No stop-loss / take-profit. End
// of history triggers the force-close.
func TestBacktest_ForceClosePersistsExitAndRecordsExitSignal(t *testing.T) {
	cfg := testCfg(2, 3, decimal.Zero, decimal.Zero)
	s := momentum.New(cfg, nil)
	bt := momentum.NewBacktester(s, nil)

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	obs := make([]types.YieldObservation, 0, 8)
	for i := range 8 {
		obs = append(obs, types.YieldObservation{
			Time:   base.Add(time.Duration(i) * time.Minute),
			BondID: "b",
			Price:  decimal.MustNew(int64(100+i), 0),
		})
	}

	res, err := bt.Run(context.Background(), obs)
	require.NoError(t, err)

	closedTradesAny := res.GetClosedTrades()
	recordsAny := res.GetTradeRecords()
	closedTrades, ok := closedTradesAny.([]momentum.ClosedTrade)
	require.True(t, ok, "closedTrades type mismatch: %T", closedTradesAny)
	tradeRecords, ok := recordsAny.([]momentum.TradeRecord)
	require.True(t, ok, "tradeRecords type mismatch: %T", recordsAny)

	// Force-close produces exactly 1 closed trade (round-trip).
	require.Len(t, closedTrades, 1, "force-close must close exactly 1 position")

	// And exactly 2 trade_records: entry + exit. Pre-fix this would
	// be 1 (entry only), causing /trades to show a dangling open row.
	require.Len(t, tradeRecords, 2,
		"force-close must append an exit TradeRecord so /trades shows "+
			"a matched pair, not a dangling open entry")

	ct := closedTrades[0]
	require.Equal(t, types.SignalBuy, ct.Signal, "open direction is Buy")
	require.Equal(t, momentum.ExitReasonStrategyExit, ct.ExitReason,
		"force-close must tag the close with ExitReasonStrategyExit")

	// PnL integrity (round-4 review: no test in the package asserted a
	// single PnL value, so a sign flip in computePnL was a silent
	// mutation). For a Buy, PnL = Exit×Qty − Entry×Qty, recomputed from
	// the trade's own recorded fields in the same op order as
	// computePnL (decimal arithmetic is not distributive: computing
	// (Exit−Entry)×Qty rounds differently).
	cost, err := ct.EntryPrice.Mul(ct.Quantity)
	require.NoError(t, err)
	proceeds, err := ct.ExitPrice.Mul(ct.Quantity)
	require.NoError(t, err)
	wantPnL, err := proceeds.Sub(cost)
	require.NoError(t, err)
	require.True(t, ct.PnL.Cmp(wantPnL) == 0,
		"PnL must be Exit×Qty−Entry×Qty for a long: want %s, got %s", wantPnL, ct.PnL)
	require.True(t, ct.PnL.IsPos(), "rising fixture must close a long at a profit, got %s", ct.PnL)

	// Aggregate consistency: TotalPnL is Σ closed-trade PnL and the
	// win/loss classifier counts this single profitable trade as a win.
	sum := decimal.Zero
	for _, c := range closedTrades {
		sum, err = sum.Add(c.PnL)
		require.NoError(t, err)
	}
	require.True(t, res.GetTotalPnL().Cmp(sum) == 0,
		"TotalPnL must equal Σ closed-trade PnL: want %s, got %s", sum, res.GetTotalPnL())
	require.Equal(t, 1, res.GetWinCount(), "single profitable trade must be one win")
	require.Equal(t, 0, res.GetLossCount())

	// Sanity: the trade_records pair brackets the closed trade.
	require.Equal(t, tradeRecords[0].Time, ct.OpenTime)
	require.Equal(t, tradeRecords[1].Time, ct.CloseTime)

	// The force-close exit TradeRecord must carry the same MA / ATR
	// state as the in-loop exit rows; pre-fix this was zero because
	// the force-close Decision was built without inheriting from
	// lastDecision. Required so persisted rows are uniform across
	// exit paths.
	exitRec := tradeRecords[1]
	require.True(t, exitRec.FastMA.IsPos(),
		"force-close exit TradeRecord FastMA must match lastDecision, got %s", exitRec.FastMA)
	require.True(t, exitRec.SlowMA.IsPos(),
		"force-close exit TradeRecord SlowMA must match lastDecision, got %s", exitRec.SlowMA)
}
