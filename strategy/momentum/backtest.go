package momentum

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy/stats"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
)

// Backtester replays observations through a Strategy and records trades.
// One open position at a time; no transaction costs / spread / financing
// (matches the existing backtesters' deliberate simplicity).
type Backtester struct {
	strategy *Strategy
	writer   stats.BacktestTradeWriter
}

// NewBacktester wraps a Strategy for backtesting. The writer receives one
// row per simulated trade event; pass nil to skip persistence.
func NewBacktester(s *Strategy, writer stats.BacktestTradeWriter) *Backtester {
	return &Backtester{strategy: s, writer: writer}
}

// Run replays obs chronologically and returns a BacktestResult. Exits
// follow the priority stop_loss > take_profit > reversal (delegated to
// Strategy.ShouldExit).
func (b *Backtester) Run(ctx context.Context, obs []types.YieldObservation) (BacktestResult, error) {
	var (
		closedTrades []ClosedTrade
		tradeRecords []TradeRecord
		openTrade    *TradeRecord
		// lastDecision is captured for the force-close path so the
		// ClosedTrade.ExitSignal reflects the strategy's signal at the
		// final observation, not the open direction. FastMA/SlowMA/ATR
		// are also inherited so the persisted force-close TradeRecord
		// matches the in-loop exit shape.
		lastDecision Decision
	)

	for _, o := range obs {
		select {
		case <-ctx.Done():
			return BacktestResult{}, errors.New("backtest cancelled by user")
		default:
		}
		decision, err := b.strategy.Update(o)
		if err != nil {
			return BacktestResult{}, err
		}
		lastDecision = decision

		if openTrade != nil {
			exit, reason := b.strategy.ShouldExit(openTrade.Signal, decision, openTrade.Price, openTrade.EntryATR)
			if exit {
				ct, err := closeAtPrice(openTrade, decision, reason)
				if err != nil {
					return BacktestResult{}, err
				}
				closedTrades = append(closedTrades, ct)
				tradeRecords = append(tradeRecords, exitRecord(openTrade, decision))
				openTrade = nil
			}
			continue
		}

		// Flat: open on a fresh signal.
		if decision.Signal() == types.SignalHold {
			continue
		}
		price := decision.Price()
		if price.IsZero() {
			continue
		}
		quantity, ok, err := b.strategy.cappedOrderQuantity(decision.PositionSize(), decimal.Zero, price)
		if err != nil {
			return BacktestResult{}, err
		}
		if !ok || quantity.IsZero() {
			continue
		}
		rec := TradeRecord{
			Time: decision.Time(), BondID: decision.BondID(), Signal: decision.Signal(),
			Price: price, Quantity: quantity, PositionSize: decision.PositionSize(),
			FastMA: decision.FastMA, SlowMA: decision.SlowMA, EntryATR: decision.ATR,
		}
		tradeRecords = append(tradeRecords, rec)
		openTrade = &tradeRecords[len(tradeRecords)-1]
	}

	// Force-close any position still open at end of history.
	if openTrade != nil {
		last := obs[len(obs)-1]
		// Use lastDecision.Signal() for the close, not openTrade.Signal:
		// the ClosedTrade.ExitSignal field should record the strategy's
		// signal at the moment of close, which is whatever the last
		// observation produced. meanreversion does the same. Inherit
		// FastMA/SlowMA/ATR from lastDecision so the persisted force-
		// close TradeRecord has the same MA state as in-loop exits.
		d := Decision{
			time:   last.Time,
			bondID: openTrade.BondID,
			price:  last.Price,
			signal: lastDecision.Signal(),
			FastMA: lastDecision.FastMA,
			SlowMA: lastDecision.SlowMA,
			ATR:    lastDecision.ATR,
		}
		ct, err := closeAtPrice(openTrade, d, ExitReasonStrategyExit)
		if err != nil {
			return BacktestResult{}, err
		}
		closedTrades = append(closedTrades, ct)
		// Mirror the in-loop exit path: persist a trade_records entry
		// for the close so /trades shows a matched pair instead of a
		// dangling open entry with no exit.
		tradeRecords = append(tradeRecords, exitRecord(openTrade, d))
	}

	if b.writer != nil {
		streamTrades(ctx, b.writer, tradeRecords, closedTrades)
		if err := b.writer.Flush(ctx); err != nil {
			slog.Error("flush backtest writer", "err", err)
		}
	}

	if len(obs) == 0 {
		return BacktestResult{}, nil
	}
	start, end := obs[0].Time, obs[len(obs)-1].Time
	return summarise(closedTrades, tradeRecords, start, end)
}

// closeAtPrice converts an open TradeRecord + a closing Decision into a
// ClosedTrade. Cash flow is intentionally not tracked — momentum's
// sizing uses cfg.InitialBalance × collateralWeight × Leverage and
// holds one position at a time, so per-trade balance evolution is
// not a meaningful signal.
func closeAtPrice(
	open *TradeRecord,
	d Decision,
	reason string,
) (ClosedTrade, error) {
	exitPrice := d.Price()
	ct := ClosedTrade{
		BondID: open.BondID, OpenTime: open.Time, CloseTime: d.Time(),
		Signal: open.Signal, ExitSignal: d.Signal(),
		EntryPrice: open.Price, ExitPrice: exitPrice, EntryATR: open.EntryATR,
		Quantity: open.Quantity, PositionSize: open.PositionSize, ExitReason: reason,
	}
	pnl, err := computePnL(ct)
	if err != nil {
		return ClosedTrade{}, err
	}
	ct.PnL = pnl
	return ct, nil
}

func exitRecord(open *TradeRecord, d Decision) TradeRecord {
	return TradeRecord{
		Time: d.Time(), BondID: open.BondID, Signal: open.Signal,
		Price: d.Price(), Quantity: open.Quantity, PositionSize: open.PositionSize,
		FastMA: d.FastMA, SlowMA: d.SlowMA, EntryATR: open.EntryATR,
	}
}

// computePnL returns the per-trade profit/loss in price terms (Quantity
// × price-difference). For BUY (long) profit = exit − entry; for SELL
// (short) profit = entry − exit.
func computePnL(ct ClosedTrade) (decimal.Decimal, error) {
	cost, err := ct.EntryPrice.Mul(ct.Quantity)
	if err != nil {
		return decimal.Zero, err
	}
	proceeds, err := ct.ExitPrice.Mul(ct.Quantity)
	if err != nil {
		return decimal.Zero, err
	}
	switch ct.Signal {
	case types.SignalBuy:
		return proceeds.Sub(cost)
	case types.SignalSell:
		return cost.Sub(proceeds)
	default:
		return decimal.Zero, nil
	}
}

// summarise aggregates closed trades into a BacktestResult.
func summarise(trades []ClosedTrade, records []TradeRecord, start, end time.Time) (BacktestResult, error) {
	points := make([]stats.PnLPoint, len(trades))
	for i, t := range trades {
		points[i] = stats.PnLPoint{PnL: t.PnL, CloseTime: t.CloseTime}
	}
	summary, err := stats.Summarise(points, start, end)
	if err != nil {
		return BacktestResult{}, err
	}
	return BacktestResult{
		ClosedTrades: trades, TradeRecords: records,
		TotalPnL: summary.TotalPnL, WinCount: summary.WinCount, LossCount: summary.LossCount,
		MaxDrawdown: summary.MaxDrawdown, SharpeRatio: summary.SharpeRatio,
	}, nil
}

// streamTrades mirrors meanreversion/backtest.go's writer loop, adapted
// to momentum's TradeRecord / ClosedTrade fields. EntryATR on the trade
// record reuses the breakout-only slot in the shared insert struct
// (semantically the same: ATR captured at open).
func streamTrades(ctx context.Context, w stats.BacktestTradeWriter, records []TradeRecord, closed []ClosedTrade) {
	for _, r := range records {
		rec := stats.TradeRecordInsert{
			BacktestID:   uuid.Nil,
			Time:         r.Time,
			BondID:       r.BondID,
			Signal:       r.Signal.String(),
			Price:        r.Price,
			Quantity:     r.Quantity,
			PositionSize: r.PositionSize,
			EntryATR:     r.EntryATR,
		}
		if err := w.WriteTradeRecord(ctx, rec); err != nil {
			slog.Error("write trade record", "err", err)
		}
	}
	for _, c := range closed {
		rec := stats.ClosedTradeInsert{
			BacktestID:   uuid.Nil,
			OpenTime:     c.OpenTime,
			CloseTime:    c.CloseTime,
			BondID:       c.BondID,
			OpenSignal:   c.Signal.String(),
			CloseSignal:  c.ExitSignal.String(),
			Quantity:     c.Quantity,
			EntryPrice:   c.EntryPrice,
			ExitPrice:    c.ExitPrice,
			PnL:          c.PnL,
			PositionSize: c.PositionSize,
			ExitReason:   c.ExitReason,
		}
		if err := w.WriteClosedTrade(ctx, rec); err != nil {
			slog.Error("write closed trade", "err", err)
		}
	}
}
