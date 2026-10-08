package momentum

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/dora-network/bond-trading-strategies/prices"
	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/stats"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
)

// Backtester replays historical bars through a Strategy and records
// trades. One open position at a time; no transaction costs / spread /
// financing (matches the existing backtesters' deliberate simplicity).
type Backtester struct {
	strategy *Strategy
	writer   stats.BacktestTradeWriter
	// ticks are the price_history ticks covering the bar window, in
	// chronological order. Empty disables tick replay: intrabar exits
	// fall back to the bar-extreme approximation.
	ticks []prices.AssetPrice
	// TradeFrom / TradeTo define the trading window. Bars before
	// TradeFrom only warm indicators and trade filters; entries
	// execute only for bars with Time >= TradeFrom (and, when set,
	// Time < TradeTo). Summary bounds come from these fields when
	// non-zero, else the bar slice. Zero values = unrestricted.
	TradeFrom time.Time
	TradeTo   time.Time
}

// NewBacktester wraps a Strategy for backtesting. The writer receives one
// row per simulated trade event; pass nil to skip persistence.
func NewBacktester(s *Strategy, writer stats.BacktestTradeWriter) *Backtester {
	return &Backtester{strategy: s, writer: writer}
}

// Run replays bars oldest-first and returns a BacktestResult. Exits
// follow the priority stop_loss > take_profit > close-decision (band
// exit at close or reversal). While a position is open, the stop band
// is evaluated against the bar's ADVERSE extreme (Low for a long,
// High for a short) and the take-profit band against the FAVORABLE
// extreme, so an intra-bar spike triggers the exit even when the close
// is back inside the bands.
//
//nolint:funlen // backtest simulation with multiple phases
func (b *Backtester) Run(ctx context.Context, bars []types.Bar) (BacktestResult, error) {
	var (
		closedTrades []ClosedTrade
		tradeRecords []TradeRecord
		openTrade    *TradeRecord
		// lastDecision is captured for the force-close path so the
		// ClosedTrade.ExitSignal reflects the strategy's signal at the
		// final bar, not the open direction. FastMA/SlowMA/ATR are
		// also inherited so the persisted force-close TradeRecord
		// matches the in-loop exit shape.
		lastDecision Decision
	)

	res := strategy.ResolutionDuration(b.strategy.cfg.Resolution)
	tickIdx := 0
	// prevWindowHadTicks records whether the window (close_{i-1}, close_i]
	// contained ticks; when it did, those ticks already drove the intrabar
	// band checks, so bar i's extreme check is skipped (no double-fire).
	prevWindowHadTicks := false

	for i, bar := range bars {
		select {
		case <-ctx.Done():
			return BacktestResult{}, errors.New("backtest cancelled by user")
		default:
		}
		decision, err := b.strategy.Update(bar)
		if err != nil {
			return BacktestResult{}, err
		}
		lastDecision = decision

		if openTrade != nil {
			var (
				exit      bool
				fillPrice decimal.Decimal
				reason    string
			)
			if prevWindowHadTicks {
				// Ticks already drove the intrabar band checks for this
				// bar's formation window; only the close-decision remains.
				if e, r := b.strategy.ShouldExit(openTrade.Signal, decision, openTrade.Price, openTrade.EntryATR); e {
					exit, fillPrice, reason = true, decision.Price(), r
				}
			} else {
				exit, fillPrice, reason = b.exitForBar(openTrade, bar, decision)
			}
			if exit {
				d := decision
				d.price = fillPrice
				// Bar-driven exit: the close-decision fires at the
				// bar's CLOSE, so the persisted close must carry
				// bar.Time + res. The tick-exit path further down
				// overrides d.time with the tick timestamp and is
				// unaffected. Without this shift, a re-entry on the
				// next bar could be stamped at the bar START
				// (earlier than this exit's CLOSE) and break timeline
				// monotonicity.
				d.time = bar.Time.Add(res)
				ct, err := closeAtPrice(openTrade, d, reason)
				if err != nil {
					return BacktestResult{}, err
				}
				closedTrades = append(closedTrades, ct)
				tradeRecords = append(tradeRecords, exitRecord(openTrade, d))
				openTrade = nil
			}
		} else if decision.Signal() != types.SignalHold &&
			(b.TradeFrom.IsZero() || !bar.Time.Before(b.TradeFrom)) &&
			(b.TradeTo.IsZero() || !bar.Time.Add(res).After(b.TradeTo)) {
			// Flat: open on a fresh signal, but only when the bar
			// sits inside the trading window. Entries execute at
			// bar completion (bar.Time + res), so every entry
			// timestamp falls in (TradeFrom, TradeTo]; the
			// TradeFrom side stays `Before`-strict (bars starting
			// before TradeFrom never trade even if they complete
			// exactly at TradeFrom — warmup contract). Bars
			// outside the window still update indicators, trade
			// filters, and run exits — only the entry branch is
			// gated, so a position opened in-bounds can still run
			// to end of data.
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
			// Bar-driven entry: the decision fires at the bar's CLOSE
			// (decision.Time = bar.Time = bar START), so the persisted
			// trade record must carry the CLOSE time (bar.Time + res).
			// Without this shift, a re-entry after a tick-driven exit
			// could be stamped at the bar START — earlier than the
			// preceding trade's tick exit time, breaking timeline
			// monotonicity. Tick-driven exits are unaffected.
			rec := TradeRecord{
				Time: decision.Time().Add(res), BondID: decision.bondID, Signal: decision.Signal(),
				Price: price, Quantity: quantity, PositionSize: decision.PositionSize(),
				FastMA: decision.FastMA, SlowMA: decision.SlowMA, EntryATR: decision.ATR,
			}
			tradeRecords = append(tradeRecords, rec)
			openTrade = &tradeRecords[len(tradeRecords)-1]
		}

		// Tick replay: any position open at bar i's close (held or freshly
		// entered) sees the price_history ticks in (close_i, close_{i+1}]
		// through the SAME bandExit math the live run loop uses, exiting
		// at the first crossing tick's price. Live consumes exactly these
		// ticks between Update(bar i) and Update(bar i+1).
		if openTrade != nil {
			nextClose := bar.Time.Add(res * barsAhead)
			if i+1 < len(bars) {
				nextClose = bars[i+1].Time.Add(res)
			}
			window := b.tickWindow(&tickIdx, bar.Time.Add(res), nextClose)
			prevWindowHadTicks = len(window) > 0
			for _, tick := range window {
				if reason, exit := b.strategy.bandExit(openTrade.Signal, openTrade.Price, openTrade.EntryATR, tick.Price); exit {
					d := decision
					d.price, d.time = tick.Price, tick.Time
					ct, err := closeAtPrice(openTrade, d, reason)
					if err != nil {
						return BacktestResult{}, err
					}
					closedTrades = append(closedTrades, ct)
					tradeRecords = append(tradeRecords, exitRecord(openTrade, d))
					openTrade = nil
					break
				}
			}
		}
	}

	// Force-close any position still open at end of history.
	if openTrade != nil {
		last := bars[len(bars)-1]
		// Use lastDecision.Signal() for the close, not openTrade.Signal:
		// the ClosedTrade.ExitSignal field should record the strategy's
		// signal at the moment of close, which is whatever the last
		// bar produced. meanreversion does the same. Inherit
		// FastMA/SlowMA/ATR from lastDecision so the persisted force-
		// close TradeRecord has the same MA state as in-loop exits.
		// Force-close is a bar-driven event: stamp the close at the
		// last bar's CLOSE (last.Time + res) so it sorts AFTER any
		// tick exit that fired in the last bar's formation window.
		d := Decision{
			time:   last.Time.Add(res),
			bondID: openTrade.BondID,
			price:  last.Close,
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

	if len(bars) == 0 {
		return BacktestResult{}, nil
	}
	start, end := bars[0].Time, bars[len(bars)-1].Time
	// When the runner pinned the trading window, report bounds from
	// TradeFrom/TradeTo so the summary covers only the requested
	// period (not the warmup slice).
	if !b.TradeFrom.IsZero() {
		start = b.TradeFrom
	}
	if !b.TradeTo.IsZero() {
		end = b.TradeTo
	}
	return summarise(closedTrades, tradeRecords, start, end)
}

// barsAhead is the bar-duration count used to bound the final tick
// window (the last bar has no successor close to anchor on).
const barsAhead = 2

// tickWindow advances the tick cursor, returning the ticks with
// timestamps in (from, to]. Ticks at or before `from` are consumed
// without evaluation (the position was flat or they predate the first
// decision bar's close). Assumes ticks are sorted ascending.
func (b *Backtester) tickWindow(idx *int, from, to time.Time) []prices.AssetPrice {
	var out []prices.AssetPrice
	for *idx < len(b.ticks) && !b.ticks[*idx].Time.After(to) {
		if b.ticks[*idx].Time.After(from) {
			out = append(out, b.ticks[*idx])
		}
		*idx++
	}
	return out
}

// exitForBar evaluates the open position's exits for one bar:
// stop-loss against the adverse extreme, take-profit against the
// favorable extreme, then the close-decision bands/reversal. Priority
// stop_loss > take_profit > close-decision. On a band exit the returned
// fill price is the band level (entry ± mult×ATR) gap-capped at
// bar.Open — an order can't fill better than the market's first price
// when the bar opens through the band. Signal/close-decision exits
// fill at the decision close.
func (b *Backtester) exitForBar(open *TradeRecord, bar types.Bar, decision Decision) (bool, decimal.Decimal, string) {
	adverse, favorable := bar.Low, bar.High
	if open.Signal == types.SignalSell {
		adverse, favorable = bar.High, bar.Low
	}
	stop, tp, hasStop, hasTP := b.strategy.bandLevels(open.Signal, open.Price, open.EntryATR)
	// The adverse extreme can only cross the stop band (the TP
	// threshold lies beyond the favorable extreme); likewise the
	// favorable extreme can only cross take-profit.
	if hasStop {
		crossed := (open.Signal == types.SignalBuy && adverse.Cmp(stop) <= 0) ||
			(open.Signal == types.SignalSell && adverse.Cmp(stop) >= 0)
		if crossed {
			return true, stopFill(open.Signal, stop, bar.Open), ExitReasonStopLoss
		}
	}
	if hasTP {
		crossed := (open.Signal == types.SignalBuy && favorable.Cmp(tp) >= 0) ||
			(open.Signal == types.SignalSell && favorable.Cmp(tp) <= 0)
		if crossed {
			return true, tpFill(open.Signal, tp, bar.Open), ExitReasonTakeProfit
		}
	}
	if exit, reason := b.strategy.ShouldExit(open.Signal, decision, open.Price, open.EntryATR); exit {
		return true, decision.Price(), reason
	}
	return false, decimal.Zero, ""
}

// stopFill returns the gap-capped stop fill: long → min(level, open)
// (open gapped below stop → fill at open, worse); short → max(level,
// open).
func stopFill(openSignal types.Signal, level, open decimal.Decimal) decimal.Decimal {
	if openSignal == types.SignalBuy {
		if open.Cmp(level) < 0 {
			return open
		}
		return level
	}
	if open.Cmp(level) > 0 {
		return open
	}
	return level
}

// tpFill returns the gap-capped TP fill: long → max(level, open) (open
// gapped above TP → limit fills at open, better); short → min(level,
// open).
func tpFill(openSignal types.Signal, level, open decimal.Decimal) decimal.Decimal {
	if openSignal == types.SignalBuy {
		if open.Cmp(level) > 0 {
			return open
		}
		return level
	}
	if open.Cmp(level) < 0 {
		return open
	}
	return level
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
