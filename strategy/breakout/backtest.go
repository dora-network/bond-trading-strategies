package breakout

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/dora-network/bond-trading-strategies/prices"
	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/stats"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/bond-trading-strategies/streams"
	"github.com/google/uuid"
	"github.com/govalues/decimal"

	"github.com/dora-network/bond-trading-strategies/trades"
)

// Backtester replays a slice of historical closed bars through a
// Strategy and records every simulated trade and its PnL.
//
// The simulation is deliberately simple — one open position at a time,
// no transaction costs, no bid-ask spread, no financing costs. Its purpose
// is to validate the signal logic and measure basic performance
// characteristics before live deployment.
//
// Exit logic: a position is closed when the strategy emits the opposite
// signal (a BUY is closed by a SELL and vice versa), or when the bar's
// extremes cross the stop-loss or take-profit band (StopLossATR /
// units from entry). Stop-loss has priority over reversal when both fire
// on the same bar. Positions still open at end of history are
// strategy-exited at the last observation's price (ExitReasonStrategyExit).
//
// The optional writer receives one WriteTradeRecord / WriteClosedTrade
// call per row produced by the simulation; pass nil to skip persistence.
type Backtester struct {
	strategy *Strategy
	writer   stats.BacktestTradeWriter
	// ticks are the price_history ticks covering the bar window, in
	// chronological order. Empty disables tick replay: intrabar exits
	// fall back to the bar-extreme approximation.
	ticks []prices.AssetPrice
}

// NewBacktester creates a Backtester wrapping the given Strategy. The
// writer receives one WriteTradeRecord / WriteClosedTrade call per row;
// pass nil to skip persistence.
func NewBacktester(s *Strategy, writer stats.BacktestTradeWriter) *Backtester {
	return &Backtester{strategy: s, writer: writer}
}

// Run replays bars in chronological order and returns a BacktestResult.
//
// bars must all belong to the same bond (same BondID). For multi-bond
//
// Position sizing uses the bond price from each observation:
//
//	budget = effectiveCapital × decision.PositionSize
//	quantity = budget / entryPrice
//
// Remaining balance starts at effectiveCapital = InitialBalance × Leverage
// and is updated on every entry and exit, so the simulation respects the
// capital constraint.
//
//nolint:funlen // backtest simulation with multiple phases
func (b *Backtester) Run(ctx context.Context, bars []types.Bar) (BacktestResult, error) {
	if len(bars) == 0 {
		return BacktestResult{}, nil
	}

	var (
		closedTrades []ClosedTrade
		tradeRecords []TradeRecord
		openTrade    *TradeRecord // nil when flat
		lastDecision Decision     // last strategy decision, used for strategy_exit
	)

	effectiveCapital, err := b.strategy.cfg.InitialBalance.Mul(b.strategy.cfg.Leverage)
	if err != nil {
		return BacktestResult{}, err
	}
	remainingBalance := effectiveCapital

	// Load and interleave trade history for OBV computation when
	// volume confirmation is enabled. The backtest clock advances with
	// each observation; trades are ingested in chronological order so
	// OBV is correct at every signal point.
	trades := b.loadTrades(ctx, bars)
	tradeIdx := 0
	// Bar.Time is the bar START; the decision happens at the bar CLOSE
	// (bar.Time+res). Trades during the signal bar must fold into OBV
	// before that bar's gate evaluation, so the cutoff is the close.
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
			// Apply any trades with time <= current observation before
			// updating the strategy so OBV reflects all activity up to
			// this point.
			tradeIdx = b.ingestTradesUpTo(trades, tradeIdx, bar.Time.Add(res))
			decision, err := b.strategy.Update(bar)
			if err != nil {
				return BacktestResult{}, err
			}
			lastDecision = decision

			if openTrade != nil {
				var (
					exit   bool
					reason string
				)
				if prevWindowHadTicks {
					// Ticks already drove the intrabar band checks for
					// this bar's formation window; only the reversal
					// close-decision remains.
					if isReversal(openTrade.Signal, decision.Signal()) {
						exit, reason = true, ExitReasonReversal
					}
				} else {
					exit, reason = b.exitForBar(openTrade, bar, decision)
				}
				if exit {
					if reason == ExitReasonReversal {
						// Persist the exit row so /trades shows a matched
						// pair (mirrors the pre-cutover behaviour; band exits
						// record only the ClosedTrade).
						tradeRecords = append(tradeRecords, TradeRecord{
							Time:             decision.Time(),
							BondID:           openTrade.BondID,
							Signal:           openTrade.Signal,
							Price:            decision.Price(),
							Quantity:         openTrade.Quantity,
							PositionSize:     openTrade.Quantity,
							CompressionRatio: openTrade.CompressionRatio,
							EntryATR:         openTrade.EntryATR,
						})
					}
					ct, newBalance, err := b.closeAtPrice(openTrade, decision, remainingBalance, reason)
					if err != nil {
						return BacktestResult{}, err
					}
					remainingBalance = newBalance
					closedTrades = append(closedTrades, ct)
					openTrade = nil
				}
			} else if decision.Signal() != types.SignalHold {
				// Flat — open on a fresh entry signal.
				entryPrice := decision.Price()
				budget, err := remainingBalance.Mul(decision.PositionSize())
				if err != nil {
					return BacktestResult{}, err
				}
				qty, err := budget.Quo(entryPrice)
				if err != nil {
					return BacktestResult{}, err
				}
				qty = qty.Floor(0)
				if qty.IsZero() {
					// Budget too small to buy even one bond; skip.
					continue
				}
				tradeRecords = append(tradeRecords, TradeRecord{
					Time:     decision.Time(),
					BondID:   decision.BondID(),
					Signal:   decision.Signal(),
					Price:    entryPrice,
					Quantity: qty,
					// PositionSize holds the computed bond quantity so the
					// API consumer sees the actual order size, not the
					// fraction of capital deployed.
					PositionSize:     qty,
					CompressionRatio: decision.ArmedCompressionRatio,
					EntryATR:         decision.ATR,
				})
				openTrade = &tradeRecords[len(tradeRecords)-1]

				cashFlow, err := entryPrice.Mul(qty)
				if err != nil {
					return BacktestResult{}, err
				}
				switch decision.Signal() {
				case types.SignalBuy:
					remainingBalance, err = remainingBalance.Sub(cashFlow)
				case types.SignalSell:
					remainingBalance, err = remainingBalance.Add(cashFlow)
				default:
					_ = remainingBalance
				}
				if err != nil {
					return BacktestResult{}, err
				}
			}

			// Tick replay: any position open at bar i's close (held or
			// freshly entered) sees the price_history ticks in
			// (close_i, close_{i+1}] through the SAME liveCheckSLTP math
			// the live run loop uses, exiting at the first crossing
			// tick's price. Live consumes exactly these ticks between
			// Update(bar i) and Update(bar i+1).
			if openTrade != nil {
				nextClose := bar.Time.Add(res * barsAhead)
				if i+1 < len(bars) {
					nextClose = bars[i+1].Time.Add(res)
				}
				window := b.tickWindow(&tickIdx, bar.Time.Add(res), nextClose)
				prevWindowHadTicks = len(window) > 0
				for _, tick := range window {
					if reason, ok := liveCheckSLTP(openTrade.Signal, openTrade.Price, openTrade.EntryATR, tick.Price, b.strategy.cfg); ok {
						d := decision
						d.price, d.time = tick.Price, tick.Time
						ct, newBalance, err := b.closeAtPrice(openTrade, d, remainingBalance, reason)
						if err != nil {
							return BacktestResult{}, err
						}
						remainingBalance = newBalance
						closedTrades = append(closedTrades, ct)
						openTrade = nil
						break
					}
				}
			}
		}
	}

	// Force-close any position still open at end of history.
	if openTrade != nil && len(bars) > 0 {
		last := bars[len(bars)-1]
		exitPrice := lastDecision.Price()
		exitQty := openTrade.Quantity

		cashFlow, err := exitPrice.Mul(exitQty)
		if err != nil {
			return BacktestResult{}, err
		}
		switch openTrade.Signal {
		case types.SignalBuy:
			if _, err = remainingBalance.Add(cashFlow); err != nil {
				return BacktestResult{}, err
			}
		case types.SignalSell:
			if _, err = remainingBalance.Sub(cashFlow); err != nil {
				return BacktestResult{}, err
			}
		default:
			_ = remainingBalance
		}

		tradeRecords = append(tradeRecords, TradeRecord{
			Time:         last.Time,
			BondID:       openTrade.BondID,
			Signal:       openTrade.Signal,
			Price:        exitPrice,
			Quantity:     exitQty,
			PositionSize: exitQty,
			// Same as reversal exits: the armed ratio lives on the entry
			// record, lastDecision's copy is zero on HOLD ticks.
			CompressionRatio: openTrade.CompressionRatio,
		})

		ct := ClosedTrade{
			BondID:                openTrade.BondID,
			OpenTime:              openTrade.Time,
			CloseTime:             last.Time,
			Signal:                openTrade.Signal,
			ExitSignal:            lastDecision.Signal(),
			EntryPrice:            openTrade.Price,
			ExitPrice:             exitPrice,
			Quantity:              exitQty,
			PositionSize:          openTrade.PositionSize,
			ExitReason:            ExitReasonStrategyExit,
			EntryCompressionRatio: openTrade.CompressionRatio,
			ExitCompressionRatio:  lastDecision.CompressionRatio,
		}
		pnl, err := computePnL(ct)
		if err != nil {
			return BacktestResult{}, err
		}
		ct.PnL = pnl
		closedTrades = append(closedTrades, ct)
	}
	_ = remainingBalance

	if b.writer != nil {
		streamTrades(ctx, b.writer, tradeRecords, closedTrades)
		if err := b.writer.Flush(ctx); err != nil {
			slog.Error("flush backtest writer", "err", err)
		}
	}

	start, end := bars[0].Time, bars[len(bars)-1].Time
	metrics, err := computeSummary(closedTrades, start, end)
	if err != nil {
		return BacktestResult{}, fmt.Errorf("compute summary: %w", err)
	}
	return BacktestResult{
		ClosedTrades: closedTrades,
		TradeRecords: tradeRecords,
		TotalPnL:     metrics.TotalPnL,
		WinCount:     metrics.WinCount,
		LossCount:    metrics.LossCount,
		MaxDrawdown:  metrics.MaxDrawdown,
		SharpeRatio:  metrics.SharpeRatio,
	}, nil
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
// stop-loss against the bar's adverse extreme (Low for a long, High
// for a short), take-profit against the favorable extreme, then the
// opposite-signal reversal. Priority stop_loss > take_profit >
// reversal, so a fast move that blows through both bands records the
// worse outcome. Exit price is the bar's close (matches the live loop,
// which acts on closed-bar decisions).
func (b *Backtester) exitForBar(open *TradeRecord, bar types.Bar, decision Decision) (bool, string) {
	adverse, favorable := bar.Low, bar.High
	if open.Signal == types.SignalSell {
		adverse, favorable = bar.High, bar.Low
	}
	// The adverse extreme can only cross the stop band (the TP
	// threshold lies beyond the favorable extreme); likewise the
	// favorable extreme can only cross take-profit.
	if reason, ok := checkStopLossTakeProfit(open, adverse, b.strategy.cfg); ok {
		return true, reason
	}
	if reason, ok := checkStopLossTakeProfit(open, favorable, b.strategy.cfg); ok {
		return true, reason
	}
	if isReversal(open.Signal, decision.Signal()) {
		return true, ExitReasonReversal
	}
	return false, ""
}

// loadTrades loads all trades for the backtest date range into a sorted
// slice when volume confirmation is enabled and a trade store is
// configured. Returns nil when neither applies, so the backtest loop
// can call ingestTradesUpTo unconditionally.

func (b *Backtester) loadTrades(
	ctx context.Context,
	bars []types.Bar,
) []trades.Trade {
	if b.strategy.cfg.OBVWindow == 0 || b.strategy.tradeHistoryStore == nil {
		b.strategy.logger().Info("backtest trade history NOT wired",
			"obvWindow", b.strategy.cfg.OBVWindow,
			"storeNil", b.strategy.tradeHistoryStore == nil)
		return nil
	}
	if len(bars) == 0 {
		return nil
	}
	b.strategy.logger().Info("backtest trade history wired, loading trades",
		"orderBookID", b.strategy.cfg.OrderBookID,
		"start", bars[0].Time, "end", bars[len(bars)-1].Time)
	ch, errCh := b.strategy.tradeHistoryStore.StreamTrades(
		ctx, b.strategy.cfg.OrderBookID, bars[0].Time, bars[len(bars)-1].Time,
	)
	var trades []trades.Trade
	chClosed := false
	for !chClosed {
		select {
		case t, ok := <-ch:
			if !ok {
				chClosed = true
				continue
			}
			trades = append(trades, t)
		case err := <-errCh:
			if err != nil {
				b.strategy.logger().Error("backtest trade stream error", "err", err)
			}
		}
	}
	// Drain any remaining error after ch is closed.
	select {
	case err := <-errCh:
		if err != nil {
			b.strategy.logger().Error("backtest trade stream error", "err", err)
		}
	default:
	}
	return trades
}

// ingestTradesUpTo advances `idx` through `trades`, applying every
// trade with time <= cutoff to the Strategy's OBV accumulator via
// applyTradeEvent. Returns the updated index. The store is assumed
// to return trades in chronological order.
func (b *Backtester) ingestTradesUpTo(
	trades []trades.Trade,
	idx int,
	cutoff time.Time,
) int {
	for idx < len(trades) && !trades[idx].Time.After(cutoff) {
		b.strategy.applyTradeEvent(streams.TradeEvent{
			Price:    trades[idx].Price,
			Quantity: trades[idx].Quantity,
			Side:     trades[idx].Side,
		})
		idx++
	}
	return idx
}

func streamTrades(
	ctx context.Context,
	w stats.BacktestTradeWriter,
	records []TradeRecord,
	closed []ClosedTrade,
) {
	for _, r := range records {
		rec := stats.TradeRecordInsert{
			BacktestID:       uuid.Nil,
			Time:             r.Time,
			BondID:           r.BondID,
			Signal:           r.Signal.String(),
			Price:            r.Price,
			Quantity:         r.Quantity,
			PositionSize:     r.PositionSize,
			CompressionRatio: r.CompressionRatio,
			EntryATR:         r.EntryATR,
		}
		if err := w.WriteTradeRecord(ctx, rec); err != nil {
			slog.Error("write trade record", "err", err)
		}
	}
	for _, c := range closed {
		rec := stats.ClosedTradeInsert{
			BacktestID:            uuid.Nil,
			OpenTime:              c.OpenTime,
			CloseTime:             c.CloseTime,
			BondID:                c.BondID,
			OpenSignal:            c.Signal.String(),
			CloseSignal:           c.ExitSignal.String(),
			Quantity:              c.Quantity,
			EntryPrice:            c.EntryPrice,
			ExitPrice:             c.ExitPrice,
			PnL:                   c.PnL,
			PositionSize:          c.PositionSize,
			ExitReason:            c.ExitReason,
			EntryCompressionRatio: c.EntryCompressionRatio,
			ExitCompressionRatio:  c.ExitCompressionRatio,
		}
		if err := w.WriteClosedTrade(ctx, rec); err != nil {
			slog.Error("write closed trade", "err", err)
		}
	}
}

// isReversal reports whether a new signal would close an existing
// position opened with the open signal. A reversal is defined as the
// strategy emitting the opposite direction (open ≠ current) AND a
// non-HOLD new signal (HOLD ticks do not close open positions).
// The caller guarantees `open` is Buy or Sell (it checks openTrade != nil).
func isReversal(open, current types.Signal) bool {
	return open != current && current != types.SignalHold
}

// computePnL returns the cash profit/loss of a closed trade.
//   - BUY (long):  PnL = Quantity × (ExitPrice − EntryPrice)
//   - SELL (short): PnL = Quantity × (EntryPrice − ExitPrice)
func computePnL(ct ClosedTrade) (decimal.Decimal, error) {
	costBasis, err := ct.EntryPrice.Mul(ct.Quantity)
	if err != nil {
		return decimal.Zero, err
	}
	proceeds, err := ct.ExitPrice.Mul(ct.Quantity)
	if err != nil {
		return decimal.Zero, err
	}
	switch ct.Signal {
	case types.SignalBuy:
		return proceeds.Sub(costBasis)
	case types.SignalSell:
		return costBasis.Sub(proceeds)
	default:
		return decimal.Zero, nil
	}
}

// computeSummary converts breakout ClosedTrade values to stats.PnLPoint
// and delegates to stats.Summarise, which builds an equity curve from
// daily-PnL buckets and computes TotalPnL, WinCount, LossCount,
// MaxDrawdown (peak-to-trough of the equity curve), and SharpeRatio
// (annualised, based on daily PnL).
func computeSummary(closed []ClosedTrade, start, end time.Time) (stats.Summary, error) {
	points := make([]stats.PnLPoint, len(closed))
	for i, ct := range closed {
		points[i] = stats.PnLPoint{
			PnL:       ct.PnL,
			CloseTime: ct.CloseTime,
		}
	}
	return stats.Summarise(points, start, end)
}

// checkStopLossTakeProfit returns the exit reason ("stop_loss" or
// "take_profit") if the current price has crossed the SL or TP band
// for the open position, or ("", false) if neither band was hit (or
// both are disabled via a zero multiplier).
//
// For a long (BUY): SL is below entry, TP is above.
// For a short (SELL): SL is above entry, TP is below.
// Stop-loss is checked before take-profit so a single fast move that
// blows through both bands records the worse outcome (the SL).
func checkStopLossTakeProfit(open *TradeRecord, currentPrice decimal.Decimal, cfg Config) (string, bool) {
	if open.EntryATR.IsZero() {
		return "", false
	}
	switch open.Signal {
	case types.SignalBuy:
		if cfg.StopLossATR.IsPos() {
			if slHit, ok := exitPriceCrosses(open.Price, open.EntryATR, cfg.StopLossATR, currentPrice, false); ok && slHit {
				return ExitReasonStopLoss, true
			}
		}
		if cfg.TakeProfitATR.IsPos() {
			if tpHit, ok := exitPriceCrosses(open.Price, open.EntryATR, cfg.TakeProfitATR, currentPrice, true); ok && tpHit {
				return ExitReasonTakeProfit, true
			}
		}
	case types.SignalSell:
		if cfg.StopLossATR.IsPos() {
			if slHit, ok := exitPriceCrosses(open.Price, open.EntryATR, cfg.StopLossATR, currentPrice, true); ok && slHit {
				return ExitReasonStopLoss, true
			}
		}
		if cfg.TakeProfitATR.IsPos() {
			if tpHit, ok := exitPriceCrosses(open.Price, open.EntryATR, cfg.TakeProfitATR, currentPrice, false); ok && tpHit {
				return ExitReasonTakeProfit, true
			}
		}
	default:
		// open.Signal is Hold; unreachable in production (the backtest
		// only opens on Buy or Sell).
		return "", false
	}
	// Reached when the SignalBuy or SignalSell cases don't fire their
	// inner returns (e.g. multipliers are non-positive or the price
	// hasn't crossed the band).
	return "", false
}

// exitPriceCrosses reports whether the current price has crossed the
// exit threshold derived from entry price +/- multiplier*entryATR.
// For longs, "above" is the profitable direction; for shorts, "below".
// Returns (false, false) if the multiplier is non-positive or the
// arithmetic errors.
func exitPriceCrosses(entryPrice, entryATR, multiplier, currentPrice decimal.Decimal, wantAbove bool) (bool, bool) {
	if !multiplier.IsPos() {
		return false, false
	}
	distance, err := multiplier.Mul(entryATR)
	if err != nil {
		return false, false
	}
	var threshold decimal.Decimal
	if wantAbove {
		threshold, err = entryPrice.Add(distance)
	} else {
		threshold, err = entryPrice.Sub(distance)
	}
	if err != nil {
		return false, false
	}
	if wantAbove {
		return currentPrice.Cmp(threshold) >= 0, true
	}
	return currentPrice.Cmp(threshold) <= 0, true
}

// closeAtPrice closes the open trade at the current decision's price
// and records the exit with the given reason. The remaining balance is
// returned alongside the ClosedTrade so the caller can update its
// tracked balance (the helper does not own that state).
func (b *Backtester) closeAtPrice(
	open *TradeRecord,
	decision Decision,
	balance decimal.Decimal,
	reason string,
) (ClosedTrade, decimal.Decimal, error) {
	exitPrice := decision.Price()
	exitQty := open.Quantity

	cashFlow, err := exitPrice.Mul(exitQty)
	if err != nil {
		return ClosedTrade{}, balance, err
	}
	var newBalance decimal.Decimal
	switch open.Signal {
	case types.SignalBuy:
		newBalance, err = balance.Add(cashFlow)
	case types.SignalSell:
		newBalance, err = balance.Sub(cashFlow)
	default:
		newBalance = balance
	}
	if err != nil {
		return ClosedTrade{}, balance, err
	}

	ct := ClosedTrade{
		BondID:                open.BondID,
		OpenTime:              open.Time,
		CloseTime:             decision.Time(),
		Signal:                open.Signal,
		ExitSignal:            decision.Signal(),
		EntryPrice:            open.Price,
		ExitPrice:             exitPrice,
		Quantity:              exitQty,
		PositionSize:          open.PositionSize,
		ExitReason:            reason,
		EntryCompressionRatio: open.CompressionRatio,
		ExitCompressionRatio:  decision.CompressionRatio,
	}
	pnl, err := computePnL(ct)
	if err != nil {
		return ClosedTrade{}, balance, err
	}
	ct.PnL = pnl
	return ct, newBalance, nil
}
