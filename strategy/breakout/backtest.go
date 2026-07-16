package breakout

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy/stats"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/bond-trading-strategies/streams"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
)

// Backtester replays a slice of historical YieldObservations through a
// Strategy and records every simulated trade and its PnL.
//
// The simulation is deliberately simple — one open position at a time,
// no transaction costs, no bid-ask spread, no financing costs. Its purpose
// is to validate the signal logic and measure basic performance
// characteristics before live deployment.
//
// Exit logic: a position is closed when the strategy emits the opposite
// signal (a BUY is closed by a SELL and vice versa), or when the price
// crosses the stop-loss or take-profit band (StopLossATR / TakeProfitATR
// units from entry). Stop-loss has priority over reversal when both fire
// on the same bar. Positions still open at end of history are
// strategy-exited at the last observation's price (ExitReasonStrategyExit).
//
// The optional writer receives one WriteTradeRecord / WriteClosedTrade
// call per row produced by the simulation; pass nil to skip persistence.
type Backtester struct {
	strategy *Strategy
	writer   stats.BacktestTradeWriter
}

// NewBacktester creates a Backtester wrapping the given Strategy. The
// writer receives one WriteTradeRecord / WriteClosedTrade call per row;
// pass nil to skip persistence.
func NewBacktester(s *Strategy, writer stats.BacktestTradeWriter) *Backtester {
	return &Backtester{strategy: s, writer: writer}
}

// Run replays obs in chronological order and returns a BacktestResult.
//
// obs must all belong to the same bond (same BondID). For multi-bond
// backtests, call Run once per bond and aggregate the results externally.
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
func (b *Backtester) Run(ctx context.Context, obs []types.YieldObservation) (BacktestResult, error) {
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
	trades := b.loadTrades(ctx, obs)
	tradeIdx := 0

	for _, o := range obs {
		select {
		case <-ctx.Done():
			return BacktestResult{}, errors.New("backtest cancelled by user")
		default:
			// Apply any trades with time <= current observation before
			// updating the strategy so OBV reflects all activity up to
			// this point.
			tradeIdx = b.ingestTradesUpTo(trades, tradeIdx, o.Time)
			decision, err := b.strategy.Update(o)
			if err != nil {
				return BacktestResult{}, err
			}
			lastDecision = decision

			if openTrade != nil {
				// Priority: stop-loss > take-profit > opposite-signal
				// reversal > hold. SL/TP are evaluated first because a
				// fast move against the position can blow past both the
				// SL threshold and the opposite-signal trigger in the
				// same bar; the SL/PnL outcome is materially worse, so
				// we want to record that explicitly.
				if reason, ok := checkStopLossTakeProfit(openTrade, decision.Price(), b.strategy.cfg); ok {
					ct, newBalance, err := b.closeAtPrice(openTrade, decision, remainingBalance, reason)
					if err != nil {
						return BacktestResult{}, err
					}
					remainingBalance = newBalance
					closedTrades = append(closedTrades, ct)
					openTrade = nil
					continue
				}
				// Exit on opposite signal.
				if isReversal(openTrade.Signal, decision.Signal()) {
					exitPrice := decision.Price()
					exitQty := openTrade.Quantity

					tradeRecords = append(tradeRecords, TradeRecord{
						Time:             decision.Time(),
						BondID:           openTrade.BondID,
						Signal:           openTrade.Signal,
						Price:            exitPrice,
						Quantity:         exitQty,
						PositionSize:     exitQty,
						CompressionRatio: decision.ArmedCompressionRatio,
					})

					// Cash flow: opposite of entry.
					cashFlow, err := exitPrice.Mul(exitQty)
					if err != nil {
						return BacktestResult{}, err
					}
					switch openTrade.Signal {
					case types.SignalBuy:
						// Close long: receive proceeds.
						remainingBalance, err = remainingBalance.Add(cashFlow)
					case types.SignalSell:
						// Close short: pay to buy back.
						remainingBalance, err = remainingBalance.Sub(cashFlow)
					default:
						_ = remainingBalance
					}
					if err != nil {
						return BacktestResult{}, err
					}

					ct := ClosedTrade{
						BondID:                openTrade.BondID,
						OpenTime:              openTrade.Time,
						CloseTime:             decision.Time(),
						Signal:                openTrade.Signal,
						ExitSignal:            decision.Signal(),
						EntryPrice:            openTrade.Price,
						ExitPrice:             exitPrice,
						Quantity:              exitQty,
						PositionSize:          openTrade.PositionSize,
						ExitReason:            ExitReasonReversal,
						EntryCompressionRatio: openTrade.CompressionRatio,
						ExitCompressionRatio:  decision.CompressionRatio,
					}
					pnl, err := computePnL(ct)
					if err != nil {
						return BacktestResult{}, err
					}
					ct.PnL = pnl
					closedTrades = append(closedTrades, ct)
					openTrade = nil
				}
				// Whether we just closed or are still holding, do not open
				// a new position in the same bar.
				continue
			}

			// Flat — check for a new entry signal.
			if decision.Signal() == types.SignalHold {
				continue
			}

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
	}

	// Force-close any position still open at end of history.
	if openTrade != nil && len(obs) > 0 {
		last := obs[len(obs)-1]
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
			Time:             last.Time,
			BondID:           openTrade.BondID,
			Signal:           openTrade.Signal,
			Price:            exitPrice,
			Quantity:         exitQty,
			PositionSize:     exitQty,
			CompressionRatio: lastDecision.ArmedCompressionRatio,
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

	start, end := obs[0].Time, obs[len(obs)-1].Time
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

// loadTrades loads all trades for the backtest date range into a sorted
// slice when volume confirmation is enabled and a trade store is
// configured. Returns nil when neither applies, so the backtest loop
// can call ingestTradesUpTo unconditionally.

func (b *Backtester) loadTrades(
	ctx context.Context,
	obs []types.YieldObservation,
) []Trade {
	if b.strategy.cfg.OBVWindow == 0 || b.strategy.tradeHistoryStore == nil {
		b.strategy.logger().Info("backtest trade history NOT wired",
			"obvWindow", b.strategy.cfg.OBVWindow,
			"storeNil", b.strategy.tradeHistoryStore == nil)
		return nil
	}
	if len(obs) == 0 {
		return nil
	}
	b.strategy.logger().Info("backtest trade history wired, loading trades",
		"orderBookID", b.strategy.cfg.OrderBookID,
		"start", obs[0].Time, "end", obs[len(obs)-1].Time)
	ch, errCh := b.strategy.tradeHistoryStore.StreamTrades(
		ctx, b.strategy.cfg.OrderBookID, obs[0].Time, obs[len(obs)-1].Time,
	)
	var trades []Trade
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
	trades []Trade,
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
