package breakout

import (
	"context"
	"errors"

	"github.com/dora-network/bond-trading-strategies/strategy/stats"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
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
// signal (a BUY is closed by a SELL and vice versa). Take-profit and
// stop-loss exits (StopLossATR * ATR from entry) are out of scope for v1
// and land in a follow-up ticket. Positions still open at end of history
// are strategy-exited at the last observation's price (ExitReasonStrategyExit).
//
// The optional writer receives one WriteTradeRecord / WriteClosedTrade
// call per row produced by the simulation; pass nil to skip persistence.
// Writer integration is a placeholder for v1 — the stats.BacktestTradeWriter
// interface in this package accepts the breakout record shapes (the
// stats package's TradeRecordInsert / ClosedTradeInsert shapes are
// meanreversion-specific and cannot be reused here without a wider
// refactor of stats).
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

	for _, o := range obs {
		select {
		case <-ctx.Done():
			return BacktestResult{}, errors.New("backtest cancelled by user")
		default:
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
						PositionSize:     openTrade.PositionSize,
						CompressionRatio: decision.CompressionRatio,
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
				Time:             decision.Time(),
				BondID:           decision.BondID(),
				Signal:           decision.Signal(),
				Price:            entryPrice,
				Quantity:         qty,
				PositionSize:     decision.PositionSize(),
				CompressionRatio: decision.CompressionRatio,
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
			PositionSize:     openTrade.PositionSize,
			CompressionRatio: lastDecision.CompressionRatio,
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

	summary := summarise(closedTrades)
	return BacktestResult{
		ClosedTrades: closedTrades,
		TradeRecords: tradeRecords,
		TotalPnL:     summary.TotalPnL,
		WinCount:     summary.WinCount,
		LossCount:    summary.LossCount,
		MaxDrawdown:  summary.MaxDrawdown,
		SharpeRatio:  summary.SharpeRatio,
	}, nil
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

// summary holds the aggregate metrics derived from a slice of closed trades.
type summary struct {
	TotalPnL    decimal.Decimal
	WinCount    int
	LossCount   int
	MaxDrawdown decimal.Decimal
	SharpeRatio decimal.Decimal
}

// summarise aggregates closed trades into a summary. PnL is the only
// meaningful metric for v1 — MaxDrawdown and SharpeRatio require a
// time-series of equity points which the breakout backtest does not
// yet track. Both are returned as zero (matching stats.Summarise with
// an empty point series).
func summarise(trades []ClosedTrade) summary {
	s := summary{}
	for _, t := range trades {
		s.TotalPnL, _ = s.TotalPnL.Add(t.PnL)
		if t.PnL.IsPos() {
			s.WinCount++
		} else if t.PnL.IsNeg() {
			s.LossCount++
		}
	}
	return s
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
