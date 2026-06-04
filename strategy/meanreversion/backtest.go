package meanreversion

import (
	"context"
	"errors"
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy/stats"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/govalues/decimal"
)

// Backtester replays a slice of historical YieldObservations through a
// Strategy and records every simulated trade and its PnL.
//
// The simulation is deliberately simple - one open position per bond at a
// time, no transaction costs, no bid-ask spread, no financing costs. Its
// purpose is to validate the signal logic and measure basic performance
// characteristics before live deployment.
type Backtester struct {
	strategy *Strategy
}

// NewBacktester creates a Backtester wrapping the given Strategy.
func NewBacktester(s *Strategy) *Backtester {
	return &Backtester{strategy: s}
}

// Run replays obs in chronological order and returns a BacktestResult.
//
// obs must all belong to the same bond (same BondID). For multi-bond
// backtests, call Run once per bond and aggregate the results externally.
//
// Position sizing uses the bond price from each observation:
//
//	budget = remainingBalance × decision.PositionSize
//	quantity = budget / entryPrice
//
// Remaining balance starts at cfg.InitialBalance and is updated on every entry
// and exit, so the simulation respects the capital constraint.
//
//nolint:funlen // backtest simulation with multiple phases
func (b *Backtester) Run(ctx context.Context, obs []types.YieldObservation) (BacktestResult, error) {
	var (
		closedTrades []ClosedTrade
		tradeRecords []TradeRecord
		openTrade    *TradeRecord   // nil when flat
		lastDecision types.Decision // last strategy decision, used for force-close z-score
	)

	// Effective capital mirrors the live cappedOrderQuantity calculation:
	//   effectiveCapital = InitialBalance × collateralWeight × Leverage.
	// collateralWeight is 1.0 during backtests (never fetched from DORA),
	// so only Leverage is meaningful here.
	effectiveCapital, err := b.strategy.cfg.InitialBalance.Mul(b.strategy.collateralWeight)
	if err != nil {
		return BacktestResult{}, err
	}
	effectiveCapital, err = effectiveCapital.Mul(b.strategy.cfg.Leverage)
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
				// Check whether the open position should be exited.
				if shouldExit, exitReason := b.strategy.ShouldExit(openTrade.Signal, decision.ZScore); shouldExit {
					// Compute exit quantity and update remaining balance.
					exitQty := openTrade.Quantity
					exitPrice := decision.Price

					// Record the exit trade event (use the open trade's signal so the
					// exit record carries the original direction, not the HOLD signal
					// generated once the spread has reverted).
					tradeRecords = append(tradeRecords, TradeRecord{
						Time:         decision.Time,
						BondID:       openTrade.BondID,
						Signal:       openTrade.Signal,
						Spread:       decision.Spread,
						PositionSize: openTrade.PositionSize,
						ZScore:       decision.ZScore,
						Price:        exitPrice,
						Quantity:     exitQty,
					})

					// Update remaining balance on exit: the opposite of the entry effect.
					switch openTrade.Signal {
					case types.SignalBuy:
						// Close long: we receive cash = exitPrice × quantity.
						proceeds, err := exitPrice.Mul(exitQty)
						if err != nil {
							return BacktestResult{}, err
						}
						remainingBalance, err = remainingBalance.Add(proceeds)
						if err != nil {
							return BacktestResult{}, err
						}
					case types.SignalSell:
						// Close short: we spend cash to buy back = exitPrice × quantity.
						cost, err := exitPrice.Mul(exitQty)
						if err != nil {
							return BacktestResult{}, err
						}
						remainingBalance, err = remainingBalance.Sub(cost)
						if err != nil {
							return BacktestResult{}, err
						}
					default:
					}

					ct := ClosedTrade{
						BondID:       openTrade.BondID,
						OpenTime:     openTrade.Time,
						CloseTime:    decision.Time,
						Signal:       openTrade.Signal,
						ExitSignal:   decision.Signal,
						EntrySpread:  openTrade.Spread,
						ExitSpread:   decision.Spread,
						EntryZScore:  openTrade.ZScore,
						ExitZScore:   decision.ZScore,
						PositionSize: openTrade.PositionSize,
						ExitReason:   exitReason,
						EntryPrice:   openTrade.Price,
						ExitPrice:    exitPrice,
						Quantity:     exitQty,
						EntryBalance: openTrade.EntryBalance,
					}
					pnl, err := computePnL(ct)
					if err != nil {
						return BacktestResult{}, err
					}
					ct.PnL = pnl
					closedTrades = append(closedTrades, ct)
					openTrade = nil
				}
				// Do not open a new position in the same bar we just closed.
				continue
			}

			// No open position - check for a new entry signal.
			if decision.Signal != types.SignalHold {
				entryPrice := decision.Price
				budget, err := remainingBalance.Mul(decision.PositionSize)
				if err != nil {
					return BacktestResult{}, err
				}
				// Compute the number of bonds we can buy/sell with this budget.
				// Bond quantity must be a whole number (no fractional bonds).
				qty, err := budget.Quo(entryPrice)
				if err != nil {
					return BacktestResult{}, err
				}
				qty = qty.Floor(0)
				if qty.IsZero() {
					// Budget too small to buy even one bond; skip this signal.
					continue
				}

				// Record the entry trade event with the remaining balance before
				// any cash-flow adjustment, so the PnL of the closed trade
				// matches the actual return on the deployed capital.
				tradeRecords = append(tradeRecords, TradeRecord{
					Time:         decision.Time,
					BondID:       decision.BondID,
					Signal:       decision.Signal,
					Spread:       decision.Spread,
					PositionSize: decision.PositionSize,
					ZScore:       decision.ZScore,
					Price:        entryPrice,
					Quantity:     qty,
					EntryBalance: remainingBalance,
				})
				openTrade = &tradeRecords[len(tradeRecords)-1]

				// Update remaining balance on entry using the actual cash
				// flow (quantity × entryPrice), not the budget, because the
				// floored quantity may not use the full budget.
				cashFlow, err := entryPrice.Mul(qty)
				if err != nil {
					return BacktestResult{}, err
				}
				switch decision.Signal {
				case types.SignalBuy:
					// We spend cashFlow to buy bonds.
					remainingBalance, err = remainingBalance.Sub(cashFlow)
				case types.SignalSell:
					// We receive cashFlow from the short sale.
					remainingBalance, err = remainingBalance.Add(cashFlow)
				default:
					_ = remainingBalance
				}
				if err != nil {
					return BacktestResult{}, err
				}
			}
		}
	}

	// Force-close any position still open at end of history.
	if openTrade != nil && len(obs) > 0 {
		last := obs[len(obs)-1]
		lastSpread, err := last.Spread()
		if err != nil {
			return BacktestResult{}, err
		}
		exitPrice := lastDecision.Price
		exitQty := openTrade.Quantity

		// Update remaining balance on force-close.
		switch openTrade.Signal {
		case types.SignalBuy:
			proceeds, err := exitPrice.Mul(exitQty)
			if err != nil {
				return BacktestResult{}, err
			}
			if _, err = remainingBalance.Add(proceeds); err != nil {
				return BacktestResult{}, err
			}
		case types.SignalSell:
			cost, err := exitPrice.Mul(exitQty)
			if err != nil {
				return BacktestResult{}, err
			}
			if _, err = remainingBalance.Sub(cost); err != nil {
				return BacktestResult{}, err
			}
		default:
		}

		// Record the exit trade event (use the original signal for direction,
		// not the HOLD signal from the last observation). The z-score comes
		// from the last strategy decision captured in the loop above.
		tradeRecords = append(tradeRecords, TradeRecord{
			Time:         last.Time,
			BondID:       openTrade.BondID,
			Signal:       openTrade.Signal,
			Spread:       lastSpread,
			PositionSize: openTrade.PositionSize,
			ZScore:       lastDecision.ZScore,
			Price:        exitPrice,
			Quantity:     exitQty,
		})

		ct := ClosedTrade{
			BondID:       openTrade.BondID,
			OpenTime:     openTrade.Time,
			CloseTime:    last.Time,
			Signal:       openTrade.Signal,
			ExitSignal:   lastDecision.Signal,
			EntrySpread:  openTrade.Spread,
			ExitSpread:   lastSpread,
			EntryZScore:  openTrade.ZScore,
			ExitZScore:   lastDecision.ZScore,
			PositionSize: openTrade.PositionSize,
			ExitReason:   ExitReasonForceClose,
			EntryPrice:   openTrade.Price,
			ExitPrice:    exitPrice,
			Quantity:     exitQty,
			EntryBalance: openTrade.EntryBalance,
		}
		pnl, err := computePnL(ct)
		if err != nil {
			return BacktestResult{}, err
		}
		ct.PnL = pnl
		closedTrades = append(closedTrades, ct)
	}

	var start, end time.Time
	if len(obs) > 0 {
		start = obs[0].Time
		end = obs[len(obs)-1].Time
	}
	return summarise(closedTrades, tradeRecords, start, end)
}

// computePnL calculates the profit/loss of a closed trade from the actual
// cash flows (quantity × price difference), not from the full EntryBalance.
// PositionSize controls how much of the balance is deployed; PnL reflects
// the actual return on the deployed capital, so the EntryBalance of the
// next trade equals the previous EntryBalance + this PnL.
//
//   - BUY (long):  PnL = Quantity × (ExitPrice − EntryPrice)
//   - SELL (short): PnL = Quantity × (EntryPrice − ExitPrice)
//
// In both cases a positive PnL means the reversion prediction was correct.
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
		// Long: profit when exit price > entry price.
		return proceeds.Sub(costBasis)
	case types.SignalSell:
		// Short: profit when entry price > exit price.
		return costBasis.Sub(proceeds)
	default:
		return decimal.Zero, nil
	}
}

// summarise aggregates closed trades into a BacktestResult.
func summarise(trades []ClosedTrade, tradeRecords []TradeRecord, start, end time.Time) (BacktestResult, error) {
	res := BacktestResult{ClosedTrades: trades, TradeRecords: tradeRecords}

	if len(trades) == 0 {
		return res, nil
	}

	// Total PnL, win/loss count, and running equity for drawdown.
	equity := decimal.Zero
	peak := decimal.Zero
	maxDD := decimal.Zero

	// Aggregate PnL by day for Sharpe Ratio
	dailyPnLMap := make(map[string]decimal.Decimal)

	for _, t := range trades {
		var err error
		res.TotalPnL, err = res.TotalPnL.Add(t.PnL)
		if err != nil {
			return BacktestResult{}, err
		}

		// Accumulate daily PnL based on trade close time
		dateStr := t.CloseTime.Format("2006-01-02")
		current, ok := dailyPnLMap[dateStr]
		if !ok {
			current = decimal.Zero
		}
		dailyPnLMap[dateStr], err = current.Add(t.PnL)
		if err != nil {
			return BacktestResult{}, err
		}

		if t.PnL.IsPos() {
			res.WinCount++
		} else {
			res.LossCount++
		}

		equity, err = equity.Add(t.PnL)
		if err != nil {
			return BacktestResult{}, err
		}
		if equity.Cmp(peak) > 0 {
			peak = equity
		}
		dd, err := peak.Sub(equity)
		if err != nil {
			return BacktestResult{}, err
		}
		if dd.Cmp(maxDD) > 0 {
			maxDD = dd
		}
	}

	res.MaxDrawdown = maxDD

	// Build daily PnL array spanning the whole backtest duration
	var dailyPnLs []decimal.Decimal
	startDay := start.Truncate(24 * time.Hour)                       //nolint:mnd
	endDay := end.Truncate(24 * time.Hour)                           //nolint:mnd
	for d := startDay; !d.After(endDay); d = d.Add(24 * time.Hour) { //nolint:mnd
		dateStr := d.Format("2006-01-02")
		pnl, ok := dailyPnLMap[dateStr]
		if !ok {
			pnl = decimal.Zero
		}
		dailyPnLs = append(dailyPnLs, pnl)
	}

	var err error
	res.SharpeRatio, err = sharpe(dailyPnLs)
	if err != nil {
		return BacktestResult{}, err
	}

	return res, nil
}

// sharpe computes an annualised Sharpe ratio assuming daily PnL observations.
// Delegates to strategy/stats.Sharpe.
func sharpe(pnls []decimal.Decimal) (decimal.Decimal, error) {
	return stats.Sharpe(pnls)
}
