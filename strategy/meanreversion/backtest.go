package meanreversion

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/dora-network/bond-trading-strategies/prices"
	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/stats"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/bond-trading-strategies/streams"
	"github.com/dora-network/bond-trading-strategies/trades"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
)

// Backtester replays a series of historical bars (oldest-first) through a
// Strategy and records every simulated trade and its PnL.
//
// The simulation is deliberately simple - one open position per bond at a
// time, no transaction costs, no bid-ask spread, no financing costs. Its
// purpose is to validate the signal logic and measure basic performance
// characteristics before live deployment.
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

// NewBacktester creates a Backtester wrapping the given Strategy. The
// writer receives one WriteTradeRecord / WriteClosedTrade call per row
// produced by the simulation; pass nil to skip persistence.
func NewBacktester(s *Strategy, writer stats.BacktestTradeWriter) *Backtester {
	return &Backtester{strategy: s, writer: writer}
}

// Run replays bars in chronological order and returns a BacktestResult.
//
// bars must all belong to the same order book and typically include
// LookbackWindow+1 warmup bars before the decision window so the rolling
// window is full when decisions begin.
//
// Position sizing uses the bond price from each bar:
//
//	budget = remainingBalance × decision.PositionSize
//	quantity = budget / entryPrice
//
// Remaining balance starts at cfg.InitialBalance and is updated on every entry
// and exit, so the simulation respects the capital constraint.
//
//nolint:funlen // backtest simulation with multiple phases
func (b *Backtester) Run(ctx context.Context, bars []types.Bar) (BacktestResult, error) {
	var (
		closedTrades []ClosedTrade
		tradeRecords []TradeRecord
		openTrade    *TradeRecord // nil when flat
		lastDecision Decision     // last strategy decision, used for force-close z-score
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

	// Imbalance gate: load historical trades and interleave them with
	// the bar replay so the signed-flow window is correct at every bar.
	// When the gate is disabled or no source is wired, loadTrades
	// returns nil and the gate passes every entry through.
	trades := b.loadTrades(ctx, bars)
	tradeIdx := 0
	// Bar.Time is the bar START; the decision happens at the bar CLOSE
	// (bar.Time+res). Trades during the signal bar must fold into OBV
	// before that bar's gate evaluation, so the cutoff is the close.
	// Bar-driven trade records (entries, bar-driven exits, force-closes)
	// are stamped at the bar CLOSE, not the START, so they sort AFTER
	// any tick-driven exit that fired inside the preceding bar's
	// formation window. Tick-driven exits (closeOnTick) keep the
	// tick's own timestamp.
	res := strategy.ResolutionDuration(b.strategy.cfg.Resolution)
	tickIdx := 0
	// prevWindowHadTicks records whether the window (close_{i-1}, close_i]
	// contained ticks; when it did, those ticks already drove the intrabar
	// exit checks, so bar i's extreme check is skipped (no double-fire).
	prevWindowHadTicks := false

	for i, bar := range bars {
		select {
		case <-ctx.Done():
			return BacktestResult{}, errors.New("backtest cancelled by user")
		default:
			tradeIdx = b.ingestTradesUpTo(trades, tradeIdx, bar.Time.Add(res))
			decision, err := b.strategy.Update(bar)
			if err != nil {
				return BacktestResult{}, err
			}
			lastDecision = decision
			closedThisBar := false

			if openTrade != nil {
				// Two exit evaluations per bar: the adverse intra-bar
				// extreme first (stop-loss can fire on the spread's worst
				// point, not just its close), then the bar's close
				// decision (z-reversion profit-take). When ticks covered
				// this bar's formation window they already drove the
				// intrabar check (see the tick replay below), so the
				// extreme approximation is skipped.
				exitZ := decision.ZScore
				shouldExit, exitReason := b.strategy.ShouldExit(openTrade.Signal, decision.ZScore)
				var (
					adverseExitSpread decimal.Decimal
					adverseExit       bool
				)
				if !prevWindowHadTicks {
					if adverseYTM, ok := adverseExtreme(openTrade.Signal, bar); ok {
						adverseSpread, err := adverseYTM.Sub(bar.BenchmarkYield)
						if err != nil {
							return BacktestResult{}, err
						}
						if z, ok := b.strategy.zAgainstWindow(adverseSpread); ok {
							if stop, reason := b.strategy.ShouldExit(openTrade.Signal, z); stop {
								shouldExit, exitReason, exitZ = true, reason, z
								adverseExitSpread = adverseSpread
								adverseExit = true
							}
						}
					}
				}
				if shouldExit {
					// Compute exit quantity and update remaining balance.
					exitQty := openTrade.Quantity
					// YTM extremes have no direct price mapping; the
					// backtest records the bar's adverse PRICE extreme
					// (long → Low, short → High) as the "worse outcome"
					// fill. Close-decision exits keep the decision close.
					exitPrice := decision.Price()
					exitSpread := decision.Spread
					if adverseExit {
						if openTrade.Signal == types.SignalBuy {
							exitPrice = bar.Low
						} else {
							exitPrice = bar.High
						}
						exitSpread = adverseExitSpread
					}

					// Record the exit trade event (use the open trade's signal so the
					// exit record carries the original direction, not the HOLD signal
					// generated once the spread has reverted).
					// Bar-driven exit: stamp the close at the bar's CLOSE
					// (decision.Time() = bar.Time = bar START, so we add res).
					// Without this shift, a re-entry on the next bar could
					// be stamped at the bar START (earlier than this exit)
					// and break timeline monotonicity. Tick-driven exits
					// are unaffected (handled in closeOnTick with tick.Time).
					tradeRecords = append(tradeRecords, TradeRecord{
						Time:         decision.Time().Add(res),
						BondID:       openTrade.BondID,
						Signal:       openTrade.Signal,
						Spread:       exitSpread,
						PositionSize: openTrade.PositionSize,
						ZScore:       exitZ,
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
						BondID:   openTrade.BondID,
						OpenTime: openTrade.Time,
						// Bar-driven exit: stamp CloseTime at the bar's
						// CLOSE (decision.Time() = bar START, so add res).
						// Tick-driven exits use the tick's own time via
						// closeOnTick (see below).
						CloseTime:    decision.Time().Add(res),
						Signal:       openTrade.Signal,
						ExitSignal:   decision.Signal(),
						EntrySpread:  openTrade.Spread,
						ExitSpread:   exitSpread,
						EntryZScore:  openTrade.ZScore,
						ExitZScore:   exitZ,
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
				if openTrade == nil {
					closedThisBar = true
				}
			}

			// No open position - check for a new entry signal. A position
			// closed on this bar's decision does not re-enter the same bar.
			// Entry is also gated on the trading window: bars before
			// TradeFrom (warmup) or at/after TradeTo only feed Update,
			// ingestTradesUpTo (imbalance), and run exits — they never
			// open a position. Entries execute at bar completion
			// (bar.Time + res), so every entry timestamp falls in
			// (TradeFrom, TradeTo]; the TradeFrom side stays
			// `Before`-strict (bars starting before TradeFrom never
			// trade even if they complete exactly at TradeFrom —
			// warmup contract).
			if openTrade == nil && !closedThisBar &&
				decision.Signal() != types.SignalHold && b.strategy.imbalanceAllows(decision.Signal()) &&
				(b.TradeFrom.IsZero() || !bar.Time.Before(b.TradeFrom)) &&
				(b.TradeTo.IsZero() || !bar.Time.Add(res).After(b.TradeTo)) {
				entryPrice := decision.Price()
				budget, err := remainingBalance.Mul(decision.PositionSize())
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
				// Bar-driven entry: stamp the entry at the bar's CLOSE
				// (decision.Time() = bar START, so add res). Without this
				// shift, a re-entry after a tick-driven exit could be
				// stamped at the bar START — earlier than the tick exit —
				// breaking timeline monotonicity. Tick exits are
				// unaffected.
				tradeRecords = append(tradeRecords, TradeRecord{
					Time:         decision.Time().Add(res),
					BondID:       decision.BondID(),
					Signal:       decision.Signal(),
					Spread:       decision.Spread,
					PositionSize: decision.PositionSize(),
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
				switch decision.Signal() {
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

			// Tick replay: any position open at bar i's close (held or
			// freshly entered) sees the price_history ticks in
			// (close_i, close_{i+1}] through the SAME intrabar math the
			// live run loop uses — tick YTM vs this bar's benchmark yield,
			// zAgainstWindow, ShouldExit — exiting at the first crossing
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
					if tick.YTM == nil {
						continue
					}
					spread, err := tick.YTM.Sub(bar.BenchmarkYield)
					if err != nil {
						return BacktestResult{}, err
					}
					z, ok := b.strategy.zAgainstWindow(spread)
					if !ok {
						continue
					}
					if stop, reason := b.strategy.ShouldExit(openTrade.Signal, z); stop {
						ct, rec, bal, err := b.closeOnTick(
							openTrade, tick, spread, z, decision.Signal(), reason, remainingBalance)
						if err != nil {
							return BacktestResult{}, err
						}
						remainingBalance = bal
						closedTrades = append(closedTrades, ct)
						tradeRecords = append(tradeRecords, rec)
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
		lastSpread, err := last.CloseYTM.Sub(last.BenchmarkYield)
		if err != nil {
			return BacktestResult{}, err
		}
		exitPrice := lastDecision.Price()
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
		// not the HOLD signal from the last bar). The z-score comes
		// from the last strategy decision captured in the loop above.
		// Force-close is a bar-driven event: stamp the close at the
		// last bar's CLOSE (last.Time + res) so it sorts AFTER any
		// tick exit that fired in the last bar's formation window.
		tradeRecords = append(tradeRecords, TradeRecord{
			Time:         last.Time.Add(res),
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
			CloseTime:    last.Time.Add(res),
			Signal:       openTrade.Signal,
			ExitSignal:   lastDecision.Signal(),
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
	if len(bars) > 0 {
		start = bars[0].Time
		end = bars[len(bars)-1].Time
	}
	// When the runner pinned the trading window, report bounds from
	// TradeFrom/TradeTo so the summary covers only the requested
	// period (not the warmup slice).
	if !b.TradeFrom.IsZero() {
		start = b.TradeFrom
	}
	if !b.TradeTo.IsZero() {
		end = b.TradeTo
	}

	if b.writer != nil {
		streamTrades(ctx, b.writer, tradeRecords, closedTrades)
		// Flush drains any rows still buffered by a batching writer.
		if err := b.writer.Flush(ctx); err != nil {
			slog.Error("flush backtest writer", "err", err)
		}
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

// closeOnTick closes an open position at a tick's price and time,
// mirroring the live loop's intrabar closePosition. Returns the closed
// trade, its trade record, and the updated remaining balance.
func (b *Backtester) closeOnTick(
	open *TradeRecord,
	tick prices.AssetPrice,
	spread decimal.Decimal,
	exitZ decimal.Decimal,
	exitSignal types.Signal,
	reason string,
	remainingBalance decimal.Decimal,
) (ClosedTrade, TradeRecord, decimal.Decimal, error) {
	rec := TradeRecord{
		Time:         tick.Time,
		BondID:       open.BondID,
		Signal:       open.Signal,
		Spread:       spread,
		PositionSize: open.PositionSize,
		ZScore:       exitZ,
		Price:        tick.Price,
		Quantity:     open.Quantity,
	}

	var newBalance decimal.Decimal
	switch open.Signal {
	case types.SignalBuy:
		proceeds, err := tick.Price.Mul(open.Quantity)
		if err != nil {
			return ClosedTrade{}, TradeRecord{}, remainingBalance, err
		}
		newBalance, err = remainingBalance.Add(proceeds)
		if err != nil {
			return ClosedTrade{}, TradeRecord{}, remainingBalance, err
		}
	case types.SignalSell:
		cost, err := tick.Price.Mul(open.Quantity)
		if err != nil {
			return ClosedTrade{}, TradeRecord{}, remainingBalance, err
		}
		newBalance, err = remainingBalance.Sub(cost)
		if err != nil {
			return ClosedTrade{}, TradeRecord{}, remainingBalance, err
		}
	default:
		newBalance = remainingBalance
	}

	ct := ClosedTrade{
		BondID:       open.BondID,
		OpenTime:     open.Time,
		CloseTime:    tick.Time,
		Signal:       open.Signal,
		ExitSignal:   exitSignal,
		EntrySpread:  open.Spread,
		ExitSpread:   spread,
		EntryZScore:  open.ZScore,
		ExitZScore:   exitZ,
		PositionSize: open.PositionSize,
		ExitReason:   reason,
		EntryPrice:   open.Price,
		ExitPrice:    tick.Price,
		Quantity:     open.Quantity,
		EntryBalance: open.EntryBalance,
	}
	pnl, err := computePnL(ct)
	if err != nil {
		return ClosedTrade{}, TradeRecord{}, newBalance, err
	}
	ct.PnL = pnl
	return ct, rec, newBalance, nil
}

// loadTrades loads all historical trades for the backtest bar range into a
// chronological slice when the imbalance gate is enabled and a trade source
// is wired. Returns nil otherwise (and logs once), so the replay loop can
// call ingestTradesUpTo unconditionally and the gate simply passes through.
func (b *Backtester) loadTrades(ctx context.Context, bars []types.Bar) []trades.Trade {
	if b.strategy.cfg.ImbalanceWindow <= 0 || b.strategy.tradeHistoryStore == nil {
		b.strategy.logger().Info("backtest imbalance gate passthrough",
			"imbalanceWindow", b.strategy.cfg.ImbalanceWindow,
			"storeNil", b.strategy.tradeHistoryStore == nil)
		return nil
	}
	if len(bars) == 0 {
		return nil
	}
	ch, errCh := b.strategy.tradeHistoryStore.StreamTrades(
		ctx, b.strategy.cfg.OrderBookID, bars[0].Time, bars[len(bars)-1].Time,
	)
	var trades []trades.Trade
readCh:
	for {
		select {
		case <-ctx.Done():
			return trades
		case t, ok := <-ch:
			if !ok {
				break readCh
			}
			trades = append(trades, t)
		}
	}
	// Drain ch fully before touching errCh: both channels are often ready
	// at once and a random select could abandon buffered trades.
	select {
	case <-ctx.Done():
		return trades
	case err, ok := <-errCh:
		if ok && err != nil {
			b.strategy.logger().Error("backtest trade stream error", "err", err)
		}
	}
	return trades
}

// ingestTradesUpTo folds every not-yet-ingested trade with Time <= ts into
// the imbalance window and returns the advanced cursor. Mirrors breakout's
// backtester interleave.
func (b *Backtester) ingestTradesUpTo(trades []trades.Trade, idx int, ts time.Time) int {
	for idx < len(trades) && !trades[idx].Time.After(ts) {
		b.strategy.applyTrade(streams.TradeEvent{
			Quantity: trades[idx].Quantity,
			Price:    trades[idx].Price,
			Side:     trades[idx].Side,
		})
		idx++
	}
	return idx
}

// adverseExtreme returns the bar's YTM extreme that is adverse to an open
// position: the high for a long (spread widening hurts), the low for a short
// (spread tightening hurts). ok=false when the extreme is unset or the
// direction is neutral.
func adverseExtreme(openSignal types.Signal, bar types.Bar) (decimal.Decimal, bool) {
	switch openSignal {
	case types.SignalBuy:
		return bar.HighYTM, !bar.HighYTM.IsZero()
	case types.SignalSell:
		return bar.LowYTM, !bar.LowYTM.IsZero()
	default:
		return decimal.Zero, false
	}
}

func streamTrades(
	ctx context.Context,
	w stats.BacktestTradeWriter,
	records []TradeRecord,
	closed []ClosedTrade,
) {
	for _, r := range records {
		rec := stats.TradeRecordInsert{
			BacktestID:   uuid.Nil,
			Time:         r.Time,
			BondID:       r.BondID,
			Signal:       r.Signal.String(),
			Price:        r.Price,
			Quantity:     r.Quantity,
			EntryBalance: r.EntryBalance,
			Spread:       r.Spread,
			PositionSize: r.PositionSize,
			ZScore:       r.ZScore,
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
			EntryBalance: c.EntryBalance,
			EntrySpread:  c.EntrySpread,
			ExitSpread:   c.ExitSpread,
			EntryZScore:  c.EntryZScore,
			ExitZScore:   c.ExitZScore,
			PositionSize: c.PositionSize,
			ExitReason:   c.ExitReason,
		}
		if err := w.WriteClosedTrade(ctx, rec); err != nil {
			slog.Error("write closed trade", "err", err)
		}
	}
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
	points := make([]stats.PnLPoint, len(trades))
	for i, t := range trades {
		points[i] = stats.PnLPoint{PnL: t.PnL, CloseTime: t.CloseTime}
	}
	summary, err := stats.Summarise(points, start, end)
	if err != nil {
		return BacktestResult{}, err
	}
	return BacktestResult{
		ClosedTrades: trades,
		TradeRecords: tradeRecords,
		TotalPnL:     summary.TotalPnL,
		WinCount:     summary.WinCount,
		LossCount:    summary.LossCount,
		MaxDrawdown:  summary.MaxDrawdown,
		SharpeRatio:  summary.SharpeRatio,
	}, nil
}
