package copytrading

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy/stats"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
)

const (
	// initialBacktestBalance is the default starting cash for a backtest
	// when the caller doesn't specify initial_balance. Set to $10,000 USD
	// as a representative small retail account size. A configurable
	// value overrides this at runtime; the constant exists so existing
	// tests and zero-value Config literals continue to behave
	// sensibly.
	initialBacktestBalance = 10000
)

type Backtester struct {
	strategy *Strategy
	history  tradesHistoryStore
	writer   stats.BacktestTradeWriter
}

func NewBacktester(s *Strategy, store tradesHistoryStore, writer stats.BacktestTradeWriter) *Backtester {
	return &Backtester{strategy: s, history: store, writer: writer}
}

// startingBalance returns the configured InitialBalance if the caller set
// one, otherwise the package-level default. A zero value in the config
// is treated as "use the default" rather than as a literal balance of 0.
func (b *Backtester) startingBalance() decimal.Decimal {
	if bal := b.strategy.cfg.InitialBalance; !bal.IsZero() {
		return bal
	}
	return decimal.MustNew(initialBacktestBalance, 0)
}

// availableForNewPos returns the notional that can still be deployed
// into new or extended positions, given the realised equity
// (runningBalance), the leverage, and the notional already committed
// to open positions. The result is in the same units as the asset
// price (i.e., USD for a bond priced in USD), so it can be divided by
// trade.Price to obtain a quantity.
//
// The model is:
//
//	availableForNewPos = runningBalance × Leverage − totalDeployedNotional
//
// where totalDeployedNotional = Σ |pos.qty| × pos.avgEntry across
// all open positions (longs pay this in cash; shorts are reserved
// against it to cover the buyback). Without this cap, short sale
// proceeds inflate the available balance and the position grows past
// what the realised equity can fund — extends compound on top of
// each other instead of being bounded by the deployed capital.
func (b *Backtester) availableForNewPos(
	runningBalance decimal.Decimal,
	positions map[string]*position,
	leverage decimal.Decimal,
) decimal.Decimal {
	maxNotional, _ := runningBalance.Mul(leverage)
	deployed := decimal.Zero
	for _, p := range positions {
		if p == nil {
			continue
		}
		// Use pos.avgEntry (not the current price) so the cap is
		// stable across the lifetime of a position — extending at
		// a worse price still consumes the same notional budget.
		notional, _ := p.qty.Abs().Mul(p.avgEntry)
		deployed, _ = deployed.Add(notional)
	}
	avail, _ := maxNotional.Sub(deployed)
	if avail.IsNeg() {
		return decimal.Zero
	}
	return avail
}

func (b *Backtester) Run(ctx context.Context, start, end time.Time) (BacktestResult, error) {
	followedTrader := b.strategy.cfg.FollowedTrader.String()

	min, max, count, err := b.history.TradeBounds(ctx, followedTrader)
	if err != nil {
		return BacktestResult{}, fmt.Errorf("trades history bounds: %w", err)
	}
	if count == 0 {
		return BacktestResult{}, fmt.Errorf(
			"no trades in trades_history for user %s; sync required", followedTrader,
		)
	}
	if start.Before(min) || end.After(max) {
		return BacktestResult{}, fmt.Errorf(
			"window [%s,%s] outside available data [%s,%s] for user %s",
			start.Format(time.RFC3339), end.Format(time.RFC3339),
			min.Format(time.RFC3339), max.Format(time.RFC3339),
			followedTrader,
		)
	}

	ch, done := b.history.StreamTrades(ctx, followedTrader, start, end)
	result, simErr := b.simulate(ctx, ch, start, end)
	prodErr := <-done

	if prodErr != nil {
		return BacktestResult{}, fmt.Errorf("stream trades: %w", prodErr)
	}
	if simErr != nil {
		return BacktestResult{}, simErr
	}
	return result, nil
}

type position struct {
	qty         decimal.Decimal
	avgEntry    decimal.Decimal
	openTime    time.Time
	openTradeID uuid.UUID
}

func (b *Backtester) simulate(ctx context.Context, ch <-chan Trade, start, end time.Time) (BacktestResult, error) {
	var (
		tradeRecords []TradeRecord
		closedTrades []ClosedTrade
	)

	cash := b.startingBalance()
	positions := make(map[string]*position)
	// runningBalance is the compounding equity that drives each
	// ClosedTrade's EntryBalance. It starts at the initial balance
	// and advances by the PnL of every previously closed trade, so
	// the entry balance reported on the next closed trade reflects
	// the realised equity at the time the position was opened — not
	// the cash on hand, which is depleted by the cost of the opening
	// trade itself. (e.g. after a 1000-balance backtest opens a 500
	// long and closes it for -0.03 PnL, the next closed trade's
	// EntryBalance is 999.97, not 500.)
	runningBalance := b.startingBalance()

	for trade := range ch {
		select {
		case <-ctx.Done():
			return BacktestResult{}, errors.New("backtest cancelled")
		default:
		}

		if trade.Price.IsZero() {
			continue
		}

		tradeID, _ := uuid.Parse(trade.TransactionID)

		var ourSignal types.Signal
		if trade.Side == "BUY" {
			ourSignal = types.SignalBuy
		} else {
			ourSignal = types.SignalSell
		}

		// Determine order size based on current position and signal.
		// For closes: use full position size (like live strategy).
		// For opens/extends: use calculateOrderSize based on current cash.
		// The current cash naturally limits position sizes because it
		// reflects the capital already deployed in open positions.
		pos := positions[trade.Asset]
		var ourQty decimal.Decimal
		isClose := false

		if ourSignal == types.SignalBuy && pos != nil && pos.qty.IsNeg() {
			// Buy while short = close short
			isClose = true
			ourQty = pos.qty.Abs()
		} else if ourSignal == types.SignalSell && pos != nil && pos.qty.IsPos() {
			// Sell while long = close long
			isClose = true
			ourQty = pos.qty
		}

		if !isClose {
			// Open or extend: size the new notional against the
			// remaining headroom under the runningBalance × leverage
			// ceiling. Without this cap, the position accumulates
			// extends indefinitely because each short's proceeds are
			// implicitly recycled into the next open — the live DORA
			// margin model instead ties deployable capital to the
			// realised equity, not the cash in hand.
			headroom := b.availableForNewPos(runningBalance, positions, b.strategy.cfg.Leverage)
			if headroom.IsZero() {
				continue
			}
			ourQty = calculateOrderSize(
				headroom,
				b.strategy.cfg.PercentageOfAvailable,
				b.strategy.cfg.Leverage,
				b.strategy.cfg.MinOrderSize,
				b.strategy.cfg.MaxOrderSize,
			)
			// Convert notional-based order size to quantity
			if !trade.Price.IsZero() {
				ourQty, _ = ourQty.Quo(trade.Price)
			}
			// Cap the order at the remaining headroom (the percentage
			// of headroom × leverage is bounded by headroom itself for
			// leverage ≤ 1, but with higher leverage a single extend
			// can exceed it).
			orderCost, _ := ourQty.Mul(trade.Price)
			if orderCost.Cmp(headroom) > 0 {
				// Not enough headroom; skip this trade
				continue
			}
		}

		if ourQty.IsZero() || ourQty.IsNeg() {
			continue
		}

		prevTradeCount := len(tradeRecords)
		prevClosedCount := len(closedTrades)

		cash, tradeRecords, closedTrades = applyTrade(
			trade, tradeID, ourSignal, ourQty, trade.Price, cash, positions,
			tradeRecords, closedTrades,
			b.strategy.cfg.PercentageOfAvailable,
			b.strategy.cfg.Leverage,
			b.strategy.cfg.MinOrderSize,
			b.strategy.cfg.MaxOrderSize,
		)

		// Replace each newly added closed trade's EntryBalance with the
		// running compounding balance, then advance it by the trade's
		// PnL so the next closed trade inherits the post-close equity.
		for i := prevClosedCount; i < len(closedTrades); i++ {
			closedTrades[i].EntryBalance = runningBalance
			runningBalance, _ = runningBalance.Add(closedTrades[i].PnL)
		}

		// Write new trades incrementally if writer is configured
		if b.writer != nil {
			b.writeNewTrades(ctx, tradeRecords, closedTrades, prevTradeCount, prevClosedCount)
		}
	}

	// Flush drains any rows still buffered by a batching writer.
	// The summarise below returns the summary synchronously; without
	// Flush the /trades endpoint could read an empty buffer.
	if b.writer != nil {
		if err := b.writer.Flush(ctx); err != nil {
			slog.Error("flush backtest writer", "err", err)
		}
	}

	return summarise(tradeRecords, closedTrades, start, end)
}

func (b *Backtester) writeNewTrades(
	ctx context.Context,
	tradeRecords []TradeRecord,
	closedTrades []ClosedTrade,
	prevTradeCount, prevClosedCount int,
) {
	for i := prevTradeCount; i < len(tradeRecords); i++ {
		r := tradeRecords[i]
		rec := stats.TradeRecordInsert{
			BacktestID:   uuid.Nil,
			Time:         r.Time,
			BondID:       r.BondID,
			Signal:       r.Signal.String(),
			Price:        r.Price,
			Quantity:     r.Quantity,
			EntryBalance: decimal.Zero,
			OrderSize:    r.OrderSize,
			Cash:         r.Cash,
			OpenPosition: r.OpenPosition,
			TradeID:      r.TradeID,
		}
		if err := b.writer.WriteTradeRecord(ctx, rec); err != nil {
			slog.Error("write trade record", "err", err)
		}
	}
	for i := prevClosedCount; i < len(closedTrades); i++ {
		c := closedTrades[i]
		rec := stats.ClosedTradeInsert{
			BacktestID:   uuid.Nil,
			OpenTime:     c.OpenTime,
			CloseTime:    c.CloseTime,
			BondID:       c.BondID,
			OpenSignal:   c.OpenSignal.String(),
			CloseSignal:  c.CloseSignal.String(),
			Quantity:     c.Quantity,
			EntryPrice:   c.EntryPrice,
			ExitPrice:    c.ExitPrice,
			PnL:          c.PnL,
			EntryBalance: c.EntryBalance,
			OpenTradeID:  c.OpenTradeID,
			CloseTradeID: c.CloseTradeID,
		}
		if err := b.writer.WriteClosedTrade(ctx, rec); err != nil {
			slog.Error("write closed trade", "err", err)
		}
	}
}

func applyTrade(
	trade Trade,
	tradeID uuid.UUID,
	ourSignal types.Signal,
	ourQty, price decimal.Decimal,
	cash decimal.Decimal,
	positions map[string]*position,
	tradeRecords []TradeRecord,
	closedTrades []ClosedTrade,
	percentage, leverage decimal.Decimal,
	minOrderSize, maxOrderSize int,
) (decimal.Decimal, []TradeRecord, []ClosedTrade) {
	pos := positions[trade.Asset]

	if ourSignal == types.SignalBuy {
		return applyBuy(
			trade, tradeID, ourQty, price, cash, pos, positions, tradeRecords, closedTrades,
			percentage, leverage, minOrderSize, maxOrderSize,
		)
	}
	return applySell(
		trade, tradeID, ourQty, price, cash, pos, positions, tradeRecords, closedTrades,
		percentage, leverage, minOrderSize, maxOrderSize,
	)
}

func applyBuy(
	trade Trade,
	tradeID uuid.UUID,
	ourQty, price decimal.Decimal,
	cash decimal.Decimal,
	pos *position,
	positions map[string]*position,
	tradeRecords []TradeRecord,
	closedTrades []ClosedTrade,
	percentage, leverage decimal.Decimal,
	minOrderSize, maxOrderSize int,
) (decimal.Decimal, []TradeRecord, []ClosedTrade) {
	if pos != nil && pos.qty.IsNeg() {
		return buyClosesShort(
			trade, tradeID, price, cash, pos, positions, tradeRecords, closedTrades,
			percentage, leverage, minOrderSize, maxOrderSize,
		)
	}
	return buyOpensOrAddsLong(trade, tradeID, ourQty, price, cash, pos, positions, tradeRecords, closedTrades)
}

func buyClosesShort(
	trade Trade,
	tradeID uuid.UUID,
	price decimal.Decimal,
	cash decimal.Decimal,
	pos *position,
	positions map[string]*position,
	tradeRecords []TradeRecord,
	closedTrades []ClosedTrade,
	percentage, leverage decimal.Decimal,
	minOrderSize, maxOrderSize int,
) (decimal.Decimal, []TradeRecord, []ClosedTrade) {
	absQty := pos.qty.Abs()

	// Pre-close cash: the balance before the buyback is deducted. This
	// is the value the trade record reports so the recorded Cash field
	// reflects the portfolio state at the moment the order was sized,
	// not after the cash flow has settled.
	preCloseCash := cash

	// Close the full short position, then open a new long — mirroring
	// the live strategy which always closes the entire position on a
	// direction reversal.
	cash, pos.qty, closedTrades = closeShortPosition(trade, tradeID, pos, absQty, price, cash, closedTrades)
	tradeRecords = emitTradeRecord(trade, tradeID, types.SignalBuy, absQty, price, preCloseCash, pos.qty, tradeRecords)

	// Compute new order size based on cash AFTER the close (includes proceeds from closing).
	// The cash now reflects the proceeds from buying back the short.
	newOrderSize := calculateOrderSize(
		cash,
		percentage,
		leverage,
		minOrderSize,
		maxOrderSize,
	)
	newQty := decimal.Zero
	if !price.IsZero() {
		newQty, _ = newOrderSize.Quo(price)
	}

	if newQty.IsZero() || newQty.IsNeg() {
		// No cash to open new position
		delete(positions, trade.Asset)
		return cash, tradeRecords, closedTrades
	}

	// Check if we have enough cash for the new position
	cost, _ := newQty.Mul(price)
	if cost.Cmp(cash) > 0 {
		// Not enough cash, reduce quantity to what we can afford
		if price.IsPos() {
			newQty, _ = cash.Quo(price)
			cost, _ = newQty.Mul(price)
		}
	}

	if newQty.IsZero() || newQty.IsNeg() {
		delete(positions, trade.Asset)
		return cash, tradeRecords, closedTrades
	}

	// Open new long position: pay cash to buy bonds. Capture the
	// pre-open cash so the trade record reflects the balance at the
	// time the new order was sized, not after the cost is deducted.
	preOpenCash := cash
	cash, _ = cash.Sub(cost)
	pos = &position{
		qty:         newQty,
		avgEntry:    price,
		openTime:    trade.CreatedAt,
		openTradeID: tradeID,
	}
	positions[trade.Asset] = pos
	tradeRecords = emitTradeRecord(trade, tradeID, types.SignalBuy, newQty, price, preOpenCash, pos.qty, tradeRecords)
	return cash, tradeRecords, closedTrades
}

func buyOpensOrAddsLong(
	trade Trade,
	tradeID uuid.UUID,
	ourQty, price decimal.Decimal,
	cash decimal.Decimal,
	pos *position,
	positions map[string]*position,
	tradeRecords []TradeRecord,
	closedTrades []ClosedTrade,
) (decimal.Decimal, []TradeRecord, []ClosedTrade) {
	// When opening/extending a long, we pay cash to buy bonds.
	// Capture pre-trade cash so the trade record reflects the balance
	// at the time the order was sized, not after the cost is deducted.
	preTradeCash := cash
	cost, _ := ourQty.Mul(price)
	cash, _ = cash.Sub(cost)

	if pos == nil || pos.qty.IsZero() {
		pos = &position{
			qty:         ourQty,
			avgEntry:    price,
			openTime:    trade.CreatedAt,
			openTradeID: tradeID,
		}
	} else {
		oldCost, _ := pos.qty.Mul(pos.avgEntry)
		newCost, _ := ourQty.Mul(price)
		totalCost, _ := oldCost.Add(newCost)
		totalQty, _ := pos.qty.Add(ourQty)
		avg, _ := totalCost.Quo(totalQty)
		pos.qty = totalQty
		pos.avgEntry = avg
	}
	positions[trade.Asset] = pos
	tradeRecords = emitTradeRecord(trade, tradeID, types.SignalBuy, ourQty, price, preTradeCash, pos.qty, tradeRecords)
	return cash, tradeRecords, closedTrades
}

func applySell(
	trade Trade,
	tradeID uuid.UUID,
	ourQty, price decimal.Decimal,
	cash decimal.Decimal,
	pos *position,
	positions map[string]*position,
	tradeRecords []TradeRecord,
	closedTrades []ClosedTrade,
	percentage, leverage decimal.Decimal,
	minOrderSize, maxOrderSize int,
) (decimal.Decimal, []TradeRecord, []ClosedTrade) {
	if pos != nil && pos.qty.IsPos() {
		return sellClosesLong(
			trade, tradeID, price, cash, pos, positions, tradeRecords, closedTrades,
			percentage, leverage, minOrderSize, maxOrderSize,
		)
	}
	return sellOpensOrAddsShort(trade, tradeID, ourQty, price, cash, pos, positions, tradeRecords, closedTrades)
}

func sellClosesLong(
	trade Trade,
	tradeID uuid.UUID,
	price decimal.Decimal,
	cash decimal.Decimal,
	pos *position,
	positions map[string]*position,
	tradeRecords []TradeRecord,
	closedTrades []ClosedTrade,
	percentage, leverage decimal.Decimal,
	minOrderSize, maxOrderSize int,
) (decimal.Decimal, []TradeRecord, []ClosedTrade) {
	// Close the full long position, then open a new short — mirroring
	// the live strategy which always closes the entire position on a
	// direction reversal. Pre-close cash is captured so the close
	// record reflects the balance at the time the close order was
	// sized, not after the proceeds are added.
	preCloseCash := cash
	origQty := pos.qty
	cash, pos.qty, closedTrades = closeLongPosition(trade, tradeID, pos, origQty, price, cash, closedTrades)
	tradeRecords = emitTradeRecord(trade, tradeID, types.SignalSell, origQty, price, preCloseCash, pos.qty, tradeRecords)

	// Compute new order size based on cash AFTER the close (includes proceeds from closing).
	// The cash now reflects the proceeds from selling the long position.
	newOrderSize := calculateOrderSize(
		cash,
		percentage,
		leverage,
		minOrderSize,
		maxOrderSize,
	)
	newQty := decimal.Zero
	if !price.IsZero() {
		newQty, _ = newOrderSize.Quo(price)
	}

	if newQty.IsZero() || newQty.IsNeg() {
		// No cash to open new position
		delete(positions, trade.Asset)
		return cash, tradeRecords, closedTrades
	}

	// Open new short position: receive cash from selling borrowed bonds.
	// Capture pre-open cash so the open-short record reflects the
	// balance at the time the new order was sized, not after the
	// proceeds are added.
	preOpenCash := cash
	proceeds, _ := newQty.Mul(price)
	cash, _ = cash.Add(proceeds)
	pos = &position{
		qty:         newQty.Neg(),
		avgEntry:    price,
		openTime:    trade.CreatedAt,
		openTradeID: tradeID,
	}
	positions[trade.Asset] = pos
	tradeRecords = emitTradeRecord(trade, tradeID, types.SignalSell, newQty, price, preOpenCash, pos.qty, tradeRecords)
	return cash, tradeRecords, closedTrades
}

func sellOpensOrAddsShort(
	trade Trade,
	tradeID uuid.UUID,
	ourQty, price decimal.Decimal,
	cash decimal.Decimal,
	pos *position,
	positions map[string]*position,
	tradeRecords []TradeRecord,
	closedTrades []ClosedTrade,
) (decimal.Decimal, []TradeRecord, []ClosedTrade) {
	// When opening/extending a short, we receive cash from selling borrowed bonds.
	// Margin is collateral, not a cash outflow. Capture pre-trade cash
	// so the trade record reflects the balance at the time the order
	// was sized, not after the proceeds are added.
	preTradeCash := cash
	proceeds, _ := ourQty.Mul(price)
	cash, _ = cash.Add(proceeds)

	if pos == nil || pos.qty.IsZero() {
		pos = &position{
			qty:         ourQty.Neg(),
			avgEntry:    price,
			openTime:    trade.CreatedAt,
			openTradeID: tradeID,
		}
	} else {
		absQty := pos.qty.Abs()
		oldCost, _ := absQty.Mul(pos.avgEntry)
		newCost, _ := ourQty.Mul(price)
		totalCost, _ := oldCost.Add(newCost)
		totalAbsQty, _ := absQty.Add(ourQty)
		avg, _ := totalCost.Quo(totalAbsQty)
		pos.qty = totalAbsQty.Neg()
		pos.avgEntry = avg
	}
	positions[trade.Asset] = pos
	tradeRecords = emitTradeRecord(trade, tradeID, types.SignalSell, ourQty, price, preTradeCash, pos.qty, tradeRecords)
	return cash, tradeRecords, closedTrades
}

func closeLongPosition(
	trade Trade,
	tradeID uuid.UUID,
	pos *position,
	closeQty, price decimal.Decimal,
	cash decimal.Decimal,
	closedTrades []ClosedTrade,
) (decimal.Decimal, decimal.Decimal, []ClosedTrade) {
	pnl, _ := price.Sub(pos.avgEntry)
	pnl, _ = pnl.Mul(closeQty)
	closedTrades = append(closedTrades, ClosedTrade{
		OpenTime:    pos.openTime,
		CloseTime:   trade.CreatedAt,
		BondID:      trade.Asset,
		OpenSignal:  types.SignalBuy,
		CloseSignal: types.SignalSell,
		Quantity:    closeQty,
		EntryPrice:  pos.avgEntry,
		ExitPrice:   price,
		PnL:         pnl,
		// EntryBalance is set by simulate's running-balance pass
		// (compounding equity across closed trades).
		EntryBalance: decimal.Zero,
		OpenTradeID:  pos.openTradeID,
		CloseTradeID: tradeID,
	})
	proceeds, _ := closeQty.Mul(price)
	cash, _ = cash.Add(proceeds)
	pos.qty, _ = pos.qty.Sub(closeQty)
	return cash, pos.qty, closedTrades
}

func closeShortPosition(
	trade Trade,
	tradeID uuid.UUID,
	pos *position,
	closeQty, price decimal.Decimal,
	cash decimal.Decimal,
	closedTrades []ClosedTrade,
) (decimal.Decimal, decimal.Decimal, []ClosedTrade) {
	// Short PnL: (exitPrice - entryPrice) * qty. A short loses money
	// when exit > entry (we have to buy back at a higher price), so
	// the sign naturally reflects profit vs loss — same shape as
	// closeLongPosition. Mirrors the convention used for long closes.
	pnl, _ := price.Sub(pos.avgEntry)
	pnl, _ = pnl.Mul(closeQty)
	closedTrades = append(closedTrades, ClosedTrade{
		OpenTime:    pos.openTime,
		CloseTime:   trade.CreatedAt,
		BondID:      trade.Asset,
		OpenSignal:  types.SignalSell,
		CloseSignal: types.SignalBuy,
		Quantity:    closeQty,
		EntryPrice:  pos.avgEntry,
		ExitPrice:   price,
		PnL:         pnl,
		// EntryBalance is set by simulate's running-balance pass
		// (compounding equity across closed trades).
		EntryBalance: decimal.Zero,
		OpenTradeID:  pos.openTradeID,
		CloseTradeID: tradeID,
	})
	// When closing a short, we buy back the bonds, so we pay cash.
	buybackCost, _ := closeQty.Mul(price)
	cash, _ = cash.Sub(buybackCost)
	pos.qty, _ = pos.qty.Add(closeQty)
	return cash, pos.qty, closedTrades
}

func emitTradeRecord(
	trade Trade,
	tradeID uuid.UUID,
	signal types.Signal,
	qty, price decimal.Decimal,
	cash decimal.Decimal,
	openPos decimal.Decimal,
	tradeRecords []TradeRecord,
) []TradeRecord {
	orderSize, _ := qty.Mul(price)
	return append(tradeRecords, TradeRecord{
		Time:         trade.CreatedAt,
		BondID:       trade.Asset,
		Signal:       signal,
		Price:        price,
		Quantity:     qty,
		OrderSize:    orderSize,
		Cash:         cash,
		OpenPosition: openPos,
		TradeID:      tradeID,
	})
}

func summarise(tradeRecords []TradeRecord, closedTrades []ClosedTrade, start, end time.Time) (BacktestResult, error) {
	points := make([]stats.PnLPoint, len(closedTrades))
	for i, t := range closedTrades {
		points[i] = stats.PnLPoint{PnL: t.PnL, CloseTime: t.CloseTime}
	}
	summary, err := stats.Summarise(points, start, end)
	if err != nil {
		return BacktestResult{}, err
	}
	return BacktestResult{
		TradeRecords: tradeRecords,
		ClosedTrades: closedTrades,
		TotalPnL:     summary.TotalPnL,
		WinCount:     summary.WinCount,
		LossCount:    summary.LossCount,
		MaxDrawdown:  summary.MaxDrawdown,
		SharpeRatio:  summary.SharpeRatio,
	}, nil
}
