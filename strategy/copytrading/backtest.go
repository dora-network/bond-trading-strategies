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
	// Use initial balance (not current cash) for order sizing to prevent
	// position sizes from compounding as closes return proceeds. The
	// live strategy doesn't have this issue because it doesn't auto-open
	// a new position after closing — each trade is an independent event.
	initialBalance := b.startingBalance()
	positions := make(map[string]*position)

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
		// For opens/extends: use calculateOrderSize based on initial balance.
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
			// Open or extend: compute order size based on initial balance
			ourQty = calculateOrderSize(
				initialBalance,
				b.strategy.cfg.PercentageOfAvailable,
				b.strategy.cfg.Leverage,
				b.strategy.cfg.MinOrderSize,
				b.strategy.cfg.MaxOrderSize,
			)
			// Convert cash-based order size to quantity
			if !trade.Price.IsZero() {
				ourQty, _ = ourQty.Quo(trade.Price)
			}
			// Check if we have enough cash to cover the order
			orderCost, _ := ourQty.Mul(trade.Price)
			if orderCost.Cmp(cash) > 0 {
				// Not enough cash, skip this trade
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
			initialBalance,
			b.strategy.cfg.PercentageOfAvailable,
			b.strategy.cfg.Leverage,
			b.strategy.cfg.MinOrderSize,
			b.strategy.cfg.MaxOrderSize,
		)

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
	initialBalance, percentage, leverage decimal.Decimal,
	minOrderSize, maxOrderSize int,
) (decimal.Decimal, []TradeRecord, []ClosedTrade) {
	pos := positions[trade.Asset]

	if ourSignal == types.SignalBuy {
		return applyBuy(
			trade, tradeID, ourQty, price, cash, pos, positions, tradeRecords, closedTrades,
			initialBalance, percentage, leverage, minOrderSize, maxOrderSize,
		)
	}
	return applySell(
		trade, tradeID, ourQty, price, cash, pos, positions, tradeRecords, closedTrades,
		initialBalance, percentage, leverage, minOrderSize, maxOrderSize,
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
	initialBalance, percentage, leverage decimal.Decimal,
	minOrderSize, maxOrderSize int,
) (decimal.Decimal, []TradeRecord, []ClosedTrade) {
	if pos != nil && pos.qty.IsNeg() {
		return buyClosesShort(
			trade, tradeID, price, cash, pos, positions, tradeRecords, closedTrades,
			initialBalance, percentage, leverage, minOrderSize, maxOrderSize,
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
	initialBalance, percentage, leverage decimal.Decimal,
	minOrderSize, maxOrderSize int,
) (decimal.Decimal, []TradeRecord, []ClosedTrade) {
	absQty := pos.qty.Abs()

	// Close the full short position, then open a new long — mirroring
	// the live strategy which always closes the entire position on a
	// direction reversal.
	cash, pos.qty, closedTrades = closeShortPosition(trade, tradeID, pos, absQty, price, cash, closedTrades)
	tradeRecords = emitTradeRecord(trade, tradeID, types.SignalBuy, absQty, price, cash, pos.qty, tradeRecords)

	// Compute new order size based on initial balance (not current cash)
	// to prevent position sizes from compounding.
	newOrderSize := calculateOrderSize(
		initialBalance,
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
		positions[trade.Asset] = nil
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
		positions[trade.Asset] = nil
		return cash, tradeRecords, closedTrades
	}

	// Open new long position: pay cash to buy bonds
	cash, _ = cash.Sub(cost)
	pos = &position{
		qty:         newQty,
		avgEntry:    price,
		openTime:    trade.CreatedAt,
		openTradeID: tradeID,
	}
	positions[trade.Asset] = pos
	tradeRecords = emitTradeRecord(trade, tradeID, types.SignalBuy, newQty, price, cash, pos.qty, tradeRecords)
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
	tradeRecords = emitTradeRecord(trade, tradeID, types.SignalBuy, ourQty, price, cash, pos.qty, tradeRecords)
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
	initialBalance, percentage, leverage decimal.Decimal,
	minOrderSize, maxOrderSize int,
) (decimal.Decimal, []TradeRecord, []ClosedTrade) {
	if pos != nil && pos.qty.IsPos() {
		return sellClosesLong(
			trade, tradeID, price, cash, pos, positions, tradeRecords, closedTrades,
			initialBalance, percentage, leverage, minOrderSize, maxOrderSize,
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
	initialBalance, percentage, leverage decimal.Decimal,
	minOrderSize, maxOrderSize int,
) (decimal.Decimal, []TradeRecord, []ClosedTrade) {
	// Close the full long position, then open a new short — mirroring
	// the live strategy which always closes the entire position on a
	// direction reversal.
	origQty := pos.qty
	cash, pos.qty, closedTrades = closeLongPosition(trade, tradeID, pos, origQty, price, cash, closedTrades)
	tradeRecords = emitTradeRecord(trade, tradeID, types.SignalSell, origQty, price, cash, pos.qty, tradeRecords)

	// Compute new order size based on initial balance (not current cash)
	// to prevent position sizes from compounding.
	newOrderSize := calculateOrderSize(
		initialBalance,
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
		positions[trade.Asset] = nil
		return cash, tradeRecords, closedTrades
	}

	// Open new short position: receive cash from selling borrowed bonds
	proceeds, _ := newQty.Mul(price)
	cash, _ = cash.Add(proceeds)
	pos = &position{
		qty:         newQty.Neg(),
		avgEntry:    price,
		openTime:    trade.CreatedAt,
		openTradeID: tradeID,
	}
	positions[trade.Asset] = pos
	tradeRecords = emitTradeRecord(trade, tradeID, types.SignalSell, newQty, price, cash, pos.qty, tradeRecords)
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
	// Margin is collateral, not a cash outflow.
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
	tradeRecords = emitTradeRecord(trade, tradeID, types.SignalSell, ourQty, price, cash, pos.qty, tradeRecords)
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
		OpenTime:     pos.openTime,
		CloseTime:    trade.CreatedAt,
		BondID:       trade.Asset,
		OpenSignal:   types.SignalBuy,
		CloseSignal:  types.SignalSell,
		Quantity:     closeQty,
		EntryPrice:   pos.avgEntry,
		ExitPrice:    price,
		PnL:          pnl,
		EntryBalance: cash,
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
	pnl, _ := pos.avgEntry.Sub(price)
	pnl, _ = pnl.Mul(closeQty)
	closedTrades = append(closedTrades, ClosedTrade{
		OpenTime:     pos.openTime,
		CloseTime:    trade.CreatedAt,
		BondID:       trade.Asset,
		OpenSignal:   types.SignalSell,
		CloseSignal:  types.SignalBuy,
		Quantity:     closeQty,
		EntryPrice:   pos.avgEntry,
		ExitPrice:    price,
		PnL:          pnl,
		EntryBalance: cash,
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
