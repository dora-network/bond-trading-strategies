package copytrading

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy/stats"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
)

const (
	initialBacktestBalance = 10000
	bondQuantityScale      = 1000
)

type Backtester struct {
	strategy *Strategy
	history  tradesHistoryStore
}

func NewBacktester(s *Strategy, store tradesHistoryStore) *Backtester {
	return &Backtester{strategy: s, history: store}
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

	cash := decimal.MustNew(initialBacktestBalance, 0)
	positions := make(map[string]*position)

	margin, _ := cash.Mul(b.strategy.cfg.PercentageOfAvailable)
	margin, _ = margin.Mul(b.strategy.cfg.Leverage)
	margin = margin.Round(0)
	if b.strategy.cfg.MinOrderSize > 0 {
		minSize := decimal.MustNew(int64(b.strategy.cfg.MinOrderSize), 0)
		if margin.Cmp(minSize) < 0 {
			margin = minSize
		}
	}
	if b.strategy.cfg.MaxOrderSize > 0 {
		maxSize := decimal.MustNew(int64(b.strategy.cfg.MaxOrderSize), 0)
		if margin.Cmp(maxSize) > 0 {
			margin = maxSize
		}
	}
	scale, _ := margin.Quo(decimal.MustNew(bondQuantityScale, 0))
	scale = scale.Round(0)

	for trade := range ch {
		select {
		case <-ctx.Done():
			return BacktestResult{}, errors.New("backtest cancelled")
		default:
		}

		if margin.IsZero() || margin.IsNeg() {
			continue
		}

		if trade.Price.IsZero() {
			continue
		}

		ourQty, _ := trade.Quantity.Mul(scale)
		ourQty = ourQty.Round(0)
		tradeID, _ := uuid.Parse(trade.TransactionID)

		var ourSignal types.Signal
		if trade.Side == "BUY" {
			ourSignal = types.SignalBuy
		} else {
			ourSignal = types.SignalSell
		}

		cash, tradeRecords, closedTrades = applyTrade(
			trade, tradeID, ourSignal, ourQty, trade.Price, margin, cash, positions,
			tradeRecords, closedTrades,
		)
	}

	return summarise(tradeRecords, closedTrades, start, end)
}

func applyTrade(
	trade Trade,
	tradeID uuid.UUID,
	ourSignal types.Signal,
	ourQty, price, margin decimal.Decimal,
	cash decimal.Decimal,
	positions map[string]*position,
	tradeRecords []TradeRecord,
	closedTrades []ClosedTrade,
) (decimal.Decimal, []TradeRecord, []ClosedTrade) {
	pos := positions[trade.Asset]

	if ourSignal == types.SignalBuy {
		return applyBuy(trade, tradeID, ourQty, price, margin, cash, pos, positions, tradeRecords, closedTrades)
	}
	return applySell(trade, tradeID, ourQty, price, margin, cash, pos, positions, tradeRecords, closedTrades)
}

func applyBuy(
	trade Trade,
	tradeID uuid.UUID,
	ourQty, price, margin decimal.Decimal,
	cash decimal.Decimal,
	pos *position,
	positions map[string]*position,
	tradeRecords []TradeRecord,
	closedTrades []ClosedTrade,
) (decimal.Decimal, []TradeRecord, []ClosedTrade) {
	if pos != nil && pos.qty.IsNeg() {
		return buyClosesShort(trade, tradeID, ourQty, price, margin, cash, pos, positions, tradeRecords, closedTrades)
	}
	return buyOpensOrAddsLong(trade, tradeID, ourQty, price, margin, cash, pos, positions, tradeRecords, closedTrades)
}

func buyClosesShort(
	trade Trade,
	tradeID uuid.UUID,
	ourQty, price, margin decimal.Decimal,
	cash decimal.Decimal,
	pos *position,
	positions map[string]*position,
	tradeRecords []TradeRecord,
	closedTrades []ClosedTrade,
) (decimal.Decimal, []TradeRecord, []ClosedTrade) {
	absQty := pos.qty.Abs()
	closeQty := minDecimal(absQty, ourQty)

	cash, pos.qty, closedTrades = closeShortPosition(trade, tradeID, pos, closeQty, price, cash, closedTrades)
	tradeRecords = emitTradeRecord(trade, tradeID, types.SignalBuy, closeQty, price, cash, pos.qty, tradeRecords)

	remaining, _ := ourQty.Sub(closeQty)
	if remaining.IsZero() || remaining.IsNeg() {
		positions[trade.Asset] = pos
		return cash, tradeRecords, closedTrades
	}

	cash, _ = cash.Sub(margin)
	pos = &position{
		qty:         remaining,
		avgEntry:    price,
		openTime:    trade.CreatedAt,
		openTradeID: tradeID,
	}
	positions[trade.Asset] = pos
	tradeRecords = emitTradeRecord(trade, tradeID, types.SignalBuy, remaining, price, cash, pos.qty, tradeRecords)
	return cash, tradeRecords, closedTrades
}

func buyOpensOrAddsLong(
	trade Trade,
	tradeID uuid.UUID,
	ourQty, price, margin decimal.Decimal,
	cash decimal.Decimal,
	pos *position,
	positions map[string]*position,
	tradeRecords []TradeRecord,
	closedTrades []ClosedTrade,
) (decimal.Decimal, []TradeRecord, []ClosedTrade) {
	cash, _ = cash.Sub(margin)

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
	ourQty, price, margin decimal.Decimal,
	cash decimal.Decimal,
	pos *position,
	positions map[string]*position,
	tradeRecords []TradeRecord,
	closedTrades []ClosedTrade,
) (decimal.Decimal, []TradeRecord, []ClosedTrade) {
	if pos != nil && pos.qty.IsPos() {
		return sellClosesLong(trade, tradeID, ourQty, price, margin, cash, pos, positions, tradeRecords, closedTrades)
	}
	return sellOpensOrAddsShort(trade, tradeID, ourQty, price, margin, cash, pos, positions, tradeRecords, closedTrades)
}

func sellClosesLong(
	trade Trade,
	tradeID uuid.UUID,
	ourQty, price, margin decimal.Decimal,
	cash decimal.Decimal,
	pos *position,
	positions map[string]*position,
	tradeRecords []TradeRecord,
	closedTrades []ClosedTrade,
) (decimal.Decimal, []TradeRecord, []ClosedTrade) {
	closeQty := minDecimal(pos.qty, ourQty)

	cash, pos.qty, closedTrades = closeLongPosition(trade, tradeID, pos, closeQty, price, cash, closedTrades)
	tradeRecords = emitTradeRecord(trade, tradeID, types.SignalSell, closeQty, price, cash, pos.qty, tradeRecords)

	remaining, _ := ourQty.Sub(closeQty)
	if remaining.IsZero() || remaining.IsNeg() {
		positions[trade.Asset] = pos
		return cash, tradeRecords, closedTrades
	}

	cash, _ = cash.Sub(margin)
	pos = &position{
		qty:         remaining.Neg(),
		avgEntry:    price,
		openTime:    trade.CreatedAt,
		openTradeID: tradeID,
	}
	positions[trade.Asset] = pos
	tradeRecords = emitTradeRecord(trade, tradeID, types.SignalSell, remaining, price, cash, pos.qty, tradeRecords)
	return cash, tradeRecords, closedTrades
}

func sellOpensOrAddsShort(
	trade Trade,
	tradeID uuid.UUID,
	ourQty, price, margin decimal.Decimal,
	cash decimal.Decimal,
	pos *position,
	positions map[string]*position,
	tradeRecords []TradeRecord,
	closedTrades []ClosedTrade,
) (decimal.Decimal, []TradeRecord, []ClosedTrade) {
	cash, _ = cash.Sub(margin)

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
	cash, _ = cash.Add(pnl)
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

func minDecimal(a, b decimal.Decimal) decimal.Decimal {
	if a.Cmp(b) < 0 {
		return a
	}
	return b
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
