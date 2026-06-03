package copytrading

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
)

const (
	initialBacktestBalance = 10000
	annualTradingDays      = 252
	bondQuantityScale      = 1000
	hoursPerDay            = 24
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
	ch, done := b.history.StreamTrades(ctx, followedTrader, start, end)

	result, simErr := b.simulate(ctx, ch)
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

func (b *Backtester) simulate(ctx context.Context, ch <-chan Trade) (BacktestResult, error) {
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

		ourQty, _ := trade.Quantity0.Mul(scale)
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

	return summarise(tradeRecords, closedTrades), nil
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
	pos := positions[trade.Asset0]

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
		positions[trade.Asset0] = pos
		return cash, tradeRecords, closedTrades
	}

	cash, _ = cash.Sub(margin)
	pos = &position{
		qty:         remaining,
		avgEntry:    price,
		openTime:    trade.CreatedAt,
		openTradeID: tradeID,
	}
	positions[trade.Asset0] = pos
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
	positions[trade.Asset0] = pos
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
		positions[trade.Asset0] = pos
		return cash, tradeRecords, closedTrades
	}

	cash, _ = cash.Sub(margin)
	pos = &position{
		qty:         remaining.Neg(),
		avgEntry:    price,
		openTime:    trade.CreatedAt,
		openTradeID: tradeID,
	}
	positions[trade.Asset0] = pos
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
	positions[trade.Asset0] = pos
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
		BondID:       trade.Asset0,
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
		BondID:       trade.Asset0,
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
		BondID:       trade.Asset0,
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

func summarise(tradeRecords []TradeRecord, closedTrades []ClosedTrade) BacktestResult {
	res := BacktestResult{
		TradeRecords: tradeRecords,
		ClosedTrades: closedTrades,
	}

	if len(closedTrades) == 0 {
		return res
	}

	equity := decimal.Zero
	peak := decimal.Zero
	maxDD := decimal.Zero

	dailyPnLMap := make(map[string]decimal.Decimal)

	for _, t := range closedTrades {
		res.TotalPnL, _ = res.TotalPnL.Add(t.PnL)

		dateStr := t.CloseTime.Format("2006-01-02")
		current, ok := dailyPnLMap[dateStr]
		if !ok {
			current = decimal.Zero
		}
		dailyPnLMap[dateStr], _ = current.Add(t.PnL)

		if t.PnL.IsPos() {
			res.WinCount++
		} else if t.PnL.IsNeg() {
			res.LossCount++
		}

		equity, _ = equity.Add(t.PnL)
		if equity.Cmp(peak) > 0 {
			peak = equity
		}
		dd, _ := peak.Sub(equity)
		if dd.Cmp(maxDD) > 0 {
			maxDD = dd
		}
	}

	res.MaxDrawdown = maxDD

	var start, end time.Time
	for _, t := range closedTrades {
		if start.IsZero() || t.CloseTime.Before(start) {
			start = t.CloseTime
		}
		if t.CloseTime.After(end) {
			end = t.CloseTime
		}
	}

	if !start.IsZero() && !end.IsZero() {
		var dailyPnLs []decimal.Decimal
		startDay := start.Truncate(hoursPerDay * time.Hour)
		endDay := end.Truncate(hoursPerDay * time.Hour)
		for d := startDay; !d.After(endDay); d = d.Add(hoursPerDay * time.Hour) {
			dateStr := d.Format("2006-01-02")
			pnl, ok := dailyPnLMap[dateStr]
			if !ok {
				pnl = decimal.Zero
			}
			dailyPnLs = append(dailyPnLs, pnl)
		}
		res.SharpeRatio = sharpe(dailyPnLs)
	}

	return res
}

func sharpe(pnls []decimal.Decimal) decimal.Decimal {
	if len(pnls) < 2 { //nolint:mnd
		return decimal.Zero
	}

	sum := decimal.Zero
	for _, p := range pnls {
		sum, _ = sum.Add(p)
	}
	n := decimal.MustNew(int64(len(pnls)), 0)
	mean, err := sum.Quo(n)
	if err != nil {
		return decimal.Zero
	}

	variance := decimal.Zero
	for _, p := range pnls {
		d, err := p.Sub(mean)
		if err != nil {
			return decimal.Zero
		}
		sq, _ := d.Mul(d)
		variance, _ = variance.Add(sq)
	}
	nMinus1 := decimal.MustNew(int64(len(pnls)-1), 0)
	variance, err = variance.Quo(nMinus1)
	if err != nil {
		return decimal.Zero
	}

	sd, err := variance.Sqrt()
	if err != nil {
		return decimal.Zero
	}
	if sd.IsZero() {
		return decimal.Zero
	}

	tradingDays := decimal.MustNew(annualTradingDays, 0)
	sqrtDays, err := tradingDays.Sqrt()
	if err != nil {
		return decimal.Zero
	}
	ratio, err := mean.Quo(sd)
	if err != nil {
		return decimal.Zero
	}
	result, _ := ratio.Mul(sqrtDays)
	return result
}
