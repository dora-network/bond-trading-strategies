package copytrading

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/dora-client-go/doraclient"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
)

const (
	initialBacktestBalance = 10000
	annualTradingDays      = 252
	bondQuantityScale      = 1000
)

type tradesClient interface {
	ListOrderBooks(ctx context.Context) ([]string, error)
	GetTrades(ctx context.Context, userID string, orderBookIDs []string, start, end time.Time) ([]doraclient.Trade, error)
}

type doraTradesClient struct {
	client *doraclient.APIClient
	apiKey string
}

func newDoraTradesClient(apiKey string) *doraTradesClient {
	cfg := doraclient.NewConfiguration()
	if baseURL := os.Getenv("DORA_BASE_URL"); baseURL != "" {
		cfg.Servers = doraclient.ServerConfigurations{{
			URL:         baseURL,
			Description: "Configured DORA API server",
		}}
	}
	return &doraTradesClient{
		client: doraclient.NewAPIClient(cfg),
		apiKey: apiKey,
	}
}

func (c *doraTradesClient) ListOrderBooks(ctx context.Context) ([]string, error) {
	if c == nil || c.client == nil {
		return nil, errors.New("DORA client is not configured")
	}
	if c.apiKey == "" {
		return nil, errors.New("API_KEY is not configured")
	}
	authCtx := context.WithValue(ctx, doraclient.ContextAPIKeys, map[string]doraclient.APIKey{
		"apiKeyAuthHeader": {
			Key:    c.apiKey,
			Prefix: apiKeyPrefix,
		},
	})

	resp, _, err := c.client.DefaultAPI.ListOrderBooks(authCtx).Execute()
	if err != nil {
		return nil, fmt.Errorf("list order books: %w", err)
	}
	if resp == nil {
		return nil, nil
	}
	ids := make([]string, 0, len(resp.Data))
	for _, ob := range resp.Data {
		ids = append(ids, ob.OrderBookId)
	}
	return ids, nil
}

func (c *doraTradesClient) GetTrades(
	ctx context.Context,
	userID string,
	orderBookIDs []string,
	start, end time.Time,
) ([]doraclient.Trade, error) {
	if c == nil || c.client == nil {
		return nil, errors.New("DORA client is not configured")
	}
	if c.apiKey == "" {
		return nil, errors.New("API_KEY is not configured")
	}
	authCtx := context.WithValue(ctx, doraclient.ContextAPIKeys, map[string]doraclient.APIKey{
		"apiKeyAuthHeader": {
			Key:    c.apiKey,
			Prefix: apiKeyPrefix,
		},
	})

	var allTrades []doraclient.Trade
	limit := int32(1000) //nolint:mnd
	page := int32(1)

	for {
		req := c.client.DefaultAPI.GetTrades(authCtx).Limit(limit).Page(page)
		if userID != "" {
			req = req.UserIds([]string{userID})
		}
		if len(orderBookIDs) > 0 {
			req = req.OrderBookIds(orderBookIDs)
		}
		if !start.IsZero() {
			req = req.Start(start)
		}
		if !end.IsZero() {
			req = req.End(end)
		}

		resp, _, err := req.Execute()
		if err != nil {
			return nil, fmt.Errorf("get trades: %w", err)
		}

		if resp == nil || resp.Data == nil {
			break
		}

		allTrades = append(allTrades, resp.Data...)

		if len(resp.Data) < int(limit) {
			break
		}

		page++
	}

	return allTrades, nil
}

type Backtester struct {
	strategy *Strategy
	trades   tradesClient
}

func NewBacktester(s *Strategy) *Backtester {
	return &Backtester{strategy: s}
}

func (b *Backtester) Run(ctx context.Context, start, end time.Time) (BacktestResult, error) {
	if b.trades == nil {
		apiKey := os.Getenv("DORA_API_KEY")
		if apiKey == "" {
			return BacktestResult{}, errors.New("DORA_API_KEY not set")
		}
		b.trades = newDoraTradesClient(apiKey)
	}

	orderBooks, err := b.trades.ListOrderBooks(ctx)
	if err != nil {
		return BacktestResult{}, fmt.Errorf("list order books: %w", err)
	}

	followedTrader := b.strategy.cfg.FollowedTrader.String()
	var allTrades []doraclient.Trade
	for _, obID := range orderBooks {
		select {
		case <-ctx.Done():
			return BacktestResult{}, errors.New("backtest cancelled")
		default:
		}
		trades, err := b.trades.GetTrades(ctx, followedTrader, []string{obID}, start, end)
		if err != nil {
			return BacktestResult{}, fmt.Errorf("get trades for order book %s: %w", obID, err)
		}
		allTrades = append(allTrades, trades...)
	}

	filtered := allTrades[:0]
	for _, tr := range allTrades {
		if tr.UserId == followedTrader {
			filtered = append(filtered, tr)
		}
	}
	allTrades = filtered

	sort.Slice(allTrades, func(i, j int) bool {
		return allTrades[i].CreatedAt.Before(allTrades[j].CreatedAt)
	})

	return b.simulate(ctx, allTrades)
}

type position struct {
	qty         decimal.Decimal
	avgEntry    decimal.Decimal
	openTime    time.Time
	openTradeID uuid.UUID
}

func (b *Backtester) simulate(ctx context.Context, trades []doraclient.Trade) (BacktestResult, error) {
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

	for _, trade := range trades {
		select {
		case <-ctx.Done():
			return BacktestResult{}, errors.New("backtest cancelled")
		default:
		}

		if margin.IsZero() || margin.IsNeg() {
			continue
		}

		price, err := decimal.Parse(trade.Price)
		if err != nil {
			return BacktestResult{}, fmt.Errorf("parse price %q: %w", trade.Price, err)
		}
		if price.IsZero() {
			continue
		}

		traderQty, err := decimal.Parse(trade.Quantity0)
		if err != nil {
			return BacktestResult{}, fmt.Errorf("parse quantity %q: %w", trade.Quantity0, err)
		}

		ourQty, _ := traderQty.Mul(scale)
		ourQty = ourQty.Round(0)
		tradeID, _ := uuid.Parse(trade.TransactionId)

		var ourSignal types.Signal
		if trade.Side == doraclient.SIDE_BUY {
			ourSignal = types.SignalBuy
		} else {
			ourSignal = types.SignalSell
		}

		cash, tradeRecords, closedTrades = applyTrade(
			trade, tradeID, ourSignal, ourQty, price, margin, cash, positions,
			tradeRecords, closedTrades,
		)
	}

	return summarise(tradeRecords, closedTrades), nil
}

func applyTrade(
	trade doraclient.Trade,
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
	trade doraclient.Trade,
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
	trade doraclient.Trade,
	tradeID uuid.UUID,
	ourQty, price, margin decimal.Decimal,
	cash decimal.Decimal,
	pos *position,
	positions map[string]*position,
	tradeRecords []TradeRecord,
	closedTrades []ClosedTrade,
) (decimal.Decimal, []TradeRecord, []ClosedTrade) {
	absQty := pos.qty.Abs()
	closeQty, _ := minDecimal(absQty, ourQty)

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
	trade doraclient.Trade,
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
	trade doraclient.Trade,
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
	trade doraclient.Trade,
	tradeID uuid.UUID,
	ourQty, price, margin decimal.Decimal,
	cash decimal.Decimal,
	pos *position,
	positions map[string]*position,
	tradeRecords []TradeRecord,
	closedTrades []ClosedTrade,
) (decimal.Decimal, []TradeRecord, []ClosedTrade) {
	closeQty, _ := minDecimal(pos.qty, ourQty)

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
	trade doraclient.Trade,
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
	trade doraclient.Trade,
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
	trade doraclient.Trade,
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
	trade doraclient.Trade,
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

func minDecimal(a, b decimal.Decimal) (decimal.Decimal, error) {
	if a.Cmp(b) < 0 {
		return a, nil
	}
	return b, nil
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
		startDay := start.Truncate(24 * time.Hour) //nolint:mnd
		endDay := end.Truncate(24 * time.Hour)
		for d := startDay; !d.After(endDay); d = d.Add(24 * time.Hour) {
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
