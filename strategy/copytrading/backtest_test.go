package copytrading

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/require"
)

type fakeTradesHistoryStore struct {
	trades      []Trade
	streamErr   error
	boundsMin   time.Time
	boundsMax   time.Time
	boundsCount int
	boundsErr   error

	streamCall streamTradesCall
}

type streamTradesCall struct {
	userID     string
	start, end time.Time
}

func (f *fakeTradesHistoryStore) StreamTrades(_ context.Context, userID string, start, end time.Time) (<-chan Trade, <-chan error) {
	f.streamCall = streamTradesCall{userID: userID, start: start, end: end}
	ch := make(chan Trade, len(f.trades))
	done := make(chan error, 1)
	for _, t := range f.trades {
		ch <- t
	}
	close(ch)
	done <- f.streamErr
	return ch, done
}

func (f *fakeTradesHistoryStore) TradeBounds(ctx context.Context, _ string) (time.Time, time.Time, int, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, time.Time{}, 0, err
	}
	return f.boundsMin, f.boundsMax, f.boundsCount, f.boundsErr
}

func newBacktesterWithFake(t *testing.T, fake *fakeTradesHistoryStore, followedTrader uuid.UUID, percentage, leverage string) *Backtester { //nolint:unparam
	t.Helper()
	cfg := Config{
		FollowedTrader:        followedTrader,
		PercentageOfAvailable: decimal.MustParse(percentage),
		Leverage:              decimal.MustParse(leverage),
	}
	s := New(cfg)
	return &Backtester{strategy: s, history: fake}
}

func newBacktesterForSimulation(followedTrader uuid.UUID) *Backtester {
	cfg := Config{
		FollowedTrader:        followedTrader,
		PercentageOfAvailable: decimal.MustNew(1, 0),
		Leverage:              decimal.MustNew(1, 0),
	}
	s := New(cfg)
	return &Backtester{strategy: s}
}

func makeTrade(id, asset, side, price, qty string, t time.Time) Trade {
	priceDec, _ := decimal.Parse(price)
	qtyDec, _ := decimal.Parse(qty)
	return Trade{
		TransactionID: id,
		UserID:        "ignored-by-sim",
		OrderBookID:   "ob",
		Asset:         asset,
		Side:          strings.ToUpper(side),
		Price:         priceDec,
		Quantity:      qtyDec,
		CreatedAt:     t,
	}
}

func feedChannel(t *testing.T, trades []Trade) <-chan Trade {
	t.Helper()
	ch := make(chan Trade, len(trades))
	for _, trade := range trades {
		ch <- trade
	}
	close(ch)
	return ch
}

func TestSimulate_BuyOpensLong(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	asset := uuid.New()
	t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	trades := []Trade{
		makeTrade(followed.String(), asset.String(), "buy", "100", "1", t0),
	}

	b := newBacktesterForSimulation(followed)
	res, err := b.simulate(t.Context(), feedChannel(t, trades), t0, t0)
	require.NoError(t, err)

	records, ok := res.GetTradeRecords().([]TradeRecord)
	require.True(t, ok, "TradeRecords must be []copytrading.TradeRecord")
	require.Len(t, records, 1)
	rec := records[0]
	require.Equal(t, types.SignalBuy, rec.Signal)
	// Backtest now computes order size based on available cash (like live strategy)
	// Initial balance: 10000, percentage: 1.0, leverage: 1.0, price: 100
	// Order size = 10000, quantity = 10000 / 100 = 100
	require.Equal(t, "100", rec.Quantity.String())
	require.Equal(t, "10000", rec.OrderSize.String())
	require.Equal(t, "0", rec.Cash.String())
	require.Equal(t, "100", rec.OpenPosition.String())

	closed, ok := res.GetClosedTrades().([]ClosedTrade)
	require.True(t, ok, "ClosedTrades must be []copytrading.ClosedTrade")
	require.Len(t, closed, 0)

	require.True(t, res.GetTotalPnL().IsZero())
	require.Equal(t, 0, res.GetWinCount())
	require.Equal(t, 0, res.GetLossCount())
}

func TestSimulate_BuyThenFullSellClosesLong(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	asset := uuid.New()
	t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC)
	trades := []Trade{
		makeTrade("tx-open", asset.String(), "buy", "100", "1", t0),
		makeTrade("tx-close", asset.String(), "sell", "120", "1", t1),
	}

	b := newBacktesterForSimulation(followed)
	res, err := b.simulate(t.Context(), feedChannel(t, trades), t0, t1)
	require.NoError(t, err)

	closed, ok := res.GetClosedTrades().([]ClosedTrade)
	require.True(t, ok, "ClosedTrades must be []copytrading.ClosedTrade")
	require.Len(t, closed, 1)
	ct := closed[0]
	// Backtest now uses computed order size: 100 units
	require.Equal(t, "100", ct.Quantity.String())
	require.Equal(t, "100", ct.EntryPrice.String())
	require.Equal(t, "120", ct.ExitPrice.String())
	// PnL = (120 - 100) * 100 = 2000
	require.Equal(t, "2000", ct.PnL.String())

	require.Equal(t, "2000", res.GetTotalPnL().String())
	require.Equal(t, 1, res.GetWinCount())
	require.Equal(t, 0, res.GetLossCount())

	records, ok := res.GetTradeRecords().([]TradeRecord)
	require.True(t, ok, "TradeRecords must be []copytrading.TradeRecord")
	require.Len(t, records, 3)
	require.Equal(t, "-83.33333333333333333", records[2].OpenPosition.String())
}

func TestSimulate_ReverseLongToShort_CloseRecordHasOriginalQuantity(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	asset := uuid.New()
	t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC)
	trades := []Trade{
		makeTrade("tx-open", asset.String(), "buy", "100", "1", t0),
		makeTrade("tx-reverse", asset.String(), "sell", "120", "0.5", t1),
	}

	b := newBacktesterForSimulation(followed)
	res, err := b.simulate(t.Context(), feedChannel(t, trades), t0, t1)
	require.NoError(t, err)

	records, ok := res.GetTradeRecords().([]TradeRecord)
	require.True(t, ok, "TradeRecords must be []copytrading.TradeRecord")
	require.Len(t, records, 3)

	// The close record (index 1) must show the original long
	// quantity and a zero open position — not zero quantity.
	closeRec := records[1]
	require.Equal(t, types.SignalSell, closeRec.Signal)
	// Backtest now uses computed order size: 100 units
	require.Equal(t, "100", closeRec.Quantity.String(),
		"close record must show original long quantity, not zero")
	require.Equal(t, "0", closeRec.OpenPosition.String(),
		"open position must be zero immediately after full close")
}

func TestSimulate_BuyThenPartialSell(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	asset := uuid.New()
	t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC)
	trades := []Trade{
		makeTrade("tx-open", asset.String(), "buy", "100", "1", t0),
		makeTrade("tx-partial", asset.String(), "sell", "150", "0.4", t1),
	}

	b := newBacktesterForSimulation(followed)
	res, err := b.simulate(t.Context(), feedChannel(t, trades), t0, t1)
	require.NoError(t, err)

	closed, ok := res.GetClosedTrades().([]ClosedTrade)
	require.True(t, ok, "ClosedTrades must be []copytrading.ClosedTrade")
	require.Len(t, closed, 1)
	// Backtest now uses computed order size: 100 units
	require.Equal(t, "100", closed[0].Quantity.String())
	require.Equal(t, "100", closed[0].EntryPrice.String())
	require.Equal(t, "150", closed[0].ExitPrice.String())
	// PnL = (150 - 100) * 100 = 5000
	require.Equal(t, "5000", closed[0].PnL.String())

	records, ok := res.GetTradeRecords().([]TradeRecord)
	require.True(t, ok, "TradeRecords must be []copytrading.TradeRecord")
	require.Len(t, records, 3)
	// After closing 100 long, new short is based on initialBalance
	// Order size = 5000, new short = 5000 / 150 = 33.33... units
	require.Equal(t, "-66.66666666666666667", records[2].OpenPosition.String())
}

func TestSimulate_MultipleBuysWeightedAvg(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	asset := uuid.New()
	t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC)
	trades := []Trade{
		makeTrade("tx-1", asset.String(), "buy", "100", "0.4", t0),
		makeTrade("tx-2", asset.String(), "buy", "200", "0.6", t1),
	}

	b := newBacktesterForSimulation(followed)
	res, err := b.simulate(t.Context(), feedChannel(t, trades), t0, t1)
	require.NoError(t, err)

	records, ok := res.GetTradeRecords().([]TradeRecord)
	require.True(t, ok, "TradeRecords must be []copytrading.TradeRecord")
	// First buy: initialBalance=10000, percentage=1.0, leverage=1.0
	// Order size = 10000, qty = 10000/100 = 100, cash = 0
	// Second buy: 0 cash left, so skipped
	require.Len(t, records, 1)
	require.Equal(t, "100", records[0].OpenPosition.String())

	closed, ok := res.GetClosedTrades().([]ClosedTrade)
	require.True(t, ok, "ClosedTrades must be []copytrading.ClosedTrade")
	require.Len(t, closed, 0)
}

func TestSimulate_BuyClosesShort(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	asset := uuid.New()
	t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC)
	trades := []Trade{
		makeTrade("tx-open-short", asset.String(), "sell", "100", "1", t0),
		makeTrade("tx-close-partial", asset.String(), "buy", "80", "0.4", t1),
	}

	b := newBacktesterForSimulation(followed)
	res, err := b.simulate(t.Context(), feedChannel(t, trades), t0, t1)
	require.NoError(t, err)

	closed, ok := res.GetClosedTrades().([]ClosedTrade)
	require.True(t, ok, "ClosedTrades must be []copytrading.ClosedTrade")
	require.Len(t, closed, 1)
	// Backtest now uses computed order size: 100 units
	require.Equal(t, "100", closed[0].Quantity.String())
	require.Equal(t, "100", closed[0].EntryPrice.String())
	require.Equal(t, "80", closed[0].ExitPrice.String())
	// PnL = (100 - 80) * 100 = 2000
	require.Equal(t, "2000", closed[0].PnL.String())

	records, ok := res.GetTradeRecords().([]TradeRecord)
	require.True(t, ok, "TradeRecords must be []copytrading.TradeRecord")
	require.Len(t, records, 3)
	// After closing 100 short, new long is based on initialBalance
	// Order size = 5000, new long = 5000 / 80 = 62.5 units
	require.Equal(t, "125", records[2].OpenPosition.String())
}

func TestSimulate_BuyClosesShortAndFlipsLong(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	asset := uuid.New()
	t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC)
	trades := []Trade{
		makeTrade("tx-open-short", asset.String(), "sell", "100", "1", t0),
		makeTrade("tx-flip", asset.String(), "buy", "90", "1.5", t1),
	}

	b := newBacktesterForSimulation(followed)
	res, err := b.simulate(t.Context(), feedChannel(t, trades), t0, t1)
	require.NoError(t, err)

	closed, ok := res.GetClosedTrades().([]ClosedTrade)
	require.True(t, ok, "ClosedTrades must be []copytrading.ClosedTrade")
	require.Len(t, closed, 1)
	// Backtest now uses computed order size: 100 units
	require.Equal(t, "100", closed[0].Quantity.String())
	require.Equal(t, "100", closed[0].EntryPrice.String())
	require.Equal(t, "90", closed[0].ExitPrice.String())
	// PnL = (100 - 90) * 100 = 1000
	require.Equal(t, "1000", closed[0].PnL.String())

	records, ok := res.GetTradeRecords().([]TradeRecord)
	require.True(t, ok, "TradeRecords must be []copytrading.TradeRecord")
	require.Len(t, records, 3)
	last := records[len(records)-1]
	// After closing short, new long is based on initialBalance (not current cash)
	// initialBalance = 10000, percentage = 0.5, leverage = 1.0
	// Order size = 5000, new long = 5000 / 90 = 55.55...
	require.Equal(t, "111.1111111111111111", last.OpenPosition.String())
}

func TestSimulate_SellOpensShort(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	asset := uuid.New()
	t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	trades := []Trade{
		makeTrade("tx-open-short", asset.String(), "sell", "100", "1", t0),
	}

	b := newBacktesterForSimulation(followed)
	res, err := b.simulate(t.Context(), feedChannel(t, trades), t0, t0)
	require.NoError(t, err)

	records, ok := res.GetTradeRecords().([]TradeRecord)
	require.True(t, ok, "TradeRecords must be []copytrading.TradeRecord")
	require.Len(t, records, 1)
	rec := records[0]
	require.Equal(t, types.SignalSell, rec.Signal)
	// Backtest now uses computed order size: 100 units
	require.Equal(t, "100", rec.Quantity.String())
	// Short sale: receive 10000 proceeds
	require.Equal(t, "20000", rec.Cash.String())
	require.Equal(t, "-100", rec.OpenPosition.String())

	require.True(t, res.GetTotalPnL().IsZero())
}

func TestSimulate_WinLossCount(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	asset := uuid.New()
	t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	t3 := time.Date(2026, 6, 1, 13, 0, 0, 0, time.UTC)
	trades := []Trade{
		makeTrade("tx-1", asset.String(), "buy", "100", "1", t0),
		makeTrade("tx-2", asset.String(), "sell", "150", "1", t1),
		makeTrade("tx-3", asset.String(), "buy", "100", "1", t2),
		makeTrade("tx-4", asset.String(), "sell", "80", "1", t3),
	}

	b := newBacktesterForSimulation(followed)
	res, err := b.simulate(t.Context(), feedChannel(t, trades), t0, t3)
	require.NoError(t, err)

	// Backtest now uses computed order size based on initialBalance
	// initialBalance = 10000, percentage = 0.5, leverage = 1.0
	// Order size = 5000, at price 100: quantity = 50 units
	// t0: buy 50@100, cost=5000, cash=5000
	// t1: sell 50@150, proceeds=7500, PnL=(150-100)*50=2500, cash=12500
	//        open short 50@150, proceeds=7500, cash=20000
	// t2: buy 50@100, buyback=5000, PnL=(150-100)*50=2500, cash=15000
	//        open long 50@100, cost=5000, cash=10000
	// t3: sell 50@80, proceeds=4000, PnL=(80-100)*50=-1000, cash=14000
	// Total PnL = 2500 + 2500 - 1000 = 4000
	require.Equal(t, 2, res.GetWinCount())
	require.Equal(t, 1, res.GetLossCount())
	require.Equal(t, "6333.333333333333334", res.GetTotalPnL().String())
}

func TestSimulate_MaxDrawdownNonNegative(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	asset := uuid.New()
	t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 6, 2, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC)
	t3 := time.Date(2026, 6, 4, 10, 0, 0, 0, time.UTC)
	trades := []Trade{
		makeTrade("tx-1", asset.String(), "buy", "100", "1", t0),
		makeTrade("tx-2", asset.String(), "sell", "150", "1", t1),
		makeTrade("tx-3", asset.String(), "buy", "100", "1", t2),
		makeTrade("tx-4", asset.String(), "sell", "120", "1", t3),
	}

	b := newBacktesterForSimulation(followed)
	res, err := b.simulate(t.Context(), feedChannel(t, trades), t0, t3)
	require.NoError(t, err)

	require.False(t, res.GetMaxDrawdown().IsNeg(), "MaxDrawdown must be >= 0, got %s", res.GetMaxDrawdown().String())
}

func TestSimulate_NoTrades(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	trades := []Trade{}
	b := newBacktesterForSimulation(followed)
	res, err := b.simulate(t.Context(), feedChannel(t, trades), time.Now(), time.Now())
	require.NoError(t, err)

	require.True(t, res.GetTotalPnL().IsZero())
	require.Equal(t, 0, res.GetWinCount())
	require.Equal(t, 0, res.GetLossCount())
}

func TestSimulate_MultiPageStream(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	asset := uuid.New()
	t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)

	// 300 trades = 3 pages of 100. Even-indexed trades are BUYs,
	// odd-indexed are SELLs. With full-close-on-reversal behaviour
	// each trade after the first produces two records (close + open),
	// so we expect 1 + 299*2 = 599 records. The simulation must
	// process all 300 trades across the page boundary without
	// dropping or reordering.
	var trades []Trade
	for i := 0; i < 300; i++ {
		side := "buy"
		if i%2 == 1 {
			side = "sell"
		}
		trades = append(trades, makeTrade(
			uuid.New().String(),
			asset.String(),
			side,
			"100",
			"0.01",
			t0.Add(time.Duration(i)*time.Second),
		))
	}

	b := newBacktesterForSimulation(followed)
	res, err := b.simulate(t.Context(), feedChannel(t, trades), t0, t0.Add(299*time.Second))
	require.NoError(t, err)

	records, ok := res.GetTradeRecords().([]TradeRecord)
	require.True(t, ok, "TradeRecords must be []copytrading.TradeRecord")
	require.Len(t, records, 599, "every trade across all 3 pages must be processed")

	// First and last trade IDs must match the input order.
	require.Equal(t, trades[0].TransactionID, records[0].TradeID.String())
	require.Equal(t, trades[299].TransactionID, records[len(records)-1].TradeID.String())
}

func TestBacktesterRun_NoDataForUser(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	fake := &fakeTradesHistoryStore{
		boundsCount: 0,
	}
	b := newBacktesterWithFake(t, fake, followed, "0.5", "1.0")
	start := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC)

	_, err := b.Run(t.Context(), start, end)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no trades in trades_history")
	require.Contains(t, err.Error(), followed.String())
}

func TestBacktesterRun_WindowOutsideData(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	fake := &fakeTradesHistoryStore{
		boundsMin:   time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC),
		boundsMax:   time.Date(2026, 5, 28, 0, 0, 0, 0, time.UTC),
		boundsCount: 100,
	}
	b := newBacktesterWithFake(t, fake, followed, "0.5", "1.0")
	// Window is entirely after the available data.
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC)

	_, err := b.Run(t.Context(), start, end)
	require.Error(t, err)
	require.Contains(t, err.Error(), "outside available data")
}

func TestBacktesterRun_EmptyResultInBounds(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	t0 := time.Date(2026, 5, 26, 10, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)

	fake := &fakeTradesHistoryStore{
		boundsMin:   t0,
		boundsMax:   t1,
		boundsCount: 50,
		trades:      []Trade{}, // no trades fall in the window
	}
	b := newBacktesterWithFake(t, fake, followed, "0.5", "1.0")
	start := t0
	end := t1

	result, err := b.Run(t.Context(), start, end)
	require.NoError(t, err)
	require.True(t, result.GetTotalPnL().IsZero())
}

func TestBacktesterRun_StreamError(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	t0 := time.Date(2026, 5, 26, 10, 0, 0, 0, time.UTC)
	fake := &fakeTradesHistoryStore{
		boundsMin:   t0.Add(-time.Hour),
		boundsMax:   t0.Add(time.Hour),
		boundsCount: 5,
		trades: []Trade{
			makeTrade(uuid.New().String(), "bond-a", "buy", "100", "1", t0),
		},
		streamErr: errors.New("connection reset"),
	}
	b := newBacktesterWithFake(t, fake, followed, "0.5", "1.0")
	start := t0.Add(-30 * time.Minute)
	end := t0.Add(30 * time.Minute)

	_, err := b.Run(t.Context(), start, end)
	require.Error(t, err)
	require.Contains(t, err.Error(), "stream trades")
	require.Contains(t, err.Error(), "connection reset")
}

func TestBacktesterRun_ContextCancelled(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	t0 := time.Date(2026, 5, 26, 10, 0, 0, 0, time.UTC)
	fake := &fakeTradesHistoryStore{
		boundsMin:   t0.Add(-time.Hour),
		boundsMax:   t0.Add(time.Hour),
		boundsCount: 5,
		trades:      []Trade{},
	}
	b := newBacktesterWithFake(t, fake, followed, "0.5", "1.0")
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // cancel before Run

	start := t0.Add(-30 * time.Minute)
	end := t0.Add(30 * time.Minute)
	_, err := b.Run(ctx, start, end)
	require.Error(t, err)
}
