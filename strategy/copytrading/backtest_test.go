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
	// Backtest computes order size based on current cash
	// Initial balance: 10000, percentage: 1.0, leverage: 1.0, price: 100
	// Order size = 10000, quantity = 10000 / 100 = 100
	// Trade record reports pre-trade cash (the balance at the time the
	// order was sized), not the post-trade cash.
	require.Equal(t, "100", rec.Quantity.String())
	require.Equal(t, "10000", rec.OrderSize.String())
	require.Equal(t, "10000", rec.Cash.String())
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
	// Backtest computes order size based on current cash
	// t0: buy at 100, order size = 10000, qty = 100, cost = 10000, cash = 0
	// t1: sell at 120, close 100 long, proceeds = 12000, cash = 12000, PnL = 2000
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
	require.Len(t, records, 2)
	// t0: BUY opens long 100 @ 100 (qty=100, pos=+100)
	// t1: SELL closes the long. The reversal does NOT auto-open a new
	// short — the next source trade in the new direction would do that.
	require.Equal(t, "100", records[0].OpenPosition.String())
	require.Equal(t, "0", records[1].OpenPosition.String())
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
	require.Len(t, records, 2)

	// The close record (index 1) must show the original long
	// quantity and a zero open position — not zero quantity. We do
	// NOT auto-open a new short on reversal; the close record is the
	// only record emitted for the t1 source trade.
	closeRec := records[1]
	require.Equal(t, types.SignalSell, closeRec.Signal)
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
	// Backtest computes order size based on current cash
	// t0: buy at 100, order size = 10000, qty = 100, cost = 10000, cash = 0
	// t1: sell at 150, close 100 long, proceeds = 15000, cash = 15000, PnL = 5000
	require.Equal(t, "100", closed[0].Quantity.String())
	require.Equal(t, "100", closed[0].EntryPrice.String())
	require.Equal(t, "150", closed[0].ExitPrice.String())
	// PnL = (150 - 100) * 100 = 5000
	require.Equal(t, "5000", closed[0].PnL.String())

	records, ok := res.GetTradeRecords().([]TradeRecord)
	require.True(t, ok, "TradeRecords must be []copytrading.TradeRecord")
	require.Len(t, records, 2)
	// t0: BUY opens long 100 @ 100
	// t1: SELL closes the long. No auto-open on reversal.
	require.Equal(t, "100", records[0].OpenPosition.String())
	require.Equal(t, "0", records[1].OpenPosition.String())
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
	// Backtest computes order size based on current cash
	// t0: sell at 100, order size = 10000, qty = 100, cash = 20000 (short proceeds)
	// t1: buy at 80, close 100 short, buyback = 8000, cash = 12000, PnL = -2000
	require.Equal(t, "100", closed[0].Quantity.String())
	require.Equal(t, "100", closed[0].EntryPrice.String())
	require.Equal(t, "80", closed[0].ExitPrice.String())
	// PnL = (exitPrice - entryPrice) * qty = (80 - 100) * 100 = -2000
	// (we sold high and bought back low only if exit < entry, so the
	// sign reflects the actual loss when exit > entry)
	require.Equal(t, "-2000", closed[0].PnL.String())

	records, ok := res.GetTradeRecords().([]TradeRecord)
	require.True(t, ok, "TradeRecords must be []copytrading.TradeRecord")
	require.Len(t, records, 2)
	// t0: SELL opens short 100 @ 100
	// t1: BUY closes the short. No auto-open on reversal.
	require.Equal(t, "-100", records[0].OpenPosition.String())
	require.Equal(t, "0", records[1].OpenPosition.String())
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
	// Backtest computes order size based on current cash
	// t0: sell at 100, order size = 10000, qty = 100, cash = 20000
	// t1: buy at 90, close 100 short, buyback = 9000, cash = 11000, PnL = -1000
	require.Equal(t, "100", closed[0].Quantity.String())
	require.Equal(t, "100", closed[0].EntryPrice.String())
	require.Equal(t, "90", closed[0].ExitPrice.String())
	// PnL = (exitPrice - entryPrice) * qty = (90 - 100) * 100 = -1000
	require.Equal(t, "-1000", closed[0].PnL.String())

	records, ok := res.GetTradeRecords().([]TradeRecord)
	require.True(t, ok, "TradeRecords must be []copytrading.TradeRecord")
	require.Len(t, records, 2)
	// t0: SELL opens short 100 @ 100
	// t1: BUY closes the short. No auto-open on reversal.
	require.Equal(t, "-100", records[0].OpenPosition.String())
	require.Equal(t, "0", records[1].OpenPosition.String())
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
	// Backtest computes order size based on current cash
	// sell at 100, order size = 10000, qty = 10000/100 = 100
	require.Equal(t, "100", rec.Quantity.String())
	// Short sale proceeds: receive 10000, total cash becomes 20000.
	// Trade record reports pre-trade cash (the balance at the time the
	// order was sized), not the post-trade cash.
	require.Equal(t, "10000", rec.Cash.String())
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

	// Backtest computes order size based on current cash. With the
	// no-auto-open-on-reversal model, each source trade is mirrored
	// as at most one copy trade.
	// initialBalance = 10000, percentage = 1.0, leverage = 1.0
	// t0: buy at 100, headroom = 10000*1 - 0 = 10000, qty = 100, cost = 10000, cash = 0
	// t1: sell at 150, close 100 long, proceeds = 15000,
	//        PnL = (150-100)*100 = 5000 (win). No auto-open. cash = 15000.
	//        runningBalance advances: 10000 + 5000 = 15000.
	// t2: buy at 100, flat. headroom = 15000*1 - 0 = 15000, qty = 150,
	//        cost = 15000, cash = 0.
	// t3: sell at 80, close 150 long, proceeds = 12000,
	//        PnL = (80-100)*150 = -3000 (loss). cash = 12000.
	//        runningBalance: 15000 - 3000 = 12000.
	// Total PnL = 5000 - 3000 = 2000
	require.Equal(t, 1, res.GetWinCount())
	require.Equal(t, 1, res.GetLossCount())
	require.Equal(t, "2000", res.GetTotalPnL().String())
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
	// odd-indexed are SELLs. Without auto-open-on-reversal each source
	// trade produces exactly one record (an open, extend, or close —
	// never both). We expect 300 records. The simulation must process
	// all 300 trades across the page boundary without dropping or
	// reordering.
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
	require.Len(t, records, 300, "every trade across all 3 pages must be processed as one record each")

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

// TestSimulate_TradeRecordCashIsPreTrade verifies that the Cash field on
// each trade record is the balance at the time the order was sized, not
// the balance after the trade settles. Without this, the first SELL's
// cash would show initial_balance + proceeds, which makes it look like
// the order was sized from a balance that already includes its own
// proceeds (compounding illusion).
func TestSimulate_TradeRecordCashIsPreTrade(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	asset := uuid.New()
	t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	trades := []Trade{
		makeTrade("tx-sell-1", asset.String(), "sell", "100", "1", t0),
		makeTrade("tx-buy-1", asset.String(), "buy", "100", "1", t0.Add(time.Minute)),
	}

	cfg := Config{
		FollowedTrader:        followed,
		InitialBalance:        decimal.MustNew(1000, 0),
		PercentageOfAvailable: decimal.MustNew(1, 0),
		Leverage:              decimal.MustNew(1, 0),
	}
	s := New(cfg)
	b := &Backtester{strategy: s}

	res, err := b.simulate(t.Context(), feedChannel(t, trades), t0, t0.Add(time.Hour))
	require.NoError(t, err)

	records, ok := res.GetTradeRecords().([]TradeRecord)
	require.True(t, ok)
	require.Len(t, records, 2)

	// First record: SELL that opens a short. Order size = 100% of
	// pre-trade cash (1000), qty = 10, proceeds = 1000, post-trade
	// cash = 2000. The record must report 1000 (pre-trade), not 2000.
	require.Equal(t, types.SignalSell, records[0].Signal)
	require.Equal(t, "1000", records[0].OrderSize.String())
	require.Equal(t, "10", records[0].Quantity.String())
	require.Equal(t, "1000", records[0].Cash.String(),
		"first SELL record should report pre-trade cash (initial balance), not post-trade cash")

	// Second record: BUY that closes the short. Pre-close cash = 2000
	// (post-open), buyback cost = 10*100 = 1000, post-close cash = 1000.
	// The record must report 2000 (pre-close, the balance at the time
	// the close order was sized), not 1000. There is no third record:
	// reversal does not auto-open a new long.
	require.Equal(t, types.SignalBuy, records[1].Signal)
	require.Equal(t, "10", records[1].Quantity.String())
	require.Equal(t, "2000", records[1].Cash.String(),
		"close-short record should report pre-close cash, not post-close cash")

	// The first closed trade is the BUY that closed the short opened
	// by records[0]. With the running-balance model, the first closed
	// trade's EntryBalance is the initial balance (1000), since no
	// prior PnL exists to compound. Subsequent closed trades would
	// inherit runningBalance = EntryBalance + PnL.
	closed, ok := res.GetClosedTrades().([]ClosedTrade)
	require.True(t, ok)
	require.NotEmpty(t, closed)
	require.Equal(t, "1000", closed[0].EntryBalance.String(),
		"first closed trade EntryBalance should be the initial balance")
}

// TestSimulate_ClosedTradeEntryBalance_LongRoundTrip verifies that
// a single round-trip's ClosedTrade reports the initial balance as
// its EntryBalance (the running balance before any PnL is realised).
func TestSimulate_ClosedTradeEntryBalance_LongRoundTrip(t *testing.T) {
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
	require.True(t, ok)
	require.Len(t, closed, 1)
	ct := closed[0]
	// Initial balance is 10000, no prior closed trades, so the
	// running balance is 10000.
	require.Equal(t, "10000", ct.EntryBalance.String(),
		"single closed trade EntryBalance should be the initial balance")
}

// TestSimulate_ClosedTradeEntryBalance_CompoundsAcrossCloses verifies
// that each ClosedTrade's EntryBalance equals the previous closed
// trade's EntryBalance plus its PnL, i.e. the running compounding
// equity, not the cash snapshot at the moment the position was opened.
//
// Trade sequence (initial balance 10000, percentage 1.0, leverage 1.0):
//   - Trade 1: BUY  @ 100. Opens long 100 @ 100, cash 0.
//   - Trade 2: SELL @ 120. Closes long, pnl = (120-100)*100 = +2000,
//     then opens short from post-close cash 12000.
//     Sizing: 12000 * 1.0 / 120 = 100, so short = -100.
//   - Trade 3: BUY  @ 100. Closes short, pnl = (100-120)*100 = -2000,
//     then opens long from post-close cash 20000.
//     Sizing: 20000 * 1.0 / 100 = 200, so long = +200.
//   - Trade 4: SELL @ 100. Closes long, pnl = (100-100)*200 = 0.
//
// Expected running balances:
//   - closed[0].EntryBalance = 10000 (initial)
//   - closed[1].EntryBalance = 10000 + 2000 = 12000
//   - closed[2].EntryBalance = 12000 - 2000 = 10000
func TestSimulate_ClosedTradeEntryBalance_CompoundsAcrossCloses(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	asset := uuid.New()
	t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	t3 := time.Date(2026, 6, 1, 13, 0, 0, 0, time.UTC)
	trades := []Trade{
		makeTrade("tx-1", asset.String(), "buy", "100", "1", t0),
		makeTrade("tx-2", asset.String(), "sell", "120", "1", t1),
		makeTrade("tx-3", asset.String(), "buy", "100", "1", t2),
		makeTrade("tx-4", asset.String(), "sell", "100", "1", t3),
	}

	b := newBacktesterForSimulation(followed)
	res, err := b.simulate(t.Context(), feedChannel(t, trades), t0, t3.Add(time.Hour))
	require.NoError(t, err)

	closed, ok := res.GetClosedTrades().([]ClosedTrade)
	require.True(t, ok)
	require.Len(t, closed, 2)

	// First round-trip: BUY @ 100 opens long, SELL @ 120 closes it
	// for +2000 PnL. entry_balance is the initial 10000.
	require.Equal(t, "10000", closed[0].EntryBalance.String())
	require.Equal(t, "2000", closed[0].PnL.String())

	// Second round-trip: BUY @ 100 opens a new long, SELL @ 100
	// closes it for 0 PnL. entry_balance = prior EntryBalance + PnL
	// = 10000 + 2000 = 12000. (The reversal on t1 does NOT auto-open
	// a short; trade 3 opens the new long from flat.)
	require.Equal(t, "12000", closed[1].EntryBalance.String(),
		"second closed trade EntryBalance should be prior EntryBalance + prior PnL, not cash snapshot")
	require.Equal(t, "0", closed[1].PnL.String())
}

// TestSimulate_ExtendsAreCappedByRunningBalanceNotional verifies that
// when the source trader fires a sequence of signals in the same
// direction, extends are bounded by the realised equity ceiling
// (runningBalance * leverage) minus the notional already deployed.
// Extends past that cap are skipped, not silently over-sized.
func TestSimulate_ExtendsAreCappedByRunningBalanceNotional(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	asset := uuid.New()
	t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	trades := []Trade{
		// Open long at 100. Order size = runningBalance (10000) *
		// 1.0 (percentage) * 1 (leverage) = 10000. Deployed notional
		// after this = 10000, leaving 0 headroom.
		makeTrade("tx-1", asset.String(), "buy", "100", "1", t0),
		// Attempt to extend long. With 0 headroom, this is skipped
		// (no more notional available), and a close record should not
		// be created.
		makeTrade("tx-2", asset.String(), "buy", "100", "1", t0.Add(time.Minute)),
	}

	b := newBacktesterForSimulation(followed)
	res, err := b.simulate(t.Context(), feedChannel(t, trades), t0, t0.Add(time.Hour))
	require.NoError(t, err)

	records, ok := res.GetTradeRecords().([]TradeRecord)
	require.True(t, ok)
	require.Len(t, records, 1, "second extend should be skipped (no headroom)")
	require.Equal(t, types.SignalBuy, records[0].Signal)
	require.Equal(t, "10000", records[0].OrderSize.String())
	require.Equal(t, "100", records[0].Quantity.String())

	closed, ok := res.GetClosedTrades().([]ClosedTrade)
	require.True(t, ok)
	require.Empty(t, closed, "no round-trip should have completed")
}

// TestSimulate_ReversalDoesNotAutoOpen verifies that when a source
// trade reverses our position direction (e.g. SELL while we are
// long), the reversal closes the full position but does NOT open a
// new one in the opposite direction. The next source trade in the
// new direction triggers the open at the configured size, sized
// from the actual available notional at that point — not from the
// proceeds of the just-closed position.
func TestSimulate_ReversalDoesNotAutoOpen(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	asset := uuid.New()
	t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC)
	trades := []Trade{
		makeTrade("tx-open", asset.String(), "buy", "100", "1", t0),
		makeTrade("tx-reverse", asset.String(), "sell", "120", "1", t1),
	}

	b := newBacktesterForSimulation(followed)
	res, err := b.simulate(t.Context(), feedChannel(t, trades), t0, t1)
	require.NoError(t, err)

	// Two source trades produce exactly two trade records (one
	// open, one close) — no auto-open on reversal.
	records, ok := res.GetTradeRecords().([]TradeRecord)
	require.True(t, ok)
	require.Len(t, records, 2, "reversal must NOT auto-open a new position")

	require.Equal(t, types.SignalBuy, records[0].Signal)
	require.Equal(t, "100", records[0].OpenPosition.String())
	require.Equal(t, types.SignalSell, records[1].Signal)
	require.Equal(t, "0", records[1].OpenPosition.String(),
		"close record should leave position at zero; no auto-open should fill it")

	// One closed trade from the round-trip.
	closed, ok := res.GetClosedTrades().([]ClosedTrade)
	require.True(t, ok)
	require.Len(t, closed, 1)
	require.Equal(t, "2000", closed[0].PnL.String())
}
