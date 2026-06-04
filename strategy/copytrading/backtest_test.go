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
	require.Equal(t, "10", rec.Quantity.String())
	require.Equal(t, "1000", rec.OrderSize.String())
	require.Equal(t, "0", rec.Cash.String())
	require.Equal(t, "10", rec.OpenPosition.String())

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
	require.Equal(t, "10", ct.Quantity.String())
	require.Equal(t, "100", ct.EntryPrice.String())
	require.Equal(t, "120", ct.ExitPrice.String())
	require.Equal(t, "200", ct.PnL.String())

	require.Equal(t, "200", res.GetTotalPnL().String())
	require.Equal(t, 1, res.GetWinCount())
	require.Equal(t, 0, res.GetLossCount())

	records, ok := res.GetTradeRecords().([]TradeRecord)
	require.True(t, ok, "TradeRecords must be []copytrading.TradeRecord")
	require.Len(t, records, 2)
	require.Equal(t, "0", records[1].OpenPosition.String())
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
	require.Equal(t, "4", closed[0].Quantity.String())
	require.Equal(t, "100", closed[0].EntryPrice.String())
	require.Equal(t, "150", closed[0].ExitPrice.String())
	require.Equal(t, "200", closed[0].PnL.String())

	records, ok := res.GetTradeRecords().([]TradeRecord)
	require.True(t, ok, "TradeRecords must be []copytrading.TradeRecord")
	require.Len(t, records, 2)
	require.Equal(t, "6", records[1].OpenPosition.String())
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
	require.Len(t, records, 2)
	require.Equal(t, "10", records[1].OpenPosition.String())

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
	require.Equal(t, "4", closed[0].Quantity.String())
	require.Equal(t, "100", closed[0].EntryPrice.String())
	require.Equal(t, "80", closed[0].ExitPrice.String())
	require.Equal(t, "80", closed[0].PnL.String())

	records, ok := res.GetTradeRecords().([]TradeRecord)
	require.True(t, ok, "TradeRecords must be []copytrading.TradeRecord")
	require.Len(t, records, 2)
	require.Equal(t, "-6", records[1].OpenPosition.String())
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
	require.Equal(t, "10", closed[0].Quantity.String())
	require.Equal(t, "100", closed[0].EntryPrice.String())
	require.Equal(t, "90", closed[0].ExitPrice.String())
	require.Equal(t, "100", closed[0].PnL.String())

	records, ok := res.GetTradeRecords().([]TradeRecord)
	require.True(t, ok, "TradeRecords must be []copytrading.TradeRecord")
	require.GreaterOrEqual(t, len(records), 2)
	last := records[len(records)-1]
	require.Equal(t, "5", last.OpenPosition.String())
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
	require.Equal(t, "10", rec.Quantity.String())
	require.Equal(t, "0", rec.Cash.String())
	require.Equal(t, "-10", rec.OpenPosition.String())

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

	require.Equal(t, 1, res.GetWinCount())
	require.Equal(t, 1, res.GetLossCount())
	require.Equal(t, "300", res.GetTotalPnL().String())
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

	// 300 trades = 3 pages of 100. Even-indexed trades are BUYs that
	// accumulate into a long position; odd-indexed are SELLs that
	// close them. The simulation must process all 300 across the
	// page boundary without dropping or reordering.
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
	require.Len(t, records, 300, "every trade across all 3 pages must be processed")

	// First and last trade IDs must match the input order.
	require.Equal(t, trades[0].TransactionID, records[0].TradeID.String())
	require.Equal(t, trades[299].TransactionID, records[299].TradeID.String())
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
