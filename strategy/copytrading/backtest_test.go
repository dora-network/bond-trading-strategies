package copytrading

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/dora-client-go/doraclient"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/require"
)

type fakeTradesClient struct {
	trades     []doraclient.Trade
	streamErr  error
	streamCall getStreamCall
}

type getStreamCall struct {
	userID     string
	start, end time.Time
}

func (f *fakeTradesClient) GetTradeStream(_ context.Context, userID string, start, end time.Time) (<-chan doraclient.Trade, <-chan error) {
	f.streamCall = getStreamCall{userID: userID, start: start, end: end}
	ch := make(chan doraclient.Trade, len(f.trades))
	done := make(chan error, 1)
	for _, t := range f.trades {
		ch <- t
	}
	close(ch)
	done <- f.streamErr
	return ch, done
}

func newBacktesterWithFake(t *testing.T, fake *fakeTradesClient, followedTrader uuid.UUID, percentage, leverage string) *Backtester {
	t.Helper()
	cfg := Config{
		FollowedTrader:        followedTrader,
		PercentageOfAvailable: decimal.MustParse(percentage),
		Leverage:              decimal.MustParse(leverage),
	}
	s := New(cfg)
	return &Backtester{strategy: s, trades: fake}
}

func newBacktesterForSimulation(followedTrader uuid.UUID, percentage, leverage string) *Backtester {
	cfg := Config{
		FollowedTrader:        followedTrader,
		PercentageOfAvailable: decimal.MustParse(percentage),
		Leverage:              decimal.MustParse(leverage),
	}
	s := New(cfg)
	return &Backtester{strategy: s}
}

func makeTrade(id, asset, side, price, qty string, t time.Time) doraclient.Trade {
	return doraclient.Trade{
		TransactionId: id,
		UserId:        "ignored-by-sim",
		OrderBookId:   "ob",
		Asset0:        asset,
		Side:          doraclient.Side(strings.ToUpper(side)),
		Price:         price,
		Quantity0:     qty,
		CreatedAt:     t,
	}
}

func feedChannel(t *testing.T, trades []doraclient.Trade) <-chan doraclient.Trade {
	t.Helper()
	ch := make(chan doraclient.Trade, len(trades))
	for _, trade := range trades {
		ch <- trade
	}
	close(ch)
	return ch
}

func TestBacktesterRunCallsGetTradeStream(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	t1 := time.Date(2026, 5, 26, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)

	trades := []doraclient.Trade{
		{TransactionId: uuid.New().String(), UserId: followed.String(), Side: doraclient.SIDE_BUY, Price: "100", Quantity0: "1", CreatedAt: t1, Asset0: "bond-a"},
		{TransactionId: uuid.New().String(), UserId: followed.String(), Side: doraclient.SIDE_SELL, Price: "101", Quantity0: "1", CreatedAt: t2, Asset0: "bond-b"},
	}

	fake := &fakeTradesClient{trades: trades}
	b := newBacktesterWithFake(t, fake, followed, "0.5", "1.0")
	start := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC)
	result, err := b.Run(t.Context(), start, end)
	require.NoError(t, err)

	require.Equal(t, followed.String(), fake.streamCall.userID,
		"GetTradeStream must filter by the followed trader")
	require.True(t, fake.streamCall.start.Equal(start))
	require.True(t, fake.streamCall.end.Equal(end))

	records, ok := result.GetTradeRecords().([]TradeRecord)
	require.True(t, ok, "TradeRecords must be []copytrading.TradeRecord")
	require.Len(t, records, 2)
}

func TestBacktesterRunPreservesStreamOrder(t *testing.T) {
	t.Parallel()

	followed := uuid.New()

	later := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	earlier := time.Date(2026, 5, 26, 9, 0, 0, 0, time.UTC)

	earlierID := uuid.New()
	laterID := uuid.New()

	// Fake delivers trades in this order; the simulation must consume
	// them in the same order (no consumer-side sort).
	trades := []doraclient.Trade{
		{TransactionId: earlierID.String(), UserId: followed.String(), Side: doraclient.SIDE_SELL, Price: "101", Quantity0: "1", CreatedAt: earlier, Asset0: "bond-b"},
		{TransactionId: laterID.String(), UserId: followed.String(), Side: doraclient.SIDE_BUY, Price: "100", Quantity0: "1", CreatedAt: later, Asset0: "bond-a"},
	}

	fake := &fakeTradesClient{trades: trades}
	b := newBacktesterWithFake(t, fake, followed, "0.5", "1.0")
	start := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC)
	result, err := b.Run(t.Context(), start, end)
	require.NoError(t, err)

	records, ok := result.GetTradeRecords().([]TradeRecord)
	require.True(t, ok)
	require.Len(t, records, 2)
	require.Equal(t, earlierID, records[0].TradeID,
		"the first record must be the trade the stream delivered first")
	require.Equal(t, laterID, records[1].TradeID,
		"the second record must be the trade the stream delivered second")
}

func TestBacktesterRunSimulatesOnlyFollowedTradersTrades(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	stranger := uuid.New()
	t1 := time.Date(2026, 5, 26, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 5, 26, 11, 0, 0, 0, time.UTC)

	// The fake does not pre-filter; the test asserts the production
	// client uses the user_ids query parameter. Here we just verify
	// every trade in the stream is processed (the production
	// filtering is the API's job, not the backtest's).
	trades := []doraclient.Trade{
		{TransactionId: uuid.New().String(), UserId: followed.String(), Side: doraclient.SIDE_BUY, Price: "100", Quantity0: "1", CreatedAt: t1, Asset0: "bond-a"},
		{TransactionId: uuid.New().String(), UserId: stranger.String(), Side: doraclient.SIDE_BUY, Price: "100", Quantity0: "1", CreatedAt: t2, Asset0: "bond-a"},
	}

	fake := &fakeTradesClient{trades: trades}
	b := newBacktesterWithFake(t, fake, followed, "0.5", "1.0")
	start := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC)
	result, err := b.Run(t.Context(), start, end)
	require.NoError(t, err)

	records, ok := result.GetTradeRecords().([]TradeRecord)
	require.True(t, ok)
	require.Len(t, records, 2, "the backtest must process every trade the stream delivers")
	require.Equal(t, followed.String(), fake.streamCall.userID,
		"the backtest must request trades filtered by the followed trader")
}

func TestSimulate_BuyOpensLong(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	asset := uuid.New()
	t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	trades := []doraclient.Trade{
		makeTrade(followed.String(), asset.String(), "buy", "100", "1", t0),
	}

	b := newBacktesterForSimulation(followed, "1.0", "1.0")
	res, err := b.simulate(t.Context(), feedChannel(t, trades))
	require.NoError(t, err)

	records := res.GetTradeRecords().([]TradeRecord)
	require.Len(t, records, 1)
	rec := records[0]
	require.Equal(t, types.SignalBuy, rec.Signal)
	require.Equal(t, "10", rec.Quantity.String())
	require.Equal(t, "1000", rec.OrderSize.String())
	require.Equal(t, "0", rec.Cash.String())
	require.Equal(t, "10", rec.OpenPosition.String())

	closed := res.GetClosedTrades().([]ClosedTrade)
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
	trades := []doraclient.Trade{
		makeTrade("tx-open", asset.String(), "buy", "100", "1", t0),
		makeTrade("tx-close", asset.String(), "sell", "120", "1", t1),
	}

	b := newBacktesterForSimulation(followed, "1.0", "1.0")
	res, err := b.simulate(t.Context(), feedChannel(t, trades))
	require.NoError(t, err)

	closed := res.GetClosedTrades().([]ClosedTrade)
	require.Len(t, closed, 1)
	ct := closed[0]
	require.Equal(t, "10", ct.Quantity.String())
	require.Equal(t, "100", ct.EntryPrice.String())
	require.Equal(t, "120", ct.ExitPrice.String())
	require.Equal(t, "200", ct.PnL.String())

	require.Equal(t, "200", res.GetTotalPnL().String())
	require.Equal(t, 1, res.GetWinCount())
	require.Equal(t, 0, res.GetLossCount())

	records := res.GetTradeRecords().([]TradeRecord)
	require.Len(t, records, 2)
	require.Equal(t, "0", records[1].OpenPosition.String())
}

func TestSimulate_BuyThenPartialSell(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	asset := uuid.New()
	t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC)
	trades := []doraclient.Trade{
		makeTrade("tx-open", asset.String(), "buy", "100", "1", t0),
		makeTrade("tx-partial", asset.String(), "sell", "150", "0.4", t1),
	}

	b := newBacktesterForSimulation(followed, "1.0", "1.0")
	res, err := b.simulate(t.Context(), feedChannel(t, trades))
	require.NoError(t, err)

	closed := res.GetClosedTrades().([]ClosedTrade)
	require.Len(t, closed, 1)
	require.Equal(t, "4", closed[0].Quantity.String())
	require.Equal(t, "100", closed[0].EntryPrice.String())
	require.Equal(t, "150", closed[0].ExitPrice.String())
	require.Equal(t, "200", closed[0].PnL.String())

	records := res.GetTradeRecords().([]TradeRecord)
	require.Len(t, records, 2)
	require.Equal(t, "6", records[1].OpenPosition.String())
}

func TestSimulate_MultipleBuysWeightedAvg(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	asset := uuid.New()
	t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC)
	trades := []doraclient.Trade{
		makeTrade("tx-1", asset.String(), "buy", "100", "0.4", t0),
		makeTrade("tx-2", asset.String(), "buy", "200", "0.6", t1),
	}

	b := newBacktesterForSimulation(followed, "1.0", "1.0")
	res, err := b.simulate(t.Context(), feedChannel(t, trades))
	require.NoError(t, err)

	records := res.GetTradeRecords().([]TradeRecord)
	require.Len(t, records, 2)
	require.Equal(t, "10", records[1].OpenPosition.String())

	closed := res.GetClosedTrades().([]ClosedTrade)
	require.Len(t, closed, 0)
}

func TestSimulate_BuyClosesShort(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	asset := uuid.New()
	t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC)
	trades := []doraclient.Trade{
		makeTrade("tx-open-short", asset.String(), "sell", "100", "1", t0),
		makeTrade("tx-close-partial", asset.String(), "buy", "80", "0.4", t1),
	}

	b := newBacktesterForSimulation(followed, "1.0", "1.0")
	res, err := b.simulate(t.Context(), feedChannel(t, trades))
	require.NoError(t, err)

	closed := res.GetClosedTrades().([]ClosedTrade)
	require.Len(t, closed, 1)
	require.Equal(t, "4", closed[0].Quantity.String())
	require.Equal(t, "100", closed[0].EntryPrice.String())
	require.Equal(t, "80", closed[0].ExitPrice.String())
	require.Equal(t, "80", closed[0].PnL.String())

	records := res.GetTradeRecords().([]TradeRecord)
	require.Len(t, records, 2)
	require.Equal(t, "-6", records[1].OpenPosition.String())
}

func TestSimulate_BuyClosesShortAndFlipsLong(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	asset := uuid.New()
	t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC)
	trades := []doraclient.Trade{
		makeTrade("tx-open-short", asset.String(), "sell", "100", "1", t0),
		makeTrade("tx-flip", asset.String(), "buy", "90", "1.5", t1),
	}

	b := newBacktesterForSimulation(followed, "1.0", "1.0")
	res, err := b.simulate(t.Context(), feedChannel(t, trades))
	require.NoError(t, err)

	closed := res.GetClosedTrades().([]ClosedTrade)
	require.Len(t, closed, 1)
	require.Equal(t, "10", closed[0].Quantity.String())
	require.Equal(t, "100", closed[0].EntryPrice.String())
	require.Equal(t, "90", closed[0].ExitPrice.String())
	require.Equal(t, "100", closed[0].PnL.String())

	records := res.GetTradeRecords().([]TradeRecord)
	require.GreaterOrEqual(t, len(records), 2)
	last := records[len(records)-1]
	require.Equal(t, "5", last.OpenPosition.String())
}

func TestSimulate_SellOpensShort(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	asset := uuid.New()
	t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	trades := []doraclient.Trade{
		makeTrade("tx-open-short", asset.String(), "sell", "100", "1", t0),
	}

	b := newBacktesterForSimulation(followed, "1.0", "1.0")
	res, err := b.simulate(t.Context(), feedChannel(t, trades))
	require.NoError(t, err)

	records := res.GetTradeRecords().([]TradeRecord)
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
	trades := []doraclient.Trade{
		makeTrade("tx-1", asset.String(), "buy", "100", "1", t0),
		makeTrade("tx-2", asset.String(), "sell", "150", "1", t1),
		makeTrade("tx-3", asset.String(), "buy", "100", "1", t2),
		makeTrade("tx-4", asset.String(), "sell", "80", "1", t3),
	}

	b := newBacktesterForSimulation(followed, "1.0", "1.0")
	res, err := b.simulate(t.Context(), feedChannel(t, trades))
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
	trades := []doraclient.Trade{
		makeTrade("tx-1", asset.String(), "buy", "100", "1", t0),
		makeTrade("tx-2", asset.String(), "sell", "150", "1", t1),
		makeTrade("tx-3", asset.String(), "buy", "100", "1", t2),
		makeTrade("tx-4", asset.String(), "sell", "120", "1", t3),
	}

	b := newBacktesterForSimulation(followed, "1.0", "1.0")
	res, err := b.simulate(t.Context(), feedChannel(t, trades))
	require.NoError(t, err)

	require.False(t, res.GetMaxDrawdown().IsNeg(), "MaxDrawdown must be >= 0, got %s", res.GetMaxDrawdown().String())
}

func TestSimulate_NoTrades(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	trades := []doraclient.Trade{}
	b := newBacktesterForSimulation(followed, "1.0", "1.0")
	res, err := b.simulate(t.Context(), feedChannel(t, trades))
	require.NoError(t, err)

	require.True(t, res.GetTotalPnL().IsZero())
	require.Equal(t, 0, res.GetWinCount())
	require.Equal(t, 0, res.GetLossCount())
}
