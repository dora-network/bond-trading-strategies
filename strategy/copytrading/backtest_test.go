package copytrading

import (
	"context"
	"testing"
	"time"

	"github.com/dora-network/dora-client-go/doraclient"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/require"
)

type fakeTradesClient struct {
	orderBooks      []string
	tradesByOrderBK map[string][]doraclient.Trade
	listOBErr       error
	getTradesErr    error

	getTradesCalls []getTradesCall
}

type getTradesCall struct {
	userID       string
	orderBookIDs []string
	start, end   time.Time
}

func (f *fakeTradesClient) ListOrderBooks(_ context.Context) ([]string, error) {
	if f.listOBErr != nil {
		return nil, f.listOBErr
	}
	return f.orderBooks, nil
}

func (f *fakeTradesClient) GetTrades(_ context.Context, userID string, orderBookIDs []string, start, end time.Time) ([]doraclient.Trade, error) {
	f.getTradesCalls = append(f.getTradesCalls, getTradesCall{
		userID:       userID,
		orderBookIDs: append([]string(nil), orderBookIDs...),
		start:        start,
		end:          end,
	})
	if f.getTradesErr != nil {
		return nil, f.getTradesErr
	}
	var out []doraclient.Trade
	for _, obID := range orderBookIDs {
		out = append(out, f.tradesByOrderBK[obID]...)
	}
	return out, nil
}

func newBacktesterWithFake(t *testing.T, fake *fakeTradesClient, followedTrader uuid.UUID) *Backtester {
	t.Helper()
	cfg := Config{
		FollowedTrader:        followedTrader,
		PercentageOfAvailable: decimal.MustParse("0.5"),
		Leverage:              decimal.MustParse("1.0"),
	}
	s := New(cfg)
	return &Backtester{strategy: s, trades: fake}
}

func TestBacktesterRunWalksAllOrderBooks(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	ob1 := "ob-1"
	ob2 := "ob-2"
	other := "ob-other"

	t1 := time.Date(2026, 5, 26, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)

	tradesOB1 := []doraclient.Trade{
		{TransactionId: "tx-1", UserId: followed.String(), OrderBookId: ob1, Side: doraclient.SIDE_BUY, Price: "100", Quantity0: "1", CreatedAt: t1},
	}
	tradesOB2 := []doraclient.Trade{
		{TransactionId: "tx-2", UserId: followed.String(), OrderBookId: ob2, Side: doraclient.SIDE_SELL, Price: "101", Quantity0: "1", CreatedAt: t2},
	}

	fake := &fakeTradesClient{
		orderBooks: []string{ob1, ob2, other},
		tradesByOrderBK: map[string][]doraclient.Trade{
			ob1: tradesOB1,
			ob2: tradesOB2,
		},
	}

	b := newBacktesterWithFake(t, fake, followed)
	start := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC)
	result, err := b.Run(t.Context(), start, end)
	require.NoError(t, err)

	require.Len(t, fake.getTradesCalls, 3, "GetTrades must be called once per order book")
	for i, call := range fake.getTradesCalls {
		require.Equal(t, followed.String(), call.userID, "call %d must filter by followed trader", i)
		require.Len(t, call.orderBookIDs, 1, "call %d must target exactly one order book", i)
	}
	require.Equal(t, []string{ob1, ob2, other}, []string{
		fake.getTradesCalls[0].orderBookIDs[0],
		fake.getTradesCalls[1].orderBookIDs[0],
		fake.getTradesCalls[2].orderBookIDs[0],
	})

	require.Len(t, result.TradeRecords, 2)
}

func TestBacktesterRunSortsByCreatedAt(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	ob1 := "ob-1"
	ob2 := "ob-2"

	later := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	earlier := time.Date(2026, 5, 26, 9, 0, 0, 0, time.UTC)

	tradesOB1 := []doraclient.Trade{
		{TransactionId: "tx-later", UserId: followed.String(), OrderBookId: ob1, Side: doraclient.SIDE_BUY, Price: "100", Quantity0: "1", CreatedAt: later},
	}
	tradesOB2 := []doraclient.Trade{
		{TransactionId: "tx-earlier", UserId: followed.String(), OrderBookId: ob2, Side: doraclient.SIDE_SELL, Price: "101", Quantity0: "1", CreatedAt: earlier},
	}

	fake := &fakeTradesClient{
		orderBooks: []string{ob1, ob2},
		tradesByOrderBK: map[string][]doraclient.Trade{
			ob1: tradesOB1,
			ob2: tradesOB2,
		},
	}

	b := newBacktesterWithFake(t, fake, followed)
	start := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC)
	result, err := b.Run(t.Context(), start, end)
	require.NoError(t, err)

	require.Len(t, result.TradeRecords, 2)
	require.True(t, result.TradeRecords[0].Time.Before(result.TradeRecords[1].Time),
		"trades must be sorted by time ascending, got %v then %v",
		result.TradeRecords[0].Time, result.TradeRecords[1].Time)
}

func TestBacktesterRunFiltersNonFollowedTraders(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	stranger := uuid.New()
	ob1 := "ob-1"

	tradesOB1 := []doraclient.Trade{
		{TransactionId: "tx-followed", UserId: followed.String(), OrderBookId: ob1, Side: doraclient.SIDE_BUY, Price: "100", Quantity0: "1", CreatedAt: time.Date(2026, 5, 26, 10, 0, 0, 0, time.UTC)},
		{TransactionId: "tx-stranger", UserId: stranger.String(), OrderBookId: ob1, Side: doraclient.SIDE_BUY, Price: "100", Quantity0: "1", CreatedAt: time.Date(2026, 5, 26, 11, 0, 0, 0, time.UTC)},
	}

	fake := &fakeTradesClient{
		orderBooks: []string{ob1},
		tradesByOrderBK: map[string][]doraclient.Trade{
			ob1: tradesOB1,
		},
	}

	b := newBacktesterWithFake(t, fake, followed)
	start := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC)
	result, err := b.Run(t.Context(), start, end)
	require.NoError(t, err)

	require.Len(t, result.TradeRecords, 1, "only the followed trader's trades must be simulated")
}
