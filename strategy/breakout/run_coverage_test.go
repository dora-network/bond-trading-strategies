package breakout

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/breakout/breakoutfakes"
	"github.com/dora-network/bond-trading-strategies/strategy/strategyfakes"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
)

const coverageTimeout = 3 * time.Second

func coverageStrategy(t *testing.T, feed *strategyfakes.FakeCandleFeed) *Strategy {
	t.Helper()
	cfg := DefaultConfig()
	cfg.OrderBookID = uuid.Must(uuid.NewV7())
	fake := &strategyfakes.FakeMarketAPIClient{}
	fake.BaseAssetIDReturns("asset-A", nil)
	fake.AssetPositionReturns(decimal.Zero, decimal.Zero, nil)
	s := New(cfg, nil, WithMarketAPIClient(fake), WithCandleFeed(feed))
	s.cancel = func() {}
	return s
}

// TestRun_NoCandleDataFailsFast: a live run on a book with zero candle
// history must fail at start (spec error-handling) instead of silently
// trading on ticks.
func TestRun_NoCandleDataFailsFast(t *testing.T) {
	t.Parallel()

	feed := &strategyfakes.FakeCandleFeed{}
	feed.SubscribeBarsReturns(make(chan types.Bar, 1), func() {}, nil)
	s := coverageStrategy(t, feed)
	store := &breakoutfakes.FakeCandleHistoryStore{}
	store.CandleRangeReturns(nil, nil, nil)
	SetCandleHistoryStore(s, store)

	err := s.runLoop(context.Background(), make(chan strategy.Message), nil, nil)
	require.ErrorContains(t, err, "no candle data for order book")
	require.Equal(t, 0, feed.SubscribeBarsCallCount(), "must fail before subscribing bars")
}

// TestRun_CandleCoverageProceeds: any candle coverage at all lets the
// run start (the stream bootstrap handles the rest).
func TestRun_CandleCoverageProceeds(t *testing.T) {
	t.Parallel()

	feed := &strategyfakes.FakeCandleFeed{}
	feed.SubscribeBarsReturns(make(chan types.Bar, 1), func() {}, nil)
	s := coverageStrategy(t, feed)
	lo := time.Now().Add(-48 * time.Hour)
	hi := time.Now()
	store := &breakoutfakes.FakeCandleHistoryStore{}
	store.CandleRangeReturns(&lo, &hi, nil)
	SetCandleHistoryStore(s, store)

	ctx, cancel := context.WithTimeout(context.Background(), coverageTimeout)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.runLoop(ctx, make(chan strategy.Message), nil, nil)
	}()
	require.Eventually(t, func() bool {
		return feed.SubscribeBarsCallCount() == 1
	}, coverageTimeout, 10*time.Millisecond, "run must reach the bar subscription")
	cancel()
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(coverageTimeout):
		t.Fatal("run did not return after cancel")
	}
}
