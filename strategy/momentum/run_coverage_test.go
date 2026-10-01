package momentum_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	strategypkg "github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/momentum"
	"github.com/dora-network/bond-trading-strategies/strategy/momentum/momentumfakes"
	"github.com/dora-network/bond-trading-strategies/strategy/strategyfakes"
)

func coverageStrategy(t *testing.T, store *momentumfakes.FakeCandleHistoryStore, feed *strategyfakes.FakeCandleFeed) *momentum.Strategy {
	t.Helper()
	s := momentum.New(momRunConfig(uuid.Must(uuid.NewV7())), nil,
		momentum.WithMarketAPIClient(momFakeClient(0)),
		momentum.WithCandleFeed(feed),
	)
	momentum.SetCandleHistoryStore(s, store)
	return s
}

// TestRun_NoCandleDataFailsFast: a live run on a book with zero candle
// history must fail at start (spec error-handling) instead of silently
// trading on ticks.
func TestRun_NoCandleDataFailsFast(t *testing.T) {
	t.Parallel()

	feed := newBarFeed(nil)
	store := &momentumfakes.FakeCandleHistoryStore{}
	store.CandleRangeReturns(nil, nil, nil)
	s := coverageStrategy(t, store, feed)

	ctx, cancel := context.WithTimeout(context.Background(), runLoopTimeout)
	defer cancel()
	err := momentum.RunSync(ctx, s, make(chan strategypkg.Message), nil)
	require.ErrorContains(t, err, "no candle data for order book")
	require.Equal(t, 0, feed.SubscribeBarsCallCount(), "must fail before subscribing bars")
}

// TestRun_CandleCoverageProceeds: any candle coverage at all lets the
// run start (the stream bootstrap handles the rest).
func TestRun_CandleCoverageProceeds(t *testing.T) {
	t.Parallel()

	feed := newBarFeed(nil)
	lo := time.Now().Add(-48 * time.Hour)
	hi := time.Now()
	store := &momentumfakes.FakeCandleHistoryStore{}
	store.CandleRangeReturns(&lo, &hi, nil)
	s := coverageStrategy(t, store, feed)

	ctx, cancel := context.WithTimeout(context.Background(), runLoopTimeout)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- momentum.RunSync(ctx, s, make(chan strategypkg.Message), nil)
	}()
	require.Eventually(t, func() bool {
		return feed.SubscribeBarsCallCount() == 1
	}, runLoopTimeout, 10*time.Millisecond, "run must reach the bar subscription")
	cancel()
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(runLoopTimeout):
		t.Fatal("run did not return after cancel")
	}
}
