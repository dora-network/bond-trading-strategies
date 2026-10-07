package meanreversion_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/meanreversion"
	"github.com/dora-network/bond-trading-strategies/strategy/meanreversion/meanreversionfakes"
)

// TestRun_NoCandleDataFailsFast: a live run on a book with zero candle
// history must fail at start (spec error-handling) instead of silently
// trading on ticks.
func TestRun_NoCandleDataFailsFast(t *testing.T) {
	t.Parallel()

	cfg := runLoopConfig(uuid.Must(uuid.NewV7()))
	feed := newBarFeed(nil)
	s := meanreversion.New(cfg, nil,
		meanreversion.WithLogger(slog.Default()),
		meanreversion.WithCandleFeed(feed),
	)
	meanreversion.SetLookupClient(s, fakeClient(decimal.Zero))
	store := &meanreversionfakes.FakeCandleHistoryStore{}
	store.CandleRangeReturns(nil, nil, nil)
	meanreversion.SetCandleHistoryStore(s, store)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	err := meanreversion.RunWithPrices(ctx, s, make(chan strategy.Message), nil)
	require.ErrorContains(t, err, "no candle data for order book")
	require.Equal(t, 0, feed.SubscribeBarsCallCount(), "must fail before subscribing bars")
}

// TestRun_CandleCoverageProceeds: any candle coverage at all lets the
// run start (the stream bootstrap handles the rest).
func TestRun_CandleCoverageProceeds(t *testing.T) {
	t.Parallel()

	cfg := runLoopConfig(uuid.Must(uuid.NewV7()))
	feed := newBarFeed(nil)
	s := meanreversion.New(cfg, nil,
		meanreversion.WithLogger(slog.Default()),
		meanreversion.WithCandleFeed(feed),
	)
	meanreversion.SetLookupClient(s, fakeClient(decimal.Zero))
	lo := time.Now().Add(-48 * time.Hour)
	hi := time.Now()
	store := &meanreversionfakes.FakeCandleHistoryStore{}
	store.CandleRangeReturns(&lo, &hi, nil)
	meanreversion.SetCandleHistoryStore(s, store)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- meanreversion.RunWithPrices(ctx, s, make(chan strategy.Message), nil)
	}()
	require.Eventually(t, func() bool {
		return feed.SubscribeBarsCallCount() == 1
	}, timeout, 10*time.Millisecond, "run must reach the bar subscription")
	cancel()
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(timeout):
		t.Fatal("run did not return after cancel")
	}
}
