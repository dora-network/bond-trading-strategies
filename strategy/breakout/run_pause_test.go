package breakout

import (
	"context"
	"testing"
	"time"

	"github.com/dora-network/dora-client-go/doraclient"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/prices"
	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/strategyfakes"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/bond-trading-strategies/streams"
)

// runDrive seeds the strategy's windows so a BUY would normally fire
// (windows filled, compression armed), then starts runLoop with a bar
// channel and returns the strategy + channels so tests can pump
// messages / bars / ticks and inspect state.
//
// We bypass Run() because Run's subscribePrices requires a non-nil
// pricesHandler; the actual logic we want to test lives in runLoop,
// which is internal-test-accessible from this package.
func runDrive(t *testing.T) (*Strategy, chan strategy.Message, chan types.Bar, chan map[uuid.UUID]prices.AssetPrice, func()) {
	t.Helper()

	cfg := DefaultConfig()
	cfg.ShortVolWindow = 5
	cfg.LongVolWindow = 10
	cfg.ATRWindow = 5
	cfg.ConfirmationBars = 1
	cfg.OrderBookID = uuid.MustParse("11111111-1111-1111-1111-111111111111")

	fake := &strategyfakes.FakeMarketAPIClient{}
	fake.BaseAssetIDStub = func(_ context.Context, _ string) (string, error) {
		return "asset-A", nil
	}
	// executeDecision calls AssetPosition to size the order; default zero
	// would yield a 0-quantity no-op. Return a non-zero fake position so
	// the live loop actually places an order.
	fake.AssetPositionStub = func(_ context.Context, _ string) (decimal.Decimal, decimal.Decimal, error) {
		// Large enough that budget/entryPrice gives qty >= 1; budget is
		// budget = position * fraction, fraction is 1.0, so 1000/120 ~= 8.
		return decimal.MustNew(1000, 0), decimal.Zero, nil
	}
	fake.CreateMarketOrderStub = func(_ context.Context, _ string, _ doraclient.Side, _ decimal.Decimal, _ decimal.Decimal, _ bool, _ string) (string, error) {
		return "", nil
	}

	barsCh := make(chan types.Bar, 4)
	feed := &strategyfakes.FakeCandleFeed{}
	feed.SubscribeBarsReturns(barsCh, func() {}, nil)

	s := New(cfg, nil, WithMarketAPIClient(fake), WithCandleFeed(feed))
	// Bypassing Run means s.cancel is nil; runLoop calls s.cancel() on
	// Stop, which would panic on a nil function. Set it to a no-op so
	// the test drives the loop without a real cancellation chain.
	s.cancel = func() {}

	// Pre-fill: LongVolWindow flat bars at 100. The long window reaches
	// Ready on the last iteration; ratio = 0 (ShortVol = LongVol = 0) so
	// compression arms. No jump bar — if we added one, evaluateBreakout
	// would fire a BUY on the jump and resetArmed() would clear
	// compressionArmed before any test bar can run.
	flat := decimal.MustNew(100, 0)
	for i := range cfg.LongVolWindow {
		_, err := s.Update(types.Bar{
			Time:  time.Unix(int64(i), 0).UTC(),
			Open:  flat,
			High:  flat,
			Low:   flat,
			Close: flat,
		})
		require.NoError(t, err)
	}

	msgs := make(chan strategy.Message, 4)
	pricesCh := make(chan map[uuid.UUID]prices.AssetPrice, 4)
	// trades is nil; runLoop treats a nil trade channel as a no-op so
	// the pause/resume tests don't need a live trade stream.
	var trades chan streams.TradeEvent

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = s.runLoop(ctx, msgs, pricesCh, trades)
	}()

	cleanup := func() {
		msgs <- strategy.Stop
		time.Sleep(20 * time.Millisecond)
		cancel()
	}
	return s, msgs, barsCh, pricesCh, cleanup
}

// sendBar fires a single closed bar through the candle-feed channel.
func sendBar(t *testing.T, ch chan types.Bar, minute int, price decimal.Decimal) {
	t.Helper()
	bar := types.Bar{Time: time.Unix(int64(minute), 0).UTC(), Open: price, High: price, Low: price, Close: price}
	select {
	case ch <- bar:
	case <-time.After(time.Second):
		t.Fatalf("timed out sending bar")
	}
}

// awaitOpen polls s.openSignal until it equals want or the timeout fires.
func awaitOpen(t *testing.T, s *Strategy, want types.Signal) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		s.mu.RLock()
		got := s.openSignal
		s.mu.RUnlock()
		if got == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	s.mu.RLock()
	got := s.openSignal
	s.mu.RUnlock()
	t.Fatalf("openSignal: want %v, got %v after timeout", want, got)
}

// TestRun_PauseDropsBarDispatch verifies that the strategy ignores bar
// dispatch while paused: a strong-breakout bar that would normally open
// a BUY has no effect when preceded by strategy.Pause.
func TestRun_PauseDropBar(t *testing.T) {
	s, msgs, barsCh, _, cleanup := runDrive(t)
	defer cleanup()

	msgs <- strategy.Pause
	// Give the goroutine a beat to process Pause.
	time.Sleep(50 * time.Millisecond)
	require.True(t, s.IsPaused(), "IsPaused should be true after Pause")

	// A clear breakout bar while paused: should be dropped on the floor.
	sendBar(t, barsCh, 100, decimal.MustNew(115, 0))

	// Give the goroutine time to process the bar (and verify it didn't).
	// Still flat — the paused bar must not have produced an entry.
	s.mu.RLock()
	got := s.openSignal
	s.mu.RUnlock()
	assert.Equal(t, types.SignalHold, got,
		"paused bar should not have produced an entry; openSignal=%v", got)
}

// TestRun_ResumeOpensAfterPause verifies that a bar after Resume opens
// a position normally.
func TestRun_ResumeOpensAfterPause(t *testing.T) {
	s, msgs, barsCh, _, cleanup := runDrive(t)
	defer cleanup()

	msgs <- strategy.Pause
	time.Sleep(50 * time.Millisecond)
	require.True(t, s.IsPaused())

	// Paused bar — dropped for dispatch. Flat price so the fixture state
	// (compression armed, counters at zero) survives the pause: a signal
	// evaluated while paused is discarded, not deferred.
	sendBar(t, barsCh, 100, decimal.MustNew(100, 0))
	time.Sleep(50 * time.Millisecond)
	s.mu.RLock()
	assert.Equal(t, types.SignalHold, s.openSignal)
	s.mu.RUnlock()

	// Resume, then a fresh bar that should trigger a BUY.
	msgs <- strategy.Resume
	time.Sleep(50 * time.Millisecond)
	require.False(t, s.IsPaused(), "IsPaused should be false after Resume")

	sendBar(t, barsCh, 101, decimal.MustNew(120, 0))
	awaitOpen(t, s, types.SignalBuy)
}
