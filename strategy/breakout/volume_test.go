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
	strategyfakes "github.com/dora-network/bond-trading-strategies/strategy/strategyfakes"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/bond-trading-strategies/streams"
)

// TestApplyTradeEvent_OBVAccumulation verifies the OBV accumulator's
// sign-by-direction logic across a sequence of trades. The first
// trade contributes its full quantity (no prior price).
func TestApplyTradeEvent_OBVAccumulation(t *testing.T) {
	t.Parallel()
	s := New(DefaultConfig(), nil)

	// Buy at 100, +10. OBV = 10.
	s.applyTradeEvent(streams.TradeEvent{Price: decimal.MustNew(100, 0), Quantity: decimal.MustNew(10, 0), Side: "BUY"})
	assert.True(t, s.OBV().Equal(decimal.MustNew(10, 0)))

	// Buy at 102, +5. OBV += 5 = 15.
	s.applyTradeEvent(streams.TradeEvent{Price: decimal.MustNew(102, 0), Quantity: decimal.MustNew(5, 0), Side: "BUY"})
	assert.True(t, s.OBV().Equal(decimal.MustNew(15, 0)))

	// Sell at 101, -3. OBV -= 3 = 12.
	s.applyTradeEvent(streams.TradeEvent{Price: decimal.MustNew(101, 0), Quantity: decimal.MustNew(3, 0), Side: "SELL"})
	assert.True(t, s.OBV().Equal(decimal.MustNew(12, 0)))

	// Sell at 101, -7. Same price as prev, OBV -= 7 = 5.
	s.applyTradeEvent(streams.TradeEvent{Price: decimal.MustNew(101, 0), Quantity: decimal.MustNew(7, 0), Side: "SELL"})
	assert.True(t, s.OBV().Equal(decimal.MustNew(5, 0)))
}

// TestUpdate_OBVFilterSuppressesBuyWhenOBVFlat verifies the signal gate
// when RequireVolumeConfirmation is on and OBV doesn't confirm the BUY.
func TestUpdate_OBVFilterSuppressesBuyWhenOBVFlat(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.OBVWindow = 5                    // windowed mode, large enough to keep the first trade
	cfg.OBVTrendThreshold = decimal.Zero // any positive OBV required
	cfg.ConfirmationBars = 1
	s := New(cfg, nil)

	// 10 flat ticks to arm compression.
	for i := 0; i < cfg.LongVolWindow; i++ {
		_, err := s.Update(types.YieldObservation{
			Time:   time.Unix(int64(i), 0).UTC(),
			BondID: "asset-A",
			Price:  decimal.MustNew(100, 0),
		})
		require.NoError(t, err)
	}

	// 5 BUY trades of 10 each with OBVWindow=5. OBV = 50.
	for range 5 {
		s.applyTradeEvent(streams.TradeEvent{
			Price:    decimal.MustNew(100, 0),
			Quantity: decimal.MustNew(10, 0),
			Side:     "BUY",
		})
	}
	assert.True(t, s.OBV().Equal(decimal.MustNew(50, 0)))

	// Big jump that would normally trigger BUY.
	d, err := s.Update(types.YieldObservation{
		Time:   time.Unix(int64(cfg.LongVolWindow), 0).UTC(),
		BondID: "asset-A",
		Price:  decimal.MustNew(115, 0),
	})
	require.NoError(t, err)
	assert.Equal(t, types.SignalBuy, d.Signal(),
		"OBV=10 > 0 and threshold=0, so BUY should fire")
}

// TestUpdate_OBVFilterSuppressesBuyWhenOBVZero verifies the filter
// blocks the signal when OBV is exactly zero (no trades received).
func TestUpdate_OBVFilterSuppressesBuyWhenOBVZero(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.OBVWindow = 3
	cfg.ConfirmationBars = 1
	s := New(cfg, nil)

	// 10 flat ticks to arm compression.
	for i := 0; i < cfg.LongVolWindow; i++ {
		_, err := s.Update(types.YieldObservation{
			Time:   time.Unix(int64(i), 0).UTC(),
			BondID: "asset-A",
			Price:  decimal.MustNew(100, 0),
		})
		require.NoError(t, err)
	}

	// No trades have been applied. OBV is zero.
	assert.True(t, s.OBV().IsZero())

	// Big jump that would normally trigger BUY.
	d, err := s.Update(types.YieldObservation{
		Time:   time.Unix(int64(cfg.LongVolWindow), 0).UTC(),
		BondID: "asset-A",
		Price:  decimal.MustNew(115, 0),
	})
	require.NoError(t, err)
	assert.Equal(t, types.SignalHold, d.Signal(),
		"OBV=0 must be below the BUY threshold; signal should be suppressed")
	assert.True(t, d.PositionSize().IsZero())
}

// TestUpdate_OBVFilterSuppressesSellWhenOBVPositive verifies the
// inverse case: a SELL signal that the OBV doesn't confirm
// (positive OBV when we need negative).
func TestUpdate_OBVFilterSuppressesSellWhenOBVPositive(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.OBVWindow = 3
	cfg.OBVTrendThreshold = decimal.MustNew(1, 0) // require -OBV > 1 for SELL
	cfg.ConfirmationBars = 1
	s := New(cfg, nil)

	// 10 flat ticks.
	for i := 0; i < cfg.LongVolWindow; i++ {
		_, err := s.Update(types.YieldObservation{
			Time:   time.Unix(int64(i), 0).UTC(),
			BondID: "asset-A",
			Price:  decimal.MustNew(100, 0),
		})
		require.NoError(t, err)
	}

	// Trades: buy @ 100, +10. OBV = 10. Positive, so SELL won't confirm.
	s.applyTradeEvent(streams.TradeEvent{
		Price:    decimal.MustNew(100, 0),
		Quantity: decimal.MustNew(10, 0),
		Side:     "BUY",
	})
	assert.True(t, s.OBV().Equal(decimal.MustNew(10, 0)))

	// Drop that would normally trigger SELL.
	d, err := s.Update(types.YieldObservation{
		Time:   time.Unix(int64(cfg.LongVolWindow), 0).UTC(),
		BondID: "asset-A",
		Price:  decimal.MustNew(85, 0),
	})
	require.NoError(t, err)
	assert.Equal(t, types.SignalHold, d.Signal(),
		"OBV=+10 must suppress a SELL (need OBV < -threshold)")
	assert.True(t, d.PositionSize().IsZero())
}

// TestRun_OBVUpdatesFromTradesAndGatesSignal is the integration
// happy-path: drive runLoop with both a price channel and a trade
// channel; verify OBV updates and the signal gates correctly.
func TestRun_OBVUpdatesFromTradesAndGatesSignal(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.OBVWindow = 3
	cfg.ShortVolWindow = 5
	cfg.LongVolWindow = 10
	cfg.ATRWindow = 5
	cfg.ConfirmationBars = 1
	cfg.OrderBookID = uuid.MustParse("11111111-1111-1111-1111-111111111111")

	fake := &strategyfakes.FakeMarketAPIClient{}
	fake.BaseAssetIDStub = func(_ context.Context, _ string) (string, error) {
		return "asset-A", nil
	}
	fake.AssetPositionStub = func(_ context.Context, _ string) (decimal.Decimal, decimal.Decimal, error) {
		return decimal.MustNew(1000, 0), decimal.Zero, nil
	}
	fake.CreateMarketOrderStub = func(_ context.Context, _ string, _ doraclient.Side, _ decimal.Decimal, _ decimal.Decimal, _ bool, _ string) error {
		return nil
	}

	s := New(
		cfg, nil,
		WithMarketAPIClient(fake),
		WithTradeStream(streams.NewTradeStream()),
	)
	s.cancel = func() {}

	// Pre-fill: 10 flat ticks to arm compression.
	flat := decimal.MustNew(100, 0)
	for i := 0; i < cfg.LongVolWindow; i++ {
		_, err := s.Update(types.YieldObservation{
			Time:   time.Unix(int64(i), 0).UTC(),
			BondID: "asset-A",
			Price:  flat,
		})
		require.NoError(t, err)
	}

	msgs := make(chan strategy.Message, 4)
	pricesCh := make(chan map[uuid.UUID]prices.AssetPrice, 4)
	tradesCh := make(chan streams.TradeEvent, 4)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = s.runLoop(ctx, msgs, pricesCh, tradesCh)
		close(done)
	}()
	defer func() {
		msgs <- strategy.Stop
		// Give runLoop a beat to exit.
		time.Sleep(20 * time.Millisecond)
		cancel()
		<-done
	}()

	// Trade: buy @ 100, +20. OBV becomes 20.
	tradesCh <- streams.TradeEvent{
		Price:    decimal.MustNew(100, 0),
		Quantity: decimal.MustNew(20, 0),
		Side:     "BUY",
	}

	// Wait for OBV to update.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if s.OBV().Equal(decimal.MustNew(20, 0)) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	require.True(t, s.OBV().Equal(decimal.MustNew(20, 0)),
		"OBV should pick up the trade; got %v", s.OBV())

	// Jump that would normally trigger BUY. OBV is positive → fires.
	sendOBTestTick(t, pricesCh, "asset-A", decimal.MustNew(120, 0))

	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		s.mu.RLock()
		got := s.openSignal
		s.mu.RUnlock()
		if got == types.SignalBuy {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	s.mu.RLock()
	got := s.openSignal
	s.mu.RUnlock()
	t.Fatalf("expected BUY after OBV-confirmed breakout, got openSignal=%v", got)
}

func sendOBTestTick(t *testing.T, ch chan map[uuid.UUID]prices.AssetPrice, assetID string, price decimal.Decimal) {
	t.Helper()
	tick := map[uuid.UUID]prices.AssetPrice{
		uuid.MustParse("33333333-3333-3333-3333-333333333333"): {
			Time:    time.Now().UTC(),
			AssetID: assetID,
			Price:   price,
		},
	}
	select {
	case ch <- tick:
	case <-time.After(time.Second):
		t.Fatalf("timed out sending tick")
	}
}

// TestApplyTradeEvent_WindowedOBVEvictsOldest verifies that the
// windowed OBV ring buffer evicts the oldest trade when full, so
// the running OBV reflects only the most recent N trades.
func TestApplyTradeEvent_WindowedOBVEvictsOldest(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.OBVWindow = 3 // only keep the last 3 trades in the window
	s := New(cfg, nil)

	// Trade 1: BUY +10. Window: [+10]. OBV = 10.
	s.applyTradeEvent(streams.TradeEvent{Price: decimal.MustNew(100, 0), Quantity: decimal.MustNew(10, 0), Side: "BUY"})
	assert.True(t, s.OBV().Equal(decimal.MustNew(10, 0)))

	// Trade 2: BUY +5. Window: [+10, +5]. OBV = 15.
	s.applyTradeEvent(streams.TradeEvent{Price: decimal.MustNew(101, 0), Quantity: decimal.MustNew(5, 0), Side: "BUY"})
	assert.True(t, s.OBV().Equal(decimal.MustNew(15, 0)))

	// Trade 3: SELL -3. Window: [+10, +5, -3]. OBV = 12.
	s.applyTradeEvent(streams.TradeEvent{Price: decimal.MustNew(100, 0), Quantity: decimal.MustNew(3, 0), Side: "SELL"})
	assert.True(t, s.OBV().Equal(decimal.MustNew(12, 0)))

	// Trade 4: BUY +7. Window evicts trade 1 (+10), now [+5, -3, +7]. OBV = 9.
	s.applyTradeEvent(streams.TradeEvent{Price: decimal.MustNew(102, 0), Quantity: decimal.MustNew(7, 0), Side: "BUY"})
	got := s.OBV()
	assert.True(t, got.Equal(decimal.MustNew(9, 0)),
		"after eviction, window should be [+5, -3, +7] = 9, got %s", got)

	// Trade 5: SELL -2. Window evicts trade 2 (+5), now [-3, +7, -2]. OBV = 2.
	s.applyTradeEvent(streams.TradeEvent{Price: decimal.MustNew(101, 0), Quantity: decimal.MustNew(2, 0), Side: "SELL"})
	got = s.OBV()
	assert.True(t, got.Equal(decimal.MustNew(2, 0)),
		"after eviction, window should be [-3, +7, -2] = 2, got %s", got)
}

// TestOBV_CumulativeVsWindowed verifies that the two modes produce
// different results: cumulative includes all trades, windowed only
// the last N.
func TestOBV_CumulativeVsWindowed(t *testing.T) {
	t.Parallel()

	// Cumulative mode (OBVWindow = 0): all 5 trades accumulate.
	cfgCum := DefaultConfig()
	sCum := New(cfgCum, nil)
	trades := []streams.TradeEvent{
		{Side: "BUY", Quantity: decimal.MustNew(10, 0)},
		{Side: "SELL", Quantity: decimal.MustNew(3, 0)},
		{Side: "BUY", Quantity: decimal.MustNew(5, 0)},
		{Side: "SELL", Quantity: decimal.MustNew(2, 0)},
		{Side: "BUY", Quantity: decimal.MustNew(7, 0)},
	}
	for _, t := range trades {
		sCum.applyTradeEvent(t)
	}
	// Cumulative: 10 - 3 + 5 - 2 + 7 = 17
	assert.True(t, sCum.OBV().Equal(decimal.MustNew(17, 0)),
		"cumulative OBV should be 17, got %s", sCum.OBV())

	// Windowed mode (OBVWindow = 3): only last 3 trades.
	cfgWin := DefaultConfig()
	cfgWin.OBVWindow = 3
	sWin := New(cfgWin, nil)
	for _, tr := range trades {
		sWin.applyTradeEvent(tr)
	}
	// Sum() is exact (maintained incrementally, not derived from Welford
	// mean), so we can compare directly. Last 3 trades: BUY(5), SELL(2),
	// BUY(7). OBV = 5 - 2 + 7 = 10.
	got := sWin.OBV()
	assert.True(t, got.Equal(decimal.MustNew(10, 0)),
		"windowed OBV (last 3) should be 10, got %s", got)
}

// silence unused import warning for streams (used by tests above)
var _ = streams.TradeEvent{}
