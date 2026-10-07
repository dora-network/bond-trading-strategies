package breakout

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/prices"
	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
)

// sendTick fires a single tick through the prices channel.
func sendTick(t *testing.T, ch chan map[uuid.UUID]prices.AssetPrice, assetID string, price decimal.Decimal) {
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

// TestRunLoop_TickOnlyNoEntry is the cutover guard: ticks alone must
// never drive an entry after the bar cutover, however extreme their
// price. Flat bars keep the windows warm; extreme uptrend ticks must
// produce no order.
func TestRunLoop_TickOnlyNoEntry(t *testing.T) {
	s, _, barsCh, pricesCh, cleanup := runDrive(t)
	defer cleanup()

	// Flat bars: windows warm, compression armed, no signal.
	for i := range s.cfg.LongVolWindow {
		sendBar(t, barsCh, 50+i, decimal.MustNew(100, 0))
	}
	// Extreme uptrend ticks.
	for i := range 6 {
		sendTick(t, pricesCh, "asset-A", decimal.MustNew(int64(500+i*100), 0))
	}
	time.Sleep(100 * time.Millisecond)

	s.mu.RLock()
	got := s.openSignal
	lastPrice := s.lastPrice
	s.mu.RUnlock()
	assert.Equal(t, types.SignalHold, got,
		"ticks must not drive entries after the bar cutover; openSignal=%v", got)
	assert.True(t, lastPrice.Equal(decimal.MustNew(100, 0)),
		"ticks must not update the rolling-window state (lastPrice=%s, want 100)",
		lastPrice.String())
	s.mu.RLock()
	baseAsset := s.baseAssetID
	s.mu.RUnlock()
	assert.Equal(t, "asset-A", baseAsset,
		"run loop must store the resolved base asset for Decision.bondID")
}

// TestRunLoop_IntrabarStopLossOnTick: with a long open (entry 101,
// ATR 1, SL distance 1×ATR → stop at 100), a tick at 95 must trigger
// the intrabar stop-loss close without waiting for a closed bar.
func TestRunLoop_IntrabarStopLossOnTick(t *testing.T) {
	s, _, _, pricesCh, cleanup := runDrive(t)
	defer cleanup()

	s.cfg.StopLossATR = decimal.One
	s.mu.Lock()
	s.openSignal = types.SignalBuy
	s.entryPrice = decimal.MustNew(101, 0)
	s.entryATR = decimal.One
	s.mu.Unlock()

	sendTick(t, pricesCh, "asset-A", decimal.MustNew(95, 0))

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		s.mu.RLock()
		got := s.openSignal
		s.mu.RUnlock()
		if got == types.SignalHold {
			return // position closed by the intrabar stop
		}
		time.Sleep(5 * time.Millisecond)
	}
	s.mu.RLock()
	got := s.openSignal
	s.mu.RUnlock()
	t.Fatalf("tick at 95 should trigger the intrabar stop-loss close; openSignal=%v", got)
}

// TestRunLoop_PausedSuppressesIntrabarExit: the same stop-crossing tick
// must NOT close the position while the run is paused.
func TestRunLoop_PausedSuppressesIntrabarExit(t *testing.T) {
	s, msgs, _, pricesCh, cleanup := runDrive(t)
	defer cleanup()

	s.cfg.StopLossATR = decimal.One
	s.mu.Lock()
	s.openSignal = types.SignalBuy
	s.entryPrice = decimal.MustNew(101, 0)
	s.entryATR = decimal.One
	s.mu.Unlock()

	msgs <- strategy.Pause
	time.Sleep(50 * time.Millisecond)
	require.True(t, s.IsPaused())

	sendTick(t, pricesCh, "asset-A", decimal.MustNew(95, 0))
	time.Sleep(100 * time.Millisecond)

	s.mu.RLock()
	got := s.openSignal
	s.mu.RUnlock()
	assert.Equal(t, types.SignalBuy, got,
		"paused run must not close the position on a tick; openSignal=%v", got)
}
