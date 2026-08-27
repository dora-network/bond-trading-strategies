package momentum

import (
	"testing"
	"time"

	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/strategy/types"
)

// newAnchoredStrategy builds a price-mode strategy with small windows and
// feeds it rising observations so lastPrice and the ATR window are
// populated (mirroring a successful prefillWindow before a restart).
func newAnchoredStrategy(t *testing.T, stopATR float64) *Strategy {
	t.Helper()
	cfg := DefaultConfig()
	cfg.SignalSource = SignalSourcePrice
	cfg.FastWindow = 2
	cfg.SlowWindow = 3
	cfg.ATRWindow = 2
	stop, err := decimal.NewFromFloat64(stopATR)
	if err != nil {
		panic(err)
	}
	cfg.StopLossATR = stop
	s := New(cfg, nil)
	bond := "asset-A"
	for i, px := range []int64{100, 101, 102} {
		_, err := s.Update(types.YieldObservation{
			Time:   time.Now().Add(time.Duration(i) * time.Second),
			BondID: bond,
			Price:  decimal.MustNew(px, 0),
		})
		require.NoError(t, err)
	}
	return s
}

// TestSeedResumeAnchor_RestoresExitAnchor pins the restart-recovery fix:
// a position that exists when the run starts (server restart with an open
// exchange position, or an inherited position) must get an entry anchor so
// ShouldExit's stop-loss/take-profit branches — gated on entryATR.IsPos() —
// can fire. Pre-fix the anchors stayed zero for the position's whole life
// and only an opposite MA crossover could close it.
func TestSeedResumeAnchor_RestoresExitAnchor(t *testing.T) {
	s := newAnchoredStrategy(t, 1)

	s.mu.Lock()
	s.openSignal = types.SignalBuy // initializeBalances outcome for a long position
	s.bondQty = decimal.MustNew(5, 0)
	s.mu.Unlock()
	s.seedResumeAnchor()

	s.mu.RLock()
	entryPrice, entryATR := s.entryPrice, s.entryATR
	s.mu.RUnlock()
	assert.True(t, entryPrice.Cmp(decimal.MustNew(102, 0)) == 0,
		"anchor price should be the last clean price, got %s", entryPrice)
	assert.True(t, entryATR.IsPos(), "anchor ATR should be the ATR window mean, got %s", entryATR)

	// The actual contract: a crash tick fires the stop. abs diffs are
	// {1,1} over a 2-window → ATR=1, stopDist=1×1 → threshold 101; a
	// price of 100 is at/below it.
	d := Decision{
		price:  decimal.MustNew(100, 0),
		signal: types.SignalBuy,
	}
	shouldExit, reason := s.ShouldExit(types.SignalBuy, d, entryPrice, entryATR)
	assert.True(t, shouldExit, "stop-loss must fire for an inherited long after anchoring")
	assert.Equal(t, ExitReasonStopLoss, reason)
}

// TestSeedResumeAnchor_NoOpCases pins the guards: no seed on Hold (nothing
// to protect), no seed without price/ATR history (nothing to anchor to),
// and no overwrite of an anchor set by a real entry.
func TestSeedResumeAnchor_NoOpCases(t *testing.T) {
	t.Run("hold does not seed", func(t *testing.T) {
		s := newAnchoredStrategy(t, 1)
		s.seedResumeAnchor()
		assert.False(t, s.entryATR.IsPos(), "no anchor expected while flat")
	})

	t.Run("no history does not seed", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.SignalSource = SignalSourcePrice
		cfg.FastWindow = 2
		cfg.SlowWindow = 3
		cfg.ATRWindow = 2
		s := New(cfg, nil)
		s.mu.Lock()
		s.openSignal = types.SignalBuy
		s.mu.Unlock()
		s.seedResumeAnchor()
		assert.False(t, s.entryATR.IsPos(), "no anchor expected without price/ATR history")
	})

	t.Run("existing anchor is not overwritten", func(t *testing.T) {
		s := newAnchoredStrategy(t, 1)
		s.mu.Lock()
		s.openSignal = types.SignalBuy
		s.entryPrice = decimal.MustNew(90, 0) // true entry from a real fill
		s.entryATR = decimal.One
		s.mu.Unlock()
		s.seedResumeAnchor()
		s.mu.RLock()
		defer s.mu.RUnlock()
		assert.True(t, s.entryPrice.Cmp(decimal.MustNew(90, 0)) == 0,
			"existing anchor must survive, got %s", s.entryPrice)
	})
}
