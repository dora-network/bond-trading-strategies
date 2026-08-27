package breakout_test

import (
	"testing"
	"time"

	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/strategy/breakout"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
)

var epoch = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

// defaultCfg returns a small-window config so tests fill the rolling windows
// quickly. LongVolWindow must be at least 2 for Ready() to ever become true.
func defaultCfg() breakout.Config {
	cfg := breakout.DefaultConfig()
	cfg.ShortVolWindow = 5
	cfg.LongVolWindow = 30
	cfg.CompressionThreshold = decimal.MustNew(5, 1) // 0.5
	cfg.ATRWindow = 14
	cfg.BreakoutATRMultiple = decimal.MustNew(15, 1) // 1.5
	cfg.ConfirmationBars = 2
	cfg.StopLossATR = decimal.MustNew(30, 1) // 3.0
	cfg.MinLongVolFloor = decimal.Zero
	cfg.InitialBalance = decimal.MustNew(10000, 0)
	cfg.Leverage = decimal.One
	return cfg
}

// flatObs returns an observation at a constant price so the price volatility
// window sees zero variation. Useful for filling windows before a test signal.
func flatObs(i int, price int64) types.YieldObservation {
	return types.YieldObservation{
		Time:           epoch.Add(time.Duration(i) * time.Minute),
		BondID:         "bond-A",
		YTM:            decimal.MustNew(5, 2),
		BenchmarkYield: decimal.MustNew(5, 2),
		Price:          decimal.MustNew(price, 0),
	}
}

// TestUpdate_HoldBeforeLongWindowReady verifies that the strategy emits
// SignalHold with Reason "warming_up" before the long volatility window
// has filled. The early ticks cannot trade on a partial distribution.
func TestUpdate_HoldBeforeLongWindowReady(t *testing.T) {
	t.Parallel()
	cfg := defaultCfg()
	s := breakout.New(cfg, nil)

	for i := 0; i < cfg.LongVolWindow-1; i++ {
		d, err := s.Update(flatObs(i, 100))
		require.NoError(t, err)
		assert.Equal(t, types.SignalHold, d.Signal(),
			"tick %d should be HOLD while long window is filling", i)
		assert.Equal(t, breakout.DecisionReasonWarmingUp, d.Reason(),
			"tick %d should report warming_up reason", i)
	}
}

// TestUpdate_CompressionArmsAfterFlatSeries verifies that after feeding
// LongVolWindow flat ticks, the strategy reports the compression flag set
// and a CompressionRatio below the configured threshold.
func TestUpdate_CompressionArmsAfterFlatSeries(t *testing.T) {
	t.Parallel()
	cfg := defaultCfg()
	s := breakout.New(cfg, nil)

	for i := 0; i < cfg.LongVolWindow; i++ {
		_, err := s.Update(flatObs(i, 100))
		require.NoError(t, err)
	}

	// One more tick on top of a fully-warmed, perfectly flat series:
	// ShortVol=0, LongVol=0, ratio=0 (no movement), so compression is
	// firmly armed.
	d, err := s.Update(flatObs(cfg.LongVolWindow, 100))
	require.NoError(t, err)
	assert.True(t, d.CompressionArmed,
		"compression should be armed after LongVolWindow flat ticks; ShortVol=%s LongVol=%s Ratio=%s",
		d.ShortVol.String(), d.LongVol.String(), d.CompressionRatio.String())
	assert.True(t, d.CompressionRatio.Cmp(cfg.CompressionThreshold) <= 0,
		"compression ratio (%s) should be <= threshold (%s) after flat series",
		d.CompressionRatio.String(), cfg.CompressionThreshold.String())
}

// TestUpdate_BuySignalOnBreakout constructs a price series: LongVolWindow
// flat ticks (compresses volatility and arms the flag), then 1 rising tick
// (price breaks above trigger). ConfirmationBars=1 simplifies the test:
// the trigger is anchored to the previous close, so a second same-price
// tick would NOT cross the trigger once lastPrice has moved. This is a
// property of the test series, not a bug in the algorithm.
func TestUpdate_BuySignalOnBreakout(t *testing.T) {
	t.Parallel()
	cfg := defaultCfg()
	cfg.ConfirmationBars = 1 // single tick above trigger fires
	s := breakout.New(cfg, nil)

	// Fill the long window with flat ticks (no price movement).
	for i := 0; i < cfg.LongVolWindow; i++ {
		_, err := s.Update(flatObs(i, 100))
		require.NoError(t, err)
	}

	// A clear upward move (≈ 10 %) crosses the trigger with the ATR we
	// have accumulated (≈ 0.33 on a previously flat series).
	d, err := s.Update(flatObs(cfg.LongVolWindow, 110))
	require.NoError(t, err)
	assert.Equal(t, types.SignalBuy, d.Signal(),
		"breakout tick should emit BUY; got Reason=%s", d.Reason())
}

// TestUpdate_SellSignalOnBreakout constructs a flat series followed by
// a falling close. The strategy must emit SignalSell.
func TestUpdate_SellSignalOnBreakout(t *testing.T) {
	t.Parallel()
	cfg := defaultCfg()
	cfg.ConfirmationBars = 1
	s := breakout.New(cfg, nil)

	for i := 0; i < cfg.LongVolWindow; i++ {
		_, err := s.Update(flatObs(i, 100))
		require.NoError(t, err)
	}

	d, err := s.Update(flatObs(cfg.LongVolWindow, 90))
	require.NoError(t, err)
	assert.Equal(t, types.SignalSell, d.Signal(),
		"breakdown tick should emit SELL; got Reason=%s", d.Reason())
}

// TestUpdate_ReasonContainsCompressionBreakout asserts that the breakout
// reason code is surfaced via the accessor, so persistence can record it.
func TestUpdate_ReasonContainsCompressionBreakout(t *testing.T) {
	t.Parallel()
	cfg := defaultCfg()
	cfg.ConfirmationBars = 1
	s := breakout.New(cfg, nil)

	for i := 0; i < cfg.LongVolWindow; i++ {
		_, err := s.Update(flatObs(i, 100))
		require.NoError(t, err)
	}

	d, err := s.Update(flatObs(cfg.LongVolWindow, 110))
	require.NoError(t, err)
	require.Equal(t, types.SignalBuy, d.Signal())
	assert.Equal(t, breakout.DecisionReasonCompressionEntry, d.Reason(),
		"breakout should publish the compression_breakout reason code")
}

// TestUpdate_HoldWhenLongVolBelowFloor verifies that MinLongVolFloor
// suppresses trading on a completely flat baseline (where LongVol=0).
// The strategy must report Reason "vol_too_low" instead of firing on noise.
func TestUpdate_HoldWhenLongVolBelowFloor(t *testing.T) {
	t.Parallel()
	cfg := defaultCfg()
	// Tiny but nonzero floor so a perfectly flat series (LongVol=0) trips it.
	cfg.MinLongVolFloor = decimal.MustNew(1, 4) // 0.0001
	cfg.ConfirmationBars = 1
	s := breakout.New(cfg, nil)

	var lastD breakout.Decision
	for i := 0; i < cfg.LongVolWindow; i++ {
		d, err := s.Update(flatObs(i, 100))
		require.NoError(t, err)
		lastD = d
	}
	// The last flat tick is the moment we have a full LongVolWindow of
	// identical prices — LongVol=0 < 0.0001 must trigger vol_too_low.
	assert.Equal(t, types.SignalHold, lastD.Signal())
	assert.Equal(t, breakout.DecisionReasonVolTooLow, lastD.Reason(),
		"expected vol_too_low after LongVolWindow flat ticks with a nonzero floor; got Reason=%s", lastD.Reason())
}
