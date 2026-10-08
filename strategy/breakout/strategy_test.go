package breakout_test

import (
	"testing"
	"time"

	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/candles"
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
	cfg.Resolution = "1m"
	cfg.InitialBalance = decimal.MustNew(10000, 0)
	cfg.Leverage = decimal.One
	return cfg
}

// flatBar returns a bar at a constant price (O=H=L=C) so the volatility
// windows see zero variation. Useful for filling windows before a test
// signal.
func flatBar(i int, price int64) types.Bar {
	p := decimal.MustNew(price, 0)
	return types.Bar{
		Time:  epoch.Add(time.Duration(i) * time.Minute),
		Open:  p,
		High:  p,
		Low:   p,
		Close: p,
	}
}

// TestDefaultConfigBars pins the bar-calibrated defaults: 5m resolution,
// short/ATR windows of 12 bars (≈1h), long window of 96 bars (≈8h), and
// 3 confirmation bars.
func TestDefaultConfigBars(t *testing.T) {
	t.Parallel()
	cfg := breakout.DefaultConfig()
	assert.Equal(t, candles.Resolution5m, cfg.Resolution)
	assert.Equal(t, 12, cfg.ShortVolWindow)
	assert.Equal(t, 96, cfg.LongVolWindow)
	assert.Equal(t, 12, cfg.ATRWindow)
	assert.Equal(t, 3, cfg.ConfirmationBars)
	assert.Equal(t, decimal.MustNew(3, 1), cfg.CompressionThreshold)
	assert.Equal(t, decimal.MustNew(15, 1), cfg.BreakoutATRMultiple)
	assert.Equal(t, decimal.MustNew(20, 0), cfg.StopLossATR)
	assert.True(t, cfg.TakeProfitATR.IsZero())
}

// TestUpdate_HoldBeforeLongWindowReady verifies that the strategy emits
// SignalHold with Reason "warming_up" before the long volatility window
// has filled. The early bars cannot trade on a partial distribution.
func TestUpdate_HoldBeforeLongWindowReady(t *testing.T) {
	t.Parallel()
	cfg := defaultCfg()
	s := breakout.New(cfg, nil)

	for i := range cfg.LongVolWindow - 1 {
		d, err := s.Update(flatBar(i, 100))
		require.NoError(t, err)
		assert.Equal(t, types.SignalHold, d.Signal(),
			"bar %d should be HOLD while long window is filling", i)
		assert.Equal(t, breakout.DecisionReasonWarmingUp, d.Reason(),
			"bar %d should report warming_up reason", i)
	}
}

// TestUpdate_CompressionArmsAfterFlatSeries verifies that after feeding
// LongVolWindow flat bars, the strategy reports the compression flag set
// and a CompressionRatio below the configured threshold.
func TestUpdate_CompressionArmsAfterFlatSeries(t *testing.T) {
	t.Parallel()
	cfg := defaultCfg()
	s := breakout.New(cfg, nil)

	for i := range cfg.LongVolWindow {
		_, err := s.Update(flatBar(i, 100))
		require.NoError(t, err)
	}

	// One more bar on top of a fully-warmed, perfectly flat series:
	// ShortVol=0, LongVol=0, ratio=0 (no movement), so compression is
	// firmly armed.
	d, err := s.Update(flatBar(cfg.LongVolWindow, 100))
	require.NoError(t, err)
	assert.True(t, d.CompressionArmed,
		"compression should be armed after LongVolWindow flat bars; ShortVol=%s LongVol=%s Ratio=%s",
		d.ShortVol.String(), d.LongVol.String(), d.CompressionRatio.String())
	assert.True(t, d.CompressionRatio.Cmp(cfg.CompressionThreshold) <= 0,
		"compression ratio (%s) should be <= threshold (%s) after flat series",
		d.CompressionRatio.String(), cfg.CompressionThreshold.String())
}

// TestUpdate_BuySignalOnBreakout constructs a price series: LongVolWindow
// flat bars (compresses volatility and arms the flag), then 1 rising bar
// (close breaks above trigger). ConfirmationBars=1 simplifies the test:
// the trigger is anchored to the previous close, so a second same-price
// bar would NOT cross the trigger once lastPrice has moved. This is a
// property of the test series, not a bug in the algorithm.
func TestUpdate_BuySignalOnBreakout(t *testing.T) {
	t.Parallel()
	cfg := defaultCfg()
	cfg.ConfirmationBars = 1 // single bar above trigger fires
	s := breakout.New(cfg, nil)

	// Fill the long window with flat bars (no price movement).
	for i := range cfg.LongVolWindow {
		_, err := s.Update(flatBar(i, 100))
		require.NoError(t, err)
	}

	// A clear upward move (≈ 10 %) crosses the trigger with the ATR we
	// have accumulated (≈ 0.33 on a previously flat series).
	d, err := s.Update(flatBar(cfg.LongVolWindow, 110))
	require.NoError(t, err)
	assert.Equal(t, types.SignalBuy, d.Signal(),
		"breakout bar should emit BUY; got Reason=%s", d.Reason())
}

// TestUpdate_SellSignalOnBreakout constructs a flat series followed by
// a falling close. The strategy must emit SignalSell.
func TestUpdate_SellSignalOnBreakout(t *testing.T) {
	t.Parallel()
	cfg := defaultCfg()
	cfg.ConfirmationBars = 1
	s := breakout.New(cfg, nil)

	for i := range cfg.LongVolWindow {
		_, err := s.Update(flatBar(i, 100))
		require.NoError(t, err)
	}

	d, err := s.Update(flatBar(cfg.LongVolWindow, 90))
	require.NoError(t, err)
	assert.Equal(t, types.SignalSell, d.Signal(),
		"breakdown bar should emit SELL; got Reason=%s", d.Reason())
}

// TestUpdate_ReasonContainsCompressionBreakout asserts that the breakout
// reason code is surfaced via the accessor, so persistence can record it.
func TestUpdate_ReasonContainsCompressionBreakout(t *testing.T) {
	t.Parallel()
	cfg := defaultCfg()
	cfg.ConfirmationBars = 1
	s := breakout.New(cfg, nil)

	for i := range cfg.LongVolWindow {
		_, err := s.Update(flatBar(i, 100))
		require.NoError(t, err)
	}

	d, err := s.Update(flatBar(cfg.LongVolWindow, 110))
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
	for i := range cfg.LongVolWindow {
		d, err := s.Update(flatBar(i, 100))
		require.NoError(t, err)
		lastD = d
	}
	// The last flat bar is the moment we have a full LongVolWindow of
	// identical prices — LongVol=0 < 0.0001 must trigger vol_too_low.
	assert.Equal(t, types.SignalHold, lastD.Signal())
	assert.Equal(t, breakout.DecisionReasonVolTooLow, lastD.Reason(),
		"expected vol_too_low after LongVolWindow flat bars with a nonzero floor; got Reason=%s", lastD.Reason())
}

// TestConfirmationBarsAreBars: with ConfirmationBars=3, three consecutive
// closes beyond the trigger are required — the signal fires on the 3rd
// bar, not on the 1st or 2nd.
func TestConfirmationBarsAreBars(t *testing.T) {
	t.Parallel()
	cfg := defaultCfg()
	cfg.ConfirmationBars = 3
	s := breakout.New(cfg, nil)

	for i := range cfg.LongVolWindow {
		_, err := s.Update(flatBar(i, 100))
		require.NoError(t, err)
	}

	// Rising bars well above any trigger the flat baseline can produce.
	// Each bar's close moves up, so each recomputed trigger (prevClose ±
	// k·ATR) is cleared again on the next bar.
	for i := 1; i <= 3; i++ {
		d, err := s.Update(flatBar(cfg.LongVolWindow+i, int64(100+10*i)))
		require.NoError(t, err)
		if i < 3 {
			assert.Equal(t, types.SignalHold, d.Signal(),
				"bar %d of 3 must not fire; got Reason=%s", i, d.Reason())
			assert.Equal(t, i, d.BarsAboveTrigger,
				"bars-above-trigger counter must count bars, got %d after bar %d", d.BarsAboveTrigger, i)
		} else {
			assert.Equal(t, types.SignalBuy, d.Signal(),
				"3rd consecutive close beyond the trigger must fire; got Reason=%s", d.Reason())
		}
	}
}

// TestTrueRangeFeedsATRWindow: with High/Low ≠ Close, the ATR must be
// the mean true range (max(H-L, |H-prevClose|, |L-prevClose|)), not the
// mean |Δclose|. Bars alternate with wide ranges but identical closes:
// TR per bar = 4 (H-L dominates), Δclose = 0. ATR must be 4.
func TestTrueRangeFeedsATRWindow(t *testing.T) {
	cfg := defaultCfg()
	cfg.ATRWindow = 4
	cfg.LongVolWindow = 4 // reach Ready() within the fixture so d.ATR is populated
	s := breakout.New(cfg, nil)

	for i := range cfg.ATRWindow {
		bar := types.Bar{
			Time:  epoch.Add(time.Duration(i) * time.Minute),
			Open:  decimal.MustNew(102, 0),
			High:  decimal.MustNew(104, 0),
			Low:   decimal.MustNew(100, 0),
			Close: decimal.MustNew(102, 0),
		}
		if i%2 == 1 {
			// Alternate the range side; closes stay identical.
			bar.High = decimal.MustNew(104, 0)
			bar.Low = decimal.MustNew(100, 0)
		}
		d, err := s.Update(bar)
		require.NoError(t, err)
		if i == cfg.ATRWindow-1 {
			assert.Equal(t, decimal.MustNew(4, 0), d.ATR,
				"ATR must be the mean true range (4), not |Δclose| (0); got %s", d.ATR.String())
		}
	}
}
