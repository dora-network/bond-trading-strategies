package momentum_test

import (
	"testing"

	"github.com/dora-network/bond-trading-strategies/candles"
	"github.com/dora-network/bond-trading-strategies/strategy/momentum"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/require"
)

func newTestStrategy(t *testing.T, source string) *momentum.Strategy {
	t.Helper()
	cfg := momentum.DefaultConfig()
	cfg.SignalSource = source
	cfg.FastWindow = 3
	cfg.SlowWindow = 5
	cfg.ATRWindow = 3
	cfg.VolumeAvgWindow = 0 // gate tested separately in TestVolumeGate*
	return momentum.New(cfg, nil)
}

func ytmP(d float64) decimal.Decimal {
	return decimal.MustNew(int64(d*1e6), 6)
}

// bar builds a closed bar: High/Low default to the close so the true
// range is |Δclose| unless the caller overrides them.
func bar(close decimal.Decimal) types.Bar {
	return types.Bar{Open: close, High: close, Low: close, Close: close}
}

// risingBars feeds an uptrend: close steps up each bar, YTM steady.
func risingBars(n int) []types.Bar {
	b := make([]types.Bar, n)
	for i := range b {
		b[i] = bar(decimal.MustNew(int64(100+i), 0))
		b[i].CloseYTM = ytmP(0.05)
		b[i].BenchmarkYield = decimal.MustNew(4, 2)
	}
	return b
}

func TestUpdate_PriceSource_RisingTrend_IsBuy(t *testing.T) {
	s := newTestStrategy(t, momentum.SignalSourcePrice)
	var last momentum.Decision
	for _, b := range risingBars(7) {
		d, err := s.Update(b)
		require.NoError(t, err)
		last = d
	}
	require.Equal(t, types.SignalBuy, last.Signal()) // price up -> long
	require.Equal(t, "up", last.Trend)
	require.True(t, last.Price().Equal(decimal.MustNew(106, 0)), "decision price must be the bar close")
}

func TestUpdate_YTMSource_RisingTrend_IsSell(t *testing.T) {
	// YTM rising = price falling = downtrend -> Sell (direction inverted).
	s := newTestStrategy(t, momentum.SignalSourceYTM)
	var last momentum.Decision
	for i := range 7 {
		b := bar(decimal.MustNew(100, 0))
		b.CloseYTM = ytmP(0.04 + float64(i)*0.001)
		d, err := s.Update(b)
		require.NoError(t, err)
		last = d
	}
	require.Equal(t, types.SignalSell, last.Signal())
}

func TestUpdate_SpreadSource_RisingTrend_IsSell(t *testing.T) {
	// Spread rising (YTM up, benchmark steady) = cheapening = Sell.
	s := newTestStrategy(t, momentum.SignalSourceSpread)
	var last momentum.Decision
	for i := range 7 {
		b := bar(decimal.MustNew(100, 0))
		b.CloseYTM = ytmP(0.05 + float64(i)*0.001)
		b.BenchmarkYield = decimal.MustNew(4, 2)
		d, err := s.Update(b)
		require.NoError(t, err)
		last = d
	}
	require.Equal(t, types.SignalSell, last.Signal())
}

func TestUpdate_WarmingUp_HoldsBeforeWindowsReady(t *testing.T) {
	s := newTestStrategy(t, momentum.SignalSourcePrice)
	d, err := s.Update(risingBars(1)[0])
	require.NoError(t, err)
	require.Equal(t, types.SignalHold, d.Signal())
	require.Equal(t, momentum.DecisionReasonWarmingUp, d.Reason())
}

func TestUpdate_YTMSource_ZeroCloseYTM_BarDropped(t *testing.T) {
	s := newTestStrategy(t, momentum.SignalSourceYTM)
	b := bar(decimal.MustNew(100, 0)) // zero CloseYTM
	d, err := s.Update(b)
	require.NoError(t, err)
	require.Equal(t, types.SignalHold, d.Signal()) // bar dropped, no window update
}

func TestDefaultConfigBars(t *testing.T) {
	cfg := momentum.DefaultConfig()
	require.Equal(t, candles.Resolution15m, cfg.Resolution)
	require.Equal(t, 8, cfg.FastWindow)
	require.Equal(t, 96, cfg.SlowWindow)
	require.Equal(t, 24, cfg.ATRWindow)
	require.Equal(t, 20, cfg.VolumeAvgWindow)
	require.True(t, cfg.VolumeRatioThreshold.Equal(decimal.One))
}

// TestTrueRangeFeedsATRWindow pins that ATR is the mean true range, not
// the mean |Δclose|: two bars with wide H/L ranges but unchanged closes
// would give |Δclose| = 0 while the true ranges are 20 and 30.
func TestTrueRangeFeedsATRWindow(t *testing.T) {
	cfg := momentum.DefaultConfig()
	cfg.SignalSource = momentum.SignalSourcePrice
	cfg.FastWindow = 2
	cfg.SlowWindow = 3
	cfg.ATRWindow = 2
	cfg.VolumeAvgWindow = 0
	s := momentum.New(cfg, nil)

	b1 := bar(decimal.MustNew(100, 0))
	b1.High, b1.Low = decimal.MustNew(110, 0), decimal.MustNew(90, 0) // TR 20
	b2 := bar(decimal.MustNew(100, 0))
	b2.High, b2.Low = decimal.MustNew(130, 0), decimal.MustNew(100, 0) // TR max(30,|130-100|,0)=30
	for _, b := range []types.Bar{b1, b2} {
		_, err := s.Update(b)
		require.NoError(t, err)
	}
	d, err := s.Update(b1)
	require.NoError(t, err)
	// Window (2) holds TRs 30 and 20 -> mean 25. Mean |Δclose| would be 0.
	require.True(t, d.ATR.Equal(decimal.MustNew(25, 0)),
		"ATR must be the mean true range (25), got %s", d.ATR)
}

// TestVolumeGateBlocksThinVolume: warm volume window mean 10; a bar with
// volume 5 (ratio 1.0) downgrades a Buy signal to Hold, a follow-up bar
// with volume 15 lets the entry through.
func TestVolumeGateBlocksThinVolume(t *testing.T) {
	cfg := momentum.DefaultConfig()
	cfg.SignalSource = momentum.SignalSourcePrice
	cfg.FastWindow = 3
	cfg.SlowWindow = 5
	cfg.ATRWindow = 3
	cfg.VolumeAvgWindow = 2
	cfg.VolumeRatioThreshold = decimal.One
	s := momentum.New(cfg, nil)

	// Six rising bars at volume 10: slowWin (5) fills on bar 5 and the
	// signal is Buy, but the bar index is still warming until then.
	for i, b := range risingBars(6) {
		b.Volume = decimal.MustNew(10, 0)
		d, err := s.Update(b)
		require.NoError(t, err)
		if i >= 4 {
			require.Equal(t, types.SignalBuy, d.Signal(), "full-volume rising bar %d must be a Buy", i)
		}
	}

	// Thin-volume bar: same rising close trend, volume 5 < 1.0×mean(10).
	thin := risingBars(1)[0]
	thin.Close = decimal.MustNew(200, 0)
	thin.Volume = decimal.MustNew(5, 0)
	d, err := s.Update(thin)
	require.NoError(t, err)
	require.Equal(t, types.SignalHold, d.Signal(), "thin volume must downgrade the entry to Hold")
	require.Equal(t, momentum.DecisionReasonVolumeNotConfirmed, d.Reason())

	// Confirmed-volume bar with the same trend lets the entry through.
	fat := risingBars(1)[0]
	fat.Close = decimal.MustNew(201, 0)
	fat.Volume = decimal.MustNew(15, 0)
	d, err = s.Update(fat)
	require.NoError(t, err)
	require.Equal(t, types.SignalBuy, d.Signal(), "volume 15 >= 1.0×mean must allow the entry")
	require.Equal(t, momentum.DecisionReasonMACrossoverUp, d.Reason())
}

func TestVolumeGateDisabled(t *testing.T) {
	cfg := momentum.DefaultConfig()
	cfg.SignalSource = momentum.SignalSourcePrice
	cfg.FastWindow = 3
	cfg.SlowWindow = 5
	cfg.ATRWindow = 3
	cfg.VolumeAvgWindow = 0 // gate off
	s := momentum.New(cfg, nil)

	var last momentum.Decision
	for _, b := range risingBars(7) { // volume 0 on every bar
		d, err := s.Update(b)
		require.NoError(t, err)
		last = d
	}
	require.Equal(t, types.SignalBuy, last.Signal(),
		"VolumeAvgWindow 0 must disable the gate: zero-volume bars still enter")
}

// TestIntrabarNoiseInvariance: only closes drive the signal; two bar
// series with identical closes but different H/L noise must agree.
func TestIntrabarNoiseInvariance(t *testing.T) {
	flat := risingBars(7)
	noisy := risingBars(7)
	wide := decimal.MustNew(5, 0)
	for i := range noisy {
		noisy[i].High, _ = noisy[i].Close.Add(wide)
		noisy[i].Low, _ = noisy[i].Close.Sub(wide)
	}
	sA := newTestStrategy(t, momentum.SignalSourcePrice)
	sB := newTestStrategy(t, momentum.SignalSourcePrice)
	for i := range flat {
		dA, err := sA.Update(flat[i])
		require.NoError(t, err)
		dB, err := sB.Update(noisy[i])
		require.NoError(t, err)
		require.Equal(t, dA.Signal(), dB.Signal(), "bar %d: H/L noise must not change the signal", i)
		require.True(t, dA.SeriesValue.Equal(dB.SeriesValue))
	}
}

func TestShouldExit_StopLoss_Long(t *testing.T) {
	cfg := momentum.DefaultConfig()
	cfg.FastWindow = 2
	cfg.SlowWindow = 3
	cfg.StopLossATR = decimal.MustNew(2, 0) // 2 ATR
	s := momentum.New(cfg, nil)
	entryPrice := decimal.MustNew(100, 0)
	entryATR := decimal.MustNew(5, 0) // stop distance = 10
	// Price 89 (< 100-10=90) -> stop loss.
	d := momentum.NewExitDecision(types.SignalBuy, decimal.MustNew(89, 0))
	exit, reason := s.ShouldExit(types.SignalBuy, d, entryPrice, entryATR)
	require.True(t, exit)
	require.Equal(t, momentum.ExitReasonStopLoss, reason)
}

func TestShouldExit_TakeProfit_Long(t *testing.T) {
	cfg := momentum.DefaultConfig()
	cfg.StopLossATR = decimal.Zero
	cfg.TakeProfitATR = decimal.MustNew(2, 0)
	s := momentum.New(cfg, nil)
	entryPrice := decimal.MustNew(100, 0)
	entryATR := decimal.MustNew(5, 0) // tp distance = 10
	d := momentum.NewExitDecision(types.SignalBuy, decimal.MustNew(111, 0))
	exit, reason := s.ShouldExit(types.SignalBuy, d, entryPrice, entryATR)
	require.True(t, exit)
	require.Equal(t, momentum.ExitReasonTakeProfit, reason)
}

func TestShouldExit_Reversal_Long(t *testing.T) {
	cfg := momentum.DefaultConfig()
	cfg.StopLossATR = decimal.Zero
	cfg.TakeProfitATR = decimal.Zero
	s := momentum.New(cfg, nil)
	entryPrice := decimal.MustNew(100, 0)
	entryATR := decimal.MustNew(1, 0)
	// Opened Buy; decision now Sell -> reversal.
	d := momentum.NewExitDecision(types.SignalSell, decimal.MustNew(100, 0))
	exit, reason := s.ShouldExit(types.SignalBuy, d, entryPrice, entryATR)
	require.True(t, exit)
	require.Equal(t, momentum.ExitReasonReversal, reason)
}

func TestShouldExit_Hold_NoExit(t *testing.T) {
	cfg := momentum.DefaultConfig()
	cfg.StopLossATR = decimal.MustNew(2, 0)
	s := momentum.New(cfg, nil)
	d := momentum.NewExitDecision(types.SignalBuy, decimal.MustNew(100, 0))
	exit, _ := s.ShouldExit(types.SignalBuy, d, decimal.MustNew(100, 0), decimal.MustNew(1, 0))
	require.False(t, exit)
}

func TestCappedOrderQuantity_MinSizeSkips_MaxSizeClamps(t *testing.T) {
	cfg := momentum.DefaultConfig()
	cfg.InitialBalance = decimal.MustNew(1000, 0)
	cfg.MinOrderSize = decimal.MustNew(5, 0) // need >= 5 units
	cfg.MaxOrderSize = decimal.MustNew(3, 0) // but cap at 3
	s := momentum.New(cfg, nil)
	// budget/price = 1000/100 = 10 -> clamped to MaxOrderSize 3 (>= MinSize 5? no, 3<5 -> skip)
	_, ok, err := momentum.CappedOrderQuantity(s, decimal.One, decimal.Zero, decimal.MustNew(100, 0))
	require.NoError(t, err)
	require.False(t, ok, "3 < min 5 -> skip")

	cfg.MaxOrderSize = decimal.Zero
	s = momentum.New(cfg, nil)
	qty, ok, err := momentum.CappedOrderQuantity(s, decimal.One, decimal.Zero, decimal.MustNew(100, 0))
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, qty.Equal(decimal.MustNew(10, 0)))
}

// TestShouldExit_StopLoss_Short pins the short stop-loss threshold
// (entryPrice + stopDist) using a no-fire boundary. With entry 100,
// stopDist 10, and price 105: correct code sees price < 110 -> no
// exit. If the Sell branch accidentally uses entryPrice - stopDist
// (or sign-flips the comparison), threshold becomes 90 and the
// stop-loss would fire on a 5% move up. The exit==false assertion
// pins the threshold direction.
func TestShouldExit_StopLoss_Short(t *testing.T) {
	cfg := momentum.DefaultConfig()
	cfg.StopLossATR = decimal.MustNew(2, 0)
	s := momentum.New(cfg, nil)
	entryPrice := decimal.MustNew(100, 0)
	entryATR := decimal.MustNew(5, 0) // stopDist = 10
	d := momentum.NewExitDecision(types.SignalSell, decimal.MustNew(105, 0))
	exit, reason := s.ShouldExit(types.SignalSell, d, entryPrice, entryATR)
	require.False(t, exit,
		"price 105 is between mutated-threshold 90 and correct "+
			"threshold 110; should NOT fire stop-loss, got reason=%q", reason)
}

// TestShouldExit_StopLoss_ShortFires is the positive-fire companion
// to TestShouldExit_StopLoss_Short. With entry 100 and stopDist 10
// (short stop at entry+stopDist = 110), price 111 must trigger
// ExitReasonStopLoss. Without this test, deleting the entire
// SignalSell arm of the stop-loss switch in strategy.go would
// leave every test green.
func TestShouldExit_StopLoss_ShortFires(t *testing.T) {
	cfg := momentum.DefaultConfig()
	cfg.StopLossATR = decimal.MustNew(2, 0)
	s := momentum.New(cfg, nil)
	entryPrice := decimal.MustNew(100, 0)
	entryATR := decimal.MustNew(5, 0) // stopDist = 10
	d := momentum.NewExitDecision(types.SignalSell, decimal.MustNew(111, 0))
	exit, reason := s.ShouldExit(types.SignalSell, d, entryPrice, entryATR)
	require.True(t, exit, "price 111 >= short stop 110 must fire stop-loss")
	require.Equal(t, momentum.ExitReasonStopLoss, reason)
}

// TestShouldExit_TakeProfit_Short mirrors TestShouldExit_TakeProfit_Long.
// Take-profit for shorts is BELOW the entry price.
func TestShouldExit_TakeProfit_Short(t *testing.T) {
	cfg := momentum.DefaultConfig()
	cfg.StopLossATR = decimal.Zero
	cfg.TakeProfitATR = decimal.MustNew(2, 0)
	s := momentum.New(cfg, nil)
	entryPrice := decimal.MustNew(100, 0)
	entryATR := decimal.MustNew(5, 0)
	d := momentum.NewExitDecision(types.SignalSell, decimal.MustNew(89, 0))
	exit, reason := s.ShouldExit(types.SignalSell, d, entryPrice, entryATR)
	require.True(t, exit)
	require.Equal(t, momentum.ExitReasonTakeProfit, reason)
}

// TestShouldExit_Reversal_Short mirrors TestShouldExit_Reversal_Long.
// Opened Sell; current decision is Buy -> reversal.
func TestShouldExit_Reversal_Short(t *testing.T) {
	cfg := momentum.DefaultConfig()
	cfg.StopLossATR = decimal.Zero
	cfg.TakeProfitATR = decimal.Zero
	s := momentum.New(cfg, nil)
	d := momentum.NewExitDecision(types.SignalBuy, decimal.MustNew(100, 0))
	exit, reason := s.ShouldExit(types.SignalSell, d, decimal.MustNew(100, 0), decimal.MustNew(1, 0))
	require.True(t, exit)
	require.Equal(t, momentum.ExitReasonReversal, reason)
}

// TestShouldExit_Hold_NoExit_Short mirrors TestShouldExit_Hold_NoExit.
// Short position with stop-loss configured but price moves WITHIN
// range: no exit.
func TestShouldExit_Hold_NoExit_Short(t *testing.T) {
	cfg := momentum.DefaultConfig()
	cfg.StopLossATR = decimal.MustNew(2, 0)
	s := momentum.New(cfg, nil)
	d := momentum.NewExitDecision(types.SignalSell, decimal.MustNew(100, 0))
	exit, _ := s.ShouldExit(types.SignalSell, d, decimal.MustNew(100, 0), decimal.MustNew(1, 0))
	require.False(t, exit)
}
