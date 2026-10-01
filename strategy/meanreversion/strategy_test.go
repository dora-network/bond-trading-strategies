package meanreversion_test

import (
	"context"
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/candles"
	"github.com/dora-network/bond-trading-strategies/strategy/meanreversion/meanreversionfakes"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/bond-trading-strategies/strategy/window"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/strategy/meanreversion"
)

var (
	epoch   = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	timeout = 10 * time.Second
)

// bondPriceFromYTM approximates the clean price of a 10-year 5 % coupon bond
// given its yield-to-maturity. This is used so the backtest PnL (which is now
// price-based) correctly reflects the YTM move.
func bondPriceFromYTM(ytm decimal.Decimal) decimal.Decimal {
	// Linear approximation: Price ≈ 100 − Duration × (YTM − 0.05) × 100
	// where Duration ≈ 7.5 for a 10y 5 % bond.
	coupon := decimal.MustNew(5, 2)                // 0.05
	diff, _ := ytm.Sub(coupon)                     // YTM − 0.05
	scaled, _ := diff.Mul(decimal.MustNew(750, 0)) // × 750
	par := decimal.MustNew(100, 0)
	result, _ := par.Sub(scaled)
	if result.IsNeg() {
		return decimal.Zero
	}
	return result
}

func defaultConfig() meanreversion.Config {
	return meanreversion.Config{
		LookbackWindow:  20,
		EntryZScore:     decimal.Two,
		ExitZScore:      decimal.MustNew(5, 1),
		StopLossZScore:  decimal.MustNew(35, 1),
		MinStdDev:       decimal.MustNew(5, 4),
		MaxPositionSize: decimal.One,
		InitialBalance:  decimal.One,
	}
}

func bar(i int, ytm, bench decimal.Decimal) types.Bar {
	return types.Bar{
		Time:           epoch.Add(time.Duration(i) * time.Hour),
		Close:          bondPriceFromYTM(ytm),
		CloseYTM:       ytm,
		BenchmarkYield: bench,
	}
}

// makeBars returns n bars with a constant spread centred at meanSpread.
func makeBars(n int, meanSpread decimal.Decimal) []types.Bar {
	bench := decimal.MustNew(5, 2) // 0.05
	ytm, _ := bench.Add(meanSpread)
	b := make([]types.Bar, n)
	for i := range b {
		b[i] = bar(i, ytm, bench)
	}
	return b
}

func TestDefaultConfigResolutionAndLookback(t *testing.T) {
	t.Parallel()
	cfg := meanreversion.DefaultConfig()
	assert.Equal(t, candles.Resolution1h, cfg.Resolution)
	assert.Equal(t, 24, cfg.LookbackWindow)
}

// TestStrategy_IntrabarNoiseInvariance verifies the z-score depends only on
// the bar close: two bars with identical Close/CloseYTM but different
// High/Low produce identical decisions.
func TestStrategy_IntrabarNoiseInvariance(t *testing.T) {
	t.Parallel()
	newWarm := func() *meanreversion.Strategy {
		s := meanreversion.New(defaultConfig(), nil)
		for i, b := range makeBars(20, decimal.MustNew(1, 2)) {
			b.Time = epoch.Add(time.Duration(i) * time.Hour)
			_, err := s.Update(b)
			require.NoError(t, err)
		}
		return s
	}

	quiet := bar(20, decimal.MustNew(1, 1), decimal.MustNew(5, 2))
	noisy := quiet
	noisy.High = decimal.MustNew(2, 1)
	noisy.Low = decimal.MustNew(4, 2)
	noisy.HighYTM = decimal.MustNew(2, 1)
	noisy.LowYTM = decimal.MustNew(4, 2)
	noisy.Volume = decimal.MustNew(999, 0)

	d1, err := newWarm().Update(quiet)
	require.NoError(t, err)
	d2, err := newWarm().Update(noisy)
	require.NoError(t, err)
	assert.True(t, d1.ZScore.Equal(d2.ZScore), "z must ignore intra-bar noise")
	assert.Equal(t, d1.Signal(), d2.Signal())
	assert.True(t, d1.Spread.Equal(d2.Spread))
}

func TestRollingWindow_NotReadyUntilFull(t *testing.T) {
	w := window.NewRollingWindow(5)
	for i := range 4 {
		require.NoError(t, w.Add(decimal.MustNew(int64(i), 0)))
		assert.False(t, w.Ready(), "window should not be ready after %d additions", i+1)
	}
	require.NoError(t, w.Add(decimal.MustNew(4, 0)))
	assert.True(t, w.Ready())
}

func TestRollingWindow_MeanAndStdDev_ConstantSeries(t *testing.T) {
	w := window.NewRollingWindow(5)
	for range 10 {
		require.NoError(t, w.Add(decimal.MustNew(3, 0)))
	}
	assert.True(t, w.Mean().Equal(decimal.MustNew(3, 0)), "mean of constant series")
	sd, err := w.StdDev()
	require.NoError(t, err)
	assert.True(t, sd.IsZero(), "stddev of constant series")
}

func TestRollingWindow_MeanUpdatesCorrectly(t *testing.T) {
	// Window of size 3: add 1, 2, 3, then 4 - oldest (1) should be evicted.
	// Mean of [2,3,4] = 3.0
	w := window.NewRollingWindow(3)
	require.NoError(t, w.Add(decimal.MustNew(1, 0)))
	require.NoError(t, w.Add(decimal.MustNew(2, 0)))
	require.NoError(t, w.Add(decimal.MustNew(3, 0)))
	require.NoError(t, w.Add(decimal.MustNew(4, 0))) // evicts 1
	assert.True(t, w.Mean().Equal(decimal.MustNew(3, 0)))
}

func TestRollingWindow_StdDev_KnownValues(t *testing.T) {
	// Population [2,4,4,4,5,5,7,9] has sample stddev ~= 2.138.
	// We use a window large enough to hold all values and verify the result
	// lands within a small delta.
	vals := []decimal.Decimal{
		decimal.MustNew(2, 0),
		decimal.MustNew(4, 0),
		decimal.MustNew(4, 0),
		decimal.MustNew(4, 0),
		decimal.MustNew(5, 0),
		decimal.MustNew(5, 0),
		decimal.MustNew(7, 0),
		decimal.MustNew(9, 0),
	}
	w := window.NewRollingWindow(len(vals))
	for _, v := range vals {
		require.NoError(t, w.Add(v))
	}
	// Sample stddev = sqrt(sum((xi-mean)^2 / (n-1)))
	// For [2,4,4,4,5,5,7,9]: mean=5, sum-sq-dev=32, sample var=32/7~=4.571, stddev~=2.138
	assert.True(t, w.Mean().Equal(decimal.MustNew(5, 0)), "mean should be 5")
	sd, err := w.StdDev()
	require.NoError(t, err)
	sdF, _ := sd.Float64()
	assert.InDelta(t, 2.138, sdF, 0.01, "sample stddev should be ~2.138")
}

func TestRollingWindow_ZScore(t *testing.T) {
	w := window.NewRollingWindow(10)
	for range 10 {
		require.NoError(t, w.Add(decimal.MustNew(5, 2))) // 0.05
	}
	// Zero stddev -> ZScore returns 0 regardless of value.
	z, err := w.ZScore(decimal.MustNew(8, 2), decimal.MustNew(1, 3))
	require.NoError(t, err)
	assert.True(t, z.IsZero())

	// Now add some variance.
	w2 := window.NewRollingWindow(4)
	require.NoError(t, w2.Add(decimal.MustNew(1, 0)))
	require.NoError(t, w2.Add(decimal.MustNew(2, 0)))
	require.NoError(t, w2.Add(decimal.MustNew(3, 0)))
	require.NoError(t, w2.Add(decimal.MustNew(4, 0))) // mean=2.5, sample stddev=sqrt(5/3)~=1.29
	z2, err := w2.ZScore(decimal.MustNew(4, 0), decimal.MustNew(1, 3))
	require.NoError(t, err)
	assert.True(t, z2.IsPos(), "z-score of value above mean should be positive")
}

func TestStrategy_HoldBeforeWindowFull(t *testing.T) {
	cfg := defaultConfig()
	s := meanreversion.New(cfg, nil)

	for i := range cfg.LookbackWindow - 1 {
		d, err := s.Update(bar(i, decimal.MustNew(55, 3), decimal.MustNew(5, 2)))
		require.NoError(t, err)
		assert.Equal(t, types.SignalHold, d.Signal(),
			"should be HOLD before window is full (step %d)", i)
	}
}

func TestStrategy_BuySignalOnWideSpread(t *testing.T) {
	cfg := defaultConfig()
	s := meanreversion.New(cfg, nil)
	for i := range 20 {
		_, err := s.Update(bar(i, decimal.MustNew(6, 2), decimal.MustNew(5, 2)))
		require.NoError(t, err)
	}

	cfg2 := defaultConfig()
	cfg2.LookbackWindow = 10
	s2 := meanreversion.New(cfg2, nil)

	base := decimal.MustNew(5, 2)
	for i := range 10 {
		var spread decimal.Decimal
		if i%2 == 0 {
			spread = decimal.MustNew(10, 3)
		} else {
			spread = decimal.MustNew(12, 3)
		}
		ytm, _ := base.Add(spread)
		_, err := s2.Update(bar(i, ytm, base))
		require.NoError(t, err)
	}

	d, err := s2.Update(bar(10, decimal.MustNew(1, 1), decimal.MustNew(5, 2)))
	require.NoError(t, err)
	assert.Equal(t, types.SignalBuy, d.Signal())
	assert.True(t, d.PositionSize().IsPos())
	assert.True(t, d.ZScore.Cmp(cfg2.EntryZScore) > 0)
}

func TestStrategy_SellSignalOnTightSpread(t *testing.T) {
	cfg := defaultConfig()
	cfg.LookbackWindow = 10
	s := meanreversion.New(cfg, nil)

	base := decimal.MustNew(5, 2)
	for i := range 10 {
		var spread decimal.Decimal
		if i%2 == 0 {
			spread = decimal.MustNew(10, 3)
		} else {
			spread = decimal.MustNew(12, 3)
		}
		ytm, _ := base.Add(spread)
		_, err := s.Update(bar(i, ytm, base))
		require.NoError(t, err)
	}

	d, err := s.Update(bar(10, decimal.MustNew(2, 2), decimal.MustNew(5, 2)))
	require.NoError(t, err)
	assert.Equal(t, types.SignalSell, d.Signal())
	assert.True(t, d.ZScore.Cmp(cfg.EntryZScore.Neg()) < 0)
}

func TestStrategy_HoldWithinNeutralBand(t *testing.T) {
	cfg := defaultConfig()
	cfg.LookbackWindow = 10
	s := meanreversion.New(cfg, nil)

	base := decimal.MustNew(5, 2)
	for i := range 10 {
		var spread decimal.Decimal
		if i%2 == 0 {
			spread = decimal.MustNew(10, 3)
		} else {
			spread = decimal.MustNew(12, 3)
		}
		ytm, _ := base.Add(spread)
		_, err := s.Update(bar(i, ytm, base))
		require.NoError(t, err)
	}

	d, err := s.Update(bar(10, decimal.MustNew(61, 3), decimal.MustNew(5, 2)))
	require.NoError(t, err)
	assert.Equal(t, types.SignalHold, d.Signal())
}

func TestStrategy_ShouldExit_ProfitTake(t *testing.T) {
	cfg := defaultConfig()
	s := meanreversion.New(cfg, nil)

	exit, reason := s.ShouldExit(types.SignalBuy, decimal.MustNew(3, 1))
	assert.True(t, exit)
	assert.Equal(t, meanreversion.ExitReasonTakeProfit, reason)

	exit, reason = s.ShouldExit(types.SignalSell, decimal.MustNew(-2, 1))
	assert.True(t, exit)
	assert.Equal(t, meanreversion.ExitReasonTakeProfit, reason)

	exit, reason = s.ShouldExit(types.SignalBuy, decimal.Zero)
	assert.True(t, exit)
	assert.Equal(t, meanreversion.ExitReasonTakeProfit, reason)
}

func TestStrategy_ShouldExit_StopLoss(t *testing.T) {
	cfg := defaultConfig()
	s := meanreversion.New(cfg, nil)

	exit, reason := s.ShouldExit(types.SignalBuy, decimal.MustNew(36, 1))
	assert.True(t, exit)
	assert.Equal(t, meanreversion.ExitReasonStopLoss, reason)

	exit, reason = s.ShouldExit(types.SignalBuy, decimal.MustNew(34, 1))
	assert.False(t, exit)
	assert.Empty(t, reason)

	exit, reason = s.ShouldExit(types.SignalSell, decimal.MustNew(-36, 1))
	assert.True(t, exit)
	assert.Equal(t, meanreversion.ExitReasonStopLoss, reason)

	exit, reason = s.ShouldExit(types.SignalSell, decimal.MustNew(-34, 1))
	assert.False(t, exit)
	assert.Empty(t, reason)
}

func TestStrategy_ShouldExit_StopLossDisabled(t *testing.T) {
	cfg := defaultConfig()
	cfg.StopLossZScore = decimal.Zero
	s := meanreversion.New(cfg, nil)

	exit, reason := s.ShouldExit(types.SignalBuy, decimal.MustNew(10, 0))
	assert.False(t, exit)
	assert.Empty(t, reason)

	exit, reason = s.ShouldExit(types.SignalSell, decimal.MustNew(-10, 0))
	assert.False(t, exit)
	assert.Empty(t, reason)
}

func TestStrategy_LastStopLossTrigger(t *testing.T) {
	s := meanreversion.New(defaultConfig(), nil)

	z, pnl, triggered := s.LastStopLossTrigger()
	assert.False(t, triggered)
	assert.True(t, z.IsZero())
	assert.True(t, pnl.IsZero())

	exit, reason := s.ShouldExit(types.SignalBuy, decimal.MustNew(36, 1))
	require.True(t, exit)
	require.Equal(t, meanreversion.ExitReasonStopLoss, reason)

	z, pnl, triggered = s.LastStopLossTrigger()
	assert.True(t, triggered)
	assert.True(t, z.Equal(decimal.MustNew(36, 1)))
	assert.True(t, pnl.IsZero())
}

func TestSignalString(t *testing.T) {
	assert.Equal(t, "BUY", types.SignalBuy.String())
	assert.Equal(t, "SELL", types.SignalSell.String())
	assert.Equal(t, "HOLD", types.SignalHold.String())
}

// --- openSignal / restart tests ---

// TestInitializeBalances_SetsOpenSignalFromDORAPosition verifies that after
// initializeBalances runs, openSignal reflects the position DORA returned.
// This is the core of the restart-safety guarantee: the strategy must know
// it already holds a position before it processes the first price tick.
func TestInitializeBalances_SetsOpenSignalFromDORAPosition(t *testing.T) {
	t.Parallel()

	orderBookID := uuid.Must(uuid.NewV7())
	cfg := defaultConfig()
	cfg.OrderBookID = orderBookID
	cfg.InitialBalance = decimal.MustNew(10, 0)

	for _, tc := range []struct {
		name           string
		held, borrowed decimal.Decimal
		wantSignal     types.Signal
	}{
		{"long position", decimal.MustNew(5, 0), decimal.Zero, types.SignalBuy},
		{"short position", decimal.Zero, decimal.MustNew(3, 0), types.SignalSell},
		{"flat", decimal.Zero, decimal.Zero, types.SignalHold},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := meanreversion.New(cfg, nil)
			client := &meanreversionfakes.FakeMarketAPIClient{}
			client.QuoteAssetIDReturns("usd-id", nil)
			// AssetPosition is called twice: once for the bond, once for USD.
			client.AssetPositionStub = func(_ context.Context, assetID string) (decimal.Decimal, decimal.Decimal, error) {
				if assetID == "bond-id" {
					return tc.held, tc.borrowed, nil
				}
				return decimal.MustNew(100, 0), decimal.Zero, nil // USD balance
			}
			meanreversion.SetLookupClient(s, client)

			meanreversion.InitializeBalances(context.Background(), s, "bond-id")

			assert.Equal(t, tc.wantSignal, meanreversion.OpenSignal(s))
		})
	}
}

func TestStrategyTypeExported(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "mean_reversion", meanreversion.StrategyType,
		"StrategyType must be exported and equal to the documented value")
}
