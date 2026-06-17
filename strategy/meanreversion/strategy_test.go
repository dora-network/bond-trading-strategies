package meanreversion_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/prices"
	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/meanreversion/meanreversionfakes"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/bond-trading-strategies/strategy/window"
	"github.com/dora-network/dora-client-go/doraclient"
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

func obs(i int, ytm, bench decimal.Decimal) types.YieldObservation {
	return types.YieldObservation{
		Time:           epoch.Add(time.Duration(i) * 24 * time.Hour),
		BondID:         "BOND-TEST",
		YTM:            ytm,
		BenchmarkYield: bench,
		Price:          bondPriceFromYTM(ytm),
	}
}

// makeObs returns n observations with a constant spread centred at meanSpread.
func makeObs(n int, meanSpread decimal.Decimal) []types.YieldObservation {
	bench := decimal.MustNew(5, 2) // 0.05
	ytm, _ := bench.Add(meanSpread)
	o := make([]types.YieldObservation, n)
	for i := range o {
		o[i] = obs(i, ytm, bench)
	}
	return o
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
		d, err := s.Update(obs(i, decimal.MustNew(55, 3), decimal.MustNew(5, 2)))
		require.NoError(t, err)
		assert.Equal(t, types.SignalHold, d.Signal,
			"should be HOLD before window is full (step %d)", i)
	}
}

func TestStrategy_BuySignalOnWideSpread(t *testing.T) {
	cfg := defaultConfig()
	s := meanreversion.New(cfg, nil)

	for i := range 20 {
		_, err := s.Update(obs(i, decimal.MustNew(6, 2), decimal.MustNew(5, 2)))
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
		_, err := s2.Update(obs(i, ytm, base))
		require.NoError(t, err)
	}

	d, err := s2.Update(obs(10, decimal.MustNew(1, 1), decimal.MustNew(5, 2)))
	require.NoError(t, err)
	assert.Equal(t, types.SignalBuy, d.Signal)
	assert.True(t, d.PositionSize.IsPos())
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
		_, err := s.Update(obs(i, ytm, base))
		require.NoError(t, err)
	}

	d, err := s.Update(obs(10, decimal.MustNew(2, 2), decimal.MustNew(5, 2)))
	require.NoError(t, err)
	assert.Equal(t, types.SignalSell, d.Signal)
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
		_, err := s.Update(obs(i, ytm, base))
		require.NoError(t, err)
	}

	d, err := s.Update(obs(10, decimal.MustNew(61, 3), decimal.MustNew(5, 2)))
	require.NoError(t, err)
	assert.Equal(t, types.SignalHold, d.Signal)
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

func TestBacktester_NoTradesBeforeWindowFull(t *testing.T) {
	s := meanreversion.New(defaultConfig(), nil)
	bt := meanreversion.NewBacktester(s, nil)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	result, err := bt.Run(ctx, makeObs(19, decimal.MustNew(1, 2)))
	require.NoError(t, err)
	assert.Empty(t, result.ClosedTrades)
	assert.True(t, result.TotalPnL.IsZero())
}

func TestBacktester_ProfitableReversion(t *testing.T) {
	cfg := defaultConfig()
	cfg.LookbackWindow = 20
	cfg.MinStdDev = decimal.MustNew(1, 4)
	cfg.InitialBalance = decimal.MustNew(10000, 0)
	s := meanreversion.New(cfg, nil)
	bt := meanreversion.NewBacktester(s, nil)

	var observations []types.YieldObservation
	t0 := epoch
	base := decimal.MustNew(5, 2)

	for i := range 20 {
		var sp decimal.Decimal
		switch i % 3 {
		case 0:
			sp = decimal.MustNew(9, 3)
		case 1:
			sp = decimal.MustNew(10, 3)
		case 2:
			sp = decimal.MustNew(11, 3)
		}
		ytm, _ := base.Add(sp)
		observations = append(observations, types.YieldObservation{
			Time: t0.Add(time.Duration(i) * 24 * time.Hour), BondID: "B1",
			YTM: ytm, BenchmarkYield: base, Price: bondPriceFromYTM(ytm),
		})
	}

	obs20YTM := decimal.MustNew(13, 2)
	observations = append(observations, types.YieldObservation{
		Time: t0.Add(20 * 24 * time.Hour), BondID: "B1",
		YTM: obs20YTM, BenchmarkYield: decimal.MustNew(5, 2),
		Price: bondPriceFromYTM(obs20YTM),
	})

	for i := range 10 {
		obsExitYTM := decimal.MustNew(6, 2)
		observations = append(observations, types.YieldObservation{
			Time:           t0.Add(time.Duration(21+i) * 24 * time.Hour),
			BondID:         "B1",
			YTM:            obsExitYTM,
			BenchmarkYield: decimal.MustNew(5, 2),
			Price:          bondPriceFromYTM(obsExitYTM),
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	result, err := bt.Run(ctx, observations)
	require.NoError(t, err)
	require.NotEmpty(t, result.ClosedTrades, "should have at least one closed trade")
	assert.True(t, result.TotalPnL.IsPos(), "reversion trade should be profitable")
	assert.Greater(t, result.WinCount, 0)
}

func TestBacktester_LossingTradeForceClosedAtEnd(t *testing.T) {
	cfg := defaultConfig()
	cfg.LookbackWindow = 10
	cfg.StopLossZScore = decimal.Zero
	cfg.MinStdDev = decimal.MustNew(1, 4)
	cfg.InitialBalance = decimal.MustNew(10000, 0)
	s := meanreversion.New(cfg, nil)
	bt := meanreversion.NewBacktester(s, nil)

	var observations []types.YieldObservation
	t0 := epoch
	base := decimal.MustNew(5, 2)

	for i := range 10 {
		var sp decimal.Decimal
		switch i % 3 {
		case 0:
			sp = decimal.MustNew(9, 3)
		case 1:
			sp = decimal.MustNew(10, 3)
		case 2:
			sp = decimal.MustNew(11, 3)
		}
		ytm, _ := base.Add(sp)
		observations = append(observations, types.YieldObservation{
			Time: t0.Add(time.Duration(i) * 24 * time.Hour), BondID: "B2",
			YTM: ytm, BenchmarkYield: base, Price: bondPriceFromYTM(ytm),
		})
	}

	for i := range 5 {
		obsYTM := decimal.MustNew(13, 2)
		observations = append(observations, types.YieldObservation{
			Time:           t0.Add(time.Duration(10+i) * 24 * time.Hour),
			BondID:         "B2",
			YTM:            obsYTM,
			BenchmarkYield: decimal.MustNew(5, 2),
			Price:          bondPriceFromYTM(obsYTM),
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	result, err := bt.Run(ctx, observations)
	require.NoError(t, err)
	require.NotEmpty(t, result.ClosedTrades)
	assert.Equal(t, 1, len(result.ClosedTrades))
}

func TestBacktestResult_MaxDrawdown(t *testing.T) {
	cfg := defaultConfig()
	cfg.InitialBalance = decimal.MustNew(10000, 0)
	s := meanreversion.New(cfg, nil)
	bt := meanreversion.NewBacktester(s, nil)

	observations := makeObs(50, decimal.MustNew(1, 2))

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	result, err := bt.Run(ctx, observations)
	require.NoError(t, err)
	assert.True(t, !result.MaxDrawdown.IsNeg())
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

			meanreversion.InitializeBalances(s, context.Background(), "bond-id")

			assert.Equal(t, tc.wantSignal, meanreversion.OpenSignal(s))
		})
	}
}

// TestRunLoop_NoNewEntryWhenPositionOpen verifies that when the strategy
// already holds a position (openSignal != Hold, set from bondQty after
// initializeBalances) it does not place another entry order — even when
// Update returns an entry signal on the next price tick. This is the core
// restart-safety guarantee.
func TestRunLoop_NoNewEntryWhenPositionOpen(t *testing.T) {
	t.Parallel()

	orderBookID := uuid.Must(uuid.NewV7())
	// Small window so it fills quickly and produces measurable variance.
	cfg := defaultConfig()
	cfg.LookbackWindow = 10
	cfg.OrderBookID = orderBookID
	cfg.InitialBalance = decimal.MustNew(10, 0)

	log := slog.Default()

	s := meanreversion.New(cfg, nil, meanreversion.WithLogger(log))
	client := &meanreversionfakes.FakeMarketAPIClient{}
	client.BaseAssetIDReturns("bond-id", nil)
	client.QuoteAssetIDReturns("usd-id", nil)
	// Simulate an existing long position fetched from DORA on startup.
	client.AssetPositionStub = func(_ context.Context, assetID string) (decimal.Decimal, decimal.Decimal, error) {
		if assetID == "bond-id" {
			return decimal.MustNew(5, 0), decimal.Zero, nil
		}
		return decimal.MustNew(50, 0), decimal.Zero, nil // USD
	}
	meanreversion.SetLookupClient(s, client)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	msgCh := make(chan strategy.Message)
	defer close(msgCh)

	// Build observations:
	// - 10 window-fill ticks alternating 4 % / 6 % YTM (benchmark = 0,
	//   so spread == YTM). This gives mean ≈ 5 %, stddev ≈ 1 %.
	// - 5 entry-signal ticks at 8 % YTM: z ≈ (8-5)/1 = 3 >> entry (2.0),
	//   so Update returns SignalBuy. The position guard must suppress the order.
	var priceUpdates []map[uuid.UUID]prices.AssetPrice
	for i := 0; i < 10; i++ {
		var ytm decimal.Decimal
		if i%2 == 0 {
			ytm = decimal.MustNew(4, 2)
		} else {
			ytm = decimal.MustNew(6, 2)
		}
		priceUpdates = append(priceUpdates, map[uuid.UUID]prices.AssetPrice{
			uuid.New(): {AssetID: "bond-id", YTM: &ytm, Price: bondPriceFromYTM(ytm), Time: epoch.Add(time.Duration(i) * 24 * time.Hour)},
		})
	}
	for i := 10; i < 15; i++ {
		ytm := decimal.MustNew(8, 2) // wide spread → z >> entry → SignalBuy
		priceUpdates = append(priceUpdates, map[uuid.UUID]prices.AssetPrice{
			uuid.New(): {AssetID: "bond-id", YTM: &ytm, Price: bondPriceFromYTM(ytm), Time: epoch.Add(time.Duration(i) * 24 * time.Hour)},
		})
	}

	priceCh := make(chan map[uuid.UUID]prices.AssetPrice, len(priceUpdates))
	for _, u := range priceUpdates {
		priceCh <- u
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = meanreversion.RunWithPrices(s, ctx, msgCh, priceCh)
	}()

	// Give the run loop time to process all updates.
	time.Sleep(200 * time.Millisecond)
	cancel()
	<-done

	// initializeBalances fetched bondQty = 5 → openSignal = Buy. Every
	// subsequent price tick must skip entry logic and never call CreateMarketOrder.
	assert.Zero(t, client.CreateMarketOrderCallCount(),
		"expected no new entry order while a position is already open")
}

// TestRunLoop_ClosesPositionOnShouldExit verifies that the run loop calls
// closePosition (placing an opposing market order) when ShouldExit returns
// true for the current open position.
func TestRunLoop_ClosesPositionOnShouldExit(t *testing.T) {
	t.Parallel()

	orderBookID := uuid.Must(uuid.NewV7())
	cfg := defaultConfig()
	cfg.LookbackWindow = 10
	cfg.OrderBookID = orderBookID
	cfg.InitialBalance = decimal.MustNew(10, 0)

	log := slog.Default()

	s := meanreversion.New(cfg, nil, meanreversion.WithLogger(log))
	client := &meanreversionfakes.FakeMarketAPIClient{}
	client.BaseAssetIDReturns("bond-id", nil)
	client.QuoteAssetIDReturns("usd-id", nil)
	// Existing long position (5 bonds) from a prior run.
	client.AssetPositionStub = func(_ context.Context, assetID string) (decimal.Decimal, decimal.Decimal, error) {
		if assetID == "bond-id" {
			return decimal.MustNew(5, 0), decimal.Zero, nil
		}
		return decimal.MustNew(50, 0), decimal.Zero, nil
	}
	meanreversion.SetLookupClient(s, client)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	msgCh := make(chan strategy.Message)
	defer close(msgCh)

	// Build observations:
	// - 10 window-fill ticks alternating 6 % / 8 % YTM: mean ≈ 7 %, stddev ≈ 1 %.
	// - 5 reversion ticks at 4 % YTM: z ≈ (4-7)/1 = -3 ≤ ExitZScore (0.5)
	//   for a Buy position → ShouldExit returns true → closePosition is called.
	var priceUpdates []map[uuid.UUID]prices.AssetPrice
	for i := 0; i < 10; i++ {
		var ytm decimal.Decimal
		if i%2 == 0 {
			ytm = decimal.MustNew(6, 2)
		} else {
			ytm = decimal.MustNew(8, 2)
		}
		priceUpdates = append(priceUpdates, map[uuid.UUID]prices.AssetPrice{
			uuid.New(): {AssetID: "bond-id", YTM: &ytm, Price: bondPriceFromYTM(ytm), Time: epoch.Add(time.Duration(i) * 24 * time.Hour)},
		})
	}
	for i := 10; i < 15; i++ {
		ytm := decimal.MustNew(4, 2) // z ≈ -3 ≤ ExitZScore → ShouldExit(Buy) = true
		priceUpdates = append(priceUpdates, map[uuid.UUID]prices.AssetPrice{
			uuid.New(): {AssetID: "bond-id", YTM: &ytm, Price: bondPriceFromYTM(ytm), Time: epoch.Add(time.Duration(i) * 24 * time.Hour)},
		})
	}

	priceCh := make(chan map[uuid.UUID]prices.AssetPrice, len(priceUpdates))
	for _, u := range priceUpdates {
		priceCh <- u
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = meanreversion.RunWithPrices(s, ctx, msgCh, priceCh)
	}()

	// Wait for the close order to be placed.
	require.Eventually(t, func() bool {
		return client.CreateMarketOrderCallCount() >= 1
	}, timeout, 10*time.Millisecond, "expected close order to be placed")
	cancel()
	<-done

	// Exactly one close order: a SELL to close the long for the full qty.
	require.Equal(t, 1, client.CreateMarketOrderCallCount())
	_, _, side, qty, invLev, fromGlobalPos, clientOrderID := client.CreateMarketOrderArgsForCall(0)
	_ = invLev
	_ = fromGlobalPos
	_ = clientOrderID
	assert.Equal(t, doraclient.SIDE_SELL, side)
	assert.True(t, qty.Equal(decimal.MustNew(5, 0)), "should close full position quantity")
	assert.Equal(t, types.SignalHold, meanreversion.OpenSignal(s))
}

func TestRunLoop_NoNewEntryWhenQuantityZero(t *testing.T) {
	t.Parallel()

	orderBookID := uuid.Must(uuid.NewV7())
	cfg := defaultConfig()
	cfg.LookbackWindow = 10
	cfg.OrderBookID = orderBookID
	// Small budget to ensure quantity calculation truncates to 0
	cfg.InitialBalance = decimal.MustNew(1, 0) // Balance = $1

	log := slog.Default()

	s := meanreversion.New(cfg, nil, meanreversion.WithLogger(log))
	client := &meanreversionfakes.FakeMarketAPIClient{}
	client.BaseAssetIDReturns("bond-id", nil)
	client.QuoteAssetIDReturns("usd-id", nil)
	// Balance has no existing position, and tracking initialised
	client.AssetPositionStub = func(_ context.Context, assetID string) (decimal.Decimal, decimal.Decimal, error) {
		return decimal.Zero, decimal.Zero, nil
	}
	meanreversion.SetLookupClient(s, client)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	msgCh := make(chan strategy.Message)
	defer close(msgCh)

	// Price of the bond is high, e.g., $100, which is greater than budget ($1).
	// So capped quantity will be floor(1 * PositionSize / 100) = 0.
	var priceUpdates []map[uuid.UUID]prices.AssetPrice
	for i := 0; i < 10; i++ {
		var ytm decimal.Decimal
		if i%2 == 0 {
			ytm = decimal.MustNew(4, 2)
		} else {
			ytm = decimal.MustNew(6, 2)
		}
		// High price: $100
		priceUpdates = append(priceUpdates, map[uuid.UUID]prices.AssetPrice{
			uuid.New(): {AssetID: "bond-id", YTM: &ytm, Price: decimal.MustNew(100, 0), Time: epoch.Add(time.Duration(i) * 24 * time.Hour)},
		})
	}
	// Add an update that generates a buy signal (wide spread)
	for i := 10; i < 15; i++ {
		ytm := decimal.MustNew(8, 2) // wide spread → SignalBuy
		priceUpdates = append(priceUpdates, map[uuid.UUID]prices.AssetPrice{
			uuid.New(): {AssetID: "bond-id", YTM: &ytm, Price: decimal.MustNew(100, 0), Time: epoch.Add(time.Duration(i) * 24 * time.Hour)},
		})
	}

	priceCh := make(chan map[uuid.UUID]prices.AssetPrice, len(priceUpdates))
	for _, u := range priceUpdates {
		priceCh <- u
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = meanreversion.RunWithPrices(s, ctx, msgCh, priceCh)
	}()

	// Give the run loop time to process updates.
	time.Sleep(200 * time.Millisecond)
	cancel()
	<-done

	// Verify no order was created because quantity was 0
	assert.Zero(t, client.CreateMarketOrderCallCount(),
		"expected no market order when quantity to order is 0")
	// Verify that openSignal remains SignalHold (did not change to SignalBuy)
	assert.Equal(t, types.SignalHold, meanreversion.OpenSignal(s),
		"expected open signal to remain Hold when quantity is 0")
}

func TestRunLoop_SelfHealsWhenPositionDoesNotExistOnExchange(t *testing.T) {
	t.Parallel()

	orderBookID := uuid.Must(uuid.NewV7())
	cfg := defaultConfig()
	cfg.LookbackWindow = 10
	cfg.OrderBookID = orderBookID
	cfg.InitialBalance = decimal.MustNew(10, 0)

	log := slog.Default()

	s := meanreversion.New(cfg, nil, meanreversion.WithLogger(log))
	client := &meanreversionfakes.FakeMarketAPIClient{}
	client.BaseAssetIDReturns("bond-id", nil)
	client.QuoteAssetIDReturns("usd-id", nil)

	// Simulate initialization with an existing position of 5 bonds
	var count int
	var mu sync.Mutex
	client.AssetPositionStub = func(_ context.Context, assetID string) (decimal.Decimal, decimal.Decimal, error) {
		mu.Lock()
		defer mu.Unlock()
		if assetID == "bond-id" {
			count++
			if count == 1 {
				return decimal.MustNew(5, 0), decimal.Zero, nil
			}
			return decimal.Zero, decimal.Zero, nil
		}
		return decimal.MustNew(50, 0), decimal.Zero, nil
	}
	client.CreateMarketOrderReturns(errors.New("insufficient position to close"))
	meanreversion.SetLookupClient(s, client)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	msgCh := make(chan strategy.Message)
	defer close(msgCh)

	// Build observations to trigger a ShouldExit exit condition.
	// - 10 window-fill ticks: alternating 6% / 8% YTM: mean = 7%
	// - Then price ticks showing 4% YTM -> triggers ExitZScore -> ShouldExit returns true
	var priceUpdates []map[uuid.UUID]prices.AssetPrice
	for i := 0; i < 10; i++ {
		var ytm decimal.Decimal
		if i%2 == 0 {
			ytm = decimal.MustNew(6, 2)
		} else {
			ytm = decimal.MustNew(8, 2)
		}
		priceUpdates = append(priceUpdates, map[uuid.UUID]prices.AssetPrice{
			uuid.New(): {AssetID: "bond-id", YTM: &ytm, Price: bondPriceFromYTM(ytm), Time: epoch.Add(time.Duration(i) * 24 * time.Hour)},
		})
	}

	priceUpdates = append(priceUpdates, map[uuid.UUID]prices.AssetPrice{
		uuid.New(): {
			AssetID: "bond-id",
			YTM:     func() *decimal.Decimal { d := decimal.MustNew(4, 2); return &d }(),
			Price:   bondPriceFromYTM(decimal.MustNew(4, 2)),
			Time:    epoch.Add(10 * 24 * time.Hour),
		},
	})

	priceCh := make(chan map[uuid.UUID]prices.AssetPrice, len(priceUpdates))
	for _, u := range priceUpdates {
		priceCh <- u
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = meanreversion.RunWithPrices(s, ctx, msgCh, priceCh)
	}()

	// Give the run loop time to process the exit tick.
	time.Sleep(200 * time.Millisecond)
	cancel()
	<-done

	// Verify that closePosition attempted the close order, saw the failure,
	// and self-healed because the live position is actually 0.
	assert.Equal(t, 1, client.CreateMarketOrderCallCount(),
		"expected exactly 1 market order attempt")

	// Verify that tracking is self-healed: openSignal is Hold and bondQty is 0.
	assert.Equal(t, types.SignalHold, meanreversion.OpenSignal(s))
	assert.True(t, meanreversion.BondQty(s).IsZero())
}
