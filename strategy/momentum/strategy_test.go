package momentum_test

import (
	"testing"

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
	return momentum.New(cfg)
}

func ytmP(d float64) decimal.Decimal {
	return decimal.MustNew(int64(d*1e6), 6)
}

// risingPrice feeds an uptrend: price steps up each tick, YTM steady.
func risingPrice(n int) []types.YieldObservation {
	o := make([]types.YieldObservation, n)
	for i := range o {
		o[i] = types.YieldObservation{
			BondID: "b", YTM: ytmP(0.05), BenchmarkYield: decimal.MustNew(4, 2),
			Price: decimal.MustNew(int64(100+i), 0),
		}
	}
	return o
}

func TestUpdate_PriceSource_RisingTrend_IsBuy(t *testing.T) {
	s := newTestStrategy(t, momentum.SignalSourcePrice)
	var last momentum.Decision
	for _, o := range risingPrice(7) {
		d, err := s.Update(o)
		require.NoError(t, err)
		last = d
	}
	require.Equal(t, types.SignalBuy, last.Signal()) // price up -> long
	require.Equal(t, "up", last.Trend)
}

func TestUpdate_YTMSource_RisingTrend_IsSell(t *testing.T) {
	// YTM rising = price falling = downtrend -> Sell (direction inverted).
	s := newTestStrategy(t, momentum.SignalSourceYTM)
	obs := make([]types.YieldObservation, 7)
	for i := range obs {
		obs[i] = types.YieldObservation{
			BondID: "b", Price: decimal.MustNew(100, 0),
			YTM: ytmP(0.04 + float64(i)*0.001), BenchmarkYield: decimal.MustNew(4, 2),
		}
	}
	var last momentum.Decision
	for _, o := range obs {
		d, err := s.Update(o)
		require.NoError(t, err)
		last = d
	}
	require.Equal(t, types.SignalSell, last.Signal())
}

func TestUpdate_SpreadSource_RisingTrend_IsSell(t *testing.T) {
	// Spread rising (YTM up, benchmark steady) = cheapening = Sell.
	s := newTestStrategy(t, momentum.SignalSourceSpread)
	obs := make([]types.YieldObservation, 7)
	for i := range obs {
		obs[i] = types.YieldObservation{
			BondID: "b", Price: decimal.MustNew(100, 0),
			YTM: ytmP(0.05 + float64(i)*0.001), BenchmarkYield: decimal.MustNew(4, 2),
		}
	}
	var last momentum.Decision
	for _, o := range obs {
		d, err := s.Update(o)
		require.NoError(t, err)
		last = d
	}
	require.Equal(t, types.SignalSell, last.Signal())
}

func TestUpdate_WarmingUp_HoldsBeforeWindowsReady(t *testing.T) {
	s := newTestStrategy(t, momentum.SignalSourcePrice)
	d, err := s.Update(risingPrice(1)[0])
	require.NoError(t, err)
	require.Equal(t, types.SignalHold, d.Signal())
	require.Equal(t, momentum.DecisionReasonWarmingUp, d.Reason())
}

func TestUpdate_YTMSource_NilYTM_TickDropped(t *testing.T) {
	s := newTestStrategy(t, momentum.SignalSourceYTM)
	o := types.YieldObservation{BondID: "b", Price: decimal.MustNew(100, 0)} // nil YTM
	d, err := s.Update(o)
	require.NoError(t, err)
	require.Equal(t, types.SignalHold, d.Signal()) // tick dropped, no window update
}
