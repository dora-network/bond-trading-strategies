package momentum_test

import (
	"context"
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy/momentum"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/require"
)

// uptrendThenReversal: prices rise (fast>slow -> Buy), then fall so the
// MAs cross back (-> reversal exit).
func uptrendThenReversal() []types.YieldObservation {
	o := make([]types.YieldObservation, 0, 14)
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range 7 {
		o = append(o, types.YieldObservation{
			Time:   base.Add(time.Duration(i) * time.Minute),
			BondID: "b", Price: decimal.MustNew(int64(100+i), 0),
		})
	}
	for i := range 7 {
		o = append(o, types.YieldObservation{
			Time:   base.Add(time.Duration(7+i) * time.Minute),
			BondID: "b", Price: decimal.MustNew(int64(106-i), 0),
		})
	}
	return o
}

func TestBacktest_OpensAndExits(t *testing.T) {
	cfg := momentum.DefaultConfig()
	cfg.SignalSource = momentum.SignalSourcePrice
	cfg.FastWindow = 3
	cfg.SlowWindow = 5
	cfg.StopLossATR = decimal.Zero
	cfg.TakeProfitATR = decimal.Zero
	cfg.InitialBalance = decimal.MustNew(1000, 0)
	s := momentum.New(cfg, nil)
	bt := momentum.NewBacktester(s, nil)

	res, err := bt.Run(context.Background(), uptrendThenReversal())
	require.NoError(t, err)
	require.NotEmpty(t, res.GetClosedTrades(), "expected at least one closed trade")
}

func TestBacktest_StopLossExits(t *testing.T) {
	cfg := momentum.DefaultConfig()
	cfg.SignalSource = momentum.SignalSourcePrice
	cfg.FastWindow = 2
	cfg.SlowWindow = 3
	cfg.StopLossATR = decimal.MustNew(2, 0)
	cfg.TakeProfitATR = decimal.Zero
	cfg.InitialBalance = decimal.MustNew(1000, 0)
	s := momentum.New(cfg, nil)
	bt := momentum.NewBacktester(s, nil)

	// Up then sharp drop — fastMA still > slowMA, but stop-loss fires on the drop.
	obs := make([]types.YieldObservation, 0, 9)
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range 5 {
		obs = append(obs, types.YieldObservation{
			Time:   base.Add(time.Duration(i) * time.Minute),
			BondID: "b", Price: decimal.MustNew(int64(100+i), 0),
		})
	}
	obs = append(obs, types.YieldObservation{Time: base.Add(5 * time.Minute), BondID: "b", Price: decimal.MustNew(120, 0)})
	obs = append(obs, types.YieldObservation{Time: base.Add(6 * time.Minute), BondID: "b", Price: decimal.MustNew(80, 0)})
	obs = append(obs, types.YieldObservation{Time: base.Add(7 * time.Minute), BondID: "b", Price: decimal.MustNew(75, 0)})
	obs = append(obs, types.YieldObservation{Time: base.Add(8 * time.Minute), BondID: "b", Price: decimal.MustNew(70, 0)})

	res, err := bt.Run(context.Background(), obs)
	require.NoError(t, err)
	require.NotEmpty(t, res.GetClosedTrades())
}
