package breakout

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/strategy/types"
)

// epoch is the test timestamp base.
var epoch = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

// flatObs builds a YieldObservation at a constant price.
func flatObs(i int, price decimal.Decimal) types.YieldObservation {
	return types.YieldObservation{
		Time:   epoch.Add(time.Duration(i) * time.Minute),
		BondID: "bond-A",
		Price:  price,
	}
}

// recordingTradeStore is a minimal in-memory TradeHistoryStore.
type recordingTradeStore struct {
	trades []Trade
}

func (s *recordingTradeStore) StreamTrades(
	_ context.Context,
	_ uuid.UUID,
	_, _ time.Time,
) (<-chan Trade, <-chan error) {
	ch := make(chan Trade, len(s.trades))
	errCh := make(chan error, 1)
	for _, t := range s.trades {
		ch <- t
	}
	close(ch)
	close(errCh)
	return ch, errCh
}

// TestBacktest_OBVAccumulatedFromTradeHistory verifies that when
// RequireVolumeConfirmation is true and a trade history store is
// configured, the backtester ingests trades into the OBV accumulator
// alongside observations. Also exercises the pending-trade carry-over
// (a trade between two observations) and verifies trades after the
// last observation are not ingested.
func TestBacktest_OBVAccumulatedFromTradeHistory(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.OBVWindow = 3
	cfg.ConfirmationBars = 1
	cfg.InitialBalance = decimal.MustNew(10000, 0)
	cfg.Leverage = decimal.One
	cfg.LongVolWindow = 5
	cfg.ShortVolWindow = 5
	cfg.ATRWindow = 5
	cfg.OrderBookID = uuid.MustParse("11111111-1111-1111-1111-111111111111")

	// 10 flat observations at 100, then a jump to 110.
	const warmup = 10
	obs := make([]types.YieldObservation, 0, warmup+1)
	for i := range warmup {
		obs = append(obs, flatObs(i, decimal.MustNew(100, 0)))
	}
	obs = append(obs, flatObs(warmup, decimal.MustNew(110, 0)))

	store := &recordingTradeStore{
		trades: []Trade{
			// Tick 2: ingested before obs 2.
			{Time: epoch.Add(2 * time.Minute), Price: decimal.MustNew(100, 0), Quantity: decimal.MustNew(10, 0), Side: "BUY"},
			// Tick 3.5: arrives between obs 3 and obs 4. The pending-trade
			// logic must carry this over from obs 3's call to obs 4's.
			{Time: epoch.Add(3*time.Minute + 30*time.Second), Price: decimal.MustNew(101, 0), Quantity: decimal.MustNew(5, 0), Side: "BUY"},
			// Tick 6: ingested before obs 6.
			{Time: epoch.Add(6 * time.Minute), Price: decimal.MustNew(1005, 1), Quantity: decimal.MustNew(3, 0), Side: "SELL"},
			// Tick 12: AFTER the last observation. Must NOT be ingested.
			{Time: epoch.Add(12 * time.Minute), Price: decimal.MustNew(100, 0), Quantity: decimal.MustNew(99, 0), Side: "BUY"},
		},
	}

	s := New(cfg, nil, WithTradeHistoryStore(store))
	bt := NewBacktester(s, nil)
	_, err := bt.Run(context.Background(), obs)
	require.NoError(t, err)

	// Three trades ingested (ticks 2, 3.5, 6). The tick-12 trade is
	// excluded. Expected OBV: +10 +5 -3 = 12.
	assert.True(t, s.OBV().Equal(decimal.MustNew(12, 0)),
		"OBV should be 12 after the run (10+5-3), got %s", s.OBV())
}

// TestBacktest_NoTradeStoreOBVStaysZero verifies that without a trade
// history store, the OBV accumulator stays at zero.
func TestBacktest_NoTradeStoreOBVStaysZero(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.OBVWindow = 3
	cfg.ConfirmationBars = 1
	cfg.InitialBalance = decimal.MustNew(10000, 0)
	cfg.Leverage = decimal.One
	cfg.LongVolWindow = 5
	cfg.ShortVolWindow = 5
	cfg.ATRWindow = 5

	const warmup = 10
	obs := make([]types.YieldObservation, 0, warmup+1)
	for i := range warmup {
		obs = append(obs, flatObs(i, decimal.MustNew(100, 0)))
	}
	obs = append(obs, flatObs(warmup, decimal.MustNew(110, 0)))

	s := New(cfg, nil) // no WithTradeHistoryStore
	bt := NewBacktester(s, nil)
	_, err := bt.Run(context.Background(), obs)
	require.NoError(t, err)

	assert.True(t, s.OBV().IsZero(),
		"OBV should stay zero without a trade store, got %s", s.OBV())
}
