package breakout

import (
	"context"
	"time"

	"github.com/dora-network/bond-trading-strategies/prices"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
)

// White-box test helpers for the external test package
// (strategy_export_test.go's package is breakout_test).

func SetCandleHistoryStore(s *Strategy, store candleHistoryStore) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.candleStore = store
}

func SetPriceHistoryStore(s *Strategy, store priceHistorySource) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.priceHistoryStore = store
}

// SetBacktestTicks seeds the backtester's tick replay slice. For unit tests only.
func SetBacktestTicks(bt *Backtester, ticks []prices.AssetPrice) {
	bt.ticks = ticks
}

func GetBars(ctx context.Context, s *Strategy, start, end time.Time) ([]types.Bar, error) {
	return s.getBars(ctx, start, end)
}

func PreflightBacktest(ctx context.Context, s *Strategy, start, end time.Time) error {
	return s.PreflightBacktest(ctx, start, end)
}

// SetBaseAssetID seeds the resolved base-asset UUID so Update can
// stamp it onto Decision.bondID without a live market API client.
func SetBaseAssetID(s *Strategy, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.baseAssetID = id
}

// GetBaseAssetID exposes the base-asset UUID the run loop resolved.
func GetBaseAssetID(s *Strategy) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.baseAssetID
}
