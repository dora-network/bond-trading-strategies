package momentum

import (
	"context"
	"time"

	strategyPkg "github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
)

func SetLookupClient(s *Strategy, client strategyPkg.MarketAPIClient) {
	s.marketAPIClient = client
}

func SetHistoricalPriceStore(s *Strategy, store historicalPriceStore) {
	s.historyStore = store
}

func SetBenchmarkYieldClient(s *Strategy, client benchmarkYieldClient) {
	s.benchmarkClient = client
}

func LookupAssetID(s *Strategy, orderBookID uuid.UUID) (string, error) {
	return s.lookupAssetID(orderBookID)
}

func GetObservations(s *Strategy, ctx context.Context, start, end time.Time) ([]types.YieldObservation, error) {
	return s.getObservations(ctx, start, end)
}

func GetBenchmarkYield(ctx context.Context, s *Strategy, ts time.Time) decimal.Decimal {
	return s.getBenchmarkYield(ctx, ts)
}

func CurrentPosition(s *Strategy, ctx context.Context, assetID string) (decimal.Decimal, error) {
	return s.currentPosition(ctx, assetID)
}

func CappedOrderQuantity(s *Strategy, positionSize, currentPosition, price decimal.Decimal) (decimal.Decimal, bool, error) {
	return s.cappedOrderQuantity(positionSize, currentPosition, price)
}

func BondQty(s *Strategy) decimal.Decimal {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bondQty
}

func UsdBal(s *Strategy) decimal.Decimal {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.usdBal
}

func InitializeBalances(s *Strategy, ctx context.Context, assetID string) {
	s.initializeBalances(ctx, assetID)
}

func BalancesInitialized(s *Strategy) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.balancesInitialized
}

func OpenSignal(s *Strategy) types.Signal {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.openSignal
}

func EntryPrice(s *Strategy) decimal.Decimal {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.entryPrice
}

func EntryATR(s *Strategy) decimal.Decimal {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.entryATR
}
