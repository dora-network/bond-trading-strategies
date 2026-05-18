package meanreversion

import (
	"context"
	"time"

	pricesPkg "github.com/dora-network/bond-trading-strategies/prices"
	strategyPkg "github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
)

func SetLookupClient(s *Strategy, client marketAPIClient) {
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

func InitializeBalances(s *Strategy, ctx context.Context, baseAssetID string) {
	s.initializeBalances(ctx, baseAssetID)
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

// RunWithPrices runs the strategy's internal run loop with a caller-supplied
// price channel, bypassing the prices.Handler subscription. For unit tests only.
func RunWithPrices(s *Strategy, ctx context.Context, msgs <-chan strategyPkg.Message, priceCh <-chan map[uuid.UUID]pricesPkg.AssetPrice) {
	s.mu.Lock()
	if s.isRunning {
		s.mu.Unlock()
		return
	}
	var runCtx context.Context
	runCtx, s.cancel = context.WithCancel(ctx)
	s.isRunning = true
	s.mu.Unlock()
	s.run(runCtx, msgs, priceCh)
}
