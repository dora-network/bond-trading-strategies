package momentum

import (
	"context"
	"time"

	"github.com/dora-network/bond-trading-strategies/fred"
	"github.com/dora-network/bond-trading-strategies/prices"
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

// RunLoop drives the strategy's internal run loop with the supplied
// channels. Used by tests that want to exercise the live tick path
// without the pricesHandler dependency. Returns a teardown that stops
// the goroutine.
func RunLoop(s *Strategy, ctx context.Context, msgs <-chan strategyPkg.Message, pricesCh <-chan map[uuid.UUID]prices.AssetPrice) func() {
	if s.cancel == nil {
		s.cancel = func() {}
	}
	go func() {
		_ = s.run(ctx, msgs, pricesCh)
	}()
	return func() {
		if s.cancel != nil {
			s.cancel()
		}
	}
}

// MergeBenchmarkObservations seeds the strategy's in-memory benchmark
// cache with the supplied observations, applying the same percentage
// conversion the production code applies. Used by tests that want to
// exercise the cache-freshness path without re-fetching from FRED.
func MergeBenchmarkObservations(s *Strategy, obs []fred.Observation) {
	s.mergeBenchmarkObservations(obs)
}
