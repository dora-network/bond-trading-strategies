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

// White-box test helpers. Only helpers that some test in this package
// actually calls are exported; dead helpers (LookupAssetID,
// CurrentPosition, BondQty, UsdBal, InitializeBalances,
// BalancesInitialized, EntryPrice, EntryATR, UpdateObs) were trimmed
// per the 16-reviewer P3 follow-up "Nine unused exports in
// export_test.go". Keep this set aligned with what the *_test.go
// files actually import from the momentum package.

func SetLookupClient(s *Strategy, client strategyPkg.MarketAPIClient) {
	s.marketAPIClient = client
}

func SetHistoricalPriceStore(s *Strategy, store historicalPriceStore) {
	s.historyStore = store
}

func SetBenchmarkYieldClient(s *Strategy, client benchmarkYieldClient) {
	s.benchmarkClient = client
}

func GetObservations(ctx context.Context, s *Strategy, start, end time.Time) ([]types.YieldObservation, error) {
	return s.getObservations(ctx, start, end)
}

func GetBenchmarkYield(ctx context.Context, s *Strategy, ts time.Time) (decimal.Decimal, bool) {
	return s.getBenchmarkYield(ctx, ts)
}

func CappedOrderQuantity(s *Strategy, positionSize, currentPosition, price decimal.Decimal) (decimal.Decimal, bool, error) {
	return s.cappedOrderQuantity(positionSize, currentPosition, price)
}

func OpenSignal(s *Strategy) types.Signal {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.openSignal
}

// RunLoop drives the strategy's internal run loop with the supplied
// channels. Used by tests that want to exercise the live tick path
// without the pricesHandler dependency. Returns a teardown that stops
// the goroutine.
func RunLoop(ctx context.Context, s *Strategy, msgs <-chan strategyPkg.Message, pricesCh <-chan map[uuid.UUID]prices.AssetPrice) func() {
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
// cache with the supplied observations (same normalization the
// production merge applies: dates normalized, yields stored unchanged
// as decimal fractions).
func MergeBenchmarkObservations(s *Strategy, obs []fred.Observation) {
	s.mergeBenchmarkObservations(obs)
}

func PrefillWindow(ctx context.Context, s *Strategy, assetID string) error {
	return s.prefillWindow(ctx, assetID)
}

// WindowsReady reports whether the fast, slow, and ATR rolling windows
// are all populated — used by prefill tests to assert window population
// (not just store call args).
func WindowsReady(s *Strategy) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.fastWin.Ready() && s.slowWin.Ready() && s.atrWin.Ready()
}

func LatestCachedBenchmarkDate(s *Strategy) (time.Time, bool) {
	return s.latestCachedBenchmarkDate()
}
