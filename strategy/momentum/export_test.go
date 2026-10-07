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

func SetCandleHistoryStore(s *Strategy, store candleHistoryStore) {
	s.candleStore = store
}

func SetPriceHistoryStore(s *Strategy, store priceHistorySource) {
	s.priceHistoryStore = store
}

// SetBacktestTicks seeds the backtester's tick replay slice. For unit tests only.
func SetBacktestTicks(bt *Backtester, ticks []prices.AssetPrice) {
	bt.ticks = ticks
}

func SetBenchmarkYieldClient(s *Strategy, client benchmarkYieldClient) {
	s.benchmarkClient = client
}

func GetBars(ctx context.Context, s *Strategy, start, end time.Time) ([]types.Bar, error) {
	return s.getBars(ctx, start, end)
}

func PreflightBacktest(ctx context.Context, s *Strategy, start, end time.Time) error {
	return s.PreflightBacktest(ctx, start, end)
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

// RunSync runs the strategy's internal run loop synchronously and returns
// its error, so tests can assert on Run-path failures (unit tests only).
func RunSync(ctx context.Context, s *Strategy, msgs <-chan strategyPkg.Message, pricesCh <-chan map[uuid.UUID]prices.AssetPrice) error {
	s.mu.Lock()
	if s.isRunning {
		s.mu.Unlock()
		return nil
	}
	var runCtx context.Context
	runCtx, s.cancel = context.WithCancel(ctx)
	s.isRunning = true
	s.mu.Unlock()
	return s.run(runCtx, msgs, pricesCh)
}

// MergeBenchmarkObservations seeds the strategy's in-memory benchmark
// cache with the supplied observations (same normalization the
// production merge applies: dates normalized, yields stored unchanged
// as decimal fractions).
func MergeBenchmarkObservations(s *Strategy, obs []fred.Observation) {
	s.mergeBenchmarkObservations(obs)
}

func LatestCachedBenchmarkDate(s *Strategy) (time.Time, bool) {
	return s.latestCachedBenchmarkDate()
}
