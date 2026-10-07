package momentum

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dora-network/bond-trading-strategies/candles"
	"github.com/dora-network/bond-trading-strategies/fred"
	"github.com/dora-network/bond-trading-strategies/prices"
	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/govalues/decimal"
	"github.com/jackc/pgx/v5/pgxpool"
)

// candleHistoryStore reads bucketed candle history for backtests.
//
//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//counterfeiter:generate . candleHistoryStore
type candleHistoryStore interface {
	LoadCandlesBucketed(ctx context.Context, orderBookID string, resolution candles.Resolution,
		since, until time.Time) ([]candles.Candle, error)
	CandleRange(ctx context.Context, orderBookID string) (*time.Time, *time.Time, error)
}

// priceHistorySource reads tick history for backtest tick replay.
// Implemented by *prices.PGStore.
//
//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//counterfeiter:generate . priceHistorySource
type priceHistorySource interface {
	LoadHistoricalPrices(ctx context.Context, assetID string, start, end time.Time) ([]prices.AssetPrice, error)
}

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//counterfeiter:generate . benchmarkYieldClient
type benchmarkYieldClient interface {
	FetchHistoricalYields(ctx context.Context, tenor fred.Tenor, start, end time.Time) ([]fred.Observation, error)
}

// getBars loads the bar series for a backtest over [start, end],
// including (SlowWindow+1) bars of warmup before start so the slow MA
// and the ATR (seeded by the previous close) are full by the first
// decision bar. Bars are oldest-first.
func (s *Strategy) getBars(ctx context.Context, start, end time.Time) ([]types.Bar, error) {
	res := strategy.ResolutionDuration(s.cfg.Resolution)
	if res == 0 {
		return nil, fmt.Errorf("unknown resolution %q", s.cfg.Resolution)
	}
	dataStart := start.Add(-time.Duration(s.cfg.SlowWindow+1) * res)

	store, err := s.getCandleHistoryStore(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.checkCandleCoverage(ctx, store, dataStart, end); err != nil {
		return nil, err
	}

	ob := s.cfg.OrderBookID.String()
	loaded, err := store.LoadCandlesBucketed(ctx, ob, s.cfg.Resolution, dataStart, end)
	if err != nil {
		return nil, fmt.Errorf("load candles: %w", err)
	}

	needsYTM := s.cfg.SignalSource != SignalSourcePrice
	needsBenchmark := s.cfg.SignalSource == SignalSourceSpread

	// Fill benchmark yields for the whole window up front so each bar
	// can resolve its spread against the latest prior FRED observation
	// date. Price/ytm modes skip the benchmark entirely.
	if needsBenchmark {
		tenor, err := fred.ParseBenchmarkTenor(s.cfg.Tenor)
		if err != nil {
			return nil, err
		}
		benchmarkClient, err := s.getBenchmarkYieldClient()
		if err != nil {
			return nil, err
		}
		benchmarkYields, err := benchmarkClient.FetchHistoricalYields(ctx, tenor, dataStart, end)
		if err != nil {
			return nil, fmt.Errorf("fetch historical benchmark yields: %w", err)
		}
		s.setBenchmarkObservations(benchmarkYields)
	}

	bars := make([]types.Bar, 0, len(loaded))
	zeroYTM, noBenchmark := 0, 0
	for _, c := range loaded {
		if needsYTM && c.CloseYTM.IsZero() {
			// Data-contract violation in ytm/spread modes; price mode
			// never reads the YTM so such bars are kept.
			zeroYTM++
			continue
		}
		bar := strategy.BarFromCandle(c)
		if needsBenchmark {
			benchmarkYield, ok := s.cachedBenchmarkYield(bar.Time.Add(res))
			if !ok {
				noBenchmark++
				continue
			}
			bar.BenchmarkYield = benchmarkYield
		}
		bars = append(bars, bar)
	}
	if len(loaded) > 0 && len(bars) == 0 {
		return nil, fmt.Errorf("no usable candles for order book %s: %d/%d bars had zero close YTM, %d/%d had no prior benchmark yield",
			ob, zeroYTM, len(loaded), noBenchmark, len(loaded))
	}
	return bars, nil
}

// checkCandleCoverage verifies candles_history fully covers the requested
// window (including warmup) for the strategy's order book. The last
// persisted bar starts at hi and covers [hi, hi+res), so the window is
// covered when end ≤ hi+res — a bar starting at/after end is never loaded
// and must not be required.
func (s *Strategy) checkCandleCoverage(ctx context.Context, store candleHistoryStore, dataStart, end time.Time) error {
	ob := s.cfg.OrderBookID.String()
	lo, hi, err := store.CandleRange(ctx, ob)
	if err != nil {
		return fmt.Errorf("load candle range: %w", err)
	}
	if lo == nil || hi == nil {
		return &candles.ErrNoCandleCoverage{OrderBookID: ob}
	}
	res := strategy.ResolutionDuration(s.cfg.Resolution)
	if lo.After(dataStart) || hi.Add(res).Before(end) {
		return &candles.ErrNoCandleCoverage{OrderBookID: ob, Available: lo, Until: hi}
	}
	return nil
}

// PreflightBacktest validates candle coverage for a prospective backtest
// window (including warmup) without loading bars. The HTTP handler calls it
// before accepting a backtest so coverage gaps surface as a 400. It is a
// no-op when no store is injected (the self-wired path defers to Backtest).
func (s *Strategy) PreflightBacktest(ctx context.Context, start, end time.Time) error {
	s.mu.RLock()
	store := s.candleStore
	s.mu.RUnlock()
	if store == nil {
		return nil
	}
	res := strategy.ResolutionDuration(s.cfg.Resolution)
	if res == 0 {
		return fmt.Errorf("unknown resolution %q", s.cfg.Resolution)
	}
	dataStart := start.Add(-time.Duration(s.cfg.SlowWindow+1) * res)
	return s.checkCandleCoverage(ctx, store, dataStart, end)
}

func (s *Strategy) getCandleHistoryStore(ctx context.Context) (candleHistoryStore, error) {
	s.mu.RLock()
	store := s.candleStore
	s.mu.RUnlock()
	if store != nil {
		return store, nil
	}

	// The self-wired store uses a process-lifetime shared pool: one
	// pgxpool for every momentum strategy instance, not one per
	// instance (each backtest/run would otherwise leak a pool —
	// nothing in the strategy lifecycle closes it). Injected stores
	// (WithCandleHistoryStore) bypass this entirely.
	pool, err := sharedCandlePool(ctx)
	if err != nil {
		return nil, err
	}

	store = candles.NewPGStore(pool)
	s.mu.Lock()
	if s.candleStore == nil {
		s.candleStore = store
	}
	store = s.candleStore
	s.mu.Unlock()
	return store, nil
}

// requireCandleCoverage fails fast when the order book has no candle
// history at all (spec: "no candle data for a book: run fails at start"),
// instead of silently trading on ticks alone. A candle-range error (e.g.
// DB down) is best-effort, matching the prefill precedent.
func (s *Strategy) requireCandleCoverage(ctx context.Context) error {
	store, err := s.getCandleHistoryStore(ctx)
	if err != nil {
		s.logger().Warn("candle history store unavailable, skipping coverage check", "err", err)
		return nil
	}
	lo, hi, err := store.CandleRange(ctx, s.cfg.OrderBookID.String())
	if err != nil {
		s.logger().Warn("candle range lookup failed, skipping coverage check", "err", err)
		return nil
	}
	if lo == nil && hi == nil {
		return fmt.Errorf("no candle data for order book %s: cannot start a live run without candle coverage", s.cfg.OrderBookID)
	}
	return nil
}

var (
	//nolint:gochecknoglobals // Process-lifetime shared pool; see getCandleHistoryStore.
	sharedPoolOnce sync.Once
	//nolint:gochecknoglobals // Process-lifetime shared pool; see getCandleHistoryStore.
	sharedPool *pgxpool.Pool
	//nolint:gochecknoglobals // Process-lifetime shared pool; see getCandleHistoryStore.
	sharedPoolErr error
)

// sharedCandlePool lazily creates the single DATABASE_URL-backed pool
// shared by all self-wired momentum strategy instances. The pool lives
// for the process lifetime (the strategy lifecycle has no Close hook),
// so sharing it is what keeps connection count bounded across backtests
// and runs.
func sharedCandlePool(ctx context.Context) (*pgxpool.Pool, error) {
	sharedPoolOnce.Do(func() {
		dbURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
		if dbURL == "" {
			sharedPoolErr = errors.New("candle history store is not configured")
			return
		}
		pool, err := pgxpool.New(ctx, dbURL)
		if err != nil {
			sharedPoolErr = fmt.Errorf("create candle history pool: %w", err)
			return
		}
		sharedPool = pool
	})
	return sharedPool, sharedPoolErr
}

// getPriceHistoryStore returns the injected tick-history source or
// self-wires one over the shared candle pool (prices and candles share
// the same DATABASE_URL database).
func (s *Strategy) getPriceHistoryStore(ctx context.Context) (priceHistorySource, error) {
	s.mu.RLock()
	store := s.priceHistoryStore
	s.mu.RUnlock()
	if store != nil {
		return store, nil
	}
	pool, err := sharedCandlePool(ctx)
	if err != nil {
		return nil, err
	}
	store = prices.NewPGStore(pool)
	s.mu.Lock()
	if s.priceHistoryStore == nil {
		s.priceHistoryStore = store
	}
	store = s.priceHistoryStore
	s.mu.Unlock()
	return store, nil
}

// loadTicks reads the price_history ticks covering the backtest bar
// window, keyed by the base asset ID. Best-effort: on error (or no
// store at all) it logs once and returns nil so the backtester falls
// back to bar-extreme intrabar exits.
func (s *Strategy) loadTicks(ctx context.Context, assetID string, bars []types.Bar) []prices.AssetPrice {
	if len(bars) == 0 {
		return nil
	}
	store, err := s.getPriceHistoryStore(ctx)
	if err != nil {
		s.logger().Info("backtest tick replay unavailable, using bar extremes", "err", err)
		return nil
	}
	res := strategy.ResolutionDuration(s.cfg.Resolution)
	ticks, err := store.LoadHistoricalPrices(ctx, assetID, bars[0].Time, bars[len(bars)-1].Time.Add(res))
	if err != nil {
		s.logger().Info("backtest tick replay unavailable, using bar extremes", "err", err)
		return nil
	}
	return ticks
}

func (s *Strategy) getBenchmarkYieldClient() (benchmarkYieldClient, error) {
	s.mu.RLock()
	client := s.benchmarkClient
	s.mu.RUnlock()
	if client != nil {
		return client, nil
	}

	apiKey := strings.TrimSpace(os.Getenv("FRED_API_KEY"))
	if apiKey == "" {
		return nil, errors.New("benchmark yield client is not configured")
	}

	client = fred.NewClient(apiKey)
	s.mu.Lock()
	if s.benchmarkClient == nil {
		s.benchmarkClient = client
	}
	client = s.benchmarkClient
	s.mu.Unlock()
	return client, nil
}

func (s *Strategy) setBenchmarkObservations(obs []fred.Observation) {
	normalised := make([]fred.Observation, 0, len(obs))
	for _, observation := range obs {
		// fred.Observation.Yield is a decimal fraction (0.0425), the
		// same unit as the bars' CloseYTM — stored unchanged so
		// Spread() = YTM − benchmark stays unit-coherent.
		normalised = append(normalised, fred.Observation{
			Date:  fred.NormalizeDate(observation.Date),
			Yield: observation.Yield,
		})
	}

	s.mu.Lock()
	s.benchmarkObservations = normalised
	s.mu.Unlock()
}

func (s *Strategy) cachedBenchmarkYield(ts time.Time) (decimal.Decimal, bool) {
	target := fred.NormalizeDate(ts)

	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.benchmarkObservations) == 0 {
		return decimal.Zero, false
	}

	idx := sort.Search(len(s.benchmarkObservations), func(i int) bool {
		return s.benchmarkObservations[i].Date.After(target)
	})
	if idx == 0 {
		return decimal.Zero, false
	}

	return s.benchmarkObservations[idx-1].Yield, true
}

// mergeBenchmarkObservations merges new FRED observations into the in-memory
// cache, deduplicating by date and keeping the slice sorted ascending.  This
// method acquires the write lock.
func (s *Strategy) mergeBenchmarkObservations(obs []fred.Observation) {
	normalised := make([]fred.Observation, 0, len(obs))
	for _, observation := range obs {
		// Same unit contract as setBenchmarkObservations: store the
		// fraction unchanged (see the comment there).
		normalised = append(normalised, fred.Observation{
			Date:  fred.NormalizeDate(observation.Date),
			Yield: observation.Yield,
		})
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Fast path: no existing observations.
	if len(s.benchmarkObservations) == 0 {
		s.benchmarkObservations = normalised
		return
	}

	// Build a set of existing dates for dedup.
	existing := make(map[time.Time]bool, len(s.benchmarkObservations))
	for _, o := range s.benchmarkObservations {
		existing[o.Date] = true
	}

	// Append only new observations.
	for _, o := range normalised {
		if !existing[o.Date] {
			s.benchmarkObservations = append(s.benchmarkObservations, o)
			existing[o.Date] = true
		}
	}

	// Re-sort by date ascending (required by binary search in cachedBenchmarkYield).
	sort.Slice(s.benchmarkObservations, func(i, j int) bool {
		return s.benchmarkObservations[i].Date.Before(s.benchmarkObservations[j].Date)
	})
}

// latestCachedBenchmarkDate returns the date of the most recent
// benchmark observation in the cache, or ok=false when empty. Used by
// getBenchmarkYield to decide whether to refetch from FRED.
func (s *Strategy) latestCachedBenchmarkDate() (time.Time, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.benchmarkObservations) == 0 {
		return time.Time{}, false
	}
	return s.benchmarkObservations[len(s.benchmarkObservations)-1].Date, true
}
