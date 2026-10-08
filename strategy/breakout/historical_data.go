package breakout

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dora-network/bond-trading-strategies/candles"
	"github.com/dora-network/bond-trading-strategies/prices"
	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
)

// candleHistoryStore reads bucketed candle history for backtests.
//
//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//counterfeiter:generate -o breakoutfakes/fake_candle_history_store.go . candleHistoryStore
type candleHistoryStore interface {
	LoadCandlesBucketed(ctx context.Context, orderBookID string, resolution candles.Resolution,
		since, until time.Time) ([]candles.Candle, error)
	CandleRange(ctx context.Context, orderBookID string) (*time.Time, *time.Time, error)
}

// priceHistorySource reads tick history for backtest tick replay.
// Implemented by *prices.PGStore.
//
//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//counterfeiter:generate -o breakoutfakes/fake_price_history_source.go . priceHistorySource
type priceHistorySource interface {
	LoadHistoricalPrices(ctx context.Context, assetID string, start, end time.Time) ([]prices.AssetPrice, error)
}

// getBars loads the bar series for a backtest over [start, end],
// including (LongVolWindow+1) bars of warmup before start so the
// volatility windows and the ATR (seeded by the previous close) are
// full by the first decision bar. Bars are oldest-first.
//
// loadEnd floors end to the resolution grid. An unaligned end (e.g. a
// caller passing end = start + N*res + 7m30s at a 15m resolution) would
// otherwise fold the PARTIAL trailing bucket as a closed bar — the
// last bar would cover only 7m30s of real data, its CLOSE would land
// past end, and an entry on it would be stamped past TradeTo. A bar
// is final only when complete; live (BarCloser) never emits a partial
// one, so the partial bucket must not be replayed at all.
func (s *Strategy) getBars(ctx context.Context, start, end time.Time) ([]types.Bar, error) {
	res := strategy.ResolutionDuration(s.cfg.Resolution)
	if res == 0 {
		return nil, fmt.Errorf("unknown resolution %q", s.cfg.Resolution)
	}
	dataStart := start.Add(-time.Duration(s.cfg.LongVolWindow+1) * res)
	loadEnd := end.UTC().Truncate(res)

	store, err := s.getCandleHistoryStore(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.checkCandleCoverage(ctx, store, dataStart, loadEnd); err != nil {
		return nil, err
	}

	ob := s.cfg.OrderBookID.String()
	loaded, err := store.LoadCandlesBucketed(ctx, ob, s.cfg.Resolution, dataStart, loadEnd)
	if err != nil {
		return nil, fmt.Errorf("load candles: %w", err)
	}

	// Breakout is price-only: zero-CloseYTM bars pass through
	// (no YTM/spread series to drop them for).
	bars := make([]types.Bar, 0, len(loaded))
	for _, c := range loaded {
		bars = append(bars, strategy.BarFromCandle(c))
	}
	return bars, nil
}

// sourceBarDuration is the resolution of the raw candles_history rows
// CandleRange reports; the raw table stores 1m candles, not the strategy's
// bar resolution.
const sourceBarDuration = time.Minute

// checkCandleCoverage verifies candles_history fully covers the requested
// window (including warmup) for the strategy's order book. CandleRange
// reports the min/max timestamps of the RAW 1m candles_history rows: the
// last persisted row starts at hi and covers [hi, hi+1m), so the window
// is covered when end ≤ hi+1m — a 1m candle starting at/after end is
// never loaded and must not be required. The caller passes the
// resolution-truncated loadEnd (see getBars / PreflightBacktest) so
// coverage is checked against the same window the bucketed load uses
// and never against a partial trailing bucket.
func (s *Strategy) checkCandleCoverage(ctx context.Context, store candleHistoryStore, dataStart, end time.Time) error {
	ob := s.cfg.OrderBookID.String()
	lo, hi, err := store.CandleRange(ctx, ob)
	if err != nil {
		return fmt.Errorf("load candle range: %w", err)
	}
	if lo == nil || hi == nil {
		return &candles.ErrNoCandleCoverage{OrderBookID: ob}
	}
	if lo.After(dataStart) || hi.Add(sourceBarDuration).Before(end) {
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
	dataStart := start.Add(-time.Duration(s.cfg.LongVolWindow+1) * res)
	// Floor end to the resolution grid so coverage is checked against
	// the same window getBars loads — never against a partial trailing
	// bucket.
	loadEnd := end.UTC().Truncate(res)
	return s.checkCandleCoverage(ctx, store, dataStart, loadEnd)
}

func (s *Strategy) getCandleHistoryStore(ctx context.Context) (candleHistoryStore, error) {
	s.mu.RLock()
	store := s.candleStore
	s.mu.RUnlock()
	if store != nil {
		return store, nil
	}

	// The self-wired store uses a process-lifetime shared pool: one
	// pgxpool for every breakout strategy instance, not one per
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
// shared by all self-wired breakout strategy instances. The pool lives
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
