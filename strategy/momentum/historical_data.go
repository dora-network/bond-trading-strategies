package momentum

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/dora-network/bond-trading-strategies/fred"
	"github.com/dora-network/bond-trading-strategies/prices"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/govalues/decimal"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//counterfeiter:generate . historicalPriceStore
type historicalPriceStore interface {
	LoadHistoricalPrices(ctx context.Context, assetID string, start, end time.Time) ([]prices.AssetPrice, error)
	LoadLastPrices(ctx context.Context, assetID string, limit int) ([]prices.AssetPrice, error)
}

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//counterfeiter:generate . benchmarkYieldClient
type benchmarkYieldClient interface {
	FetchHistoricalYields(ctx context.Context, tenor fred.Tenor, start, end time.Time) ([]fred.Observation, error)
}

// BenchmarkTenor is re-exported from fred.
type BenchmarkTenor = fred.BenchmarkTenor

// SupportedBenchmarkTenors is re-exported from fred.
func SupportedBenchmarkTenors() []fred.BenchmarkTenor {
	return fred.SupportedBenchmarkTenors()
}

func (s *Strategy) getObservations(ctx context.Context, start, end time.Time) ([]types.YieldObservation, error) {
	assetID, err := s.lookupAssetID(s.cfg.OrderBookID)
	if err != nil {
		return nil, fmt.Errorf("lookup asset ID: %w", err)
	}

	historyStore, err := s.getHistoricalPriceStore(ctx)
	if err != nil {
		return nil, err
	}

	history, err := historyStore.LoadHistoricalPrices(ctx, assetID, start, end)
	if err != nil {
		return nil, fmt.Errorf("load historical prices: %w", err)
	}

	// Only spread mode needs the FRED benchmark; price/ytm do not.
	if s.cfg.SignalSource == SignalSourceSpread {
		tenor, err := fred.ParseBenchmarkTenor(s.cfg.Tenor)
		if err != nil {
			return nil, fmt.Errorf("parse tenor: %w", err)
		}
		benchmarkClient, err := s.getBenchmarkYieldClient()
		if err != nil {
			return nil, err
		}
		if len(history) > 0 {
			yields, err := benchmarkClient.FetchHistoricalYields(ctx, tenor, history[0].Time, history[len(history)-1].Time)
			if err != nil {
				return nil, fmt.Errorf("fetch benchmark yields: %w", err)
			}
			s.setBenchmarkObservations(yields)
		}
	}

	observations := make([]types.YieldObservation, 0, len(history))
	for _, price := range history {
		if price.YTM == nil && s.cfg.SignalSource != SignalSourcePrice {
			continue
		}
		obs := types.YieldObservation{
			Time:   price.Time,
			BondID: price.AssetID,
			Price:  price.Price,
		}
		if price.YTM != nil {
			obs.YTM = *price.YTM
		}
		if s.cfg.SignalSource == SignalSourceSpread {
			benchmarkYield, ok := s.cachedBenchmarkYield(price.Time)
			if !ok {
				continue
			}
			obs.BenchmarkYield = benchmarkYield
		}
		observations = append(observations, obs)
	}
	return observations, nil
}

func (s *Strategy) getHistoricalPriceStore(ctx context.Context) (historicalPriceStore, error) {
	s.mu.RLock()
	store := s.historyStore
	s.mu.RUnlock()
	if store != nil {
		return store, nil
	}

	dbURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if dbURL == "" {
		return nil, errors.New("historical price store is not configured")
	}
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return nil, fmt.Errorf("create price history pool: %w", err)
	}

	store = prices.NewPGStore(pool)
	s.mu.Lock()
	if s.historyStore == nil {
		s.historyStore = store
	}
	store = s.historyStore
	s.mu.Unlock()
	return store, nil
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
		yieldPct, _ := observation.Yield.Mul(decimal.MustNew(100, 0)) //nolint:mnd
		normalised = append(normalised, fred.Observation{
			Date:  fred.NormalizeDate(observation.Date),
			Yield: yieldPct,
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
		yieldPct, _ := observation.Yield.Mul(decimal.MustNew(100, 0)) //nolint:mnd
		normalised = append(normalised, fred.Observation{
			Date:  fred.NormalizeDate(observation.Date),
			Yield: yieldPct,
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

// prefillWindow loads recent historical prices into the rolling windows
// so that signals can be generated immediately on the first live price
// tick. Loads SlowWindow×2 prices to fill all three windows. Best-effort:
// if historical data is unavailable the method returns an error but
// the caller may continue with an empty window. Spread mode additionally
// fetches FRED benchmark yields.
func (s *Strategy) prefillWindow(ctx context.Context, assetID string) error {
	historyStore, err := s.getHistoricalPriceStore(ctx)
	if err != nil {
		return fmt.Errorf("get history store: %w", err)
	}

	limit := s.cfg.SlowWindow * 2 // 2× slow window fills all three windows
	history, err := historyStore.LoadLastPrices(ctx, assetID, limit)
	if err != nil {
		return fmt.Errorf("load last prices: %w", err)
	}

	if s.cfg.SignalSource == SignalSourceSpread && len(history) > 0 {
		tenor, err := fred.ParseBenchmarkTenor(s.cfg.Tenor)
		if err != nil {
			return fmt.Errorf("parse tenor: %w", err)
		}
		benchmarkClient, err := s.getBenchmarkYieldClient()
		if err != nil {
			return fmt.Errorf("get benchmark client: %w", err)
		}
		yields, err := benchmarkClient.FetchHistoricalYields(ctx, tenor, history[0].Time, history[len(history)-1].Time)
		if err != nil {
			return fmt.Errorf("fetch benchmark yields: %w", err)
		}
		s.setBenchmarkObservations(yields)
	}

	for _, price := range history {
		if price.YTM == nil && s.cfg.SignalSource != SignalSourcePrice {
			continue
		}
		obs := types.YieldObservation{
			Time:   price.Time,
			BondID: price.AssetID,
			Price:  price.Price,
		}
		if price.YTM != nil {
			obs.YTM = *price.YTM
		}
		if s.cfg.SignalSource == SignalSourceSpread {
			benchmarkYield, ok := s.cachedBenchmarkYield(price.Time)
			if !ok {
				continue
			}
			obs.BenchmarkYield = benchmarkYield
		}
		if _, err := s.Update(obs); err != nil {
			return fmt.Errorf("fill window: %w", err)
		}
	}
	return nil
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
