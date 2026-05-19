package meanreversion

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

type BenchmarkTenor struct {
	Code        string
	Description string
	Value       fred.Tenor
	Aliases     []string
}

//nolint:gochecknoglobals // Package-level constant list of known benchmark tenors
var benchmarkTenors = []BenchmarkTenor{
	{Code: "1M", Description: "1 Month Treasury", Value: fred.Tenor1Month, Aliases: []string{"1MO", "1MON", "1MONTH"}},
	{Code: "3M", Description: "3 Month Treasury", Value: fred.Tenor3Month, Aliases: []string{"3MO", "3MON", "3MONTH"}},
	{Code: "6M", Description: "6 Month Treasury", Value: fred.Tenor6Month, Aliases: []string{"6MO", "6MON", "6MONTH"}},
	{Code: "1Y", Description: "1 Year Treasury", Value: fred.Tenor1Year, Aliases: []string{"1YR", "1YEAR"}},
	{Code: "2Y", Description: "2 Year Treasury", Value: fred.Tenor2Year, Aliases: []string{"2YR", "2YEAR"}},
	{Code: "3Y", Description: "3 Year Treasury", Value: fred.Tenor3Year, Aliases: []string{"3YR", "3YEAR"}},
	{Code: "5Y", Description: "5 Year Treasury", Value: fred.Tenor5Year, Aliases: []string{"5YR", "5YEAR"}},
	{Code: "7Y", Description: "7 Year Treasury", Value: fred.Tenor7Year, Aliases: []string{"7YR", "7YEAR"}},
	{Code: "10Y", Description: "10 Year Treasury", Value: fred.Tenor10Year, Aliases: []string{"10YR", "10YEAR"}},
	{Code: "20Y", Description: "20 Year Treasury", Value: fred.Tenor20Year, Aliases: []string{"20YR", "20YEAR"}},
	{Code: "30Y", Description: "30 Year Treasury", Value: fred.Tenor30Year, Aliases: []string{"30YR", "30YEAR"}},
}

func SupportedBenchmarkTenors() []BenchmarkTenor {
	return append([]BenchmarkTenor(nil), benchmarkTenors...)
}

func (s *Strategy) getObservations(ctx context.Context, start, end time.Time) ([]types.YieldObservation, error) {
	tenor, err := parseBenchmarkTenor(s.cfg.Tenor)
	if err != nil {
		return nil, err
	}

	assetID, err := s.lookupAssetID(s.cfg.OrderBookID)
	if err != nil {
		return nil, fmt.Errorf("lookup asset ID: %w", err)
	}

	historyStore, err := s.getHistoricalPriceStore(ctx)
	if err != nil {
		return nil, err
	}
	benchmarkClient, err := s.getBenchmarkYieldClient()
	if err != nil {
		return nil, err
	}

	history, err := historyStore.LoadHistoricalPrices(ctx, assetID, start, end)
	if err != nil {
		return nil, fmt.Errorf("load historical prices: %w", err)
	}
	benchmarkYields, err := benchmarkClient.FetchHistoricalYields(ctx, tenor, start, end)
	if err != nil {
		return nil, fmt.Errorf("fetch historical benchmark yields: %w", err)
	}
	s.setBenchmarkObservations(benchmarkYields)

	observations := make([]types.YieldObservation, 0, len(history))
	for _, price := range history {
		if price.YTM == nil {
			continue
		}
		benchmarkYield, _, ok := s.cachedBenchmarkYield(price.Time)
		if !ok {
			continue
		}
		observations = append(observations, types.YieldObservation{
			Time:           price.Time,
			BondID:         price.AssetID,
			YTM:            *price.YTM,
			BenchmarkYield: benchmarkYield,
			Price:          price.Price,
		})
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
			Date:  normalizeDate(observation.Date),
			Yield: yieldPct,
		})
	}

	s.mu.Lock()
	s.benchmarkObservations = normalised
	s.mu.Unlock()
}

func (s *Strategy) cachedBenchmarkYield(ts time.Time) (value decimal.Decimal, date time.Time, ok bool) {
	target := normalizeDate(ts)

	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.benchmarkObservations) == 0 {
		return decimal.Zero, time.Time{}, false
	}

	idx := sort.Search(len(s.benchmarkObservations), func(i int) bool {
		return s.benchmarkObservations[i].Date.After(target)
	})
	if idx == 0 {
		return decimal.Zero, time.Time{}, false
	}

	obs := s.benchmarkObservations[idx-1]
	return obs.Yield, obs.Date, true
}

// mergeBenchmarkObservations merges new FRED observations into the in-memory
// cache, deduplicating by date and keeping the slice sorted ascending.  This
// method acquires the write lock.
func (s *Strategy) mergeBenchmarkObservations(obs []fred.Observation) {
	normalised := make([]fred.Observation, 0, len(obs))
	for _, observation := range obs {
		yieldPct, _ := observation.Yield.Mul(decimal.MustNew(100, 0)) //nolint:mnd
		normalised = append(normalised, fred.Observation{
			Date:  normalizeDate(observation.Date),
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

// prefillWindow loads recent historical prices and benchmark yields into the
// rolling window so that signals can be generated immediately on the first
// live price tick, without waiting for LookbackWindow observations to
// accumulate organically.
//
// Historical prices are loaded as the last (2× LookbackWindow) ticks to
// ensure enough observations exist to fill the rolling window.
//
// This is best-effort: if historical data is unavailable (no DB connection,
// no FRED API key, etc.) the method returns an error but the caller may
// choose to continue with an empty window.
func (s *Strategy) prefillWindow(ctx context.Context, assetID string) error {
	historyStore, err := s.getHistoricalPriceStore(ctx)
	if err != nil {
		return fmt.Errorf("get history store: %w", err)
	}

	tenor, err := parseBenchmarkTenor(s.cfg.Tenor)
	if err != nil {
		return fmt.Errorf("parse tenor: %w", err)
	}

	benchmarkClient, err := s.getBenchmarkYieldClient()
	if err != nil {
		return fmt.Errorf("get benchmark client: %w", err)
	}

	limit := s.cfg.LookbackWindow * 2
	history, err := historyStore.LoadLastPrices(ctx, assetID, limit)
	if err != nil {
		return fmt.Errorf("load last prices: %w", err)
	}

	if len(history) > 0 {
		benchmarkYields, err := benchmarkClient.FetchHistoricalYields(ctx, tenor, history[0].Time, history[len(history)-1].Time)
		if err != nil {
			return fmt.Errorf("fetch benchmark yields: %w", err)
		}
		s.setBenchmarkObservations(benchmarkYields)
	}

	for _, price := range history {
		if price.YTM == nil {
			continue
		}
		benchmarkYield, _, ok := s.cachedBenchmarkYield(price.Time)
		if !ok {
			continue
		}
		obs := types.YieldObservation{
			Time:           price.Time,
			BondID:         price.AssetID,
			YTM:            *price.YTM,
			BenchmarkYield: benchmarkYield,
			Price:          price.Price,
		}
		if _, err := s.Update(obs); err != nil {
			return fmt.Errorf("fill window: %w", err)
		}
	}

	return nil
}

func parseBenchmarkTenor(value string) (fred.Tenor, error) {
	normalised := normalizeTenor(value)
	for _, tenor := range benchmarkTenors {
		if normalised == tenor.Code {
			return tenor.Value, nil
		}
		for _, alias := range tenor.Aliases {
			if normalised == alias {
				return tenor.Value, nil
			}
		}
	}
	return 0, fmt.Errorf("unsupported tenor %q", value)
}

func normalizeTenor(value string) string {
	normalised := strings.ToUpper(strings.TrimSpace(value))
	normalised = strings.ReplaceAll(normalised, "-", "")
	normalised = strings.ReplaceAll(normalised, "_", "")
	normalised = strings.ReplaceAll(normalised, " ", "")
	normalised = strings.TrimSuffix(normalised, "S")
	return normalised
}

func normalizeDate(ts time.Time) time.Time {
	year, month, day := ts.UTC().Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}
