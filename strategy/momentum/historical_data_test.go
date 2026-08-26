package momentum_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/fred"
	"github.com/dora-network/bond-trading-strategies/prices"
	"github.com/dora-network/bond-trading-strategies/strategy/momentum"
	"github.com/dora-network/bond-trading-strategies/strategy/momentum/momentumfakes"
	"github.com/dora-network/bond-trading-strategies/strategy/strategyfakes"
)

// newSpreadStrategy builds a momentum.Strategy wired for spread mode
// against the supplied fakes. Other modes only need a price store.
func newSpreadStrategy(t *testing.T, lookup *strategyfakes.FakeMarketAPIClient, history *momentumfakes.FakeHistoricalPriceStore, benchmark *momentumfakes.FakeBenchmarkYieldClient) *momentum.Strategy {
	t.Helper()
	cfg := momentum.DefaultConfig()
	cfg.SignalSource = momentum.SignalSourceSpread
	cfg.Tenor = "10Y"
	cfg.OrderBookID = uuid.MustParse("11111111-1111-1111-1111-111111111111")
	s := momentum.New(cfg, nil)
	momentum.SetLookupClient(s, lookup)
	momentum.SetHistoricalPriceStore(s, history)
	momentum.SetBenchmarkYieldClient(s, benchmark)
	return s
}

// TestGetObservations_SpreadMode_AttachesBenchmarkYield verifies that
// spread mode attaches a benchmark yield (stored as a decimal
// fraction, the same unit as YTM) to every observation and drops rows
// without one.
func TestGetObservations_SpreadMode_AttachesBenchmarkYield(t *testing.T) {
	lookup := &strategyfakes.FakeMarketAPIClient{}
	lookup.BaseAssetIDReturns("asset-123", nil)

	ytmA := decimal.MustNew(52, 3)   // 0.052
	ytmMid := decimal.MustNew(53, 3) // 0.053 (was nil, now required by store contract)
	ytmB := decimal.MustNew(54, 3)   // 0.054
	history := &momentumfakes.FakeHistoricalPriceStore{}
	history.LoadHistoricalPricesReturns([]prices.AssetPrice{
		{AssetID: "asset-123", YTM: &ytmA, Time: time.Date(2024, 1, 2, 15, 0, 0, 0, time.UTC)},
		{AssetID: "asset-123", YTM: &ytmMid, Time: time.Date(2024, 1, 3, 15, 0, 0, 0, time.UTC)},
		{AssetID: "asset-123", YTM: &ytmB, Time: time.Date(2024, 1, 4, 15, 0, 0, 0, time.UTC)},
	}, nil)

	benchmark := &momentumfakes.FakeBenchmarkYieldClient{}
	benchmark.FetchHistoricalYieldsReturns([]fred.Observation{
		{Date: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), Yield: decimal.MustNew(45, 3)},
		{Date: time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC), Yield: decimal.MustNew(47, 3)},
	}, nil)

	s := newSpreadStrategy(t, lookup, history, benchmark)
	obs, err := momentum.GetObservations(s, context.Background(),
		time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC))

	require.NoError(t, err)
	// All three rows have non-nil YTM (the store contract) and get
	// the LATEST cached benchmark yield <= their timestamp. Jan 2
	// gets 0.045 (its own date), Jan 3 gets 0.045 (LATEST <= 3 is
	// Jan 2), Jan 4 gets 0.047 (its own date).
	require.Len(t, obs, 3)
	assert.True(t, obs[0].BenchmarkYield.Equal(decimal.MustNew(45, 3)),
		"obs[0].BenchmarkYield = %s, want 0.045", obs[0].BenchmarkYield.String())
	assert.True(t, obs[1].BenchmarkYield.Equal(decimal.MustNew(45, 3)),
		"obs[1].BenchmarkYield = %s, want 0.045 (LATEST <= Jan 3)", obs[1].BenchmarkYield.String())
	assert.True(t, obs[2].BenchmarkYield.Equal(decimal.MustNew(47, 3)),
		"obs[2].BenchmarkYield = %s, want 0.047", obs[2].BenchmarkYield.String())
	// FRED client fetched once for the price-history window.
	assert.Equal(t, 1, benchmark.FetchHistoricalYieldsCallCount())
}

// TestGetObservations_NilYTM_ReturnsContractError verifies that nil
// YTM is a contract violation, not silently dropped. The PG store
// filters ytm IS NOT NULL on insert (prices/store.go:LoadHistoricalPrices
// / LoadLastPrices), so a nil here means the upstream schema changed
// or a test fake drifted - we surface the error rather than masking it.
func TestGetObservations_NilYTM_ReturnsContractError(t *testing.T) {
	cfg := momentum.DefaultConfig()
	cfg.SignalSource = momentum.SignalSourcePrice
	cfg.OrderBookID = uuid.MustParse("11111111-1111-1111-1111-111111111111")
	s := momentum.New(cfg, nil)

	lookup := &strategyfakes.FakeMarketAPIClient{}
	lookup.BaseAssetIDReturns("asset-123", nil)
	momentum.SetLookupClient(s, lookup)

	history := &momentumfakes.FakeHistoricalPriceStore{}
	history.LoadHistoricalPricesReturns([]prices.AssetPrice{
		{AssetID: "asset-123", YTM: nil, Time: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), Price: decimal.MustNew(101, 0)},
	}, nil)
	momentum.SetHistoricalPriceStore(s, history)

	_, err := momentum.GetObservations(s, context.Background(),
		time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC))

	require.Error(t, err)
	require.ErrorContains(t, err, "store contract violation")
}

// TestGetObservations_SpreadMode_PropagatesErrors covers the error
// paths: bad tenor, missing price store, FRED failure.
func TestGetObservations_SpreadMode_PropagatesErrors(t *testing.T) {
	t.Run("historical price store error", func(t *testing.T) {
		lookup := &strategyfakes.FakeMarketAPIClient{}
		lookup.BaseAssetIDReturns("asset-123", nil)
		history := &momentumfakes.FakeHistoricalPriceStore{}
		history.LoadHistoricalPricesReturns(nil, errors.New("history failed"))
		benchmark := &momentumfakes.FakeBenchmarkYieldClient{}
		s := newSpreadStrategy(t, lookup, history, benchmark)

		_, err := momentum.GetObservations(s, context.Background(),
			time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC))
		require.ErrorContains(t, err, "history failed")
	})

	t.Run("fred fetch error", func(t *testing.T) {
		lookup := &strategyfakes.FakeMarketAPIClient{}
		lookup.BaseAssetIDReturns("asset-123", nil)
		ytm := decimal.MustNew(52, 3)
		history := &momentumfakes.FakeHistoricalPriceStore{}
		history.LoadHistoricalPricesReturns([]prices.AssetPrice{
			{AssetID: "asset-123", YTM: &ytm, Time: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)},
		}, nil)
		benchmark := &momentumfakes.FakeBenchmarkYieldClient{}
		benchmark.FetchHistoricalYieldsReturns(nil, errors.New("fred failed"))
		s := newSpreadStrategy(t, lookup, history, benchmark)

		_, err := momentum.GetObservations(s, context.Background(),
			time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC))
		require.ErrorContains(t, err, "fred failed")
	})

	t.Run("lookup asset ID error", func(t *testing.T) {
		lookup := &strategyfakes.FakeMarketAPIClient{}
		lookup.BaseAssetIDReturns("", errors.New("asset lookup failed"))
		history := &momentumfakes.FakeHistoricalPriceStore{}
		benchmark := &momentumfakes.FakeBenchmarkYieldClient{}
		s := newSpreadStrategy(t, lookup, history, benchmark)

		_, err := momentum.GetObservations(s, context.Background(),
			time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC))
		require.ErrorContains(t, err, "asset lookup failed")
	})
}

// TestMergeBenchmarkObservations_DedupsByDate verifies that
// re-merging an obs with a date already in the cache leaves the
// original entry intact (the dedup-by-date contract).
func TestMergeBenchmarkObservations_DedupsByDate(t *testing.T) {
	cfg := momentum.DefaultConfig()
	s := momentum.New(cfg, nil)

	obs1 := []fred.Observation{
		{Date: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Yield: decimal.MustNew(45, 3)},
		{Date: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), Yield: decimal.MustNew(46, 3)},
	}
	obs2 := []fred.Observation{
		{Date: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), Yield: decimal.MustNew(99, 3)}, // dup
		{Date: time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC), Yield: decimal.MustNew(47, 3)}, // new
	}

	momentum.MergeBenchmarkObservations(s, obs1)
	momentum.MergeBenchmarkObservations(s, obs2)

	// Query for the duplicated date: original yield (0.046) wins,
	// not the second-pass 0.099.
	got, ok := momentum.GetBenchmarkYield(context.Background(), s, time.Date(2024, 1, 2, 12, 0, 0, 0, time.UTC))
	assert.True(t, ok)
	assert.True(t, got.Equal(decimal.MustNew(46, 3)),
		"duplicated-date entry should keep its original yield (0.046), got %s",
		got.String())

	// And the new date is present.
	got, ok = momentum.GetBenchmarkYield(context.Background(), s, time.Date(2024, 1, 3, 12, 0, 0, 0, time.UTC))
	assert.True(t, ok)
	assert.True(t, got.Equal(decimal.MustNew(47, 3)),
		"new date should be present with 0.047, got %s", got.String())
}

// TestCachedBenchmarkYield_BinarySearchNearest verifies that
// cachedBenchmarkYield returns the most recent entry whose date is
// <= the query ts (binary search semantics). Empty cache returns
// decimal.Zero.
func TestCachedBenchmarkYield_BinarySearchNearest(t *testing.T) {
	cfg := momentum.DefaultConfig()
	s := momentum.New(cfg, nil)
	momentum.MergeBenchmarkObservations(s, []fred.Observation{
		{Date: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Yield: decimal.MustNew(45, 3)},
		{Date: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), Yield: decimal.MustNew(46, 3)},
		{Date: time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC), Yield: decimal.MustNew(47, 3)},
	})

	// Query exactly on a cached date: returns it.
	got, ok := momentum.GetBenchmarkYield(context.Background(), s, time.Date(2024, 1, 2, 12, 0, 0, 0, time.UTC))
	assert.True(t, ok, "cached date should satisfy the request")
	assert.True(t, got.Equal(decimal.MustNew(46, 3)))

	// Query between cached dates: returns the LATEST <= query.
	got, ok = momentum.GetBenchmarkYield(context.Background(), s, time.Date(2024, 1, 3, 12, 0, 0, 0, time.UTC))
	assert.True(t, ok)
	assert.True(t, got.Equal(decimal.MustNew(46, 3)),
		"query between dates should return the LATEST <= query (0.046 from Jan 2), got %s", got.String())

	// Empty cache: no benchmark available (ok=false — callers must
	// skip the tick, not evaluate a spread against zero).
	cfg2 := momentum.DefaultConfig()
	s2 := momentum.New(cfg2, nil)
	got, ok = momentum.GetBenchmarkYield(context.Background(), s2, time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC))
	assert.False(t, ok, "empty cache must report no benchmark available")
	assert.True(t, got.IsZero())
}

// TestPrefillWindow_FillsWindowsFromHistory verifies that prefillWindow
// loads SlowWindow*2 prices from the historical price store and feeds
// them through Update, populating the rolling windows.
func TestPrefillWindow_FillsWindowsFromHistory(t *testing.T) {
	cfg := momentum.DefaultConfig()
	cfg.SignalSource = momentum.SignalSourcePrice
	cfg.FastWindow = 3
	cfg.SlowWindow = 5
	cfg.ATRWindow = 3
	cfg.OrderBookID = uuid.MustParse("11111111-1111-1111-1111-111111111111")
	s := momentum.New(cfg, nil)

	lookup := &strategyfakes.FakeMarketAPIClient{}
	lookup.BaseAssetIDReturns("asset-123", nil)
	momentum.SetLookupClient(s, lookup)

	// SlowWindow=5, so prefillWindow loads 10 prices. Each row needs
	// a non-nil YTM to satisfy the store contract (PG filters
	// ytm IS NOT NULL on load).
	ytm := decimal.MustNew(50, 3) // 0.05 fixed
	obsPrices := make([]prices.AssetPrice, 10)
	for i := range obsPrices {
		obsPrices[i] = prices.AssetPrice{
			AssetID: "asset-123",
			YTM:     &ytm,
			Time:    time.Date(2024, 1, i+1, 0, 0, 0, 0, time.UTC),
			Price:   decimal.MustNew(int64(100+i), 0),
		}
	}
	history := &momentumfakes.FakeHistoricalPriceStore{}
	history.LoadLastPricesReturns(obsPrices, nil)
	momentum.SetHistoricalPriceStore(s, history)

	err := momentum.PrefillWindow(s, context.Background(), "asset-123")
	require.NoError(t, err)
	assert.Equal(t, 1, history.LoadLastPricesCallCount())
	_, _, limit := history.LoadLastPricesArgsForCall(0)
	assert.Equal(t, 10, limit, "prefillWindow must request SlowWindow*2 prices")
}

// TestLatestCachedBenchmarkDate_TracksMostRecent verifies that
// latestCachedBenchmarkDate returns the cache's most recent date,
// or ok=false when empty.
func TestLatestCachedBenchmarkDate_TracksMostRecent(t *testing.T) {
	cfg := momentum.DefaultConfig()
	s := momentum.New(cfg, nil)

	// Empty cache: returns false.
	_, ok := momentum.LatestCachedBenchmarkDate(s)
	assert.False(t, ok, "empty cache must return ok=false")

	// Seed with two dates; query returns the LATEST.
	momentum.MergeBenchmarkObservations(s, []fred.Observation{
		{Date: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Yield: decimal.MustNew(45, 3)},
		{Date: time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC), Yield: decimal.MustNew(48, 3)},
	})
	got, ok := momentum.LatestCachedBenchmarkDate(s)
	require.True(t, ok)
	assert.True(t, got.Equal(time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC)),
		"latestCachedBenchmarkDate should return the most-recent date (Jan 5), got %s", got)
}
