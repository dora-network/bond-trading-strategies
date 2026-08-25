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
	"github.com/dora-network/bond-trading-strategies/strategy/momentum"
)

// TestGetBenchmarkYield_StaleCacheFallbackOnFREDError verifies that
// when the FRED client errors on a date newer than the cached
// entries, momentum.getBenchmarkYield returns the most recent cached
// yield (not decimal.Zero). Pre-fix, every error path returned
// decimal.Zero, which in spread mode silently inverted the spread
// sign (YTM - 0 in price-fraction space) and let stale signals drive
// trades on garbage data.
//

// The cache is seeded directly via MergeBenchmarkObservations so the
// test controls the input exactly (0.0425 fractional, merge
// converts to 4.25 percentage). This isolates the fallback branch
// from the FRED->merge path.
func TestGetBenchmarkYield_StaleCacheFallbackOnFREDError(t *testing.T) {
	cfg := momentum.DefaultConfig()
	cfg.SignalSource = momentum.SignalSourceSpread
	cfg.Tenor = "10Y"
	cfg.OrderBookID = uuid.MustParse("11111111-1111-1111-1111-111111111111")
	s := momentum.New(cfg, nil)

	// Seed the cache with a 2026-08-23 observation at 4.25%.
	// mergeBenchmarkObservations multiplies by 100; we pre-multiply
	// so the cached value is the final 4.25.
	momentum.MergeBenchmarkObservations(s, []fred.Observation{
		{
			Date:  time.Date(2026, 8, 23, 0, 0, 0, 0, time.UTC),
			Yield: decimal.MustNew(425, 4), // 0.0425; merge converts to 4.25 (percentage)
		},
	})

	// FRED client that always errors. Wired so the function takes
	// the FRED-fetch path on the second call.
	client := &alwaysErrorClient{err: errors.New("simulated FRED outage")}
	momentum.SetBenchmarkYieldClient(s, client)

	// Query for a date strictly later than the cached entry. This
	// forces the freshness check to fail:
	//   latestDate = 2026-08-23 < normedTS = 2026-08-24
	// so the function falls through to FetchHistoricalYields, which
	// now errors. The fallback must return the cached 4.25.
	ts24 := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	got := momentum.GetBenchmarkYield(context.Background(), s, ts24)

	require.NotNil(t, got)
	assert.True(t, got.Equal(decimal.MustNew(425, 2)),
		"FRED outage on a stale-cache day must return the exact cached "+
			"yield (4.25), not decimal.Zero (which would corrupt spread-mode "+
			"signals); got %s", got.String())
}

type alwaysErrorClient struct{ err error }

func (a *alwaysErrorClient) FetchHistoricalYields(_ context.Context, _ fred.Tenor, _, _ time.Time) ([]fred.Observation, error) {
	return nil, a.err
}
