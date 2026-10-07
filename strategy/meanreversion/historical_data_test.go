package meanreversion_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/candles"
	"github.com/dora-network/bond-trading-strategies/fred"
	"github.com/dora-network/bond-trading-strategies/strategy/meanreversion"
	"github.com/dora-network/bond-trading-strategies/strategy/meanreversion/meanreversionfakes"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	btStart = time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	btEnd   = time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC)
)

func barTestConfig() meanreversion.Config {
	cfg := defaultConfig()
	cfg.OrderBookID = uuid.Must(uuid.NewV7())
	cfg.Tenor = "10Y"
	cfg.Resolution = "1h"
	return cfg
}

func coveredStore(lo, hi time.Time) *meanreversionfakes.FakeCandleHistoryStore {
	store := &meanreversionfakes.FakeCandleHistoryStore{}
	loC, hiC := lo, hi
	store.CandleRangeReturns(&loC, &hiC, nil)
	return store
}

func TestStrategyGetBars(t *testing.T) {
	t.Run("loads bucketed bars with warmup and fills benchmark yields", func(t *testing.T) {
		cfg := barTestConfig()
		s := meanreversion.New(cfg, nil)

		obTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		store := coveredStore(btStart.Add(-21*time.Hour), btEnd)
		store.LoadCandlesBucketedStub = func(
			_ context.Context, orderBookID string, resolution candles.Resolution, _, _ time.Time,
		) ([]candles.Candle, error) {
			assert.Equal(t, cfg.OrderBookID.String(), orderBookID)
			assert.Equal(t, candles.Resolution1h, resolution)
			return []candles.Candle{
				{
					OrderBookID: orderBookID, StartTimestamp: obTime,
					CloseYTM: decimal.MustNew(52, 3), Close: decimal.MustNew(99, 0),
					HighYTM: decimal.MustNew(53, 3), LowYTM: decimal.MustNew(51, 3),
				},
				{
					OrderBookID: orderBookID, StartTimestamp: obTime.Add(24 * time.Hour),
					CloseYTM: decimal.MustNew(54, 3), Close: decimal.MustNew(98, 0),
					HighYTM: decimal.MustNew(55, 3), LowYTM: decimal.MustNew(53, 3),
				},
			}, nil
		}
		meanreversion.SetCandleHistoryStore(s, store)

		benchmark := &meanreversionfakes.FakeBenchmarkYieldClient{}
		benchmark.FetchHistoricalYieldsReturns([]fred.Observation{
			{Date: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Yield: decimal.MustNew(45, 3)},
			{Date: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), Yield: decimal.MustNew(47, 3)},
		}, nil)
		meanreversion.SetBenchmarkYieldClient(s, benchmark)

		bars, err := meanreversion.GetBars(context.Background(), s, btStart, btEnd)

		require.NoError(t, err)
		require.Len(t, bars, 2)
		// First bar closes Jan 1 13:00 → latest prior FRED date is Jan 1.
		assert.True(t, bars[0].BenchmarkYield.Equal(decimal.MustNew(45, 3)))
		// Second bar closes Jan 2 14:00 → latest prior FRED date is Jan 2.
		assert.True(t, bars[1].BenchmarkYield.Equal(decimal.MustNew(47, 3)))
		assert.True(t, bars[0].Time.Equal(obTime))
		assert.True(t, bars[0].HighYTM.Equal(decimal.MustNew(53, 3)))
		assert.Equal(t, 1, store.LoadCandlesBucketedCallCount())
		_, tenor, _, _ := benchmark.FetchHistoricalYieldsArgsForCall(0)
		assert.Equal(t, fred.Tenor10Year, tenor)
	})

	t.Run("no candles at all returns ErrNoCandleCoverage", func(t *testing.T) {
		cfg := barTestConfig()
		s := meanreversion.New(cfg, nil)
		store := &meanreversionfakes.FakeCandleHistoryStore{}
		store.CandleRangeReturns(nil, nil, nil)
		meanreversion.SetCandleHistoryStore(s, store)

		_, err := meanreversion.GetBars(context.Background(), s, btStart, btEnd)

		var cov *candles.ErrNoCandleCoverage
		require.ErrorAs(t, err, &cov)
		assert.Nil(t, cov.Available)
		assert.Contains(t, err.Error(), "no candle data")
	})

	t.Run("partial coverage names the available range", func(t *testing.T) {
		cfg := barTestConfig()
		s := meanreversion.New(cfg, nil)
		lo := btStart.Add(-21 * time.Hour)
		// Last raw 1m candle is two minutes before end, so its source
		// interval [hi, hi+1m) ends a full minute short of end.
		hi := btEnd.Add(-2 * time.Minute)
		store := coveredStore(lo, hi)
		meanreversion.SetCandleHistoryStore(s, store)

		_, err := meanreversion.GetBars(context.Background(), s, btStart, btEnd)

		var cov *candles.ErrNoCandleCoverage
		require.ErrorAs(t, err, &cov)
		require.NotNil(t, cov.Available)
		assert.True(t, cov.Available.Equal(lo))
		assert.True(t, cov.Until.Equal(hi))
		assert.Contains(t, err.Error(), "insufficient candle coverage")
	})

	t.Run("window ending at the close of the last bar is covered", func(t *testing.T) {
		cfg := barTestConfig()
		s := meanreversion.New(cfg, nil)
		lo := btStart.Add(-21 * time.Hour)
		// The last raw 1m candle starts at end−1m, covering [end−1m,
		// end) — sufficient at the source level; a 1m candle starting
		// at/after end is never loaded.
		hi := btEnd.Add(-time.Minute)
		store := coveredStore(lo, hi)
		store.LoadCandlesBucketedStub = func(
			_ context.Context, _ string, _ candles.Resolution, since, _ time.Time,
		) ([]candles.Candle, error) {
			assert.Equal(t, lo, since)
			return []candles.Candle{{
				OrderBookID: cfg.OrderBookID.String(), StartTimestamp: hi,
				CloseYTM: decimal.MustNew(52, 3), Close: decimal.MustNew(99, 0),
			}}, nil
		}
		meanreversion.SetCandleHistoryStore(s, store)
		benchmark := &meanreversionfakes.FakeBenchmarkYieldClient{}
		benchmark.FetchHistoricalYieldsReturns([]fred.Observation{
			{Date: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Yield: decimal.MustNew(45, 3)},
		}, nil)
		meanreversion.SetBenchmarkYieldClient(s, benchmark)

		bars, err := meanreversion.GetBars(context.Background(), s, btStart, btEnd)

		require.NoError(t, err)
		require.Len(t, bars, 1)
		assert.True(t, bars[0].Time.Equal(hi), "the bar starting at hi is loaded")
	})

	t.Run("warmup gap before start is a coverage error", func(t *testing.T) {
		cfg := barTestConfig()
		s := meanreversion.New(cfg, nil)
		// Coverage starts after the required warmup start.
		store := coveredStore(btStart.Add(-20*time.Hour), btEnd)
		meanreversion.SetCandleHistoryStore(s, store)

		_, err := meanreversion.GetBars(context.Background(), s, btStart, btEnd)

		var cov *candles.ErrNoCandleCoverage
		require.ErrorAs(t, err, &cov)
	})

	t.Run("all bars zero close YTM returns error", func(t *testing.T) {
		cfg := barTestConfig()
		s := meanreversion.New(cfg, nil)
		store := coveredStore(btStart.Add(-21*time.Hour), btEnd)
		store.LoadCandlesBucketedStub = func(
			_ context.Context, _ string, _ candles.Resolution, _, _ time.Time,
		) ([]candles.Candle, error) {
			return []candles.Candle{
				{OrderBookID: cfg.OrderBookID.String(), StartTimestamp: btStart, Close: decimal.MustNew(99, 0)},
			}, nil
		}
		meanreversion.SetCandleHistoryStore(s, store)
		meanreversion.SetBenchmarkYieldClient(s, &meanreversionfakes.FakeBenchmarkYieldClient{})

		_, err := meanreversion.GetBars(context.Background(), s, btStart, btEnd)

		require.ErrorContains(t, err, "zero close YTM")
	})

	t.Run("all bars missing benchmark yield returns error naming the cause", func(t *testing.T) {
		cfg := barTestConfig()
		s := meanreversion.New(cfg, nil)
		store := coveredStore(btStart.Add(-21*time.Hour), btEnd)
		store.LoadCandlesBucketedStub = func(
			_ context.Context, _ string, _ candles.Resolution, _, _ time.Time,
		) ([]candles.Candle, error) {
			return []candles.Candle{
				{
					OrderBookID: cfg.OrderBookID.String(), StartTimestamp: btStart,
					CloseYTM: decimal.MustNew(52, 3), Close: decimal.MustNew(99, 0),
				},
			}, nil
		}
		meanreversion.SetCandleHistoryStore(s, store)
		// No FRED observations before the bar → benchmark never resolves.
		meanreversion.SetBenchmarkYieldClient(s, &meanreversionfakes.FakeBenchmarkYieldClient{})

		_, err := meanreversion.GetBars(context.Background(), s, btStart, btEnd)

		require.ErrorContains(t, err, "no prior benchmark yield")
	})

	t.Run("propagates candle store errors", func(t *testing.T) {
		cfg := barTestConfig()
		s := meanreversion.New(cfg, nil)
		store := coveredStore(btStart.Add(-21*time.Hour), btEnd)
		store.LoadCandlesBucketedReturns(nil, errors.New("store failed"))
		meanreversion.SetCandleHistoryStore(s, store)
		meanreversion.SetBenchmarkYieldClient(s, &meanreversionfakes.FakeBenchmarkYieldClient{})

		_, err := meanreversion.GetBars(context.Background(), s, btStart, btEnd)

		require.ErrorContains(t, err, "load candles")
	})

	t.Run("propagates benchmark client errors", func(t *testing.T) {
		cfg := barTestConfig()
		s := meanreversion.New(cfg, nil)
		store := coveredStore(btStart.Add(-21*time.Hour), btEnd)
		meanreversion.SetCandleHistoryStore(s, store)
		benchmark := &meanreversionfakes.FakeBenchmarkYieldClient{}
		benchmark.FetchHistoricalYieldsReturns(nil, errors.New("fred failed"))
		meanreversion.SetBenchmarkYieldClient(s, benchmark)

		_, err := meanreversion.GetBars(context.Background(), s, btStart, btEnd)

		require.ErrorContains(t, err, "fetch historical benchmark yields")
	})

	t.Run("backtest returns coverage errors from getBars", func(t *testing.T) {
		cfg := barTestConfig()
		s := meanreversion.New(cfg, nil)
		store := &meanreversionfakes.FakeCandleHistoryStore{}
		store.CandleRangeReturns(nil, nil, nil)
		meanreversion.SetCandleHistoryStore(s, store)

		_, err := s.Backtest(context.Background(), btStart, btEnd)

		var cov *candles.ErrNoCandleCoverage
		require.ErrorAs(t, err, &cov)
	})
}

// TestPreflightBacktest is the meanreversion counterpart of momentum's /
// breakout's preflight tests: coverage-only, no bar loading, and a no-op
// without an injected store.
func TestPreflightBacktest(t *testing.T) {
	t.Parallel()

	t.Run("coverage gap surfaces without loading bars", func(t *testing.T) {
		s := meanreversion.New(barTestConfig(), nil)
		store := coveredStore(btStart, btEnd) // no warmup coverage
		meanreversion.SetCandleHistoryStore(s, store)

		err := meanreversion.PreflightBacktest(context.Background(), s, btStart, btEnd)

		var cov *candles.ErrNoCandleCoverage
		require.ErrorAs(t, err, &cov)
		assert.Equal(t, 0, store.LoadCandlesBucketedCallCount())
	})

	t.Run("no injected store is a no-op", func(t *testing.T) {
		s := meanreversion.New(barTestConfig(), nil)

		require.NoError(t, meanreversion.PreflightBacktest(context.Background(), s, btStart, btEnd))
	})

	// Source-level guard: CandleRange reports raw 1m rows, so the
	// guard must reject when the last 1m candle is more than 1m
	// short of end, regardless of the strategy's bar resolution. The
	// pre-fix guard added the strategy resolution (1h here) to hi,
	// so it wrongly passed.
	t.Run("source resolution enforced: 1h strategy with last 1m candle 2m before end", func(t *testing.T) {
		s := meanreversion.New(barTestConfig(), nil)
		// barTestConfig sets Resolution=1h; warmup = (LookbackWindow+1)*1h = 25h.
		// Last raw 1m candle is 2m before btEnd → 1m of source data
		// is missing for the requested end.
		hi := btEnd.Add(-2 * time.Minute)
		store := coveredStore(btStart.Add(-25*time.Hour), hi)
		meanreversion.SetCandleHistoryStore(s, store)

		err := meanreversion.PreflightBacktest(context.Background(), s, btStart, btEnd)

		var cov *candles.ErrNoCandleCoverage
		require.ErrorAs(t, err, &cov)
		assert.True(t, cov.Until.Equal(hi))
	})

	// Regression: the corrected guard does not over-reject at the
	// source edge. The last raw 1m candle at end−1m covers
	// [end−1m, end), so coverage is sufficient.
	t.Run("source resolution enforced: 1h strategy with last 1m candle at end-1m passes", func(t *testing.T) {
		s := meanreversion.New(barTestConfig(), nil)
		hi := btEnd.Add(-time.Minute)
		store := coveredStore(btStart.Add(-25*time.Hour), hi)
		meanreversion.SetCandleHistoryStore(s, store)

		require.NoError(t, meanreversion.PreflightBacktest(context.Background(), s, btStart, btEnd))
	})
}
