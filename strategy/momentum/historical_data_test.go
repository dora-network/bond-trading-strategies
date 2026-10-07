package momentum_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/candles"
	"github.com/dora-network/bond-trading-strategies/fred"
	"github.com/dora-network/bond-trading-strategies/strategy/momentum"
	"github.com/dora-network/bond-trading-strategies/strategy/momentum/momentumfakes"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	btStart = time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	btEnd   = time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC)
)

// barTestConfig returns a candle-backed momentum config with a 1h
// resolution and SlowWindow 3, so the warmup prefix getBars requires is
// (3+1)×1h = 4h before start.
func barTestConfig(source string) momentum.Config {
	cfg := momentum.DefaultConfig()
	cfg.SignalSource = source
	cfg.OrderBookID = uuid.Must(uuid.NewV7())
	cfg.Tenor = "10Y"
	cfg.Resolution = "1h"
	cfg.SlowWindow = 3
	return cfg
}

// dataStart is the warmup-inclusive window start for barTestConfig.
func dataStart() time.Time { return btStart.Add(-4 * time.Hour) }

func coveredStore(lo, hi time.Time) *momentumfakes.FakeCandleHistoryStore {
	store := &momentumfakes.FakeCandleHistoryStore{}
	loC, hiC := lo, hi
	store.CandleRangeReturns(&loC, &hiC, nil)
	return store
}

func TestStrategyGetBars(t *testing.T) {
	t.Parallel()

	t.Run("spread mode loads bucketed bars with warmup and fills benchmark yields", func(t *testing.T) {
		cfg := barTestConfig(momentum.SignalSourceSpread)
		s := momentum.New(cfg, nil)

		obTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		store := coveredStore(dataStart(), btEnd)
		store.LoadCandlesBucketedStub = func(
			_ context.Context, orderBookID string, resolution candles.Resolution, since, _ time.Time,
		) ([]candles.Candle, error) {
			assert.Equal(t, cfg.OrderBookID.String(), orderBookID)
			assert.Equal(t, candles.Resolution1h, resolution)
			assert.Equal(t, dataStart(), since, "warmup must be (SlowWindow+1) bars before start")
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
		momentum.SetCandleHistoryStore(s, store)

		benchmark := &momentumfakes.FakeBenchmarkYieldClient{}
		benchmark.FetchHistoricalYieldsReturns([]fred.Observation{
			{Date: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Yield: decimal.MustNew(45, 3)},
			{Date: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), Yield: decimal.MustNew(47, 3)},
		}, nil)
		momentum.SetBenchmarkYieldClient(s, benchmark)

		bars, err := momentum.GetBars(context.Background(), s, btStart, btEnd)

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

	t.Run("price mode skips the benchmark entirely and keeps zero-YTM bars", func(t *testing.T) {
		cfg := barTestConfig(momentum.SignalSourcePrice)
		s := momentum.New(cfg, nil)

		store := coveredStore(dataStart(), btEnd)
		store.LoadCandlesBucketedReturns([]candles.Candle{
			// Zero CloseYTM: a drop in ytm/spread modes, but price mode
			// never reads the YTM so the bar survives.
			{
				OrderBookID: cfg.OrderBookID.String(), StartTimestamp: btStart,
				Close: decimal.MustNew(99, 0),
			},
		}, nil)
		momentum.SetCandleHistoryStore(s, store)
		benchmark := &momentumfakes.FakeBenchmarkYieldClient{}
		momentum.SetBenchmarkYieldClient(s, benchmark)

		bars, err := momentum.GetBars(context.Background(), s, btStart, btEnd)

		require.NoError(t, err)
		require.Len(t, bars, 1, "price mode must not drop zero-YTM bars")
		assert.True(t, bars[0].BenchmarkYield.IsZero())
		assert.Equal(t, 0, benchmark.FetchHistoricalYieldsCallCount(),
			"price mode must not call FRED")
	})

	t.Run("no candles at all returns ErrNoCandleCoverage", func(t *testing.T) {
		s := momentum.New(barTestConfig(momentum.SignalSourcePrice), nil)
		store := &momentumfakes.FakeCandleHistoryStore{}
		store.CandleRangeReturns(nil, nil, nil)
		momentum.SetCandleHistoryStore(s, store)

		_, err := momentum.GetBars(context.Background(), s, btStart, btEnd)

		var cov *candles.ErrNoCandleCoverage
		require.ErrorAs(t, err, &cov)
		assert.Nil(t, cov.Available)
		assert.Contains(t, err.Error(), "no candle data")
	})

	t.Run("partial coverage names the available range", func(t *testing.T) {
		s := momentum.New(barTestConfig(momentum.SignalSourcePrice), nil)
		lo := dataStart()
		// Last bar starts two hours before end, so its close (hi+1h)
		// is still one hour short of end.
		hi := btEnd.Add(-2 * time.Hour)
		store := coveredStore(lo, hi)
		momentum.SetCandleHistoryStore(s, store)

		_, err := momentum.GetBars(context.Background(), s, btStart, btEnd)

		var cov *candles.ErrNoCandleCoverage
		require.ErrorAs(t, err, &cov)
		require.NotNil(t, cov.Available)
		assert.True(t, cov.Available.Equal(lo))
		assert.True(t, cov.Until.Equal(hi))
		assert.Contains(t, err.Error(), "insufficient candle coverage")
	})

	t.Run("window ending at the close of the last bar is covered", func(t *testing.T) {
		cfg := barTestConfig(momentum.SignalSourcePrice)
		s := momentum.New(cfg, nil)
		lo := dataStart()
		// The last persisted bar starts at end−1h, closing exactly at
		// end — sufficient; a bar starting at/after end is never loaded.
		hi := btEnd.Add(-time.Hour)
		store := coveredStore(lo, hi)
		store.LoadCandlesBucketedStub = func(
			_ context.Context, _ string, _ candles.Resolution, since, _ time.Time,
		) ([]candles.Candle, error) {
			assert.Equal(t, lo, since)
			return []candles.Candle{{
				OrderBookID: cfg.OrderBookID.String(), StartTimestamp: hi,
				Close: decimal.MustNew(99, 0),
			}}, nil
		}
		momentum.SetCandleHistoryStore(s, store)

		bars, err := momentum.GetBars(context.Background(), s, btStart, btEnd)

		require.NoError(t, err)
		require.Len(t, bars, 1)
		assert.True(t, bars[0].Time.Equal(hi), "the bar starting at hi is loaded")
	})

	t.Run("warmup gap before start is a coverage error", func(t *testing.T) {
		s := momentum.New(barTestConfig(momentum.SignalSourcePrice), nil)
		// Coverage starts one hour after the required warmup start.
		store := coveredStore(dataStart().Add(time.Hour), btEnd)
		momentum.SetCandleHistoryStore(s, store)

		_, err := momentum.GetBars(context.Background(), s, btStart, btEnd)

		var cov *candles.ErrNoCandleCoverage
		require.ErrorAs(t, err, &cov)
	})

	t.Run("all bars unusable names both zero-YTM and benchmark causes", func(t *testing.T) {
		cfg := barTestConfig(momentum.SignalSourceSpread)
		s := momentum.New(cfg, nil)
		store := coveredStore(dataStart(), btEnd)
		store.LoadCandlesBucketedReturns([]candles.Candle{
			{OrderBookID: cfg.OrderBookID.String(), StartTimestamp: btStart, Close: decimal.MustNew(99, 0)},
			{OrderBookID: cfg.OrderBookID.String(), StartTimestamp: btStart.Add(time.Hour),
				CloseYTM: decimal.MustNew(52, 3), Close: decimal.MustNew(99, 0)},
		}, nil)
		momentum.SetCandleHistoryStore(s, store)
		// No FRED observations: the one bar with a YTM has no prior
		// benchmark and is dropped for that reason.
		benchmark := &momentumfakes.FakeBenchmarkYieldClient{}
		momentum.SetBenchmarkYieldClient(s, benchmark)

		_, err := momentum.GetBars(context.Background(), s, btStart, btEnd)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "zero close YTM", "error must name the zero-YTM cause")
		assert.Contains(t, err.Error(), "no prior benchmark yield", "error must name the benchmark cause")
	})

	t.Run("propagates load and benchmark errors", func(t *testing.T) {
		t.Run("load candles", func(t *testing.T) {
			s := momentum.New(barTestConfig(momentum.SignalSourcePrice), nil)
			store := coveredStore(dataStart(), btEnd)
			store.LoadCandlesBucketedReturns(nil, errors.New("db down"))
			momentum.SetCandleHistoryStore(s, store)

			_, err := momentum.GetBars(context.Background(), s, btStart, btEnd)

			require.ErrorContains(t, err, "load candles")
		})

		t.Run("benchmark client", func(t *testing.T) {
			s := momentum.New(barTestConfig(momentum.SignalSourceSpread), nil)
			store := coveredStore(dataStart(), btEnd)
			momentum.SetCandleHistoryStore(s, store)
			benchmark := &momentumfakes.FakeBenchmarkYieldClient{}
			benchmark.FetchHistoricalYieldsReturns(nil, errors.New("fred failed"))
			momentum.SetBenchmarkYieldClient(s, benchmark)

			_, err := momentum.GetBars(context.Background(), s, btStart, btEnd)

			require.ErrorContains(t, err, "fetch historical benchmark yields")
		})
	})
}

// TestPreflightBacktest mirrors meanreversion's: coverage-only, no bar
// loading, and a no-op without an injected store.
func TestPreflightBacktest(t *testing.T) {
	t.Parallel()

	t.Run("coverage gap surfaces without loading bars", func(t *testing.T) {
		s := momentum.New(barTestConfig(momentum.SignalSourcePrice), nil)
		store := coveredStore(btStart, btEnd) // no warmup coverage
		momentum.SetCandleHistoryStore(s, store)

		err := momentum.PreflightBacktest(context.Background(), s, btStart, btEnd)

		var cov *candles.ErrNoCandleCoverage
		require.ErrorAs(t, err, &cov)
		assert.Equal(t, 0, store.LoadCandlesBucketedCallCount())
	})

	t.Run("no injected store is a no-op", func(t *testing.T) {
		s := momentum.New(barTestConfig(momentum.SignalSourcePrice), nil)

		require.NoError(t, momentum.PreflightBacktest(context.Background(), s, btStart, btEnd))
	})
}
