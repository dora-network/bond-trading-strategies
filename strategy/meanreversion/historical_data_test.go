package meanreversion_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/fred"
	"github.com/dora-network/bond-trading-strategies/prices"
	"github.com/dora-network/bond-trading-strategies/strategy/meanreversion"
	"github.com/dora-network/bond-trading-strategies/strategy/meanreversion/meanreversionfakes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/google/uuid"
	"github.com/govalues/decimal"
)

func TestStrategyGetObservations(t *testing.T) {
	t.Run("matches prices to latest prior fred date and caches yields", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.OrderBookID = uuid.Must(uuid.NewV7())
		cfg.Tenor = "10Y"
		s := meanreversion.New(cfg, nil)

		lookup := &meanreversionfakes.FakeMarketApiClient{}
		lookup.BaseAssetIDReturns("asset-123", nil)
		meanreversion.SetLookupClient(s, lookup)

		history := &meanreversionfakes.FakeHistoricalPriceStore{}
		ytmOne := decimal.MustNew(52, 3)
		ytmTwo := decimal.MustNew(54, 3)
		history.LoadHistoricalPricesReturns([]prices.AssetPrice{
			{
				AssetID: "asset-123",
				YTM:     &ytmOne,
				Time:    time.Date(2024, 1, 2, 15, 0, 0, 0, time.UTC),
			},
			{
				AssetID: "asset-123",
				YTM:     &ytmTwo,
				Time:    time.Date(2024, 1, 3, 15, 0, 0, 0, time.UTC),
			},
		}, nil)
		meanreversion.SetHistoricalPriceStore(s, history)

		benchmark := &meanreversionfakes.FakeBenchmarkYieldClient{}
		benchmark.FetchHistoricalYieldsReturns([]fred.Observation{
			{Date: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Yield: decimal.MustNew(45, 3)},
			{Date: time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC), Yield: decimal.MustNew(47, 3)},
		}, nil)
		meanreversion.SetBenchmarkYieldClient(s, benchmark)

		obs, err := meanreversion.GetObservations(
			s,
			context.Background(),
			time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC),
		)

		require.NoError(t, err)
		require.Len(t, obs, 2)
		assert.True(t, obs[0].BenchmarkYield.Equal(decimal.MustNew(45, 3)))
		assert.True(t, obs[1].BenchmarkYield.Equal(decimal.MustNew(47, 3)))
		assert.True(t, meanreversion.GetBenchmarkYield(context.Background(), s, time.Date(2024, 1, 4, 9, 0, 0, 0, time.UTC)).Equal(decimal.MustNew(47, 3)))
		assert.Equal(t, 1, history.LoadHistoricalPricesCallCount())
		assert.Equal(t, 2, benchmark.FetchHistoricalYieldsCallCount())
		_, tenor, _, _ := benchmark.FetchHistoricalYieldsArgsForCall(0)
		assert.Equal(t, fred.Tenor10Year, tenor)
	})

	t.Run("skips rows with nil ytm or no prior benchmark", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.OrderBookID = uuid.Must(uuid.NewV7())
		cfg.Tenor = "10Y"
		s := meanreversion.New(cfg, nil)

		lookup := &meanreversionfakes.FakeMarketApiClient{}
		lookup.BaseAssetIDReturns("asset-123", nil)
		meanreversion.SetLookupClient(s, lookup)

		history := &meanreversionfakes.FakeHistoricalPriceStore{}
		ytm := decimal.MustNew(55, 3)
		history.LoadHistoricalPricesReturns([]prices.AssetPrice{
			{
				AssetID: "asset-123",
				YTM:     &ytm,
				Time:    time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
			},
			{
				AssetID: "asset-123",
				YTM:     nil,
				Time:    time.Date(2024, 1, 2, 10, 0, 0, 0, time.UTC),
			},
			{
				AssetID: "asset-123",
				YTM:     &ytm,
				Time:    time.Date(2024, 1, 3, 10, 0, 0, 0, time.UTC),
			},
		}, nil)
		meanreversion.SetHistoricalPriceStore(s, history)

		benchmark := &meanreversionfakes.FakeBenchmarkYieldClient{}
		benchmark.FetchHistoricalYieldsReturns([]fred.Observation{
			{Date: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), Yield: decimal.MustNew(46, 3)},
		}, nil)
		meanreversion.SetBenchmarkYieldClient(s, benchmark)

		obs, err := meanreversion.GetObservations(
			s,
			context.Background(),
			time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC),
		)

		require.NoError(t, err)
		require.Len(t, obs, 1)
		assert.Equal(t, time.Date(2024, 1, 3, 10, 0, 0, 0, time.UTC), obs[0].Time)
	})

	t.Run("returns invalid tenor error", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.Tenor = "bad-tenor"
		s := meanreversion.New(cfg, nil)

		_, err := meanreversion.GetObservations(s, context.Background(), time.Now().Add(-24*time.Hour), time.Now().Add(-time.Hour))

		require.ErrorContains(t, err, "unsupported tenor")
	})

	t.Run("propagates historical price store errors", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.OrderBookID = uuid.Must(uuid.NewV7())
		cfg.Tenor = "10Y"
		s := meanreversion.New(cfg, nil)

		lookup := &meanreversionfakes.FakeMarketApiClient{}
		lookup.BaseAssetIDReturns("asset-123", nil)
		meanreversion.SetLookupClient(s, lookup)

		history := &meanreversionfakes.FakeHistoricalPriceStore{}
		history.LoadHistoricalPricesReturns(nil, errors.New("history failed"))
		meanreversion.SetHistoricalPriceStore(s, history)
		meanreversion.SetBenchmarkYieldClient(s, &meanreversionfakes.FakeBenchmarkYieldClient{})

		_, err := meanreversion.GetObservations(
			s,
			context.Background(),
			time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC),
		)

		require.ErrorContains(t, err, "history failed")
	})

	t.Run("propagates benchmark client errors", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.OrderBookID = uuid.Must(uuid.NewV7())
		cfg.Tenor = "10Y"
		s := meanreversion.New(cfg, nil)

		lookup := &meanreversionfakes.FakeMarketApiClient{}
		lookup.BaseAssetIDReturns("asset-123", nil)
		meanreversion.SetLookupClient(s, lookup)

		history := &meanreversionfakes.FakeHistoricalPriceStore{}
		history.LoadHistoricalPricesReturns([]prices.AssetPrice{}, nil)
		meanreversion.SetHistoricalPriceStore(s, history)

		benchmark := &meanreversionfakes.FakeBenchmarkYieldClient{}
		benchmark.FetchHistoricalYieldsReturns(nil, errors.New("fred failed"))
		meanreversion.SetBenchmarkYieldClient(s, benchmark)

		_, err := meanreversion.GetObservations(
			s,
			context.Background(),
			time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC),
		)

		require.ErrorContains(t, err, "fred failed")
	})

	t.Run("backtest returns observation loading errors", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.OrderBookID = uuid.Must(uuid.NewV7())
		cfg.Tenor = "10Y"
		s := meanreversion.New(cfg, nil)

		lookup := &meanreversionfakes.FakeMarketApiClient{}
		lookup.BaseAssetIDReturns("asset-123", nil)
		meanreversion.SetLookupClient(s, lookup)

		history := &meanreversionfakes.FakeHistoricalPriceStore{}
		history.LoadHistoricalPricesReturns(nil, errors.New("history failed"))
		meanreversion.SetHistoricalPriceStore(s, history)
		meanreversion.SetBenchmarkYieldClient(s, &meanreversionfakes.FakeBenchmarkYieldClient{})

		_, err := s.Backtest(
			context.Background(),
			time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC),
		)

		require.ErrorContains(t, err, "history failed")
	})
}
