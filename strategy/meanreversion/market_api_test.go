package meanreversion_test

import (
	"context"
	"errors"
	"testing"

	"github.com/dora-network/bond-trading-strategies/strategy/meanreversion"
	"github.com/dora-network/bond-trading-strategies/strategy/meanreversion/meanreversionfakes"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStrategyLookupAssetID(t *testing.T) {
	t.Run("looks up base asset ID from order book", func(t *testing.T) {
		s := meanreversion.New(defaultConfig(), nil)
		orderBookID := uuid.Must(uuid.NewV7())
		lookup := &meanreversionfakes.FakeMarketAPIClient{}
		lookup.BaseAssetIDReturns("asset-123", nil)
		meanreversion.SetLookupClient(s, lookup)

		assetID, err := meanreversion.LookupAssetID(s, orderBookID)

		require.NoError(t, err)
		assert.Equal(t, "asset-123", assetID)
		assert.Equal(t, 1, lookup.BaseAssetIDCallCount())
		_, actualOrderBookID := lookup.BaseAssetIDArgsForCall(0)
		assert.Equal(t, orderBookID.String(), actualOrderBookID)
	})

	t.Run("rejects missing order book ID", func(t *testing.T) {
		s := meanreversion.New(defaultConfig(), nil)
		lookup := &meanreversionfakes.FakeMarketAPIClient{}
		meanreversion.SetLookupClient(s, lookup)

		_, err := meanreversion.LookupAssetID(s, uuid.Nil)

		require.ErrorContains(t, err, "order book ID is required")
		assert.Zero(t, lookup.BaseAssetIDCallCount())
	})

	t.Run("propagates lookup errors", func(t *testing.T) {
		s := meanreversion.New(defaultConfig(), nil)
		lookup := &meanreversionfakes.FakeMarketAPIClient{}
		lookup.BaseAssetIDReturns("", errors.New("some error"))
		meanreversion.SetLookupClient(s, lookup)

		_, err := meanreversion.LookupAssetID(s, uuid.Must(uuid.NewV7()))

		require.ErrorContains(t, err, "some error")
	})

	t.Run("rejects empty base asset ID", func(t *testing.T) {
		s := meanreversion.New(defaultConfig(), nil)
		lookup := &meanreversionfakes.FakeMarketAPIClient{}
		meanreversion.SetLookupClient(s, lookup)

		_, err := meanreversion.LookupAssetID(s, uuid.Must(uuid.NewV7()))

		require.ErrorContains(t, err, "returned an empty base asset ID")
	})
}

func TestStrategyCurrentPosition(t *testing.T) {
	t.Run("loads account asset position", func(t *testing.T) {
		s := meanreversion.New(defaultConfig(), nil)
		lookup := &meanreversionfakes.FakeMarketAPIClient{}
		lookup.SelfUserIDReturns("user-123", nil)
		lookup.AssetPositionReturns(decimal.MustNew(3, 1), decimal.Zero, nil)
		meanreversion.SetLookupClient(s, lookup)

		position, err := meanreversion.CurrentPosition(s, context.Background(), "asset-123")

		require.NoError(t, err)
		assert.True(t, position.Equal(decimal.MustNew(3, 1)))
		assert.Equal(t, 1, lookup.SelfUserIDCallCount())
		assert.Equal(t, 1, lookup.AssetPositionCallCount())
		_, actualAccountID, actualAssetID := lookup.AssetPositionArgsForCall(0)
		assert.Equal(t, "user-123", actualAccountID)
		assert.Equal(t, "asset-123", actualAssetID)
	})

	t.Run("requires self user id", func(t *testing.T) {
		s := meanreversion.New(defaultConfig(), nil)
		lookup := &meanreversionfakes.FakeMarketAPIClient{}
		lookup.SelfUserIDReturns("", errors.New("user unavailable"))
		meanreversion.SetLookupClient(s, lookup)

		_, err := meanreversion.CurrentPosition(s, context.Background(), "asset-123")

		require.ErrorContains(t, err, "user unavailable")
		assert.Equal(t, 1, lookup.SelfUserIDCallCount())
		assert.Zero(t, lookup.AssetPositionCallCount())
	})
}

func TestStrategyCappedOrderQuantity(t *testing.T) {
	price := decimal.One

	t.Run("returns target quantity when below cap", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.InitialBalance = decimal.MustNew(10, 0)
		s := meanreversion.New(cfg, nil)

		qty, ok, err := meanreversion.CappedOrderQuantity(s, decimal.MustNew(5, 1), decimal.MustNew(2, 0), price)

		require.NoError(t, err)
		assert.True(t, ok)
		assert.True(t, qty.Equal(decimal.MustNew(5, 0)))
	})

	t.Run("caps quantity at remaining room", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.InitialBalance = decimal.MustNew(10, 0)
		s := meanreversion.New(cfg, nil)

		qty, ok, err := meanreversion.CappedOrderQuantity(s, decimal.One, decimal.MustNew(85, 1), price)

		require.NoError(t, err)
		assert.True(t, ok)
		// budget = min(10×1, 10-8.5×1) = 1.5; qty = 1.5/1 = 1 (floored)
		assert.True(t, qty.Equal(decimal.MustNew(1, 0)))
	})

	t.Run("skips when already at cap", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.InitialBalance = decimal.MustNew(10, 0)
		s := meanreversion.New(cfg, nil)

		qty, ok, err := meanreversion.CappedOrderQuantity(s, decimal.One, decimal.MustNew(10, 0), price)

		require.NoError(t, err)
		assert.False(t, ok)
		assert.True(t, qty.IsZero())
	})
}
