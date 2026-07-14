package strategy

import (
	"context"

	"github.com/dora-network/dora-client-go/doraclient"
	"github.com/govalues/decimal"
)

// MarketAPIClient is the shared DORA client subset required by trading
// strategies that place market orders and resolve asset / order-book
// metadata. Concrete strategies use the subset they need via Go's
// structural typing; the full method set is the union required across
// strategies that have adopted this contract so far.
//
// The breakout strategy is the first consumer. meanreversion and
// copytrading still declare their own per-package interfaces; migrating
// them is tracked separately (DORA-5873).
//
//counterfeiter:generate -o strategyfakes/fake_market_apiclient.go . MarketAPIClient
type MarketAPIClient interface {
	BaseAssetID(ctx context.Context, orderBookID string) (string, error)
	AssetPosition(ctx context.Context, assetID string) (decimal.Decimal, decimal.Decimal, error)
	AssetCollateralWeight(ctx context.Context, assetID string) (decimal.Decimal, error)
	CreateMarketOrder(
		ctx context.Context,
		orderBookID string,
		side doraclient.Side,
		quantity decimal.Decimal,
		inverseLeverage decimal.Decimal,
		fromGlobalPosition bool,
		clientOrderID string,
	) error
}
