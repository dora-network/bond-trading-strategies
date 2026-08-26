package strategy

import (
	"context"
	"errors"
	"fmt"

	"github.com/dora-network/dora-client-go/doraclient"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
)

// MarketAPIClient is the shared DORA client subset required by trading
// strategies that place market orders and resolve asset / order-book
// metadata. Concrete strategies use the subset they need via Go's
// structural typing; the full method set is the union required across
// strategies that have adopted this contract so far (meanreversion,
// copytrading, breakout).
//
//counterfeiter:generate -o strategyfakes/fake_market_apiclient.go . MarketAPIClient
type MarketAPIClient interface {
	BaseAssetID(ctx context.Context, orderBookID string) (string, error)
	QuoteAssetID(ctx context.Context, orderBookID string) (string, error)
	AssetPosition(ctx context.Context, assetID string) (decimal.Decimal, decimal.Decimal, error)
	GetPortfolioV2(ctx context.Context) (*doraclient.AccountPortfolioV2, error)
	CreateMarketOrder(
		ctx context.Context,
		orderBookID string,
		side doraclient.Side,
		quantity decimal.Decimal,
		inverseLeverage decimal.Decimal,
		fromGlobalPosition bool,
		clientOrderID string,
	) (orderID string, err error)
	AssetCollateralWeight(ctx context.Context, assetID string) (decimal.Decimal, error)
	GetOrderFilledStatus(ctx context.Context, orderID string) (status string, filledQuantity decimal.Decimal, err error)
	// ListOrdersByClientOrderIDPrefix returns every DORA order whose
	// client_order_id starts with the given prefix. Used by execution
	// strategies on restart to import orders that were submitted to
	// DORA but never made it into persisted state (the
	// PlaceOrder/SaveState crash window). The DORA listOrders endpoint
	// treats client_order_id as a prefix; pass "<strategy>.<run_id>."
	// to get exactly the orders placed by one run. Returns an empty
	// slice (no error) when no orders match.
	ListOrdersByClientOrderIDPrefix(ctx context.Context, prefix string) ([]doraclient.Order, error)
}

// LookupAssetID resolves a DORA order book UUID to its base asset ID string
// using the provided MarketAPIClient. Returns an error if the client is nil,
// the order book ID is nil, or the lookup fails.
//
// Both meanreversion and breakout strategies use this to map orderBookID → assetID
// before querying historical price data (which is keyed by asset_id, not order_book_id).
func LookupAssetID(ctx context.Context, client MarketAPIClient, orderBookID uuid.UUID) (string, error) {
	if client == nil {
		return "", errors.New("market API client is not configured")
	}
	if orderBookID == uuid.Nil {
		return "", errors.New("order book ID is required")
	}
	assetID, err := client.BaseAssetID(ctx, orderBookID.String())
	if err != nil {
		return "", err
	}
	if assetID == "" {
		return "", fmt.Errorf("order book %s returned an empty base asset ID", orderBookID)
	}
	return assetID, nil
}
