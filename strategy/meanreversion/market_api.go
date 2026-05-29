package meanreversion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/dora-network/dora-client-go/doraclient"
	"github.com/govalues/decimal"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//counterfeiter:generate . marketAPIClient
type marketAPIClient interface {
	BaseAssetID(ctx context.Context, orderBookID string) (string, error)
	QuoteAssetID(ctx context.Context, orderBookID string) (string, error)
	SelfUserID(ctx context.Context) (string, error)
	AssetPosition(ctx context.Context, accountID, assetID string) (decimal.Decimal, decimal.Decimal, error)
	GetPortfolioV2(ctx context.Context) (*doraclient.AccountPortfolioV2, error)
	CreateMarketOrder(ctx context.Context, orderBookID string, side doraclient.Side, quantity decimal.Decimal, inverseLeverage decimal.Decimal, fromGlobalPosition bool) error //nolint:lll
	AssetCollateralWeight(ctx context.Context, assetID string) (decimal.Decimal, error)
}

type doraAPIClient struct {
	apiKey string
	client *doraclient.APIClient
}

const (
	apiKeyPrefix    = "ApiKey"
	doraQuantityDps = 3
)

func NewDoraClientWithKey(apiKey string) *doraAPIClient {
	cfg := doraclient.NewConfiguration()
	if baseURL := os.Getenv("DORA_BASE_URL"); baseURL != "" {
		cfg.Servers = doraclient.ServerConfigurations{{
			URL:         baseURL,
			Description: "Configured DORA API server",
		}}
	}
	return &doraAPIClient{
		apiKey: apiKey,
		client: doraclient.NewAPIClient(cfg),
	}
}

func newDoraClient() *doraAPIClient {
	return NewDoraClientWithKey(os.Getenv("DORA_API_KEY"))
}

func (c *doraAPIClient) BaseAssetID(ctx context.Context, orderBookID string) (string, error) {
	return c.getOrderBookAssetID(ctx, orderBookID, "base asset",
		func(data *doraclient.OrderBook) string { return data.BaseAssetId })
}

func (c *doraAPIClient) QuoteAssetID(ctx context.Context, orderBookID string) (string, error) {
	return c.getOrderBookAssetID(ctx, orderBookID, "quote asset",
		func(data *doraclient.OrderBook) string { return data.QuoteAssetId })
}

func (c *doraAPIClient) getOrderBookAssetID(ctx context.Context, orderBookID, fieldName string, getID func(*doraclient.OrderBook) string) (string, error) { //nolint:lll
	if c == nil || c.client == nil {
		return "", errors.New("DORA client is not configured")
	}
	if c.apiKey == "" {
		return "", errors.New("user API key is not configured")
	}
	authCtx := context.WithValue(ctx, doraclient.ContextAPIKeys, map[string]doraclient.APIKey{
		"apiKey": {Key: c.apiKey},
	})
	resp, _, err := c.client.DefaultAPI.GetOrderbookById(authCtx, orderBookID).Execute() //nolint:bodyclose
	if err != nil {
		return "", fmt.Errorf("get order book %s: %w", orderBookID, err)
	}
	if resp == nil || resp.Data == nil {
		return "", fmt.Errorf("get order book %s: missing response data", orderBookID)
	}
	id := getID(resp.Data)
	if id == "" {
		return "", fmt.Errorf("get order book %s: missing %s ID", orderBookID, fieldName)
	}
	return id, nil
}

func (c *doraAPIClient) AssetPosition(ctx context.Context, accountID, assetID string) (
	decimal.Decimal, decimal.Decimal, error,
) {
	if c == nil || c.client == nil {
		return decimal.Zero, decimal.Zero, errors.New("DORA client is not configured")
	}
	if c.apiKey == "" {
		return decimal.Zero, decimal.Zero, errors.New("API_KEY is not configured")
	}
	authCtx := context.WithValue(ctx, doraclient.ContextAPIKeys, map[string]doraclient.APIKey{
		"apiKeyAuthHeader": {
			Key:    c.apiKey,
			Prefix: apiKeyPrefix,
		},
	})
	resp, _, err := c.client.DefaultAPI.GetLedgerPositionsSelf(authCtx).Execute() //nolint:bodyclose
	if err != nil {
		return decimal.Zero, decimal.Zero, fmt.Errorf("get ledger positions: %w", err)
	}
	if resp == nil || resp.Data == nil || resp.Data.Portfolio == nil {
		return decimal.Zero, decimal.Zero, nil
	}
	positions := resp.Data.Portfolio.GetPosition()
	if len(positions) == 0 {
		return decimal.Zero, decimal.Zero, nil
	}
	accountPositions, ok := positions[accountID]
	if !ok {
		return decimal.Zero, decimal.Zero, nil
	}
	position, ok := accountPositions[assetID]
	if !ok {
		return decimal.Zero, decimal.Zero, nil
	}
	// Only the available balance is usable for new orders. The locked balance
	// represents funds already committed to open limit orders and must not be
	// double-counted as available capital.
	available, err := decimal.Parse(position.Available)
	if err != nil {
		return decimal.Zero, decimal.Zero, fmt.Errorf("parse position available for asset %s: %w", assetID, err)
	}

	borrowed, err := decimal.Parse(position.Borrowed)
	if err != nil {
		return decimal.Zero, decimal.Zero, fmt.Errorf("parse position borrowed for asset %s: %w", assetID, err)
	}

	return available, borrowed, nil
}

func (c *doraAPIClient) SelfUserID(ctx context.Context) (string, error) {
	if c == nil || c.client == nil {
		return "", errors.New("DORA client is not configured")
	}
	if c.apiKey == "" {
		return "", errors.New("API_KEY is not configured")
	}
	authCtx := context.WithValue(ctx, doraclient.ContextAPIKeys, map[string]doraclient.APIKey{
		"apiKeyAuthHeader": {
			Key:    c.apiKey,
			Prefix: apiKeyPrefix,
		},
	})
	resp, _, err := c.client.DefaultAPI.GetUserSelf(authCtx).Execute() //nolint:bodyclose
	if err != nil {
		return "", fmt.Errorf("get user self: %w", err)
	}
	if resp == nil || resp.Data == nil {
		return "", errors.New("get user self: missing response data")
	}
	if resp.Data.Id == "" {
		return "", errors.New("get user self: missing user ID")
	}
	return resp.Data.Id, nil
}

func (c *doraAPIClient) GetPortfolioV2(ctx context.Context) (*doraclient.AccountPortfolioV2, error) {
	if c == nil || c.client == nil {
		return nil, errors.New("DORA client is not configured")
	}
	if c.apiKey == "" {
		return nil, errors.New("API_KEY is not configured")
	}
	authCtx := context.WithValue(ctx, doraclient.ContextAPIKeys, map[string]doraclient.APIKey{
		"apiKeyAuthHeader": {
			Key:    c.apiKey,
			Prefix: apiKeyPrefix,
		},
	})
	resp, _, err := c.client.DefaultAPI.GetLedgerAccountsSelfV2(authCtx).Execute() //nolint:bodyclose
	if err != nil {
		return nil, fmt.Errorf("get ledger accounts v2: %w", err)
	}
	if resp == nil {
		return nil, nil
	}
	data, ok := resp.GetDataOk()
	if !ok || data == nil {
		return nil, nil
	}
	portfolio, ok := data.GetPortfolioOk()
	if !ok || portfolio == nil {
		return nil, nil
	}
	return portfolio, nil
}

func (c *doraAPIClient) AssetCollateralWeight(ctx context.Context, assetID string) (decimal.Decimal, error) {
	if c == nil || c.client == nil {
		return decimal.Zero, errors.New("DORA client is not configured")
	}
	if c.apiKey == "" {
		return decimal.Zero, errors.New("API_KEY is not configured")
	}
	authCtx := context.WithValue(ctx, doraclient.ContextAPIKeys, map[string]doraclient.APIKey{
		"apiKeyAuthHeader": {
			Key:    c.apiKey,
			Prefix: apiKeyPrefix,
		},
	})
	resp, rawResp, err := c.client.DefaultAPI.GetAssetById(authCtx, assetID).Execute()
	if rawResp != nil && rawResp.Body != nil {
		defer rawResp.Body.Close()
	}
	if err != nil {
		return decimal.Zero, fmt.Errorf("get asset by id %s: %w", assetID, err)
	}
	if resp == nil || resp.Data == nil {
		return decimal.Zero, fmt.Errorf("get asset by id %s: missing response data", assetID)
	}
	cw, err := decimal.NewFromFloat64(float64(resp.Data.GetCollateralWeight()))
	if err != nil {
		return decimal.Zero, fmt.Errorf("parse collateral weight for asset %s: %w", assetID, err)
	}
	return cw, nil
}

func (c *doraAPIClient) CreateMarketOrder(
	ctx context.Context,
	orderBookID string,
	side doraclient.Side,
	quantity decimal.Decimal,
	inverseLeverage decimal.Decimal,
	fromGlobalPosition bool,
) error {
	if c == nil || c.client == nil {
		return errors.New("DORA client is not configured")
	}
	if c.apiKey == "" {
		return errors.New("API_KEY is not configured")
	}
	if quantity.IsZero() || quantity.IsNeg() {
		return errors.New("order quantity must be greater than 0")
	}
	if inverseLeverage.IsNeg() {
		return errors.New("inverse leverage must be non-negative and less than or equal to 1.0")
	}

	// DORA requires quantity with at most 3 decimal places.
	quantity = quantity.Round(doraQuantityDps)
	authCtx := context.WithValue(ctx, doraclient.ContextAPIKeys, map[string]doraclient.APIKey{
		"apiKeyAuthHeader": {
			Key:    c.apiKey,
			Prefix: apiKeyPrefix,
		},
	})
	if inverseLeverage.IsZero() {
		inverseLeverage = decimal.One
	}
	request := doraclient.NewCreateOrderRequest(
		quantity.String(),
		inverseLeverage.String(),
		doraclient.ORDERKIND_MARKET,
		side,
		fromGlobalPosition,
		orderBookID,
	)
	_, rawResp, err := c.client.DefaultAPI.CreateOrder(authCtx).CreateOrderRequest(*request).Execute()
	if rawResp != nil && rawResp.Body != nil {
		defer rawResp.Body.Close()
	}
	if err != nil {
		var openAPIError *doraclient.GenericOpenAPIError
		if errors.As(err, &openAPIError) {
			body := openAPIError.Body()
			var errResp struct {
				Error *string `json:"error"`
			}
			if jsonErr := json.Unmarshal(body, &errResp); jsonErr == nil && errResp.Error != nil && *errResp.Error != "" {
				return fmt.Errorf("create market order on order book %s: %s (raw: %w)", orderBookID, *errResp.Error, err)
			}
			if len(body) > 0 {
				return fmt.Errorf("create market order on order book %s: %s (raw: %w)", orderBookID, string(body), err)
			}
		}
		return fmt.Errorf("create market order on order book %s: %w", orderBookID, err)
	}
	return nil
}
