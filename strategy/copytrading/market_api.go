package copytrading

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

type marketAPIClient interface { //nolint:unused
	GetPortfolioV2(ctx context.Context) (*doraclient.AccountPortfolioV2, error)
	CreateMarketOrder(ctx context.Context, orderBookID string, side doraclient.Side, quantity decimal.Decimal, inverseLeverage decimal.Decimal, fromGlobalPosition bool) error //nolint:lll
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
	resp, _, err := c.client.DefaultAPI.GetLedgerAccountsSelfV2(authCtx).Execute()
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
