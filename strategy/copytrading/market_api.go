package copytrading

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/dora-network/dora-client-go/doraclient"
	"github.com/govalues/decimal"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//counterfeiter:generate . marketAPIClient

type marketAPIClient interface {
	GetPortfolioV2(ctx context.Context) (*doraclient.AccountPortfolioV2, error)
	GetAssetPosition(ctx context.Context, assetID string) (decimal.Decimal, decimal.Decimal, error)
	QuoteAssetID(ctx context.Context, orderBookID string) (string, error)
	CreateMarketOrder(ctx context.Context, orderBookID string, side doraclient.Side, quantity decimal.Decimal, inverseLeverage decimal.Decimal, fromGlobalPosition bool, clientOrderID string) error //nolint:lll
}

type doraAPIClient struct {
	apiKey string
	client *doraclient.APIClient

	// cachedUserID is the bot's DORA user ID, fetched from GetUserSelf
	// on first use and cached for the lifetime of the client. It's
	// stable for the API key, so a single round-trip is enough.
	// RWMutex: many concurrent readers, one writer (the first fetch).
	userIDMu     sync.RWMutex
	cachedUserID string

	// quoteAssetByBook caches the quote (cash) asset ID for each
	// order book, fetched on first use. The mapping is stable for
	// the life of the order book, so a single round-trip per book
	// is enough. Same RWMutex + double-checked pattern as the user
	// ID cache above.
	quoteAssetMu     sync.RWMutex
	quoteAssetByBook map[string]string
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

// userID returns the cached user ID, fetching it from DORA on first
// use. The user ID is stable for the API key lifetime, so a single
// round-trip is sufficient.
func (c *doraAPIClient) userID(ctx context.Context) (string, error) {
	c.userIDMu.RLock()
	cached := c.cachedUserID
	c.userIDMu.RUnlock()
	if cached != "" {
		return cached, nil
	}
	c.userIDMu.Lock()
	defer c.userIDMu.Unlock()
	// Re-check after taking the write lock; another goroutine may
	// have populated the cache while we were upgrading.
	if c.cachedUserID != "" {
		return c.cachedUserID, nil
	}
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
	resp, _, err := c.client.DefaultAPI.GetUserSelf(authCtx).Execute()
	if err != nil {
		return "", fmt.Errorf("get user self: %w", err)
	}
	if resp == nil || resp.Data == nil {
		return "", errors.New("get user self: missing response data")
	}
	if resp.Data.Id == "" {
		return "", errors.New("get user self: missing user ID")
	}
	c.cachedUserID = resp.Data.Id
	return c.cachedUserID, nil
}

// GetAssetPosition returns the (available, borrowed) position for the
// given asset on the bot's DORA account, sourced from
// GetLedgerPositionsSelf. Resolves the user ID once (cached for the
// lifetime of the client) and looks up the position.
func (c *doraAPIClient) GetAssetPosition(ctx context.Context, assetID string) (
	decimal.Decimal, decimal.Decimal, error,
) {
	if c == nil || c.client == nil {
		return decimal.Zero, decimal.Zero, errors.New("DORA client is not configured")
	}
	if c.apiKey == "" {
		return decimal.Zero, decimal.Zero, errors.New("API_KEY is not configured")
	}
	accountID, err := c.userID(ctx)
	if err != nil {
		return decimal.Zero, decimal.Zero, err
	}
	authCtx := context.WithValue(ctx, doraclient.ContextAPIKeys, map[string]doraclient.APIKey{
		"apiKeyAuthHeader": {
			Key:    c.apiKey,
			Prefix: apiKeyPrefix,
		},
	})
	resp, _, err := c.client.DefaultAPI.GetLedgerPositionsSelf(authCtx).Execute()
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

// QuoteAssetID returns the quote (cash) asset ID for the given order
// book. Used to know which balance to size an order against — see
// balanceAssetFor in strategy.go for the rule. Cached for the
// lifetime of the client; the order book → quote asset mapping is
// stable.
func (c *doraAPIClient) QuoteAssetID(ctx context.Context, orderBookID string) (string, error) {
	if c == nil || c.client == nil {
		return "", errors.New("DORA client is not configured")
	}
	if c.apiKey == "" {
		return "", errors.New("API_KEY is not configured")
	}

	// Fast path: read lock.
	c.quoteAssetMu.RLock()
	cached, ok := c.quoteAssetByBook[orderBookID]
	c.quoteAssetMu.RUnlock()
	if ok {
		return cached, nil
	}

	// Slow path: upgrade to write lock and re-check.
	c.quoteAssetMu.Lock()
	defer c.quoteAssetMu.Unlock()
	if cached, ok := c.quoteAssetByBook[orderBookID]; ok {
		return cached, nil
	}

	authCtx := context.WithValue(ctx, doraclient.ContextAPIKeys, map[string]doraclient.APIKey{
		"apiKeyAuthHeader": {
			Key:    c.apiKey,
			Prefix: apiKeyPrefix,
		},
	})
	resp, _, err := c.client.DefaultAPI.GetOrderbookById(authCtx, orderBookID).Execute()
	if err != nil {
		return "", fmt.Errorf("get order book %s: %w", orderBookID, err)
	}
	if resp == nil || resp.Data == nil {
		return "", fmt.Errorf("get order book %s: missing response data", orderBookID)
	}
	if resp.Data.QuoteAssetId == "" {
		return "", fmt.Errorf("get order book %s: missing quote asset ID", orderBookID)
	}
	if c.quoteAssetByBook == nil {
		c.quoteAssetByBook = make(map[string]string)
	}
	c.quoteAssetByBook[orderBookID] = resp.Data.QuoteAssetId
	return resp.Data.QuoteAssetId, nil
}

func (c *doraAPIClient) CreateMarketOrder(
	ctx context.Context,
	orderBookID string,
	side doraclient.Side,
	quantity decimal.Decimal,
	inverseLeverage decimal.Decimal,
	fromGlobalPosition bool,
	clientOrderID string,
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
	if clientOrderID != "" {
		request.SetClientOrderId(clientOrderID)
	}
	slog.Info("create order request",
		"order_book", orderBookID,
		"side", side,
		"quantity", quantity.String(),
		"inverse_leverage", inverseLeverage.String(),
		"from_global", fromGlobalPosition,
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
