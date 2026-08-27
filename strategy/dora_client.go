package strategy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/dora-network/dora-client-go/doraclient"
	"github.com/govalues/decimal"
)

const (
	apiKeyPrefix    = "ApiKey"
	doraQuantityDps = 3
)

// DoraClient is the concrete implementation of MarketAPIClient. It wraps
// the upstream doraclient.APIClient, applies a per-user API key to every
// request, and caches lookups that are stable for the client's lifetime
// (the user's DORA ID, order-book metadata).
//
// Lifted from strategy/meanreversion.doraAPIClient so breakout (and any
// future strategy adopting MarketAPIClient) can use it without going
// through the meanreversion package. The meanreversion and copytrading
// packages still have their own private doraAPIClient for now; DORA-5873
// folds them into this implementation.
type DoraClient struct {
	apiKey string
	client *doraclient.APIClient

	// cachedUserID is the bot's DORA user ID, fetched from GetUserSelf
	// on first use and cached for the lifetime of the client.
	userIDMu     sync.RWMutex
	cachedUserID string

	// orderBookByID caches the full OrderBook by ID so BaseAssetID
	// can be served from a single DORA round-trip.
	orderBookMu   sync.RWMutex
	orderBookByID map[string]*doraclient.OrderBook
}

// NewDoraClientWithKey constructs a DoraClient that authenticates every
// DORA request with the given user API key. Honours the DORA_BASE_URL
// env var (same convention as the meanreversion / copytrading wrappers).
func NewDoraClientWithKey(apiKey string) *DoraClient {
	cfg := doraclient.NewConfiguration()
	if baseURL := os.Getenv("DORA_BASE_URL"); baseURL != "" {
		cfg.Servers = doraclient.ServerConfigurations{{
			URL:         baseURL,
			Description: "Configured DORA API server",
		}}
	}
	return &DoraClient{
		apiKey: apiKey,
		client: doraclient.NewAPIClient(cfg),
	}
}

// NewDoraClient returns a DoraClient authenticated with the server's
// DORA_API_KEY env var. Use NewDoraClientWithKey for per-user keys.
func NewDoraClient() *DoraClient {
	return NewDoraClientWithKey(os.Getenv("DORA_API_KEY"))
}

// Compile-time guard that *DoraClient satisfies the shared interface.
var _ MarketAPIClient = (*DoraClient)(nil)

// BaseAssetID returns the base asset ID for the given order book.
func (c *DoraClient) BaseAssetID(ctx context.Context, orderBookID string) (string, error) {
	book, err := c.orderBook(ctx, orderBookID)
	if err != nil {
		return "", err
	}
	if book.BaseAssetId == "" {
		return "", fmt.Errorf("get order book %s: missing base asset ID", orderBookID)
	}
	return book.BaseAssetId, nil
}

// QuoteAssetID returns the quote asset ID for the given order book.
func (c *DoraClient) QuoteAssetID(ctx context.Context, orderBookID string) (string, error) {
	book, err := c.orderBook(ctx, orderBookID)
	if err != nil {
		return "", err
	}
	if book.QuoteAssetId == "" {
		return "", fmt.Errorf("get order book %s: missing quote asset ID", orderBookID)
	}
	return book.QuoteAssetId, nil
}

// AssetPosition returns the (available, borrowed) position for the
// given asset on the bot's DORA account. Resolves the user ID once
// (cached for the lifetime of the client) and looks up the position.
func (c *DoraClient) AssetPosition(ctx context.Context, assetID string) (
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
	authCtx := c.authCtx(ctx)
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

// AssetCollateralWeight returns the collateral weight of the given asset.
func (c *DoraClient) AssetCollateralWeight(ctx context.Context, assetID string) (decimal.Decimal, error) {
	if c == nil || c.client == nil {
		return decimal.Zero, errors.New("DORA client is not configured")
	}
	if c.apiKey == "" {
		return decimal.Zero, errors.New("API_KEY is not configured")
	}
	authCtx := c.authCtx(ctx)
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

// GetPortfolioV2 returns the bot's full v2 portfolio (positions across
// all order books the bot trades) for use by copy-trading's mirrored-
// trader selection. Returns (nil, nil) when DORA has nothing to report;
// callers must handle the empty case.
func (c *DoraClient) GetPortfolioV2(ctx context.Context) (*doraclient.AccountPortfolioV2, error) {
	if c == nil || c.client == nil {
		return nil, errors.New("DORA client is not configured")
	}
	if c.apiKey == "" {
		return nil, errors.New("API_KEY is not configured")
	}
	authCtx := c.authCtx(ctx)
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

// CreateMarketOrder submits a market order on the given order book and
// returns once DORA has accepted the order. The DORA API requires
// quantity with at most 3 decimal places; this method rounds before
// submitting. Returns a wrapped error that includes the upstream
// error body when DORA rejects the order.
func (c *DoraClient) CreateMarketOrder(
	ctx context.Context,
	orderBookID string,
	side doraclient.Side,
	quantity decimal.Decimal,
	inverseLeverage decimal.Decimal,
	fromGlobalPosition bool,
	clientOrderID string,
) (string, error) {
	if c == nil || c.client == nil {
		return "", errors.New("DORA client is not configured")
	}
	if c.apiKey == "" {
		return "", errors.New("API_KEY is not configured")
	}
	if quantity.IsZero() || quantity.IsNeg() {
		return "", errors.New("order quantity must be greater than 0")
	}
	if inverseLeverage.IsNeg() {
		return "", errors.New("inverse leverage must be non-negative and less than or equal to 1.0")
	}

	// DORA requires quantity with at most 3 decimal places.
	quantity = quantity.Round(doraQuantityDps)
	authCtx := c.authCtx(ctx)
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
	resp, rawResp, err := c.client.DefaultAPI.CreateOrder(authCtx).CreateOrderRequest(*request).Execute()
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
				return "", fmt.Errorf("create market order on order book %s: %s (raw: %w)", orderBookID, *errResp.Error, err)
			}
			if len(body) > 0 {
				return "", fmt.Errorf("create market order on order book %s: %s (raw: %w)", orderBookID, string(body), err)
			}
		}
		return "", fmt.Errorf("create market order on order book %s: %w", orderBookID, err)
	}
	var orderID string
	if resp != nil && resp.Data != nil && resp.Data.OrderId != nil {
		orderID = *resp.Data.OrderId
	}
	return orderID, nil
}

// GetOrderFilledStatus fetches the current status and filled quantity
// of an order from DORA. Used by TWAP on restart to reconcile pending
// orders that may have filled or been cancelled while the strategy
// server was offline. Returns the order's status string (OPEN, FILLED,
// PARTIAL_FILL, CANCELLED) and its cumulative filled quantity.
func (c *DoraClient) GetOrderFilledStatus(ctx context.Context, orderID string) (status string, filledQuantity decimal.Decimal, err error) {
	if c == nil || c.client == nil {
		return "", decimal.Zero, errors.New("DORA client is not configured")
	}
	if c.apiKey == "" {
		return "", decimal.Zero, errors.New("API_KEY is not configured")
	}
	authCtx := c.authCtx(ctx)
	resp, rawResp, err := c.client.DefaultAPI.GetOrderById(authCtx, orderID).Execute()
	if rawResp != nil && rawResp.Body != nil {
		defer rawResp.Body.Close()
	}
	if err != nil {
		return "", decimal.Zero, fmt.Errorf("get order %s: %w", orderID, err)
	}
	if resp == nil || resp.Data == nil {
		return "", decimal.Zero, fmt.Errorf("get order %s: empty response", orderID)
	}
	order := *resp.Data
	status = string(order.Status)
	if order.FilledQuantity != "" {
		filledQuantity, err = decimal.Parse(order.FilledQuantity)
		if err != nil {
			return "", decimal.Zero, fmt.Errorf("get order %s: parse filled_quantity: %w", orderID, err)
		}
	}
	return status, filledQuantity, nil
}

// ListOrdersByClientOrderIDPrefix fetches every DORA order whose
// client_order_id starts with the given prefix. Used by execution
// strategies on restart to import orders that were submitted to
// DORA but never made it into the persisted run state (the
// PlaceOrder/SaveState crash window — DORA has the order, our
// state doesn't). DORA's listOrders treats client_order_id as a
// prefix; for our format (<strategy>.<run_id>.<uuidv7>) the
// strategy+run prefix yields exactly the orders placed by this run.
//
// Returns an empty slice (no error) when no orders match. Network
// errors propagate so the caller can decide whether to abort the
// restart or fall back to relying on the in-state orders alone.
func (c *DoraClient) ListOrdersByClientOrderIDPrefix(ctx context.Context, prefix string) ([]doraclient.Order, error) {
	if c == nil || c.client == nil {
		return nil, errors.New("DORA client is not configured")
	}
	if c.apiKey == "" {
		return nil, errors.New("API_KEY is not configured")
	}
	if prefix == "" {
		return nil, errors.New("client_order_id prefix is required")
	}
	resp, rawResp, err := c.client.DefaultAPI.ListOrders(c.authCtx(ctx)).ClientOrderId(prefix).Execute()
	if rawResp != nil && rawResp.Body != nil {
		defer rawResp.Body.Close()
	}
	if err != nil {
		return nil, fmt.Errorf("list orders by client_order_id prefix %q: %w", prefix, err)
	}
	if resp == nil || resp.Data == nil {
		return []doraclient.Order{}, nil
	}
	return resp.Data, nil
}

// authCtx stamps the per-user API key on the context for DORA requests
// that use the "apiKeyAuthHeader" auth scheme.
func (c *DoraClient) authCtx(ctx context.Context) context.Context {
	return context.WithValue(ctx, doraclient.ContextAPIKeys, map[string]doraclient.APIKey{
		"apiKeyAuthHeader": {
			Key:    c.apiKey,
			Prefix: apiKeyPrefix,
		},
	})
}

// orderBook returns the cached OrderBook for the given ID, fetching
// from DORA on first use. The mapping is stable for the lifetime of
// the order book, so a single round-trip is enough.
func (c *DoraClient) orderBook(ctx context.Context, orderBookID string) (*doraclient.OrderBook, error) {
	if c == nil || c.client == nil {
		return nil, errors.New("DORA client is not configured")
	}
	if c.apiKey == "" {
		return nil, errors.New("user API key is not configured")
	}

	// Fast path: read lock.
	c.orderBookMu.RLock()
	cached, ok := c.orderBookByID[orderBookID]
	c.orderBookMu.RUnlock()
	if ok {
		return cached, nil
	}

	// Slow path: upgrade to write lock and re-check.
	c.orderBookMu.Lock()
	defer c.orderBookMu.Unlock()
	if cached, ok := c.orderBookByID[orderBookID]; ok {
		return cached, nil
	}

	resp, _, err := c.client.DefaultAPI.GetOrderbookById(c.authCtx(ctx), orderBookID).Execute()
	if err != nil {
		return nil, fmt.Errorf("get order book %s: %w", orderBookID, err)
	}
	if resp == nil || resp.Data == nil {
		return nil, fmt.Errorf("get order book %s: missing response data", orderBookID)
	}
	if c.orderBookByID == nil {
		c.orderBookByID = make(map[string]*doraclient.OrderBook)
	}
	c.orderBookByID[orderBookID] = resp.Data
	return resp.Data, nil
}

// userID returns the cached user ID, fetching it from DORA on first
// use. The user ID is stable for the API key lifetime, so a single
// round-trip is sufficient.
func (c *DoraClient) userID(ctx context.Context) (string, error) {
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
	resp, _, err := c.client.DefaultAPI.GetUserSelf(c.authCtx(ctx)).Execute()
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
