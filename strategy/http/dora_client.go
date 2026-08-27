package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/dora-network/bond-trading-strategies/authctx"
	"github.com/dora-network/dora-client-go/doraclient"
)

type doraClient interface {
	ListOrderBooks(context.Context) ([]DORAOrderBookSummary, error)
	GetAssetByID(context.Context, string) (*AssetInfo, error)
	GetUserID(context.Context) (string, error)
	ListCopyTraders(context.Context) ([]CopyTrader, error)
}

// CopyTrader is a single entry returned by DORA's
// `GET /v1/user/copy_traders` endpoint. The user_id is the DORA user UUID
// (matches the `followed_trader` field accepted by CopyTradingConfig) and
// user_name is the DORA-registered handle.
type CopyTrader struct {
	UserID   string `json:"user_id"`
	UserName string `json:"user_name"`
}

const (
	apiKeyPrefix               = "ApiKey"
	copyTraderPageSize   int32 = 100
	responsePreviewBytes       = 4096
)

type liveDORAClient struct {
	client     *doraclient.APIClient
	baseURL    string
	httpClient *http.Client
}

// NewDORAClient creates a new DORA HTTP client using the DORA_BASE_URL
// environment variable (if set) for the server URL.
func NewDORAClient() *liveDORAClient {
	cfg := doraclient.NewConfiguration()
	baseURL := ""
	if len(cfg.Servers) > 0 {
		baseURL = cfg.Servers[0].URL
	}

	if baseURL := os.Getenv("DORA_BASE_URL"); baseURL != "" {
		cfg.Servers = doraclient.ServerConfigurations{{
			URL:         baseURL,
			Description: "Configured DORA API server",
		}}
	}

	if len(cfg.Servers) > 0 {
		baseURL = cfg.Servers[0].URL
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}

	return &liveDORAClient{
		client:     doraclient.NewAPIClient(cfg),
		baseURL:    strings.TrimRight(baseURL, "/"),
		httpClient: cfg.HTTPClient,
	}
}

func (c *liveDORAClient) ListOrderBooks(ctx context.Context) ([]DORAOrderBookSummary, error) {
	authCtx, err := c.authContext(ctx)
	if err != nil {
		return nil, err
	}

	resp, rawResp, err := c.client.DefaultAPI.ListOrderBooks(authCtx).Execute()
	if rawResp != nil && rawResp.Body != nil {
		defer rawResp.Body.Close()
	}
	if err != nil {
		return nil, fmt.Errorf("list order books: %w", err)
	}
	if resp == nil {
		return nil, errors.New("list order books: missing response data")
	}

	items := make([]DORAOrderBookSummary, 0, len(resp.Data))
	for _, book := range resp.Data {
		items = append(items, DORAOrderBookSummary{
			ID:           book.OrderBookId,
			DisplayName:  book.DisplayName,
			BaseAssetID:  book.BaseAssetId,
			QuoteAssetID: book.QuoteAssetId,
			Status:       string(book.Status),
		})
	}
	return items, nil
}

func (c *liveDORAClient) GetAssetByID(ctx context.Context, assetID string) (*AssetInfo, error) {
	authCtx, err := c.authContext(ctx)
	if err != nil {
		return nil, err
	}

	resp, rawResp, err := c.client.DefaultAPI.GetAssetById(authCtx, assetID).Execute()
	if rawResp != nil && rawResp.Body != nil {
		defer rawResp.Body.Close()
	}
	if err != nil {
		return nil, fmt.Errorf("get asset by id: %w", err)
	}
	if resp == nil || resp.Data == nil {
		return nil, errors.New("get asset by id: missing response data")
	}

	return &AssetInfo{
		Name:   resp.Data.Name,
		Symbol: resp.Data.Symbol,
	}, nil
}

func (c *liveDORAClient) GetUserID(ctx context.Context) (string, error) {
	if c == nil || c.httpClient == nil || c.baseURL == "" {
		return "", errors.New("DORA client is not configured")
	}

	authHeader, err := authHeader(ctx)
	if err != nil {
		return "", err
	}

	//nolint:gosec // DORA_BASE_URL is trusted deployment config and is already used by the generated DORA client.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v1/user/self", nil)
	if err != nil {
		return "", fmt.Errorf("create get user self request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", authHeader)

	//nolint:gosec // Request URL is built from trusted DORA_BASE_URL service config above.
	rawResp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("get user self: %w", err)
	}
	defer rawResp.Body.Close()

	if rawResp.StatusCode < http.StatusOK || rawResp.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(rawResp.Body, responsePreviewBytes))
		return "", fmt.Errorf("get user self: status %d: %s", rawResp.StatusCode, strings.TrimSpace(string(body)))
	}

	var resp struct {
		Data *struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(rawResp.Body).Decode(&resp); err != nil {
		return "", fmt.Errorf("get user self: decode response: %w", err)
	}
	if resp.Data == nil {
		return "", errors.New("get user self: missing response data")
	}
	if resp.Data.ID == "" {
		return "", errors.New("get user self: missing user ID")
	}

	return resp.Data.ID, nil
}

// ListCopyTraders returns the DORA users who have allow_copy_trading enabled.
// Pagination is hidden from the caller: pages of `copyTraderPageSize` are
// requested until DORA returns an empty data array or a short page.
func (c *liveDORAClient) ListCopyTraders(ctx context.Context) ([]CopyTrader, error) {
	authCtx, err := c.authContext(ctx)
	if err != nil {
		return nil, err
	}

	// ponytail: no max-pages cap. Termination relies on DORA returning either
	// an empty data array or a short one (len(resp.Data) < limit). If DORA ever
	// returns a full page indefinitely, this loops forever. Upgrade when DORA
	// exposes a total/has_more field on GetCopyTradersResponse.
	var all []CopyTrader
	for page := int32(1); ; page++ {
		resp, rawResp, err := c.client.DefaultAPI.
			GetCopyTraders(authCtx).
			Page(page).
			Limit(copyTraderPageSize).
			Execute()
		if rawResp != nil && rawResp.Body != nil {
			_ = rawResp.Body.Close()
		}
		if err != nil {
			return nil, fmt.Errorf("list copy traders: %w", err)
		}
		if resp == nil || len(resp.Data) == 0 {
			break
		}
		for _, t := range resp.Data {
			all = append(all, CopyTrader{UserID: t.UserId, UserName: t.UserName})
		}
		if len(resp.Data) < int(copyTraderPageSize) {
			break
		}
	}
	return all, nil
}

// authContext builds a context that carries the DORA auth credentials extracted
// from the incoming context by requireAuth (REST path) or by the WS router
// (cmd/strategy-server/notificationsRouter) — both of which use the
// authctx package. It supports both ApiKey and Bearer token authentication.
func (c *liveDORAClient) authContext(ctx context.Context) (context.Context, error) {
	if c == nil || c.client == nil {
		return nil, errors.New("DORA client is not configured")
	}

	info, err := authInfo(ctx)
	if err != nil {
		return nil, err
	}

	switch {
	case info.APIKey != "":
		return context.WithValue(ctx, doraclient.ContextAPIKeys, map[string]doraclient.APIKey{
			"apiKeyAuthHeader": {
				Key:    info.APIKey,
				Prefix: apiKeyPrefix,
			},
		}), nil
	case info.BearerToken != "":
		return context.WithValue(ctx, doraclient.ContextAccessToken, info.BearerToken), nil
	default:
		return nil, errors.New("no API key or bearer token in authorization context")
	}
}

func authHeader(ctx context.Context) (string, error) {
	info, err := authInfo(ctx)
	if err != nil {
		return "", err
	}

	switch {
	case info.APIKey != "":
		return apiKeyPrefix + " " + info.APIKey, nil
	case info.BearerToken != "":
		return "Bearer " + info.BearerToken, nil
	default:
		return "", errors.New("no API key or bearer token in authorization context")
	}
}

func authInfo(ctx context.Context) (*authctx.AuthInfo, error) {
	info, ok := authctx.AuthInfoFromContext(ctx)
	if !ok {
		return nil, errors.New("no authorization credentials in context")
	}
	return info, nil
}
