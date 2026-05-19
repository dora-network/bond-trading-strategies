package http

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/dora-network/dora-client-go/doraclient"
)

type doraClient interface {
	ListOrderBooks(context.Context) ([]DORAOrderBookSummary, error)
	GetAssetByID(context.Context, string) (*AssetInfo, error)
	GetUserID(context.Context) (string, error)
}

const apiKeyPrefix = "ApiKey"

type liveDORAClient struct {
	client *doraclient.APIClient
}

func newDORAClient() *liveDORAClient {
	cfg := doraclient.NewConfiguration()

	if baseURL := os.Getenv("DORA_BASE_URL"); baseURL != "" {
		cfg.Servers = doraclient.ServerConfigurations{{
			URL:         baseURL,
			Description: "Configured DORA API server",
		}}
	}

	return &liveDORAClient{
		client: doraclient.NewAPIClient(cfg),
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
	authCtx, err := c.authContext(ctx)
	if err != nil {
		return "", err
	}

	resp, rawResp, err := c.client.DefaultAPI.GetUserSelf(authCtx).Execute()
	if rawResp != nil && rawResp.Body != nil {
		defer rawResp.Body.Close()
	}
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

// authContext builds a context that carries the DORA auth credentials extracted
// from the incoming HTTP request context by the requireAuth middleware.
// It supports both ApiKey and Bearer token authentication.
func (c *liveDORAClient) authContext(ctx context.Context) (context.Context, error) {
	if c == nil || c.client == nil {
		return nil, errors.New("DORA client is not configured")
	}

	info, ok := authFromContext(ctx)
	if !ok {
		return nil, errors.New("no authorization credentials in context")
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
