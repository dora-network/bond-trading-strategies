// Package dora provides a standalone DORA API client for use by CLI daemons
// that authenticate directly with an API key (no HTTP middleware context required).
package dora

import (
	"context"
	"errors"
	"fmt"

	"github.com/dora-network/dora-client-go/doraclient"
)

const apiKeyPrefix = "ApiKey"

// OrderBookSummary is a simplified view of a DORA order book.
type OrderBookSummary struct {
	ID           string `json:"id"`
	DisplayName  string `json:"display_name"`
	BaseAssetID  string `json:"base_asset_id"`
	QuoteAssetID string `json:"quote_asset_id"`
	Status       string `json:"status"`
}

// Client wraps the generated DORA API client and authenticates with a fixed API key.
type Client struct {
	apiClient *doraclient.APIClient
	apiKey    string
}

// NewClient creates a DORA client that authenticates with the given API key.
func NewClient(apiKey, baseURL string) *Client {
	cfg := doraclient.NewConfiguration()
	if baseURL != "" {
		cfg.Servers = doraclient.ServerConfigurations{{
			URL:         baseURL,
			Description: "Configured DORA API server",
		}}
	}
	return &Client{
		apiClient: doraclient.NewAPIClient(cfg),
		apiKey:    apiKey,
	}
}

// authContext returns a context carrying the API key credentials.
func (c *Client) authContext(ctx context.Context) (context.Context, error) {
	if c.apiKey == "" {
		return nil, errors.New("DORA API key is not configured")
	}
	return context.WithValue(ctx, doraclient.ContextAPIKeys, map[string]doraclient.APIKey{
		"apiKeyAuthHeader": {
			Key:    c.apiKey,
			Prefix: apiKeyPrefix,
		},
	}), nil
}

// ListOrderBooks calls the DORA ListOrderBooks API. If status is non-empty,
// only order books matching that status are returned.
func (c *Client) ListOrderBooks(ctx context.Context, status string) ([]OrderBookSummary, error) {
	authCtx, err := c.authContext(ctx)
	if err != nil {
		return nil, err
	}

	req := c.apiClient.DefaultAPI.ListOrderBooks(authCtx)
	if status != "" {
		obStatus := doraclient.OrderBookStatus(status)
		req = req.Status(obStatus)
	}

	resp, rawResp, err := req.Execute()
	if rawResp != nil && rawResp.Body != nil {
		defer rawResp.Body.Close()
	}
	if err != nil {
		return nil, fmt.Errorf("list order books: %w", err)
	}
	if resp == nil {
		return nil, errors.New("list order books: missing response data")
	}

	items := make([]OrderBookSummary, 0, len(resp.Data))
	for _, book := range resp.Data {
		items = append(items, OrderBookSummary{
			ID:           book.OrderBookId,
			DisplayName:  book.DisplayName,
			BaseAssetID:  book.BaseAssetId,
			QuoteAssetID: book.QuoteAssetId,
			Status:       string(book.Status),
		})
	}
	return items, nil
}
