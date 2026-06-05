package http

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/dora-network/dora-client-go/doraclient"
)

type doraClient interface {
	ListOrderBooks(context.Context) ([]DORAOrderBookSummary, error)
	GetAssetByID(context.Context, string) (*AssetInfo, error)
	GetUserID(context.Context) (string, error)
	ListBotUsers(context.Context) ([]DORABotUser, error)
}

// DORABotUser is a simplified view of a DORA user that is exposed by the
// list-copy-traders placeholder endpoint.
type DORABotUser struct {
	ID        string `json:"id"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}

// isBotUser reports whether a user's first or last name starts with the
// bot-naming prefix. This is a placeholder heuristic; once DORA exposes a
// dedicated "list available copy traders" endpoint, the filter is removed.
func isBotUser(firstName, lastName string) bool {
	return hasBotPrefix(firstName) || hasBotPrefix(lastName)
}

func hasBotPrefix(s string) bool {
	lower := strings.ToLower(s)
	return strings.HasPrefix(lower, "trader_") || strings.HasPrefix(lower, "mm_")
}

const (
	apiKeyPrefix             = "ApiKey"
	copyTraderPageSize int32 = 100
	copyTraderMaxPages int32 = 10
)

type liveDORAClient struct {
	client *doraclient.APIClient
}

// NewDORAClient creates a new DORA HTTP client using the DORA_BASE_URL
// environment variable (if set) for the server URL.
func NewDORAClient() *liveDORAClient {
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

// ListBotUsers fetches DORA users and returns those whose first or last name
// starts with the bot-naming prefix (TRADER_ or MM_). This is a placeholder
// until DORA exposes a dedicated copy-trader listing endpoint.
func (c *liveDORAClient) ListBotUsers(ctx context.Context) ([]DORABotUser, error) {
	authCtx, err := c.authContext(ctx)
	if err != nil {
		return nil, err
	}

	all := make([]DORABotUser, 0)
	for page := int32(0); page < copyTraderMaxPages; page++ {
		offset := page * copyTraderPageSize
		resp, rawResp, err := c.client.DefaultAPI.
			GetUsers(authCtx).
			Limit(copyTraderPageSize).
			Offset(offset).
			Execute()
		if rawResp != nil && rawResp.Body != nil {
			_ = rawResp.Body.Close()
		}
		if err != nil {
			return nil, fmt.Errorf("list users: %w", err)
		}
		if resp == nil || len(resp.Data) == 0 {
			break
		}
		for _, u := range resp.Data {
			if !isBotUser(u.FirstName, u.LastName) {
				continue
			}
			all = append(all, DORABotUser{
				ID:        u.Id,
				FirstName: u.FirstName,
				LastName:  u.LastName,
			})
		}
		if len(resp.Data) < int(copyTraderPageSize) {
			break
		}
	}
	return all, nil
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
