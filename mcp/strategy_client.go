package mcpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type strategyClient struct {
	baseURL string
	apiKey  string
	client  *http.Client
}

type strategyErrorResponse struct {
	Error string `json:"error"`
}

type strategyRunSummary struct {
	ID           string `json:"id"`
	StrategyType string `json:"strategy_type"`
	Status       string `json:"status"`
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at"`
	StoppedAt    string `json:"stopped_at,omitempty"`
}

type strategyRunDetail struct {
	strategyRunSummary
	Config map[string]any `json:"config"`
	Error  string         `json:"error,omitempty"`
}

type strategyRunListResponse struct {
	Items []strategyRunSummary `json:"items"`
}

func newStrategyClient(baseURL, apiKey string) *strategyClient {
	return &strategyClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		client: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

func (c *strategyClient) listStrategies(ctx context.Context) (map[string]any, error) {
	return doStrategyJSON[map[string]any](ctx, c, http.MethodGet, "/v1/strategies", nil)
}

func (c *strategyClient) listDORAOrderBooks(ctx context.Context) (map[string]any, error) {
	return doStrategyJSON[map[string]any](ctx, c, http.MethodGet, "/v1/dora/orderbooks", nil)
}

func (c *strategyClient) getDORAUser(ctx context.Context) (map[string]any, error) {
	return doStrategyJSON[map[string]any](ctx, c, http.MethodGet, "/v1/dora/user", nil)
}

func (c *strategyClient) listCopyTraders(ctx context.Context) (map[string]any, error) {
	return doStrategyJSON[map[string]any](ctx, c, http.MethodGet, "/v1/copy-traders", nil)
}

func (c *strategyClient) listTenors(ctx context.Context) (map[string]any, error) {
	return doStrategyJSON[map[string]any](ctx, c, http.MethodGet, "/v1/tenors", nil)
}

func (c *strategyClient) createRun(ctx context.Context, payload any) (map[string]any, error) {
	return doStrategyJSON[map[string]any](ctx, c, http.MethodPost, "/v1/runs", payload)
}

func (c *strategyClient) listRuns(ctx context.Context) (map[string]any, error) {
	return doStrategyJSON[map[string]any](ctx, c, http.MethodGet, "/v1/runs", nil)
}

func (c *strategyClient) getRun(ctx context.Context, id string) (map[string]any, error) {
	return doStrategyJSON[map[string]any](ctx, c, http.MethodGet, "/v1/runs/"+id, nil)
}

func (c *strategyClient) pauseRun(ctx context.Context, id string) (map[string]any, error) {
	return doStrategyJSON[map[string]any](ctx, c, http.MethodPost, "/v1/runs/"+id+"/pause", nil)
}

func (c *strategyClient) resumeRun(ctx context.Context, id string) (map[string]any, error) {
	return doStrategyJSON[map[string]any](ctx, c, http.MethodPost, "/v1/runs/"+id+"/resume", nil)
}

func (c *strategyClient) stopRun(ctx context.Context, id string) (map[string]any, error) {
	return doStrategyJSON[map[string]any](ctx, c, http.MethodDelete, "/v1/runs/"+id, nil)
}

func (c *strategyClient) listRunsTyped(ctx context.Context) (strategyRunListResponse, error) {
	return doStrategyJSON[strategyRunListResponse](ctx, c, http.MethodGet, "/v1/runs", nil)
}

func (c *strategyClient) getRunTyped(ctx context.Context, id string) (strategyRunDetail, error) {
	return doStrategyJSON[strategyRunDetail](ctx, c, http.MethodGet, "/v1/runs/"+id, nil)
}

func (c *strategyClient) createBacktest(ctx context.Context, payload any) (map[string]any, error) {
	return doStrategyJSON[map[string]any](ctx, c, http.MethodPost, "/v1/backtests", payload)
}

type listBacktestsArgs struct {
	Status string
	From   string
	To     string
	Page   int
	Limit  int
}

func (c *strategyClient) listBacktests(ctx context.Context, args listBacktestsArgs) (map[string]any, error) {
	path := "/v1/backtests"
	params := url.Values{}
	if args.Status != "" {
		params.Set("status", args.Status)
	}
	if args.From != "" {
		params.Set("from", args.From)
	}
	if args.To != "" {
		params.Set("to", args.To)
	}
	if args.Page > 0 {
		params.Set("page", strconv.Itoa(args.Page))
	}
	if args.Limit > 0 {
		params.Set("limit", strconv.Itoa(args.Limit))
	}
	if len(params) > 0 {
		path += "?" + params.Encode()
	}
	return doStrategyJSON[map[string]any](ctx, c, http.MethodGet, path, nil)
}

func (c *strategyClient) getBacktest(ctx context.Context, id string) (map[string]any, error) {
	return doStrategyJSON[map[string]any](ctx, c, http.MethodGet, "/v1/backtests/"+id, nil)
}

func (c *strategyClient) getBacktestMetadata(ctx context.Context, id string) (map[string]any, error) {
	return doStrategyJSON[map[string]any](ctx, c, http.MethodGet, "/v1/backtests/"+id+"/metadata", nil)
}

func (c *strategyClient) getBacktestTrades(ctx context.Context, id string, page, limit int) (map[string]any, error) {
	path := fmt.Sprintf("/v1/backtests/%s/trades?page=%d&limit=%d", id, page, limit)
	return doStrategyJSON[map[string]any](ctx, c, http.MethodGet, path, nil)
}

func (c *strategyClient) getBacktestClosedTrades(ctx context.Context, id string, page, limit int) (map[string]any, error) {
	path := fmt.Sprintf("/v1/backtests/%s/closed-trades?page=%d&limit=%d", id, page, limit)
	return doStrategyJSON[map[string]any](ctx, c, http.MethodGet, path, nil)
}

func (c *strategyClient) cancelBacktest(ctx context.Context, id string) (map[string]any, error) {
	return doStrategyJSON[map[string]any](ctx, c, http.MethodDelete, "/v1/backtests/"+id, nil)
}

func doStrategyJSON[T any](ctx context.Context, c *strategyClient, method, path string, payload any) (T, error) {
	var zero T
	if c.baseURL == "" {
		return zero, fmt.Errorf("strategy server base URL is not configured")
	}

	var body io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return zero, fmt.Errorf("marshal request: %w", err)
		}
		body = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return zero, fmt.Errorf("build request: %w", err)
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.apiKey != "" {
		req.Header.Set("Authorization", "ApiKey "+c.apiKey)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return zero, fmt.Errorf("strategy server request failed: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return zero, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errResp strategyErrorResponse
		if err := json.Unmarshal(data, &errResp); err == nil && errResp.Error != "" {
			return zero, fmt.Errorf("%s", errResp.Error)
		}
		return zero, fmt.Errorf("strategy server returned %s", resp.Status)
	}

	if len(bytes.TrimSpace(data)) == 0 {
		var out T
		return out, nil
	}

	var out T
	if err := json.Unmarshal(data, &out); err != nil {
		return zero, fmt.Errorf("decode response: %w", err)
	}
	return out, nil
}
