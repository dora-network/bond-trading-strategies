package mcpserver_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/dora-network/bond-trading-strategies/notifications"
	"github.com/dora-network/bond-trading-strategies/testutils"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/mcptest"
	"github.com/mark3labs/mcp-go/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mcpserver "github.com/dora-network/bond-trading-strategies/mcp"
)

func newTestClient(t *testing.T) *mcptest.Server {
	t.Helper()
	strategySrv := newStrategyMockServer(t)
	mcpSrv := mcpserver.New("", "", strategySrv.URL)
	toolMap := mcpSrv.ListTools()
	srvTools := make([]server.ServerTool, 0, len(toolMap))
	for _, st := range toolMap {
		srvTools = append(srvTools, *st)
	}
	ts, err := mcptest.NewServer(t, srvTools...)
	require.NoError(t, err)
	t.Cleanup(ts.Close)
	return ts
}

func newStrategyMockServer(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/strategies":
			_, _ = w.Write([]byte(`{"items":[{"type":"copytrading","status":"not_implemented","description":"Copy trades from a followed trader subject to limits.","config_fields":[{"name":"followed_trader","type":"string(uuid)","description":"Trader UUID to mirror. Required.","required":true},{"name":"min_order_size","type":"integer","description":"Minimum copied order size. Must be non-negative.","required":false},{"name":"max_order_size","type":"integer","description":"Maximum copied order size. Must be greater than or equal to min_order_size.","required":false},{"name":"allowed_bonds","type":"array[string(uuid)]","description":"Optional allowlist of bond UUIDs. Empty means all bonds are eligible.","required":false}],"supports_run":false,"supports_backtest":false},{"type":"mean_reversion","status":"available","description":"Rolling z-score mean reversion strategy.","config_fields":[{"name":"lookback_window","type":"integer","description":"Rolling observation window. Must be at least 2.","required":false,"default":20},{"name":"entry_z_score","type":"number","description":"Entry threshold for opening positions. Must be greater than 0.","required":false,"default":2},{"name":"exit_z_score","type":"number","description":"Exit threshold for closing positions as spreads revert. Must be non-negative.","required":false,"default":0.5},{"name":"stop_loss_z_score","type":"number","description":"Stop-loss threshold for closing losing positions. Must be non-negative.","required":false,"default":3.5},{"name":"min_std_dev","type":"number","description":"Minimum spread volatility required before trading. Must be non-negative.","required":false,"default":0.0005},{"name":"max_position_size","type":"number","description":"Maximum fraction of capital allocated per trade. Must be in (0,1].","required":false,"default":1}],"supports_run":true,"supports_backtest":true}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/dora/orderbooks":
			_, _ = w.Write([]byte(`{"items":[{"id":"book-1","display_name":"UST 10Y / USD","base_asset_id":"asset-base","quote_asset_id":"asset-quote","status":"ACTIVE"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/dora/user":
			_, _ = w.Write([]byte(`{"id":"user-123"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenors":
			_, _ = w.Write([]byte(`{"items":[{"code":"1M","description":"1 Month Treasury"},{"code":"10Y","description":"10 Year Treasury"},{"code":"30Y","description":"30 Year Treasury"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runs":
			_, _ = w.Write([]byte(`{"items":[{"id":"11111111-1111-1111-1111-111111111111","strategy_type":"mean_reversion","status":"running","created_at":"2026-04-24T12:00:00Z","updated_at":"2026-04-24T12:00:00Z"},{"id":"55555555-5555-5555-5555-555555555555","strategy_type":"mean_reversion","status":"paused","created_at":"2026-04-24T11:00:00Z","updated_at":"2026-04-24T11:30:00Z"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/runs":
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			if body["strategy_type"] == "copytrading" {
				w.WriteHeader(http.StatusNotImplemented)
				_, _ = w.Write([]byte(`{"error":"strategy_type \"copytrading\" is not implemented"}`))
				return
			}
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"id":"22222222-2222-2222-2222-222222222222","strategy_type":"mean_reversion","status":"running","created_at":"2026-04-24T12:00:00Z","updated_at":"2026-04-24T12:00:00Z","config":{"lookback_window":20}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runs/22222222-2222-2222-2222-222222222222":
			_, _ = w.Write([]byte(`{"id":"22222222-2222-2222-2222-222222222222","strategy_type":"mean_reversion","status":"running","created_at":"2026-04-24T12:00:00Z","updated_at":"2026-04-24T12:00:00Z","config":{"lookback_window":20}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runs/55555555-5555-5555-5555-555555555555":
			_, _ = w.Write([]byte(`{"id":"55555555-5555-5555-5555-555555555555","strategy_type":"mean_reversion","status":"paused","created_at":"2026-04-24T11:00:00Z","updated_at":"2026-04-24T11:30:00Z","config":{"lookback_window":30,"entry_z_score":1.5},"error":"waiting for manual resume"}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/runs/22222222-2222-2222-2222-222222222222/pause":
			_, _ = w.Write([]byte(`{"id":"22222222-2222-2222-2222-222222222222","strategy_type":"mean_reversion","status":"paused","created_at":"2026-04-24T12:00:00Z","updated_at":"2026-04-24T12:01:00Z","config":{"lookback_window":20}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/runs/22222222-2222-2222-2222-222222222222/resume":
			_, _ = w.Write([]byte(`{"id":"22222222-2222-2222-2222-222222222222","strategy_type":"mean_reversion","status":"running","created_at":"2026-04-24T12:00:00Z","updated_at":"2026-04-24T12:02:00Z","config":{"lookback_window":20}}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/runs/22222222-2222-2222-2222-222222222222":
			_, _ = w.Write([]byte(`{"id":"22222222-2222-2222-2222-222222222222","strategy_type":"mean_reversion","status":"stopped","created_at":"2026-04-24T12:00:00Z","updated_at":"2026-04-24T12:03:00Z","stopped_at":"2026-04-24T12:03:00Z","config":{"lookback_window":20}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/backtests":
			_, _ = w.Write([]byte(`{"items":[{"id":"33333333-3333-3333-3333-333333333333","strategy_type":"mean_reversion","status":"running","created_at":"2026-04-24T12:00:00Z"}],"page":1,"limit":10}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/backtests":
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			if body["strategy_type"] == "copytrading" {
				w.WriteHeader(http.StatusNotImplemented)
				_, _ = w.Write([]byte(`{"error":"strategy_type \"copytrading\" is not implemented"}`))
				return
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"id":"44444444-4444-4444-4444-444444444444","strategy_type":"mean_reversion","status":"running","created_at":"2026-04-24T12:00:00Z","start":"2026-04-01T00:00:00Z","end":"2026-04-02T00:00:00Z","config":{"lookback_window":20}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/backtests/44444444-4444-4444-4444-444444444444":
			_, _ = w.Write([]byte(`{"total_pnl":"9377.78","win_count":764,"loss_count":334,"max_drawdown":"299.56","sharpe_ratio":"39.0","strategy_type":"mean_reversion","status":"completed","config":{"lookback_window":20},"asset_name":"UST 10Y","asset_symbol":"UST10Y"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/backtests/44444444-4444-4444-4444-444444444444/metadata":
			_, _ = w.Write([]byte(`{"id":"44444444-4444-4444-4444-444444444444","dora_user_id":"test-user","strategy_type":"mean_reversion","status":"completed","created_at":"2026-04-24T12:00:00Z","completed_at":"2026-04-24T12:01:00Z"}`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/backtests/44444444-4444-4444-4444-444444444444/trades"):
			_, _ = w.Write([]byte(`{"items":[{"time":"2026-04-24T11:00:00Z","bond_id":"BOND-1","signal":"BUY","spread":"0.012","zscore":"2.0","price":"100","quantity":"5","entry_balance":"10000"}]}`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/backtests/44444444-4444-4444-4444-444444444444/closed-trades"):
			_, _ = w.Write([]byte(`{"items":[{"bond_id":"BOND-1","open_time":"2026-04-24T10:00:00Z","close_time":"2026-04-24T11:00:00Z","signal":"BUY","exit_signal":"SELL","entry_spread":"0.015","exit_spread":"0.008","position_size":"5","pnl":"2","exit_reason":"take_profit","entry_price":"100","exit_price":"102","quantity":"5","entry_balance":"10000"}]}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/backtests/44444444-4444-4444-4444-444444444444":
			_, _ = w.Write([]byte(`{"id":"44444444-4444-4444-4444-444444444444","dora_user_id":"test-user","strategy_type":"mean_reversion","status":"cancelled","created_at":"2026-04-24T12:00:00Z","completed_at":"2026-04-24T12:01:00Z"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"not found"}`))
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func callTool(t *testing.T, ts *mcptest.Server, name string, args map[string]any) *mcp.CallToolResult {
	t.Helper()
	req := mcp.CallToolRequest{}
	req.Params.Name = name
	req.Params.Arguments = args

	result, err := ts.Client().CallTool(context.Background(), req)
	require.NoError(t, err)
	return result
}

func textContent(t *testing.T, result *mcp.CallToolResult) string {
	t.Helper()
	require.NotEmpty(t, result.Content, "expected non-empty content")
	tc, ok := result.Content[0].(mcp.TextContent)
	require.True(t, ok, "expected TextContent, got %T", result.Content[0])
	return tc.Text
}

func TestStrategyList(t *testing.T) {
	ts := newTestClient(t)
	result := callTool(t, ts, "strategy_list", nil)
	require.False(t, result.IsError)
	text := textContent(t, result)
	assert.Contains(t, text, "mean_reversion")
	assert.Contains(t, text, "copytrading")
	assert.Contains(t, text, "config_fields")
	assert.Contains(t, text, "lookback_window")
	assert.Contains(t, text, "followed_trader")
}

func TestStrategyDORATools(t *testing.T) {
	ts := newTestClient(t)

	orderbooks := callTool(t, ts, "strategy_dora_orderbooks", nil)
	require.False(t, orderbooks.IsError)
	orderbooksText := textContent(t, orderbooks)
	assert.Contains(t, orderbooksText, "book-1")
	assert.Contains(t, orderbooksText, "UST 10Y / USD")

	user := callTool(t, ts, "strategy_dora_user", nil)
	require.False(t, user.IsError)
	assert.Contains(t, textContent(t, user), "user-123")
}

func TestStrategyTenors(t *testing.T) {
	ts := newTestClient(t)

	result := callTool(t, ts, "strategy_tenors", nil)
	require.False(t, result.IsError)
	text := textContent(t, result)
	assert.Contains(t, text, "1M")
	assert.Contains(t, text, "10Y")
	assert.Contains(t, text, "30Y")
	assert.Contains(t, text, "10 Year Treasury")
}

func TestStrategyRunLifecycle(t *testing.T) {
	ts := newTestClient(t)

	create := callTool(t, ts, "strategy_run_create", map[string]any{
		"strategy_type": "mean_reversion",
		"config":        map[string]any{"lookback_window": 20},
	})
	require.False(t, create.IsError)
	assert.Contains(t, textContent(t, create), "22222222-2222-2222-2222-222222222222")

	get := callTool(t, ts, "strategy_run_get", map[string]any{"id": "22222222-2222-2222-2222-222222222222"})
	require.False(t, get.IsError)
	assert.Contains(t, textContent(t, get), "running")

	list := callTool(t, ts, "strategy_run_list", nil)
	require.False(t, list.IsError)
	assert.Contains(t, textContent(t, list), "11111111-1111-1111-1111-111111111111")

	pause := callTool(t, ts, "strategy_run_pause", map[string]any{"id": "22222222-2222-2222-2222-222222222222"})
	require.False(t, pause.IsError)
	assert.Contains(t, textContent(t, pause), "paused")

	resume := callTool(t, ts, "strategy_run_resume", map[string]any{"id": "22222222-2222-2222-2222-222222222222"})
	require.False(t, resume.IsError)
	assert.Contains(t, textContent(t, resume), "running")

	stop := callTool(t, ts, "strategy_run_stop", map[string]any{"id": "22222222-2222-2222-2222-222222222222"})
	require.False(t, stop.IsError)
	assert.Contains(t, textContent(t, stop), "stopped")
}

func TestStrategyRunQuestions(t *testing.T) {
	ts := newTestClient(t)

	status := callTool(t, ts, "strategy_run_status", nil)
	require.False(t, status.IsError)
	statusText := textContent(t, status)
	assert.Contains(t, statusText, "Found 2 strategy runs")
	assert.Contains(t, statusText, "1 running")
	assert.Contains(t, statusText, "1 paused")
	assert.Contains(t, statusText, "11111111-1111-1111-1111-111111111111")

	pausedOnly := callTool(t, ts, "strategy_run_status", map[string]any{"status": "paused"})
	require.False(t, pausedOnly.IsError)
	pausedText := textContent(t, pausedOnly)
	assert.Contains(t, pausedText, "Found 1 strategy runs")
	assert.Contains(t, pausedText, "55555555-5555-5555-5555-555555555555")

	describe := callTool(t, ts, "strategy_run_describe", map[string]any{"id": "55555555-5555-5555-5555-555555555555"})
	require.False(t, describe.IsError)
	detailText := textContent(t, describe)
	assert.Contains(t, detailText, "Run 55555555-5555-5555-5555-555555555555")
	assert.Contains(t, detailText, "Status: paused")
	assert.Contains(t, detailText, "Config: entry_z_score=1.5, lookback_window=30")
	assert.Contains(t, detailText, "Error: waiting for manual resume")
}

func TestStrategyBacktestLifecycle(t *testing.T) {
	ts := newTestClient(t)

	create := callTool(t, ts, "strategy_backtest_create", map[string]any{
		"strategy_type": "mean_reversion",
		"config":        map[string]any{"lookback_window": 20},
		"start":         "2026-04-01T00:00:00Z",
		"end":           "2026-04-02T00:00:00Z",
	})
	require.False(t, create.IsError)
	assert.Contains(t, textContent(t, create), "44444444-4444-4444-4444-444444444444")

	get := callTool(t, ts, "strategy_backtest_get", map[string]any{"id": "44444444-4444-4444-4444-444444444444"})
	require.False(t, get.IsError)
	getText := textContent(t, get)
	assert.Contains(t, getText, "total_pnl")
	assert.Contains(t, getText, "9377.78")
	assert.Contains(t, getText, "win_count")
	assert.Contains(t, getText, "764")
	assert.Contains(t, getText, "strategy_type")
	assert.Contains(t, getText, "mean_reversion")
	assert.Contains(t, getText, "asset_name")
	assert.Contains(t, getText, "UST 10Y")
	assert.Contains(t, getText, "asset_symbol")
	assert.Contains(t, getText, "UST10Y")
	assert.Contains(t, getText, "status")
	assert.Contains(t, getText, "completed")

	// Metadata still returns the backtest summary.
	metadata := callTool(t, ts, "strategy_backtest_metadata", map[string]any{"id": "44444444-4444-4444-4444-444444444444"})
	require.False(t, metadata.IsError)
	assert.Contains(t, textContent(t, metadata), "completed")

	list := callTool(t, ts, "strategy_backtest_list", nil)
	require.False(t, list.IsError)
	assert.Contains(t, textContent(t, list), "33333333-3333-3333-3333-333333333333")

	trades := callTool(t, ts, "strategy_backtest_trades", map[string]any{
		"id":    "44444444-4444-4444-4444-444444444444",
		"page":  1,
		"limit": 10,
	})
	require.False(t, trades.IsError)
	tradesText := textContent(t, trades)
	assert.Contains(t, tradesText, "BOND-1")
	assert.Contains(t, tradesText, "BUY")

	closedTrades := callTool(t, ts, "strategy_backtest_closed_trades", map[string]any{
		"id": "44444444-4444-4444-4444-444444444444",
	})
	require.False(t, closedTrades.IsError)
	closedText := textContent(t, closedTrades)
	assert.Contains(t, closedText, "take_profit")
	assert.Contains(t, closedText, "exit_signal")

	cancel := callTool(t, ts, "strategy_backtest_cancel", map[string]any{"id": "44444444-4444-4444-4444-444444444444"})
	require.False(t, cancel.IsError)
	assert.Contains(t, textContent(t, cancel), "cancelled")
}

func TestStrategyCopyTradingNotImplemented(t *testing.T) {
	ts := newTestClient(t)

	run := callTool(t, ts, "strategy_run_create", map[string]any{
		"strategy_type": "copytrading",
		"config": map[string]any{
			"followed_trader": "11111111-1111-1111-1111-111111111111",
			"min_order_size":  1,
			"max_order_size":  2,
		},
	})
	assert.True(t, run.IsError)
	assert.Contains(t, textContent(t, run), "not implemented")

	backtest := callTool(t, ts, "strategy_backtest_create", map[string]any{
		"strategy_type": "copytrading",
		"config": map[string]any{
			"followed_trader": "11111111-1111-1111-1111-111111111111",
			"min_order_size":  1,
			"max_order_size":  2,
		},
		"start": "2026-04-01T00:00:00Z",
		"end":   "2026-04-02T00:00:00Z",
	})
	assert.True(t, backtest.IsError)
	assert.Contains(t, textContent(t, backtest), "not implemented")
}

func TestFREDFetchSeriesNoAPIKey(t *testing.T) {
	ts := newTestClient(t)

	result := callTool(t, ts, "fred_fetch_series", map[string]any{
		"series_id": "DGS10",
	})
	assert.True(t, result.IsError)
	assert.Contains(t, textContent(t, result), "FRED API key")
}

func TestFREDFetchLatestNoAPIKey(t *testing.T) {
	ts := newTestClient(t)

	result := callTool(t, ts, "fred_fetch_latest", map[string]any{
		"series_id": "DGS10",
	})
	assert.True(t, result.IsError)
	assert.Contains(t, textContent(t, result), "FRED API key")
}

func TestFREDFetchYieldCurveNoAPIKey(t *testing.T) {
	ts := newTestClient(t)

	result := callTool(t, ts, "fred_fetch_yield_curve", map[string]any{
		"date": "2024-01-05",
	})
	assert.True(t, result.IsError)
	assert.Contains(t, textContent(t, result), "FRED API key")
}

func TestFREDFetchHistoricalYieldsNoAPIKey(t *testing.T) {
	ts := newTestClient(t)

	result := callTool(t, ts, "fred_fetch_historical_yields", map[string]any{
		"tenor":      10.0,
		"start_date": "2024-01-01",
	})
	assert.True(t, result.IsError)
	assert.Contains(t, textContent(t, result), "FRED API key")
}

func TestFREDInterpolateYield(t *testing.T) {
	ts := newTestClient(t)

	curve := map[string]any{
		"Date": "2024-01-05T00:00:00Z",
		"Points": []map[string]any{
			{"Tenor": 2.0, "Yield": 0.04},
			{"Tenor": 10.0, "Yield": 0.05},
		},
	}

	result := callTool(t, ts, "fred_interpolate_yield", map[string]any{
		"curve": curve,
		"tenor": 6.0,
	})
	require.False(t, result.IsError, textContent(t, result))

	var resp map[string]any
	require.NoError(t, json.Unmarshal([]byte(textContent(t, result)), &resp))
	want := decimal.MustNew(45, 3)
	yieldStr, _ := resp["yield"].(string)
	got, _ := decimal.Parse(yieldStr)
	assert.True(t, want.Equal(got))
}

func TestFREDBenchmarkYield(t *testing.T) {
	ts := newTestClient(t)

	curve := map[string]any{
		"Date": "2024-01-05T00:00:00Z",
		"Points": []map[string]any{
			{"Tenor": 1.0, "Yield": 0.04},
			{"Tenor": 10.0, "Yield": 0.05},
		},
	}

	result := callTool(t, ts, "fred_benchmark_yield", map[string]any{
		"curve":         curve,
		"ref_date":      "2024-01-05",
		"maturity_date": "2025-01-05",
	})
	require.False(t, result.IsError, textContent(t, result))

	var resp map[string]any
	require.NoError(t, json.Unmarshal([]byte(textContent(t, result)), &resp))
	assert.Contains(t, resp, "benchmark_yield")
	assert.Contains(t, resp, "tenor_years")

	want := decimal.MustNew(4, 2)
	benchmarkStr, _ := resp["benchmark_yield"].(string)
	got, _ := decimal.Parse(benchmarkStr)
	assert.True(t, testutils.InDelta(t, got, want, decimal.MustNew(1, 4)))
}

func TestFREDBenchmarkYieldMissingDate(t *testing.T) {
	ts := newTestClient(t)

	result := callTool(t, ts, "fred_benchmark_yield", map[string]any{
		"curve":         map[string]any{"Points": []any{}},
		"ref_date":      "",
		"maturity_date": "2025-01-05",
	})
	assert.True(t, result.IsError)
}

func TestToolsListContainsExpectedTools(t *testing.T) {
	ts := newTestClient(t)

	res, err := ts.Client().ListTools(context.Background(), mcp.ListToolsRequest{})
	require.NoError(t, err)

	names := make(map[string]bool, len(res.Tools))
	for _, tool := range res.Tools {
		names[tool.Name] = true
	}

	expected := []string{
		"strategy_list",
		"strategy_dora_orderbooks",
		"strategy_dora_user",
		"strategy_tenors",
		"strategy_run_create",
		"strategy_run_get",
		"strategy_run_list",
		"strategy_run_status",
		"strategy_run_describe",
		"strategy_run_pause",
		"strategy_run_resume",
		"strategy_run_stop",
		"strategy_backtest_create",
		"strategy_backtest_get",
		"strategy_backtest_list",
		"strategy_backtest_trades",
		"strategy_backtest_closed_trades",
		"strategy_backtest_metadata",
		"strategy_backtest_cancel",
		"fred_fetch_series",
		"fred_fetch_latest",
		"fred_fetch_yield_curve",
		"fred_fetch_historical_yields",
		"fred_interpolate_yield",
		"fred_benchmark_yield",
	}
	for _, name := range expected {
		assert.True(t, names[name], "expected tool %q to be registered", name)
	}
	assert.False(t, names["fred_backtest_from_series"])
}

// TestStrategyConfigSchemaIsTyped is a regression test for the bug where
// `config` was exposed as an untyped object and MCP clients (opencode)
// would stringify numeric/array values, causing the strategy-server to
// reject requests with "cannot unmarshal string into ... of type float64".
// The schema for `config` must declare explicit property types so clients
// know to send numbers as numbers and arrays as arrays.
func TestStrategyConfigSchemaIsTyped(t *testing.T) {
	ts := newTestClient(t)
	res, err := ts.Client().ListTools(context.Background(), mcp.ListToolsRequest{})
	require.NoError(t, err)

	toolsByName := make(map[string]mcp.Tool, len(res.Tools))
	for _, tool := range res.Tools {
		toolsByName[tool.Name] = tool
	}

	for _, name := range []string{"strategy_backtest_create", "strategy_run_create"} {
		t.Run(name, func(t *testing.T) {
			tool, ok := toolsByName[name]
			require.True(t, ok, "tool %q not registered", name)

			raw, err := json.Marshal(tool.InputSchema)
			require.NoError(t, err)
			var schema struct {
				Properties map[string]struct {
					Type       string `json:"type"`
					Properties map[string]struct {
						Type  string `json:"type"`
						Items struct {
							Type string `json:"type"`
						} `json:"items"`
					} `json:"properties"`
				} `json:"properties"`
			}
			require.NoError(t, json.Unmarshal(raw, &schema))

			config, ok := schema.Properties["config"]
			require.True(t, ok, "config property missing from %s schema", name)
			require.Equal(t, "object", config.Type, "config must be type object")
			require.NotEmpty(t, config.Properties, "config must declare explicit property types so clients don't stringify values")

			checkField := func(field, wantType string) {
				t.Helper()
				f, ok := config.Properties[field]
				require.True(t, ok, "%s.config.%s must be declared in schema", name, field)
				require.Equal(t, wantType, f.Type, "%s.config.%s must be type %q, got %q", name, field, wantType, f.Type)
			}
			checkField("percentage_of_available", "number")
			checkField("leverage", "number")
			checkField("min_order_size", "integer")
			checkField("max_order_size", "integer")
			checkField("lookback_window", "integer")
			checkField("entry_z_score", "number")
			checkField("exit_z_score", "number")
			checkField("max_position_size", "number")

			arr, ok := config.Properties["disallowed_bonds"]
			require.True(t, ok, "%s.config.disallowed_bonds must be declared in schema", name)
			require.Equal(t, "array", arr.Type, "disallowed_bonds must be type array, got %q", arr.Type)
			require.Equal(t, "string", arr.Items.Type, "disallowed_bonds items must be type string, got %q", arr.Items.Type)
		})
	}
}

// TestStartNotificationsRelay_ForwardsEventToMCPClients verifies the
// end-to-end flow: a fake strategy-server WS pushes an event, and an
// MCP client connected to the SSE server receives it as a
// `notifications/event` JSON-RPC notification.
func TestStartNotificationsRelay_ForwardsEventToMCPClients(t *testing.T) {
	// 1. Fake strategy-server WS: accepts the upgrade, writes one event,
	//    then keeps the conn open until ctx cancellation.
	runID := uuid.NewString()
	wsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/notifications/ws" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") == "" {
			http.Error(w, "missing auth", http.StatusUnauthorized)
			return
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		evt := notifications.Event{
			ID:        uuid.NewString(),
			Type:      notifications.EventRunStarted,
			UserID:    "user-1",
			RunID:     runID,
			Timestamp: time.Now().UTC(),
		}
		data, err := json.Marshal(evt)
		if err != nil {
			return
		}
		_ = conn.Write(r.Context(), websocket.MessageText, data)
		<-r.Context().Done()
	}))
	defer wsSrv.Close()
	wsURL, err := url.Parse(wsSrv.URL)
	require.NoError(t, err)
	wsURL.Scheme = "ws"
	wsURL.Path = "/v1/notifications/ws"
	fakeWSURL := wsURL.String()

	// 2. Real MCP server + SSE transport, hosted on an httptest server.
	//    We build the SSEServer without WithBaseURL so the SSE transport
	//    can resolve the message endpoint relative to the client
	//    connection origin (otherwise origin mismatches are enforced).
	mux := http.NewServeMux()
	mcpSrv := mcpserver.New("", "test-key", wsSrv.URL)
	sseSrv := server.NewSSEServer(mcpSrv, server.WithKeepAlive(true))
	mux.Handle("/", sseSrv)
	sseTS := httptest.NewServer(mux)
	defer sseTS.Close()
	defer func() { _ = sseSrv.Shutdown(context.Background()) }()
	sseURL := sseTS.URL

	// 3. Connect a real MCP client to the SSE server. The client
	//    expects the URL to end in the SSE endpoint path.
	mcpClient, err := client.NewSSEMCPClient(sseURL + "/sse")
	require.NoError(t, err)
	defer mcpClient.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, mcpClient.Start(ctx))
	initReq := mcp.InitializeRequest{}
	initReq.Params.ProtocolVersion = mcp.LATEST_PROTOCOL_VERSION
	initReq.Params.ClientInfo = mcp.Implementation{Name: "test", Version: "0.0.1"}
	_, err = mcpClient.Initialize(ctx, initReq)
	require.NoError(t, err)

	// 4. Capture the notification.
	received := make(chan mcp.JSONRPCNotification, 1)
	mcpClient.OnNotification(func(n mcp.JSONRPCNotification) {
		if n.Method != "notifications/event" {
			return
		}
		select {
		case received <- n:
		default:
		}
	})

	// 5. Start the relay.
	relayCtx, relayCancel := context.WithCancel(context.Background())
	defer relayCancel()
	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		_ = mcpserver.StartNotificationsRelay(relayCtx, mcpSrv, fakeWSURL, "test-key")
	}()

	// 6. Assert the notification arrived with the expected payload.
	select {
	case n := <-received:
		assert.Equal(t, "notifications/event", n.Method)
		params := n.Params.AdditionalFields
		assert.Equal(t, string(notifications.EventRunStarted), params["type"], "type field")
		assert.Equal(t, runID, params["run_id"], "run_id field")
		assert.Equal(t, "user-1", params["user_id"], "user_id field")
		assert.NotEmpty(t, params["id"], "id field")
	case <-time.After(3 * time.Second):
		t.Fatal("did not receive notifications/event from MCP client")
	}

	relayCancel()
	select {
	case <-relayDone:
	case <-time.After(time.Second):
		t.Fatal("relay did not stop after ctx cancel")
	}
}
