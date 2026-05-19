package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

type strategyCreateRunArgs struct {
	StrategyType string         `json:"strategy_type"`
	Config       map[string]any `json:"config"`
}

type strategyGetRunArgs struct {
	ID string `json:"id"`
}

type strategyRunStatusArgs struct {
	Status string `json:"status"`
}

type strategyPauseRunArgs struct {
	ID string `json:"id"`
}

type strategyResumeRunArgs struct {
	ID string `json:"id"`
}

type strategyStopRunArgs struct {
	ID string `json:"id"`
}

type strategyCreateBacktestArgs struct {
	StrategyType string         `json:"strategy_type"`
	Config       map[string]any `json:"config"`
	Start        string         `json:"start"`
	End          string         `json:"end"`
}

type strategyGetBacktestArgs struct {
	ID string `json:"id"`
}

type strategyBacktestTradesArgs struct {
	ID    string `json:"id"`
	Page  int    `json:"page,omitempty"`
	Limit int    `json:"limit,omitempty"`
}

type strategyCancelBacktestArgs struct {
	ID string `json:"id"`
}

func jsonText(v any) (*mcp.CallToolResult, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("marshal result: %v", err)), nil
	}
	return mcp.NewToolResultText(string(b)), nil
}

//nolint:funlen // registration function with many tool definitions
func registerStrategyTools(s *server.MCPServer, strategyBaseURL, apiKey string) {
	client := newStrategyClient(strategyBaseURL, apiKey)

	s.AddTool(
		mcp.NewTool("strategy_list",
			mcp.WithDescription("List strategies exposed by strategy-server, including availability and supported capabilities."),
		),
		func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			result, err := client.listStrategies(ctx)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return jsonText(result)
		},
	)

	s.AddTool(
		mcp.NewTool("strategy_dora_orderbooks",
			mcp.WithDescription("List DORA order books exposed by strategy-server for the configured DORA API key."),
		),
		func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			result, err := client.listDORAOrderBooks(ctx)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return jsonText(result)
		},
	)

	s.AddTool(
		mcp.NewTool("strategy_dora_user",
			mcp.WithDescription("Get the current DORA user ID exposed by strategy-server for the configured DORA API key."),
		),
		func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			result, err := client.getDORAUser(ctx)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return jsonText(result)
		},
	)

	s.AddTool(
		mcp.NewTool("strategy_tenors",
			mcp.WithDescription("List available benchmark tenors exposed by strategy-server."),
		),
		func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			result, err := client.listTenors(ctx)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return jsonText(result)
		},
	)

	s.AddTool(
		mcp.NewTool("strategy_run_create",
			mcp.WithDescription("Create a strategy run via strategy-server."),
			mcp.WithString("strategy_type", mcp.Required(), mcp.Description("Strategy type, e.g. mean_reversion or copytrading.")),
			mcp.WithObject("config", mcp.Required(), mcp.Description("Strategy config object accepted by strategy-server.")),
		),
		mcp.NewTypedToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, args strategyCreateRunArgs) (*mcp.CallToolResult, error) {
			result, err := client.createRun(ctx, map[string]any{
				"strategy_type": args.StrategyType,
				"config":        args.Config,
			})
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return jsonText(result)
		}),
	)

	s.AddTool(
		mcp.NewTool("strategy_run_get",
			mcp.WithDescription("Get one strategy run by ID from strategy-server as raw JSON. Prefer strategy_run_describe for natural-language questions about a run."), //nolint:lll
			mcp.WithString("id", mcp.Required(), mcp.Description("Strategy run ID.")),
		),
		mcp.NewTypedToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, args strategyGetRunArgs) (*mcp.CallToolResult, error) {
			result, err := client.getRun(ctx, args.ID)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return jsonText(result)
		}),
	)

	s.AddTool(
		mcp.NewTool("strategy_run_list",
			mcp.WithDescription("List strategy runs from strategy-server as raw JSON. Prefer strategy_run_status for natural-language questions about what is running, paused, or stopped."), //nolint:lll
		),
		func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			result, err := client.listRuns(ctx)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return jsonText(result)
		},
	)

	s.AddTool(
		mcp.NewTool("strategy_run_status",
			mcp.WithDescription("Answer questions about current strategy runs with a concise natural-language summary. Use this for prompts like 'what runs are active?' or 'what is paused?'. Optionally filter by status: running, paused, or stopped."), //nolint:lll
			mcp.WithString("status", mcp.Description("Optional status filter: running, paused, or stopped.")),
		),
		mcp.NewTypedToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, args strategyRunStatusArgs) (*mcp.CallToolResult, error) {
			status := strings.TrimSpace(strings.ToLower(args.Status))
			if status != "" && status != "running" && status != "paused" && status != "stopped" {
				return mcp.NewToolResultError("status must be one of: running, paused, stopped"), nil
			}
			result, err := client.listRunsTyped(ctx)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return mcp.NewToolResultText(formatRunListSummary(result.Items, status)), nil
		}),
	)

	s.AddTool(
		mcp.NewTool("strategy_run_describe",
			mcp.WithDescription("Answer questions about one strategy run in natural language, including status, timestamps, config, and any recorded error. Use this for prompts like 'tell me about run <id>' or 'why is this run paused?'."), //nolint:lll
			mcp.WithString("id", mcp.Required(), mcp.Description("Strategy run ID.")),
		),
		mcp.NewTypedToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, args strategyGetRunArgs) (*mcp.CallToolResult, error) {
			result, err := client.getRunTyped(ctx, args.ID)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return mcp.NewToolResultText(formatRunDetailSummary(result)), nil
		}),
	)

	s.AddTool(
		mcp.NewTool("strategy_run_pause",
			mcp.WithDescription("Pause a strategy run via strategy-server."),
			mcp.WithString("id", mcp.Required(), mcp.Description("Strategy run ID.")),
		),
		mcp.NewTypedToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, args strategyPauseRunArgs) (*mcp.CallToolResult, error) {
			result, err := client.pauseRun(ctx, args.ID)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return jsonText(result)
		}),
	)

	s.AddTool(
		mcp.NewTool("strategy_run_resume",
			mcp.WithDescription("Resume a strategy run via strategy-server."),
			mcp.WithString("id", mcp.Required(), mcp.Description("Strategy run ID.")),
		),
		mcp.NewTypedToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, args strategyResumeRunArgs) (*mcp.CallToolResult, error) {
			result, err := client.resumeRun(ctx, args.ID)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return jsonText(result)
		}),
	)

	s.AddTool(
		mcp.NewTool("strategy_run_stop",
			mcp.WithDescription("Stop a strategy run via strategy-server."),
			mcp.WithString("id", mcp.Required(), mcp.Description("Strategy run ID.")),
		),
		mcp.NewTypedToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, args strategyStopRunArgs) (*mcp.CallToolResult, error) {
			result, err := client.stopRun(ctx, args.ID)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return jsonText(result)
		}),
	)

	s.AddTool(
		mcp.NewTool("strategy_backtest_create",
			mcp.WithDescription("Create an asynchronous strategy backtest via strategy-server."),
			mcp.WithString("strategy_type", mcp.Required(), mcp.Description("Strategy type, e.g. mean_reversion or copytrading.")),
			mcp.WithObject("config", mcp.Required(), mcp.Description("Strategy config object accepted by strategy-server.")),
			mcp.WithString("start", mcp.Required(), mcp.Description("Backtest start timestamp in RFC3339 format.")),
			mcp.WithString("end", mcp.Required(), mcp.Description("Backtest end timestamp in RFC3339 format.")),
		),
		mcp.NewTypedToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, args strategyCreateBacktestArgs) (*mcp.CallToolResult, error) {
			result, err := client.createBacktest(ctx, map[string]any{
				"strategy_type": args.StrategyType,
				"config":        args.Config,
				"start":         args.Start,
				"end":           args.End,
			})
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return jsonText(result)
		}),
	)

	s.AddTool(
		mcp.NewTool("strategy_backtest_get",
			mcp.WithDescription("Get summarised results from one strategy backtest by ID from strategy-server."),
			mcp.WithString("id", mcp.Required(), mcp.Description("Strategy backtest ID.")),
		),
		mcp.NewTypedToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, args strategyGetBacktestArgs) (*mcp.CallToolResult, error) {
			result, err := client.getBacktest(ctx, args.ID)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return jsonText(result)
		}),
	)

	s.AddTool(
		mcp.NewTool("strategy_backtest_list",
			mcp.WithDescription("List strategy backtests from strategy-server."),
		),
		func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			result, err := client.listBacktests(ctx)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return jsonText(result)
		},
	)

	registerBacktestSubResourceTool(s, "strategy_backtest_trades", "trade records",
		func(ctx context.Context, id string, page, limit int) (map[string]any, error) {
			return client.getBacktestTrades(ctx, id, page, limit)
		},
	)

	registerBacktestSubResourceTool(s, "strategy_backtest_closed_trades", "closed trades",
		func(ctx context.Context, id string, page, limit int) (map[string]any, error) {
			return client.getBacktestClosedTrades(ctx, id, page, limit)
		},
	)

	s.AddTool(
		mcp.NewTool("strategy_backtest_metadata",
			mcp.WithDescription("Get the backtest metadata (ID, status, timestamps) by ID from strategy-server. Use this to check status or get the backtest ID — not the P&L result summary."), //nolint:lll
			mcp.WithString("id", mcp.Required(), mcp.Description("Strategy backtest ID.")),
		),
		mcp.NewTypedToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, args strategyGetBacktestArgs) (*mcp.CallToolResult, error) {
			result, err := client.getBacktestMetadata(ctx, args.ID)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return jsonText(result)
		}),
	)

	s.AddTool(
		mcp.NewTool("strategy_backtest_cancel",
			mcp.WithDescription("Cancel a strategy backtest via strategy-server."),
			mcp.WithString("id", mcp.Required(), mcp.Description("Strategy backtest ID.")),
		),
		mcp.NewTypedToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, args strategyCancelBacktestArgs) (*mcp.CallToolResult, error) {
			result, err := client.cancelBacktest(ctx, args.ID)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return jsonText(result)
		}),
	)
}

func formatRunListSummary(runs []strategyRunSummary, statusFilter string) string {
	statusFilter = strings.TrimSpace(strings.ToLower(statusFilter))
	filtered := make([]strategyRunSummary, 0, len(runs))
	for _, run := range runs {
		if statusFilter != "" && strings.ToLower(run.Status) != statusFilter {
			continue
		}
		filtered = append(filtered, run)
	}
	if len(filtered) == 0 {
		if statusFilter == "" {
			return "No strategy runs found."
		}
		return fmt.Sprintf("No %s strategy runs found.", statusFilter)
	}

	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].UpdatedAt == filtered[j].UpdatedAt {
			return filtered[i].ID < filtered[j].ID
		}
		return filtered[i].UpdatedAt > filtered[j].UpdatedAt
	})

	counts := map[string]int{}
	for _, run := range filtered {
		counts[run.Status]++
	}
	parts := make([]string, 0, 3) //nolint:mnd
	for _, status := range []string{"running", "paused", "stopped"} {
		if counts[status] == 0 {
			continue
		}
		parts = append(parts, fmt.Sprintf("%d %s", counts[status], status))
	}

	lines := []string{fmt.Sprintf("Found %d strategy runs (%s).", len(filtered), strings.Join(parts, ", "))}
	for _, run := range filtered {
		lines = append(lines, fmt.Sprintf("- %s: %s is %s (created %s, updated %s)", run.ID, run.StrategyType, run.Status, run.CreatedAt, run.UpdatedAt)) //nolint:lll
	}
	return strings.Join(lines, "\n")
}

func formatRunDetailSummary(run strategyRunDetail) string {
	lines := []string{
		fmt.Sprintf("Run %s", run.ID),
		fmt.Sprintf("Strategy: %s", run.StrategyType),
		fmt.Sprintf("Status: %s", run.Status),
		fmt.Sprintf("Created: %s", run.CreatedAt),
		fmt.Sprintf("Updated: %s", run.UpdatedAt),
	}
	if run.StoppedAt != "" {
		lines = append(lines, fmt.Sprintf("Stopped: %s", run.StoppedAt))
	}
	if len(run.Config) > 0 {
		keys := make([]string, 0, len(run.Config))
		for key := range run.Config {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		pairs := make([]string, 0, len(keys))
		for _, key := range keys {
			pairs = append(pairs, fmt.Sprintf("%s=%v", key, run.Config[key]))
		}
		lines = append(lines, fmt.Sprintf("Config: %s", strings.Join(pairs, ", ")))
	}
	if run.Error != "" {
		lines = append(lines, fmt.Sprintf("Error: %s", run.Error))
	}
	return strings.Join(lines, "\n")
}

func registerBacktestSubResourceTool(
	s *server.MCPServer, name, label string,
	fetch func(context.Context, string, int, int) (map[string]any, error),
) {
	s.AddTool(
		mcp.NewTool(name,
			mcp.WithDescription(fmt.Sprintf("Get paginated %s from a completed backtest. Default page=1, limit=10, max limit=50.", label)),
			mcp.WithString("id", mcp.Required(), mcp.Description("Strategy backtest ID.")),
			mcp.WithNumber("page", mcp.Description("Page number (default 1).")),
			mcp.WithNumber("limit", mcp.Description("Items per page (default 10, max 50).")),
		),
		mcp.NewTypedToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, args strategyBacktestTradesArgs) (*mcp.CallToolResult, error) {
			page, limit := args.Page, args.Limit
			if page < 1 {
				page = 1
			}
			if limit < 1 {
				limit = 10
			}
			result, err := fetch(ctx, args.ID, page, limit)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return jsonText(result)
		}),
	)
}
