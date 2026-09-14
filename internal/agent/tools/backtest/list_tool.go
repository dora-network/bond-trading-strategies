// Package backtest list_tool.go adds the list_backtests LLM tool. It
// mirrors the user-scoped store method (backtest.Store.ListBacktests)
// rather than the per-strategy HTTP route (which would require the
// caller to know strategy_id up front, defeating the chat case where
// the user asks "what backtests have I run").
//
// The tool accepts optional strategy_id (filter to one strategy) and
// optional status (e.g. "running" to find in-flight jobs). Ownership
// is enforced in SQL by the user_id WHERE clause -- the tool never
// sees other users' rows.
package backtest

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	bt "github.com/dora-network/bond-trading-strategies/internal/agent/backtest"
	"github.com/dora-network/bond-trading-strategies/internal/agent/llm"
)

const listToolName = "list_backtests"

// listBacktestsSchema declares the (mostly optional) input shape.
// strategy_id, status, and limit are all optional; the LLM omits what
// it doesn't care about. status is a string (not an enum) because
// bt.Status is a typed string and JSON has no enum primitive; the
// handler validates against the known set below.
//
//nolint:lll // tools/input shapes are inherently long; lll here is noise.
const listBacktestsSchema = `{"type":"object","properties":{"strategy_id":{"type":"string","description":"Optional: filter backtests to one strategy. Omit to list across all strategies owned by the caller."},"status":{"type":"string","description":"Optional: filter by status. One of: queued, running, succeeded, failed, cancelled. Omit to list any status."},"limit":{"type":"integer","description":"Optional: max rows to return (capped at 50). Omit for the default of 20."}},"required":[]}`

// parseListBacktestsStatus validates the optional status filter
// against the known bt.Status set. The LLM may pass any string;
// an unrecognised value surfaces as a 400-shape error rather
// than silently returning zero rows (the LLM couldn't tell
// "no rows match" from "unknown status -> SQL returned nothing").
// Returns nil for the empty / unspecified case.
func parseListBacktestsStatus(s string) (*bt.Status, error) {
	switch s {
	case "":
		return nil, nil
	case string(bt.StatusQueued):
		st := bt.StatusQueued
		return &st, nil
	case string(bt.StatusRunning):
		st := bt.StatusRunning
		return &st, nil
	case string(bt.StatusSucceeded):
		st := bt.StatusSucceeded
		return &st, nil
	case string(bt.StatusFailed):
		st := bt.StatusFailed
		return &st, nil
	case string(bt.StatusCancelled):
		st := bt.StatusCancelled
		return &st, nil
	default:
		return nil, fmt.Errorf("status %q invalid (want queued|running|succeeded|failed|cancelled)", s)
	}
}

// listBacktestsInput is the typed input the LLM sends.
type listBacktestsInput struct {
	StrategyID string `json:"strategy_id,omitempty"`
	Status     string `json:"status,omitempty"`
	Limit      int    `json:"limit,omitempty"`
}

// listBacktestRow is one entry in the envelope's list. Mirrors
// toolOutput minus fills (list paths never return fills -- the LLM
// can call get_backtest_result for those on the rows it cares about).
type listBacktestRow struct {
	ID           string            `json:"id"`
	StrategyID   string            `json:"strategy_id"`
	VersionID    string            `json:"version_id"`
	UserID       string            `json:"user_id"`
	Status       bt.Status         `json:"status"`
	RequestedAt  time.Time         `json:"requested_at"`
	StartedAt    *time.Time        `json:"started_at,omitempty"`
	FinishedAt   *time.Time        `json:"finished_at,omitempty"`
	ErrorMessage string            `json:"error_message,omitempty"`
	WindowStart  time.Time         `json:"window_start"`
	WindowEnd    time.Time         `json:"window_end"`
	Resolution   string            `json:"resolution"`
	Params       map[string]string `json:"params"`
	Summary      *bt.Summary       `json:"summary,omitempty"`
	FillCount    int               `json:"fill_count,omitempty"`
	ImageRef     string            `json:"image_ref,omitempty"`
}

// listBacktestsOutput is the JSON envelope returned to the agent.
// The list is always present (possibly empty) so the LLM doesn't have
// to nil-check.
type listBacktestsOutput struct {
	Backtests []listBacktestRow `json:"backtests"`
	Count     int               `json:"count"` // len(Backtests), explicit for the LLM
}

// ListSpec returns the list_backtests tool spec.
func ListSpec() llm.ToolSpec {
	return llm.ToolSpec{
		Name: listToolName,
		//nolint:lll // tool description strings are inherently long
		Description: "List the caller's backtests, newest first. All filters are optional: pass strategy_id to scope to one strategy, status to filter (queued/running/succeeded/failed/cancelled), limit to cap row count (default 20, max 50). Returns metadata only (no fills) -- call get_backtest_result for fills on rows you care about.",
		JSONSchema:  json.RawMessage(listBacktestsSchema),
	}
}

// ListHandler returns a closure-bound llm.ToolHandler that loads the
// caller's backtests via the user-scoped store method (ownership in
// SQL). HTTP-layer code (which has the principal) owns the lifecycle
// and passes userID per turn.
func ListHandler(store bt.Store, userID string) llm.ToolHandler {
	return func(ctx context.Context, _ string, input json.RawMessage) (json.RawMessage, error) {
		var in listBacktestsInput
		if err := json.Unmarshal(input, &in); err != nil {
			return nil, fmt.Errorf("list_backtests: parse input: %w", err)
		}

		statusPtr, err := parseListBacktestsStatus(in.Status)
		if err != nil {
			return nil, fmt.Errorf("list_backtests: %w", err)
		}

		// strategy_id is optional. nil = "any strategy owned by the user".
		var strategyPtr *string
		if in.StrategyID != "" {
			s := in.StrategyID
			strategyPtr = &s
		}

		rows, err := store.ListBacktests(ctx, userID, strategyPtr, statusPtr, in.Limit)
		if err != nil {
			return nil, fmt.Errorf("list_backtests: %w", err)
		}

		out := listBacktestsOutput{
			Backtests: make([]listBacktestRow, 0, len(rows)),
			Count:     len(rows),
		}
		for _, b := range rows {
			out.Backtests = append(out.Backtests, listBacktestRow{
				ID:           b.ID,
				StrategyID:   b.StrategyID,
				VersionID:    b.VersionID,
				UserID:       b.UserID,
				Status:       b.Status,
				RequestedAt:  b.RequestedAt,
				StartedAt:    b.StartedAt,
				FinishedAt:   b.FinishedAt,
				ErrorMessage: b.ErrorMessage,
				WindowStart:  b.WindowStart,
				WindowEnd:    b.WindowEnd,
				Resolution:   b.Resolution,
				Params:       b.Params,
				Summary:      b.Summary,
				FillCount:    b.FillCount,
				ImageRef:     b.ImageRef,
			})
		}
		return json.Marshal(out)
	}
}
