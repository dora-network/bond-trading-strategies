// Package backtest implements the get_backtest_result agent tool.
// The LLM hands us a backtest_id; the handler owns the row, waits for the
// terminal transition if it's still running, then returns the row + fills as
// one JSON envelope so the chat loop doesn't have to poll.
package backtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	bt "github.com/dora-network/bond-trading-strategies/internal/agent/backtest"
	"github.com/dora-network/bond-trading-strategies/internal/agent/llm"
)

// Poll caps for the tool's wait. The chat layer wants a single observable
// call rather than a "still running, retry" bounce; if the backtest stays
// queued/running past the budget we return the most recent row with whatever
// status it has — the caller can decide to extend.
const (
	PollBudget   = 30 * time.Second
	PollInterval = 2 * time.Second
)

const toolName = "get_backtest_result"

// getBacktestResultSchema declares the (only) input field. backtest_id is the
// uuid returned by the original backtest_submit tool call.
//
//nolint:lll // tools/input shapes are inherently long; lll here is noise.
const getBacktestResultSchema = `{"type":"object","properties":{"backtest_id":{"type":"string","description":"UUID returned by the backtest submission tool"}},"required":["backtest_id"]}`

// Spec returns the get_backtest_result tool spec. One tool, one schema.
func Spec() llm.ToolSpec {
	return llm.ToolSpec{
		Name: toolName,
		//nolint:lll // tool description strings are inherently long
		Description: "Fetch the status and result of a previously-submitted backtest. Waits for terminal state, then returns the row plus its fills.",
		JSONSchema:  json.RawMessage(getBacktestResultSchema),
	}
}

// getBacktestInput is the typed input the LLM sends.
type getBacktestInput struct {
	BacktestID string `json:"backtest_id"`
}

// toolOutput is the JSON envelope returned to the agent. The chat layer
// re-serializes this into the assistant message body; the runner picks the
// fields it cares about (status, summary, fills).
type toolOutput struct {
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
	Fills        []bt.Fill         `json:"fills,omitempty"`
}

// Handler returns a closure-bound llm.ToolHandler that loads the backtest
// owned by userID, polls if non-terminal, and writes the result envelope.
// HTTP-layer code (which has the principal) owns the lifecycle and passes
// userID per turn — the tools package doesn't reach into httpapi.
func Handler(store bt.Store, userID string) llm.ToolHandler {
	return func(ctx context.Context, _ string, input json.RawMessage) (json.RawMessage, error) {
		var in getBacktestInput
		if err := json.Unmarshal(input, &in); err != nil {
			return nil, fmt.Errorf("get_backtest_result: parse input: %w", err)
		}
		if in.BacktestID == "" {
			return nil, errors.New("get_backtest_result: backtest_id is required")
		}

		row, err := store.Get(ctx, in.BacktestID)
		if err != nil {
			// ErrNotFound covers both "no such row" and (in the HTTP-layer
			// wrapper) "other user's row" — the tool deliberately collapses
			// them to a single 404-shape message so a probe can't enumerate
			// other users' backtest IDs.
			return nil, fmt.Errorf("get_backtest_result: %w", err)
		}
		if row.UserID != userID {
			return nil, fmt.Errorf("get_backtest_result: %w", bt.ErrNotFound)
		}

		// Cheap path: already terminal. Avoid touching the timer + DB twice.
		if !isTerminal(row.Status) {
			row, err = bt.PollForTerminal(ctx, store, in.BacktestID, PollBudget, PollInterval)
			if err != nil {
				return nil, fmt.Errorf("get_backtest_result: poll: %w", err)
			}
		}

		// Omit individual fills from the LLM-facing output. The Summary
		// already carries every stat the agent needs (TotalReturn, Sharpe,
		// MaxDrawdown, WinRate, TradeCount, EndEquity). Including raw fills
		// in a sweep causes context bloat: 100 trades ≈ 15KB JSON per
		// get_backtest_result call, and after 3-4 iterations the working
		// context exceeds the model's effective attention. The HTTP endpoint
		// (handleGetBacktest) still returns fills for the UI.

		out := toolOutput{
			ID:           row.ID,
			StrategyID:   row.StrategyID,
			VersionID:    row.VersionID,
			UserID:       row.UserID,
			Status:       row.Status,
			RequestedAt:  row.RequestedAt,
			StartedAt:    row.StartedAt,
			FinishedAt:   row.FinishedAt,
			ErrorMessage: row.ErrorMessage,
			WindowStart:  row.WindowStart,
			WindowEnd:    row.WindowEnd,
			Resolution:   row.Resolution,
			Params:       row.Params,
			Summary:      row.Summary,
			FillCount:    row.FillCount,
			ImageRef:     row.ImageRef,
		}
		return json.Marshal(out)
	}
}

// isTerminal mirrors internal/backtest.isTerminal. We can't import the
// unexported helper, so the tool does its own one-liner check. If the
// terminal set ever grows (e.g. StatusTimedOut), update both files.
func isTerminal(s bt.Status) bool {
	switch s {
	case bt.StatusSucceeded, bt.StatusFailed, bt.StatusCancelled:
		return true
	default:
		return false
	}
}
