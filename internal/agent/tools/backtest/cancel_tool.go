// Package backtest cancel_tool.go adds the cancel_backtest LLM tool.
// It mirrors the existing HTTP POST /v1/strategies/{id}/backtests/{backtest_id}/cancel
// handler: the LLM hands us a backtest_id, the tool flips the row's status
// from queued|running to cancelled (atomic UPDATE via CancelIfRunning),
// then signals the orchestrator to unwind the runner via the per-job
// context. Idempotent -- a second call on a terminal row returns
// cancelled=false but no error.
package backtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	bt "github.com/dora-network/bond-trading-strategies/internal/agent/backtest"
	"github.com/dora-network/bond-trading-strategies/internal/agent/llm"
)

const cancelToolName = "cancel_backtest"

// cancelBacktestSchema declares the cancel_backtest input shape. The LLM
// sends the backtest_id of an in-flight (queued|running) backtest.
//
//nolint:lll // tools/input shapes are inherently long; lll here is noise.
const cancelBacktestSchema = `{"type":"object","properties":{"backtest_id":{"type":"string","description":"UUID of the backtest to cancel"}},"required":["backtest_id"]}`

// CancelSpec returns the cancel_backtest tool spec.
func CancelSpec() llm.ToolSpec {
	return llm.ToolSpec{
		Name: cancelToolName,
		//nolint:lll // tool description strings are inherently long
		Description: "Cancel an in-flight backtest. Idempotent: returns cancelled=true if the row was actually flipped from queued|running to cancelled; cancelled=false if the row was already terminal or absent. The runner goroutine + its docker/wazero process unwind via the per-job context. Errors only for storage failures -- a missing or already-terminal backtest is not an error.",
		JSONSchema:  json.RawMessage(cancelBacktestSchema),
	}
}

// cancelBacktestInput is the typed input the LLM sends.
type cancelBacktestInput struct {
	BacktestID string `json:"backtest_id"`
}

// cancelBacktestOutput is the JSON envelope returned to the agent.
type cancelBacktestOutput struct {
	BacktestID string `json:"backtest_id"`
	Cancelled  bool   `json:"cancelled"` // true if the row was flipped; false if already terminal or absent
}

// CancelHandler returns a closure-bound llm.ToolHandler that cancels a
// backtest owned by userID. Ownership is enforced by loading the row
// first and checking bt.UserID == userID -- same shape as the HTTP
// handler at internal/httpapi/job_handlers.go.
func CancelHandler(store bt.Store, orchestrator *bt.Orchestrator, userID string) llm.ToolHandler {
	return func(ctx context.Context, _ string, input json.RawMessage) (json.RawMessage, error) {
		var in cancelBacktestInput
		if err := json.Unmarshal(input, &in); err != nil {
			return nil, fmt.Errorf("cancel_backtest: parse input: %w", err)
		}
		if in.BacktestID == "" {
			return nil, errors.New("cancel_backtest: backtest_id is required")
		}

		// Load the row first so we can enforce ownership. The HTTP
		// handler does the same; collapsing "missing" and "not yours"
		// to a single ErrNotFound prevents the tool from being used to
		// enumerate other users' backtest IDs.
		row, err := store.Get(ctx, in.BacktestID)
		if err != nil {
			return nil, fmt.Errorf("cancel_backtest: %w", err)
		}
		if row.UserID != userID {
			return nil, fmt.Errorf("cancel_backtest: %w", bt.ErrNotFound)
		}

		// DB flip is the source of truth. Returns wasRunning=true if
		// the row was queued|running; false if already terminal.
		wasRunning, err := store.CancelIfRunning(ctx, in.BacktestID)
		if err != nil {
			return nil, fmt.Errorf("cancel_backtest: %w", err)
		}
		// Best-effort: signal the orchestrator so the runner unwinds
		// via the per-job context. No-op if the job already finished
		// (wasRunning was false); matches the HTTP handler.
		_ = orchestrator.Cancel(in.BacktestID)

		out := cancelBacktestOutput{BacktestID: in.BacktestID, Cancelled: wasRunning}
		return json.Marshal(out)
	}
}
