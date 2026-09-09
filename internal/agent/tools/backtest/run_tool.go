// Package backtest run_tool.go adds the run_backtest LLM tool. It mirrors
// the existing HTTP POST /v1/strategies/{id}/versions/{rev}/backtest handler:
// the LLM hands the model a strategy + revision + order book + window, the
// tool resolves the version's artifact (wasm_ref + manifest_hash for
// go-wasm), then asks the orchestrator to queue the job. The runner returns
// the new backtest_id and the tool hands it back to the agent so the next
// turn can call get_backtest_result.
package backtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	bt "github.com/dora-network/bond-trading-strategies/internal/agent/backtest"
	"github.com/dora-network/bond-trading-strategies/internal/agent/llm"
	"github.com/dora-network/bond-trading-strategies/internal/agent/migration"
	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies"
)

const runToolName = "run_backtest"

// runBacktestSchema declares the run_backtest input shape. The LLM sends
// the strategy it wants to backtest, the order book, the window, and
// optional params.
//
//nolint:lll // tools/input shapes are inherently long; lll here is noise.
const runBacktestSchema = `{"type":"object","properties":{"strategy_id":{"type":"string","description":"UUID of the strategy to backtest"},"revision_id":{"type":"string","description":"UUID of the strategy version (the built artifact)"},"order_book_id":{"type":"string","description":"Dora order book id to replay against"},"start":{"type":"string","format":"date-time","description":"Window start (RFC3339)"},"end":{"type":"string","format":"date-time","description":"Window end (RFC3339)"},"resolution":{"type":"string","description":"Candle resolution: 1m, 5m, 15m, 1h, 4h, 1d"},"params":{"type":"object","description":"Optional strategy params (key/value strings)","additionalProperties":{"type":"string"}},"warmup_candles":{"type":"integer","description":"Preamble warmup window (resolution-spaced candles) fetched before the window start so OnPreamble sees history. 0 when omitted"}},"required":["strategy_id","revision_id","order_book_id","start","end","resolution"]}`

// RunSpec returns the run_backtest tool spec. One tool, one schema.
func RunSpec() llm.ToolSpec {
	return llm.ToolSpec{
		Name: runToolName,
		Description: "Queue a backtest for a strategy version. " +
			"Returns the backtest_id; call get_backtest_result to read the outcome. " +
			"The server runs at most one backtest per user at a time; if a backtest " +
			"is already running for this user, the request is rejected with HTTP 429. " +
			"When the user wants to compare multiple windows, params, or order books " +
			"Do not fire several run_backtest calls in parallel; only one will be " +
			"accepted, the rest will 409/429 and the user sees noise. Sequence " +
			"them sequentially: wait for each prior job's terminal state before " +
			"queueing the next.",
		JSONSchema: json.RawMessage(runBacktestSchema),
	}
}

// runBacktestInput is the typed input the LLM sends.
type runBacktestInput struct {
	StrategyID    string            `json:"strategy_id"`
	RevisionID    string            `json:"revision_id"`
	OrderBookID   string            `json:"order_book_id"`
	Start         time.Time         `json:"start"`
	End           time.Time         `json:"end"`
	Resolution    string            `json:"resolution"`
	Params        map[string]string `json:"params"`
	WarmupCandles int               `json:"warmup_candles,omitempty"` // preamble fetch window; 0 = no warmup
}

// runBacktestOutput is the JSON envelope returned to the agent. One
// field: the new backtest_id. The agent is expected to follow up with
// get_backtest_result(backtest_id=...) to read the outcome.
type runBacktestOutput struct {
	BacktestID string `json:"backtest_id"`
}

// wasmBacktestStarter is the subset of *bt.WasmStarter used by the tool.
// It lets tests inject a fake without spinning a real wazero registry.
type wasmBacktestStarter interface {
	Start(ctx context.Context, b *bt.Backtest, wasmRef, manifestHash, doraAPIKey string) error
}

// RunHandler returns a closure-bound llm.ToolHandler that resolves the
// strategy + version owned by userID, then asks the orchestrator to Submit
// the job (wasm-only). The Dora API key is the per-request one resolved by
// the middleware (DoraAPIKeyFromCtx); it threads into the wasm starter so
// the strategy can use the user's own credentials when replaying historic
// data. The wasmStarter / btStore parameters are retained for signature
// stability with the tool factory in main.go; they are no longer read.
//
// The strategies.Store / *bt.Orchestrator seams are intentional: the
// existing handler already coerces unknown-id / wrong-owner lookups to
// not-found, so the tool gets the same ownership gate for free.
func RunHandler(btOrch *bt.Orchestrator, versions strategies.Store,
	wasmStarter wasmBacktestStarter, btStore bt.Store,
	userID, doraAPIKey string, migrator *migration.Migrator,
) llm.ToolHandler {
	return func(ctx context.Context, _ string, input json.RawMessage) (json.RawMessage, error) {
		var in runBacktestInput
		if err := json.Unmarshal(input, &in); err != nil {
			return nil, fmt.Errorf("run_backtest: parse input: %w", err)
		}
		// Cheap shortcut: catch the full set of parse-shape errors before
		// any DB hits so the LLM gets a single coherent error stream.
		switch {
		case in.StrategyID == "":
			return nil, errors.New("run_backtest: strategy_id is required")
		case in.RevisionID == "":
			return nil, errors.New("run_backtest: revision_id is required")
		case in.OrderBookID == "":
			return nil, errors.New("run_backtest: order_book_id is required")
		case in.Resolution == "":
			return nil, errors.New("run_backtest: resolution is required")
		}

		// Ownership gate: the strategy must belong to userID. The
		// HTTP-layer wrapper collapses missing + wrong-owner to
		// ErrNotFound so probes can't enumerate other users' IDs.
		if _, err := versions.GetStrategy(ctx, userID, in.StrategyID); err != nil {
			if errors.Is(err, strategies.ErrNotFound) {
				//nolint:lll // error messages can be long; the LLM needs the recovery hint
				return nil, llm.NewRecoveryError(
					fmt.Sprintf("run_backtest: strategy %q not found for this user — your in-memory strategy_id may be stale; call get_strategy to look up the current strategy_id, then retry", in.StrategyID),
					"get_strategy",
				)
			}
			return nil, fmt.Errorf("run_backtest: %w", err)
		}
		v, err := versions.GetVersion(ctx, in.StrategyID, strategies.Revision(in.RevisionID))
		if err != nil {
			return nil, fmt.Errorf("run_backtest: %w", err)
		}

		// Pre-flight: legacy rows (target != go-wasm or no compiled
		// artifact) cannot run on the current runtime. Steer the LLM to
		// generate_strategy instead of surfacing a plain unsupported-target
		// error it would just retry against.
		if migrator.NeedsMigration(v) {
			stale := &migration.ErrStaleFramework{
				StrategyID:        in.StrategyID,
				CurrentRevisionID: in.RevisionID,
				Target:            v.Target,
			}
			return nil, llm.NewRecoveryHintError(stale.Error(), "generate_strategy", stale)
		}

		req := &bt.Request{
			Start:         in.Start,
			End:           in.End,
			Resolution:    in.Resolution,
			OrderBookID:   in.OrderBookID,
			Params:        in.Params,
			WarmupCandles: in.WarmupCandles,
		}

		switch v.Target {
		case "go-wasm":
			// ErrVersionNotBuilt (409) and ErrWasmUnavailable (503)
			// now surface from Orchestrator.Submit itself; the LLM
			// tool no longer needs to duplicate those checks.
			if btOrch == nil {
				return nil, errors.New("run_backtest: wasm backtest unavailable")
			}
			id, err := btOrch.Submit(
				ctx, userID, in.StrategyID, in.RevisionID,
				v.WasmRef, v.ManifestHash, doraAPIKey, req,
			)
			if err != nil {
				return nil, fmt.Errorf("run_backtest: %w", err)
			}
			return json.Marshal(runBacktestOutput{BacktestID: id})
		default:
			return nil, fmt.Errorf("run_backtest: unsupported version target %q", v.Target)
		}
	}
}
