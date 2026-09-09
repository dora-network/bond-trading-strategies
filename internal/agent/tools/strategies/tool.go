// Package strategies implements the get_strategy read-side LLM tool. The
// LLM hands the tool a strategy_id; the handler returns the head
// revision's image_ref plus the recent version list so the LLM can
// call run_backtest(strategy_id, revision_id, ...) without
// regenerating the strategy. The head's image_ref is empty until
// the capture pipeline has built a local-daemon image for it
// (Phase 2); the LLM is expected to regenerate in that case.
package strategies

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/dora-network/bond-trading-strategies/internal/agent/llm"
	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies"
)

const toolName = "get_strategy"

// maxVersions caps the recent-version list included in the response
// so the LLM's context window doesn't blow up on long histories.
const maxVersions = 5

// getStrategySchema declares strategy_id as OPTIONAL. When omitted,
// the handler returns the user's most recent head — the recovery
// path the LLM uses when its cached strategy_id is stale.
//
//nolint:lll // tool input shapes are inherently long; lll here is noise.
const getStrategySchema = `{"type":"object","properties":{"strategy_id":{"type":"string","description":"UUID of the strategy. OPTIONAL: omit to get the user's most recent strategy automatically."}}}`

// Spec returns the get_strategy tool spec. One tool, one schema.
func Spec() llm.ToolSpec {
	return llm.ToolSpec{
		Name: toolName,
		//nolint:lll // tool descriptions are inherently long; lll here is noise.
		Description: "**Call this before generating or running a backtest.** Call with NO arguments to get the user's latest strategy and head revision. Returns `head.stale` (true when the head was built on an older framework — rebuild via `generate_strategy` before `run_backtest`). If `head` is null or `head.stale=true`, call `generate_strategy`.",
		JSONSchema:  json.RawMessage(getStrategySchema),
	}
}

// getStrategyInput is the typed input the LLM sends.
type getStrategyInput struct {
	StrategyID string `json:"strategy_id"`
}

// versionRow is one entry in the recent-versions list. Keeps the
// wire payload small: the model never needs rationale or
// validation here, just enough to pick a non-head revision if it
// wants a different one.
type versionRow struct {
	Revision     string `json:"revision"`
	ImageRef     string `json:"image_ref,omitempty"`
	Target       string `json:"target"`
	WasmRef      string `json:"wasm_ref,omitempty"`
	ManifestHash string `json:"manifest_hash,omitempty"`
	ModuleName   string `json:"module_name"`
	Summary      string `json:"summary"`
	Stale        bool   `json:"stale,omitempty"`
	CreatedAt    string `json:"created_at"`
}

// headBlock is the head revision row. Marshalled as nil when the
// strategy has no captures yet so the LLM sees the empty case
// distinctly (not a missing key).
type headBlock struct {
	Revision     string            `json:"revision"`
	ImageRef     string            `json:"image_ref,omitempty"`
	Target       string            `json:"target"`
	WasmRef      string            `json:"wasm_ref,omitempty"`
	ManifestHash string            `json:"manifest_hash,omitempty"`
	ModuleName   string            `json:"module_name"`
	Summary      string            `json:"summary"`
	ParamsSchema map[string]string `json:"params_schema,omitempty"`
	Stale        bool              `json:"stale,omitempty"`
	CreatedAt    string            `json:"created_at"`
}

// response is the JSON envelope returned to the agent. The chat
// layer re-serializes this into the assistant message body.
type response struct {
	StrategyID string       `json:"strategy_id"`
	Head       *headBlock   `json:"head"`
	Versions   []versionRow `json:"versions"`
}

// extractParamsSchema reads params_schema from manifest.json in the
// version files. Returns nil when no manifest exists or it cannot be
// parsed — the LLM sees an empty params_schema and can ask the user.
func extractParamsSchema(files map[string]string) map[string]string {
	raw, ok := files["manifest.json"]
	if !ok {
		return nil
	}
	var mf manifestSchema
	if err := json.Unmarshal([]byte(raw), &mf); err != nil {
		slog.Warn("get_strategy: failed to parse manifest.json", "err", err)
		return nil
	}
	return mf.ParamsSchema
}

// manifestSchema is the subset of manifest.json the tool needs.
type manifestSchema struct {
	ParamsSchema map[string]string `json:"params_schema"`
}

// Handler returns a closure-bound llm.ToolHandler that loads the
// strategy owned by userID, its head revision's image_ref, and the
// recent version list. The HTTP-layer code (which has the principal)
// owns the lifecycle and passes userID per turn — the tools package
// doesn't reach into httpapi.
func Handler(store strategies.Store, userID string) llm.ToolHandler {
	return func(ctx context.Context, _ string, input json.RawMessage) (json.RawMessage, error) {
		var in getStrategyInput
		if err := json.Unmarshal(input, &in); err != nil {
			return nil, fmt.Errorf("get_strategy: parse input: %w", err)
		}
		// No-arg form: when the LLM doesn't know the strategy_id,
		// return the user's most recent head. This is the recovery
		// path for "run_backtest said strategy not found" — the LLM
		// can call get_strategy() with no args to refresh.
		if in.StrategyID == "" {
			strategies, _, err := store.ListStrategies(ctx, userID, strategies.Page{Limit: 1})
			if err != nil {
				return nil, fmt.Errorf("get_strategy: list: %w", err)
			}
			if len(strategies) == 0 {
				return nil, errors.New("get_strategy: no strategies exist for this user; call generate_strategy first")
			}
			in.StrategyID = strategies[0].ID
		}

		// Ownership gate: the strategy must belong to userID. The
		// HTTP-layer wrapper collapses missing + wrong-owner to
		// ErrNotFound so probes can't enumerate other users' IDs.
		if _, err := store.GetStrategy(ctx, userID, in.StrategyID); err != nil {
			return nil, fmt.Errorf("get_strategy: %w", err)
		}

		resp := response{StrategyID: in.StrategyID, Versions: []versionRow{}}

		headRev, err := store.Head(ctx, in.StrategyID)
		if err != nil && !errors.Is(err, strategies.ErrNotFound) {
			return nil, fmt.Errorf("get_strategy: head: %w", err)
		}
		if headRev != "" {
			head, err := store.GetVersion(ctx, in.StrategyID, headRev)
			if err != nil {
				return nil, fmt.Errorf("get_strategy: head version: %w", err)
			}
			resp.Head = &headBlock{
				Revision:     string(head.Revision),
				ImageRef:     head.ImageRef,
				Target:       head.Target,
				WasmRef:      head.WasmRef,
				ManifestHash: head.ManifestHash,
				ModuleName:   head.Meta.ModuleName,
				Summary:      head.Meta.Summary,
				ParamsSchema: extractParamsSchema(head.Files),
				Stale:        head.Target != "go-wasm" || head.WasmRef == "",
				CreatedAt:    head.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
			}
		}

		// Cap the version list at maxVersions. ListVersions returns
		// newest-first; the LLM reads top-down for the latest.
		items, _, err := store.ListVersions(ctx, in.StrategyID, strategies.Page{Limit: maxVersions})
		if err != nil {
			return nil, fmt.Errorf("get_strategy: list versions: %w", err)
		}
		for _, v := range items {
			resp.Versions = append(resp.Versions, versionRow{
				Revision:     string(v.Revision),
				ImageRef:     v.ImageRef,
				Target:       v.Target,
				WasmRef:      v.WasmRef,
				ManifestHash: v.ManifestHash,
				ModuleName:   v.ModuleName,
				Summary:      v.Summary,
				Stale:        v.Target != "go-wasm" || v.WasmRef == "",
				CreatedAt:    v.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
			})
		}
		return json.Marshal(resp)
	}
}
