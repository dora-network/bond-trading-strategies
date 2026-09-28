// Package strategies also implements the read_strategy_sources LLM tool.
// It is the read counterpart to generate_strategy: returns the saved
// source files + params_schema + module_name + summary of the named
// (strategy_id, revision_id) so the LLM can rewrite a legacy revision
// on the current framework without inventing the source from scratch.
// Ownership-gated via strategies.Store.
//
// Always available (independent of migrator state) so the LLM can
// fetch source for any revision it can name — useful for the rebuild
// flow, for "show me what version 3 looked like", and for diff-based
// improvement suggestions.
package strategies

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dora-network/bond-trading-strategies/internal/agent/llm"
	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies"
)

const readToolName = "read_strategy_sources"

// readSchema is the JSON schema for read_strategy_sources input.
//
//nolint:lll // tool input shapes are inherently long; lll here is noise.
const readSchema = `{"type":"object","required":["strategy_id","revision_id"],"properties":{"strategy_id":{"type":"string"},"revision_id":{"type":"string"}}}`

// ReadSpec returns the read_strategy_sources tool spec.
func ReadSpec() llm.ToolSpec {
	return llm.ToolSpec{
		Name: readToolName,
		//nolint:lll // tool descriptions are inherently long; lll here is noise.
		Description: "Read the saved source files of a strategy revision. Use this when you need to rebuild a strategy on a newer framework (e.g. run_backtest or deploy_strategy returned a stale-framework hint, or head.stale=true in get_strategy). Returns main.go, go.mod, and the strategy source files (*.go) plus the manifest. Pass the output to generate_strategy as the basis for the rebuild. Always ownership-gated: wrong-owner revision ids return not-found.",
		JSONSchema:  json.RawMessage(readSchema),
	}
}

// readInput is the typed input.
type readInput struct {
	StrategyID string `json:"strategy_id"`
	RevisionID string `json:"revision_id"`
}

// readOutput is the JSON envelope returned to the agent.
type readOutput struct {
	StrategyID   string            `json:"strategy_id"`
	RevisionID   string            `json:"revision_id"`
	Target       string            `json:"target"`
	ModuleName   string            `json:"module_name"`
	Summary      string            `json:"summary"`
	Files        map[string]string `json:"files"`
	ParamsSchema map[string]string `json:"params_schema"`
}

// ReadHandler returns the closure-bound tool handler. Same ownership
// pattern as Handler: store.GetStrategy verifies the strategy belongs
// to userID; wrong-owner IDs collapse to strategies.ErrNotFound.
func ReadHandler(store strategies.Store, userID string) llm.ToolHandler {
	return func(ctx context.Context, _ string, input json.RawMessage) (json.RawMessage, error) {
		var in readInput
		if err := json.Unmarshal(input, &in); err != nil {
			return nil, fmt.Errorf("read_strategy_sources: parse input: %w", err)
		}
		if in.StrategyID == "" {
			return nil, errors.New("read_strategy_sources: strategy_id is required")
		}
		if in.RevisionID == "" {
			return nil, errors.New("read_strategy_sources: revision_id is required")
		}
		if _, err := store.GetStrategy(ctx, userID, in.StrategyID); err != nil {
			return nil, fmt.Errorf("read_strategy_sources: %w", err)
		}

		v, err := store.GetVersion(ctx, in.StrategyID, strategies.Revision(in.RevisionID))
		if err != nil {
			return nil, fmt.Errorf("read_strategy_sources: %w", err)
		}
		return json.Marshal(readOutput{
			StrategyID:   in.StrategyID,
			RevisionID:   string(v.Revision),
			Target:       v.Target,
			ModuleName:   v.Meta.ModuleName,
			Summary:      v.Meta.Summary,
			Files:        v.Files,
			ParamsSchema: extractParamsSchema(v.Files),
		})
	}
}
