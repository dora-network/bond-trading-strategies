package strategies_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies"
	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies/servertest"
	tool "github.com/dora-network/bond-trading-strategies/internal/agent/tools/strategies"
	"github.com/stretchr/testify/require"
)

// TestReadStrategySources_ReturnsFiles: the tool hands the LLM the
// saved source files + metadata of a revision so it can rebuild a
// legacy strategy without inventing the source from scratch.
func TestReadStrategySources_ReturnsFiles(t *testing.T) {
	pool := servertest.StartPostgres(t)
	store := seedStrategy(t, pool)
	rev := insertWasmVersion(t, pool, "wasm-sha", "manifest-sha")

	handle := tool.ReadHandler(store, ownerID)
	out, err := handle(t.Context(), "read_strategy_sources", json.RawMessage(
		`{"strategy_id":"`+strategyID+`","revision_id":"`+string(rev)+`"}`,
	))
	require.NoError(t, err)
	var resp struct {
		StrategyID   string            `json:"strategy_id"`
		RevisionID   string            `json:"revision_id"`
		Target       string            `json:"target"`
		ModuleName   string            `json:"module_name"`
		Summary      string            `json:"summary"`
		Files        map[string]string `json:"files"`
		ParamsSchema map[string]string `json:"params_schema"`
	}
	require.NoError(t, json.Unmarshal(out, &resp))
	require.Equal(t, strategyID, resp.StrategyID)
	require.Equal(t, string(rev), resp.RevisionID)
	require.Equal(t, "go-wasm", resp.Target)
	require.Equal(t, "mod-wasm", resp.ModuleName)
	require.Equal(t, "wasm summary", resp.Summary)
}

// TestReadStrategySources_WrongOwner_NotFound: a strategy owned by
// user A is not-found via user B's handler — same ErrNotFound collapse
// as the other read tools so probes can't enumerate other users' IDs.
func TestReadStrategySources_WrongOwner_NotFound(t *testing.T) {
	pool := servertest.StartPostgres(t)
	store := seedStrategy(t, pool)
	rev := insertWasmVersion(t, pool, "wasm-sha", "manifest-sha")

	handle := tool.ReadHandler(store, otherID)
	_, err := handle(t.Context(), "read_strategy_sources", json.RawMessage(
		`{"strategy_id":"`+strategyID+`","revision_id":"`+string(rev)+`"}`,
	))
	require.Error(t, err)
	require.True(t, errors.Is(err, strategies.ErrNotFound))
}

// TestReadStrategySources_MissingRevision_NotFound: an unknown
// revision id surfaces ErrNotFound, not a 5xx or a panic.
func TestReadStrategySources_MissingRevision_NotFound(t *testing.T) {
	pool := servertest.StartPostgres(t)
	store := seedStrategy(t, pool)
	_ = insertWasmVersion(t, pool, "wasm-sha-2", "manifest-sha-2")

	handle := tool.ReadHandler(store, ownerID)
	_, err := handle(t.Context(), "read_strategy_sources", json.RawMessage(
		`{"strategy_id":"`+strategyID+`","revision_id":"`+uuid.NewString()+`"}`,
	))
	require.Error(t, err)
	require.True(t, errors.Is(err, strategies.ErrNotFound))
}
