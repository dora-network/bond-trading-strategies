package strategies_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies"
	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies/servertest"
	tool "github.com/dora-network/bond-trading-strategies/internal/agent/tools/strategies"
)

const (
	ownerID    = "00000000-0000-0000-0000-0000000000e1"
	otherID    = "00000000-0000-0000-0000-0000000000e2"
	strategyID = "00000000-0000-0000-0000-000000000500"
)

func seedUser(t *testing.T, pool *pgxpool.Pool, userID string) {
	t.Helper()
	_, err := pool.Exec(t.Context(), `
		insert into agent.users (dora_user_id, tenant_id, roles)
		values ($1, 'test', ARRAY['TRADER'])
		on conflict (dora_user_id) do nothing`,
		userID)
	require.NoError(t, err)
}

// seedStrategyWith seeds a strategy owned by the given user. The
// helper is parameterized so the wrong-owner test can probe with
// a different user.
func seedStrategyWith(t *testing.T, pool *pgxpool.Pool, ownerOfStrategy string) *strategies.PgStore {
	t.Helper()
	seedUser(t, pool, ownerOfStrategy)
	// Fixed strategyID: clear any residue from a previous run against the
	// shared DATABASE_URL fixture (agenttest reuses the schema).
	_, err := pool.Exec(t.Context(), `
		delete from agent.strategy_versions where strategy_id=$1`, strategyID)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `
		delete from agent.strategies where id=$1`, strategyID)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `
		insert into agent.strategies (id, dora_user_id, name, source_session_id)
		values ($1, $2, 'alpha', $3)`,
		strategyID, ownerOfStrategy, uuid.NewString())
	require.NoError(t, err)
	return strategies.NewPgStore(pool)
}

// seedStrategy is the happy-path helper: a strategy owned by
// ownerID with no parent.
func seedStrategy(t *testing.T, pool *pgxpool.Pool) *strategies.PgStore {
	return seedStrategyWith(t, pool, ownerID)
}

// insertVersion writes a strategy_versions row with the given
// image_ref and advances the strategies.head_revision pointer so
// Head() returns this revision. parentRevision is the empty string
// for the first call. Returns the new revision id.
func insertVersion(t *testing.T, pool *pgxpool.Pool, imageRef string, parentRevision string) strategies.Revision {
	t.Helper()
	rev := strategies.Revision(uuid.NewString())
	var parent any
	if parentRevision != "" {
		parent = parentRevision
	}
	_, err := pool.Exec(t.Context(), `
		insert into agent.strategy_versions (revision, strategy_id, provider, model,
		    module_name, summary, rationale, validation, image_ref, parent_revision)
		values ($1, $2, 'openai', 'gpt', 'mod', 's', 'r', '{}'::jsonb, $3, $4)`,
		rev, strategyID, imageRef, parent)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(),
		`update agent.strategies set head_revision=$1 where id=$2`,
		rev, strategyID)
	require.NoError(t, err)
	return rev
}

// TestGetStrategy_Spec_NameAndSchema pins the tool surface: the LLM
// dispatches by name, the schema declares strategy_id as optional
// (the no-arg form returns the user's most recent head).
func TestGetStrategy_Spec_NameAndSchema(t *testing.T) {
	spec := tool.Spec()
	require.Equal(t, "get_strategy", spec.Name)
	require.NotEmpty(t, spec.Description)

	var schema map[string]any
	require.NoError(t, json.Unmarshal(spec.JSONSchema, &schema))
	props, ok := schema["properties"].(map[string]any)
	require.True(t, ok, "schema must have properties")
	_, hasStrategyID := props["strategy_id"]
	require.True(t, hasStrategyID, "schema must declare strategy_id")
	// strategy_id is OPTIONAL: the no-arg form returns the user's
	// most recent head. The schema must NOT list it in required.
	if required, ok := schema["required"].([]any); ok {
		require.NotContains(t, required, "strategy_id",
			"strategy_id must be optional (no-arg form returns the latest)")
	}
}

// TestGetStrategy_HappyPath: the head revision's image_ref + metadata
// plus the recent version list flow back to the LLM so it can call
// run_backtest without regenerating.
func TestGetStrategy_HappyPath(t *testing.T) {
	pool := servertest.StartPostgres(t)
	store := seedStrategy(t, pool)
	older := insertVersion(t, pool, "local/sha:old", "")
	head := insertVersion(t, pool, "local/sha:head", string(older))

	handle := tool.Handler(store, ownerID)
	out, err := handle(t.Context(), "get_strategy", json.RawMessage(`{"strategy_id":"`+strategyID+`"}`))
	require.NoError(t, err)

	var resp struct {
		StrategyID string `json:"strategy_id"`
		Head       struct {
			Revision   string    `json:"revision"`
			ImageRef   string    `json:"image_ref"`
			ModuleName string    `json:"module_name"`
			Summary    string    `json:"summary"`
			CreatedAt  time.Time `json:"created_at"`
		} `json:"head"`
		Versions []struct {
			Revision string `json:"revision"`
			ImageRef string `json:"image_ref"`
		} `json:"versions"`
	}
	require.NoError(t, json.Unmarshal(out, &resp))
	require.Equal(t, string(head), resp.Head.Revision)
	require.Equal(t, "local/sha:head", resp.Head.ImageRef)
	require.Equal(t, "mod", resp.Head.ModuleName)
	require.Equal(t, "s", resp.Head.Summary)
	require.False(t, resp.Head.CreatedAt.IsZero())
	// Versions are newest first; the head and the previous one.
	require.Len(t, resp.Versions, 2)
	require.Equal(t, string(head), resp.Versions[0].Revision)
	require.Equal(t, string(older), resp.Versions[1].Revision)
}

// TestGetStrategy_NoHeadYet: a strategy with no versions returns
// head=nil and an empty versions list. The LLM should regenerate.
func TestGetStrategy_NoHeadYet(t *testing.T) {
	pool := servertest.StartPostgres(t)
	store := seedStrategy(t, pool)
	handle := tool.Handler(store, ownerID)
	out, err := handle(t.Context(), "get_strategy", json.RawMessage(`{"strategy_id":"`+strategyID+`"}`))
	require.NoError(t, err)

	var resp struct {
		Head     map[string]any `json:"head"`
		Versions []any          `json:"versions"`
	}
	require.NoError(t, json.Unmarshal(out, &resp))
	require.Nil(t, resp.Head)
	require.Empty(t, resp.Versions)
}

// TestGetStrategy_WrongOwner: a strategy owned by user A is not-found
// when called with user B's handler. Same ErrNotFound collapse as the
// other read tools so a probe cannot enumerate other users' IDs.
func TestGetStrategy_WrongOwner(t *testing.T) {
	pool := servertest.StartPostgres(t)
	seedStrategyWith(t, pool, ownerID)
	insertVersion(t, pool, "local/sha:head", "")
	store := strategies.NewPgStore(pool)
	handle := tool.Handler(store, otherID)
	_, err := handle(t.Context(), "get_strategy", json.RawMessage(`{"strategy_id":"`+strategyID+`"}`))
	require.Error(t, err)
	require.ErrorIs(t, err, strategies.ErrNotFound)
}

// TestGetStrategy_NotFound: a strategy that doesn't exist surfaces
// ErrNotFound, not a 5xx or a panic.
func TestGetStrategy_NotFound(t *testing.T) {
	pool := servertest.StartPostgres(t)
	store := strategies.NewPgStore(pool)
	handle := tool.Handler(store, ownerID)
	_, err := handle(t.Context(), "get_strategy", json.RawMessage(`{"strategy_id":"`+uuid.NewString()+`"}`))
	require.Error(t, err)
	require.ErrorIs(t, err, strategies.ErrNotFound)
}

// TestGetStrategy_EmptyID_NoArgReturnsMostRecent: the LLM may not know
// the strategy_id; the tool returns the user's most recent head so the
// LLM can chain into run_backtest without a fresh lookup round-trip.
func TestGetStrategy_EmptyID_NoArgReturnsMostRecent(t *testing.T) {
	pool := servertest.StartPostgres(t)
	store := seedStrategy(t, pool)
	older := insertVersion(t, pool, "local/sha:old", "")
	head := insertVersion(t, pool, "local/sha:head", string(older))

	handle := tool.Handler(store, ownerID)
	out, err := handle(t.Context(), "get_strategy", json.RawMessage(`{}`))
	require.NoError(t, err)

	var resp struct {
		StrategyID string `json:"strategy_id"`
		Head       struct {
			Revision string `json:"revision"`
		} `json:"head"`
	}
	require.NoError(t, json.Unmarshal(out, &resp))
	require.Equal(t, strategyID, resp.StrategyID)
	require.Equal(t, string(head), resp.Head.Revision)
}

// TestGetStrategy_NoStrategies_ErrorsCleanly: a no-arg call from a
// user with no strategies surfaces a recoverable error so the LLM can
// react (typically by calling generate_strategy).
func TestGetStrategy_NoStrategies_ErrorsCleanly(t *testing.T) {
	pool := servertest.StartPostgres(t)
	store := strategies.NewPgStore(pool)
	// Distinct owner: the shared DATABASE_URL fixture keeps rows from
	// sibling tests, so ownerID may already own strategies here.
	handle := tool.Handler(store, "00000000-0000-0000-0000-00000000f00d")
	_, err := handle(t.Context(), "get_strategy", json.RawMessage(`{}`))
	require.Error(t, err)
}

// TestGetStrategy_MalformedInput: non-JSON must surface a parse error,
// not a panic.
func TestGetStrategy_MalformedInput(t *testing.T) {
	pool := servertest.StartPostgres(t)
	store := strategies.NewPgStore(pool)
	handle := tool.Handler(store, ownerID)
	_, err := handle(t.Context(), "get_strategy", json.RawMessage(`not json`))
	require.Error(t, err)
}

// insertWasmVersion writes a strategy_versions row for a go-wasm
// artifact and advances the strategies.head_revision pointer.
func insertWasmVersion(t *testing.T, pool *pgxpool.Pool, wasmRef, manifestHash string) strategies.Revision {
	t.Helper()
	rev := strategies.Revision(uuid.NewString())
	_, err := pool.Exec(t.Context(), `
		insert into agent.strategy_versions (revision, strategy_id, provider, model,
		    module_name, summary, rationale, validation, target, wasm_ref, manifest_hash)
		values ($1, $2, 'openai', 'gpt', 'mod-wasm', 'wasm summary', 'r', '{}'::jsonb, 'go-wasm', $3, $4)`,
		rev, strategyID, wasmRef, manifestHash)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(),
		`update agent.strategies set head_revision=$1 where id=$2`,
		rev, strategyID)
	require.NoError(t, err)
	return rev
}

// TestGetStrategy_WasmHeadIncludesWasmRef: the head revision for a
// go-wasm strategy exposes wasm_ref + manifest_hash so the LLM can
// call run_backtest without regenerating.
func TestGetStrategy_WasmHeadIncludesWasmRef(t *testing.T) {
	pool := servertest.StartPostgres(t)
	store := seedStrategy(t, pool)
	rev := insertWasmVersion(t, pool, "wasm-sha", "manifest-sha")

	handle := tool.Handler(store, ownerID)
	out, err := handle(t.Context(), "get_strategy", json.RawMessage(`{"strategy_id":"`+strategyID+`"}`))
	require.NoError(t, err)

	var resp struct {
		StrategyID string `json:"strategy_id"`
		Head       struct {
			Revision     string `json:"revision"`
			Target       string `json:"target"`
			ImageRef     string `json:"image_ref"`
			WasmRef      string `json:"wasm_ref"`
			ManifestHash string `json:"manifest_hash"`
		} `json:"head"`
	}
	require.NoError(t, json.Unmarshal(out, &resp))
	require.Equal(t, string(rev), resp.Head.Revision)
	require.Equal(t, "go-wasm", resp.Head.Target)
	require.Equal(t, "wasm-sha", resp.Head.WasmRef)
	require.Equal(t, "manifest-sha", resp.Head.ManifestHash)
	require.Empty(t, resp.Head.ImageRef)
}

// TestGetStrategy_ListVersionsLimit: the tool returns at most 5
// versions so the LLM's context window doesn't blow up on a long
// history.
func TestGetStrategy_ListVersionsLimit(t *testing.T) {
	pool := servertest.StartPostgres(t)
	store := seedStrategy(t, pool)
	// Insert 7 versions; the head advance keeps only the latest.
	for range 7 {
		_ = insertVersion(t, pool, "local/sha:v", "")
	}
	handle := tool.Handler(store, ownerID)
	out, err := handle(t.Context(), "get_strategy", json.RawMessage(`{"strategy_id":"`+strategyID+`"}`))
	require.NoError(t, err)

	var resp struct {
		Versions []struct {
			Revision string `json:"revision"`
		} `json:"versions"`
	}
	require.NoError(t, json.Unmarshal(out, &resp))
	require.Len(t, resp.Versions, 5, "tool should cap the version list at 5")
}

// TestGetStrategy_StaleField_TrueForGoDocker: the head's stale flag is
// true for a legacy go-docker revision so the LLM knows to rebuild via
// generate_strategy before run_backtest.
func TestGetStrategy_StaleField_TrueForGoDocker(t *testing.T) {
	pool := servertest.StartPostgres(t)
	store := seedStrategy(t, pool)
	_ = insertVersion(t, pool, "legacy-img", "") // target defaults to 'go-docker', wasm_ref=''

	handle := tool.Handler(store, ownerID)
	out, err := handle(t.Context(), "get_strategy", json.RawMessage(`{"strategy_id":"`+strategyID+`"}`))
	require.NoError(t, err)

	var resp struct {
		Head struct {
			Revision string `json:"revision"`
			Stale    bool   `json:"stale"`
		} `json:"head"`
	}
	require.NoError(t, json.Unmarshal(out, &resp))
	require.True(t, resp.Head.Stale, "head.stale should be true for target=go-docker")
}

// TestGetStrategy_StaleField_FalseForGoWasm: a current-framework
// go-wasm head with a wasm artifact is not stale.
func TestGetStrategy_StaleField_FalseForGoWasm(t *testing.T) {
	pool := servertest.StartPostgres(t)
	store := seedStrategy(t, pool)
	_ = insertWasmVersion(t, pool, "wasm-sha", "manifest-sha")

	handle := tool.Handler(store, ownerID)
	out, err := handle(t.Context(), "get_strategy", json.RawMessage(`{"strategy_id":"`+strategyID+`"}`))
	require.NoError(t, err)

	var resp struct {
		Head struct {
			Stale bool `json:"stale"`
		} `json:"head"`
	}
	require.NoError(t, json.Unmarshal(out, &resp))
	require.False(t, resp.Head.Stale, "head.stale should be false for target=go-wasm with WasmRef set")
}
