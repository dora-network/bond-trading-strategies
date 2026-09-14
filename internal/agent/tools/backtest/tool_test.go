package backtest_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	bt "github.com/dora-network/bond-trading-strategies/internal/agent/backtest"
	"github.com/dora-network/bond-trading-strategies/internal/agent/llm"
	"github.com/dora-network/bond-trading-strategies/internal/agent/migration"
	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies"
	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies/servertest"
	tool "github.com/dora-network/bond-trading-strategies/internal/agent/tools/backtest"
)

const (
	ownerID    = "00000000-0000-0000-0000-0000000000e1"
	strangerID = "00000000-0000-0000-0000-0000000000e2"
)

// seedFixtures provisions a Postgres, two users, a strategy, and a strategy
// version — the FK chain the backtests table hangs off. Returns the pool
// plus the strategy + version IDs callers can hang backtest rows off.
func seedFixtures(t *testing.T) (*pgxpool.Pool, string, string) {
	t.Helper()
	pool := servertest.StartPostgres(t)
	seedUser(t, pool, ownerID)
	seedUser(t, pool, strangerID)

	strategyID := uuid.NewString()
	versionID := uuid.NewString()
	ctx := t.Context()
	_, err := pool.Exec(ctx, `
		insert into agent.strategies (id, dora_user_id, name, source_session_id)
		values ($1, $2, $3, $4)`,
		strategyID, ownerID, "alpha", uuid.NewString())
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		insert into agent.strategy_versions (revision, strategy_id, provider, model,
		    module_name, summary, rationale, validation)
		values ($1, $2, 'openai', 'gpt', 'alpha', 's', 'r', '{}'::jsonb)`,
		versionID, strategyID)
	require.NoError(t, err)
	return pool, strategyID, versionID
}

func seedUser(t *testing.T, pool *pgxpool.Pool, userID string) {
	t.Helper()
	_, err := pool.Exec(t.Context(), `
		insert into agent.users (dora_user_id, tenant_id, roles)
		values ($1, 'test', ARRAY['TRADER'])
		on conflict (dora_user_id) do nothing`,
		userID)
	require.NoError(t, err)
}

// insertBacktest writes a backtest row directly so the test can pick the
// status (Create forces StatusQueued regardless of what we set). Returns
// the row ID.
func insertBacktest(t *testing.T, pool *pgxpool.Pool, userID, strategyID, versionID string, status bt.Status) string {
	t.Helper()
	id := uuid.NewString()
	now := time.Now().UTC().Truncate(time.Second)
	_, err := pool.Exec(t.Context(), `
		insert into agent.backtests (id, strategy_id, version_id, user_id, status,
		    requested_at, window_start, window_end, resolution, params, image_ref)
		values ($1, $2, $3, $4, $5, $6, $7, $8, $9, '{}'::jsonb, $10)`,
		id, strategyID, versionID, userID, string(status),
		now, now.Add(-time.Hour), now, "1m", "local/sha:test")
	require.NoError(t, err)
	return id
}

// TestTool_Spec_NameAndSchema pins the spec contract: the LLM dispatches
// by name, and the schema tells the model which fields to send. Both are
// part of the public tool surface; renaming is a model-facing change.
func TestTool_Spec_NameAndSchema(t *testing.T) {
	t.Parallel()
	spec := tool.Spec()
	require.Equal(t, "get_backtest_result", spec.Name)
	require.NotEmpty(t, spec.Description)

	var schema map[string]any
	require.NoError(t, json.Unmarshal(spec.JSONSchema, &schema))
	props, ok := schema["properties"].(map[string]any)
	require.True(t, ok, "schema must have properties")
	_, hasBacktestID := props["backtest_id"]
	require.True(t, hasBacktestID, "schema must declare backtest_id")
	required, ok := schema["required"].([]any)
	require.True(t, ok, "schema must declare required")
	require.Contains(t, required, "backtest_id")
}

// TestTool_TerminalSucceeded: insert a succeeded row (with summary + fills),
// call the handler, verify the envelope mirrors the row.
func TestTool_TerminalSucceeded(t *testing.T) {
	t.Parallel()
	pool, strategyID, versionID := seedFixtures(t)
	store := bt.NewPgStore(pool)
	ctx := t.Context()

	id := insertBacktest(t, pool, ownerID, strategyID, versionID, bt.StatusSucceeded)

	// Flip to succeeded with summary + fill_count via the proper store path.
	summary := &bt.Summary{TotalReturn: 0.05, Sharpe: 1.1, TradeCount: 3}
	require.NoError(t, store.SetSummary(ctx, id, summary, 3))

	// Add a fill so GetFills returns something non-empty.
	fillID := uuid.NewString()
	_, err := pool.Exec(ctx, `
		insert into agent.backtest_fills (id, backtest_id, timestamp, side, quantity, price, order_id, simulated_at)
		values ($1, $2, $3, 'buy', 1.0, 100.0, 'o-1', $3)`,
		fillID, id, time.Now().UTC())
	require.NoError(t, err)

	handle := tool.Handler(store, ownerID)
	out, err := handle(ctx, "get_backtest_result", json.RawMessage(`{"backtest_id":"`+id+`"}`))
	require.NoError(t, err)

	var got struct {
		ID        string      `json:"id"`
		Status    bt.Status   `json:"status"`
		Summary   *bt.Summary `json:"summary"`
		FillCount int         `json:"fill_count"`
		Fills     []bt.Fill   `json:"fills"`
	}
	require.NoError(t, json.Unmarshal(out, &got))
	require.Equal(t, id, got.ID)
	require.Equal(t, bt.StatusSucceeded, got.Status)
	require.NotNil(t, got.Summary)
	require.Equal(t, 0.05, got.Summary.TotalReturn)
	require.Equal(t, 3, got.FillCount)
	// Fills are omitted from the LLM tool output to prevent context
	// bloat during multi-iteration backtest sweeps. FillCount carries
	// the trade volume; the Summary carries the performance stats.
}

// TestTool_PollsUntilTerminal: insert a running row, flip it to succeeded
// from a goroutine after 200ms, and confirm the handler returns the
// succeeded result. Budget keeps this well under the CI per-test timeout.
func TestTool_PollsUntilTerminal(t *testing.T) {
	t.Parallel()
	pool, strategyID, versionID := seedFixtures(t)
	store := bt.NewPgStore(pool)
	ctx := t.Context()

	id := insertBacktest(t, pool, ownerID, strategyID, versionID, bt.StatusRunning)
	_, err := pool.Exec(ctx, `update agent.backtests set started_at=now() where id=$1`, id)
	require.NoError(t, err)

	// Flip from another goroutine; 200ms lands inside the first poll tick.
	go func() {
		time.Sleep(200 * time.Millisecond)
		flipCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = store.SetSummary(flipCtx, id, &bt.Summary{TotalReturn: 0.10, TradeCount: 1}, 1)
	}()

	handle := tool.Handler(store, ownerID)
	out, err := handle(ctx, "get_backtest_result", json.RawMessage(`{"backtest_id":"`+id+`"}`))
	require.NoError(t, err)

	var got struct {
		ID      string      `json:"id"`
		Status  bt.Status   `json:"status"`
		Summary *bt.Summary `json:"summary"`
	}
	require.NoError(t, json.Unmarshal(out, &got))
	require.Equal(t, bt.StatusSucceeded, got.Status)
	require.NotNil(t, got.Summary)
	require.Equal(t, 0.10, got.Summary.TotalReturn)
}

// TestTool_WrongOwner: a row owned by user A called with user B's handler
// must surface as not-found, never as 200 with the row.
func TestTool_WrongOwner(t *testing.T) {
	t.Parallel()
	pool, strategyID, versionID := seedFixtures(t)
	store := bt.NewPgStore(pool)
	ctx := t.Context()

	id := insertBacktest(t, pool, ownerID, strategyID, versionID, bt.StatusSucceeded)

	handle := tool.Handler(store, strangerID) // not ownerID
	_, err := handle(ctx, "get_backtest_result", json.RawMessage(`{"backtest_id":"`+id+`"}`))
	require.Error(t, err)
	require.ErrorIs(t, err, bt.ErrNotFound)
}

// TestTool_NotFound: nonexistent ID returns ErrNotFound, not a panic.
func TestTool_NotFound(t *testing.T) {
	t.Parallel()
	pool, _, _ := seedFixtures(t)
	store := bt.NewPgStore(pool)

	handle := tool.Handler(store, ownerID)
	_, err := handle(t.Context(), "get_backtest_result",
		json.RawMessage(`{"backtest_id":"`+uuid.NewString()+`"}`))
	require.Error(t, err)
	require.ErrorIs(t, err, bt.ErrNotFound)
}

// TestTool_EmptyBacktestID: tool must reject an empty id without a DB hit.
func TestTool_EmptyBacktestID(t *testing.T) {
	t.Parallel()
	pool, _, _ := seedFixtures(t)
	store := bt.NewPgStore(pool)

	handle := tool.Handler(store, ownerID)
	_, err := handle(t.Context(), "get_backtest_result", json.RawMessage(`{"backtest_id":""}`))
	require.Error(t, err)
}

// TestTool_MalformedInput: non-JSON must surface a parse error, not a panic.
func TestTool_MalformedInput(t *testing.T) {
	t.Parallel()
	pool, _, _ := seedFixtures(t)
	store := bt.NewPgStore(pool)

	handle := tool.Handler(store, ownerID)
	_, err := handle(t.Context(), "get_backtest_result", json.RawMessage(`not json`))
	require.Error(t, err)
}

// TestRunTool_Spec_NameAndSchema pins the run_backtest contract: the LLM
// dispatches by name, the schema tells the model which fields to send.
// Renaming is a model-facing change.
func TestRunTool_Spec_NameAndSchema(t *testing.T) {
	t.Parallel()
	spec := tool.RunSpec()
	require.Equal(t, "run_backtest", spec.Name)
	require.NotEmpty(t, spec.Description)
	// The description must tell the LLM that only one backtest can
	// run per user at a time and that batching should be sequential.
	for _, kw := range []string{"one backtest per user", "sequentially", "429"} {
		if !strings.Contains(spec.Description, kw) {
			t.Errorf("run_backtest description must mention %q for the LLM to batch sequentially; got %q", kw, spec.Description)
		}
	}
	var schema map[string]any
	require.NoError(t, json.Unmarshal(spec.JSONSchema, &schema))
	props, ok := schema["properties"].(map[string]any)
	require.True(t, ok, "schema must have properties")
	for _, p := range []string{"strategy_id", "revision_id", "order_book_id", "start", "end", "resolution"} {
		_, ok := props[p]
		require.True(t, ok, "schema must declare %s", p)
	}
	requiredList, ok := schema["required"].([]any)
	require.True(t, ok, "schema must declare required")
	for _, want := range []string{"strategy_id", "revision_id", "order_book_id", "start", "end", "resolution"} {
		require.Contains(t, requiredList, want)
	}
}

// TestRunTool_WarmupCandles_OutOfRange: bounds violations surface as a
// wrapped ErrInvalidRequest before any DB write.
func TestRunTool_WarmupCandles_OutOfRange(t *testing.T) {
	t.Parallel()
	pool, strategyID, versionID := seedFixtures(t)
	orch := &bt.Orchestrator{Store: bt.NewPgStore(pool)}
	insertWasmVersion(t, pool, versionID, "wasm-run", "manifest-run")
	versions := strategies.NewPgStore(pool)
	handle := tool.RunHandler(orch, versions, nil, bt.NewPgStore(pool), ownerID, "test-dora-key", migration.New())

	body := map[string]any{
		"strategy_id":    strategyID,
		"revision_id":    versionID,
		"order_book_id":  "ob-run",
		"start":          "2026-01-01T00:00:00Z",
		"end":            "2026-01-02T00:00:00Z",
		"resolution":     "1h",
		"warmup_candles": -1,
	}
	raw, _ := json.Marshal(body)
	_, err := handle(t.Context(), "run_backtest", raw)
	require.Error(t, err)
	require.ErrorIs(t, err, bt.ErrInvalidRequest)
}

// TestRunTool_InvalidInput: missing required fields, malformed JSON, and
// unsupported resolution all surface as parse / validation errors before
// any DB write.
func TestRunTool_InvalidInput(t *testing.T) {
	t.Parallel()
	pool, _, _ := seedFixtures(t)
	orch := &bt.Orchestrator{
		Store: bt.NewPgStore(pool),
	}
	versions := strategies.NewPgStore(pool)
	handle := tool.RunHandler(orch, versions, nil, bt.NewPgStore(pool), ownerID, "test-dora-key", migration.New())

	cases := []struct {
		name string
		body string
	}{
		{"missing_strategy_id", `{"revision_id":"x","order_book_id":"o","start":"2026-01-01T00:00:00Z","end":"2026-01-02T00:00:00Z","resolution":"1h"}`},
		{"missing_order_book_id", `{"strategy_id":"s","revision_id":"x","start":"2026-01-01T00:00:00Z","end":"2026-01-02T00:00:00Z","resolution":"1h"}`},
		{"end_before_start", `{"strategy_id":"s","revision_id":"x","order_book_id":"o","start":"2026-01-02T00:00:00Z","end":"2026-01-01T00:00:00Z","resolution":"1h"}`},
		{"bad_resolution", `{"strategy_id":"s","revision_id":"x","order_book_id":"o","start":"2026-01-01T00:00:00Z","end":"2026-01-02T00:00:00Z","resolution":"7h"}`},
		{"malformed_json", `not json`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := handle(t.Context(), "run_backtest", json.RawMessage(tc.body))
			require.Error(t, err)
		})
	}
}

// TestRunTool_UnknownStrategy: the handler rejects a strategy that doesn't
// exist (or isn't owned by userID) so the LLM gets a single canonical
// error rather than a 5xx from a downstream unhandled lookup.
func TestRunTool_UnknownStrategy(t *testing.T) {
	t.Parallel()
	pool, _, _ := seedFixtures(t)
	orch := &bt.Orchestrator{
		Store: bt.NewPgStore(pool),
	}
	versions := strategies.NewPgStore(pool)
	handle := tool.RunHandler(orch, versions, nil, bt.NewPgStore(pool), ownerID, "test-dora-key", migration.New())

	body := `{"strategy_id":"` + uuid.NewString() + `","revision_id":"x","order_book_id":"o","start":"2026-01-01T00:00:00Z","end":"2026-01-02T00:00:00Z","resolution":"1h"}`
	_, err := handle(t.Context(), "run_backtest", json.RawMessage(body))
	require.Error(t, err)
}

// TestRunTool_UnknownStrategy_HintsGetStrategy: when the LLM hands us
// a strategy_id that doesn't exist (often a stale id from a previous
// session's context window), the error must tell the LLM what to do
// next: call get_strategy to look up the current strategy_id. Without
// this hint the LLM retries the same dead id until max iterations.
func TestRunTool_UnknownStrategy_HintsGetStrategy(t *testing.T) {
	t.Parallel()
	pool, _, _ := seedFixtures(t)
	orch := &bt.Orchestrator{
		Store: bt.NewPgStore(pool),
	}
	versions := strategies.NewPgStore(pool)
	handle := tool.RunHandler(orch, versions, nil, bt.NewPgStore(pool), ownerID, "test-dora-key", migration.New())

	body := `{"strategy_id":"` + uuid.NewString() + `","revision_id":"x","order_book_id":"o","start":"2026-01-01T00:00:00Z","end":"2026-01-02T00:00:00Z","resolution":"1h"}`
	_, err := handle(t.Context(), "run_backtest", json.RawMessage(body))
	require.Error(t, err)
	msg := err.Error()
	// The hint must steer the LLM toward the recovery path. Both
	// "get_strategy" and "stale" should appear so the LLM knows
	// to retry with a fresh lookup.
	if !strings.Contains(msg, "get_strategy") {
		t.Errorf("error must mention get_strategy so the LLM recovers; got %q", msg)
	}
	if !strings.Contains(msg, "stale") {
		t.Errorf("error must flag the strategy_id as stale; got %q", msg)
	}
}

// insertWasmVersion updates the seeded version row to target go-wasm with
// the given wasm_ref and manifest_hash.
func insertWasmVersion(t *testing.T, pool *pgxpool.Pool, versionID, wasmRef, manifestHash string) {
	t.Helper()
	_, err := pool.Exec(t.Context(), `
		update agent.strategy_versions
		set target='go-wasm', wasm_ref=$2, manifest_hash=$3
		where revision=$1`,
		versionID, wasmRef, manifestHash)
	require.NoError(t, err)
}

// TestRunTool_WasmHappyPath: a go-wasm version submits through the
// orchestrator and returns the backtest_id immediately (row queued; the
// WasmStarter goroutine is a no-op with a nil Wasm seam).

// TestRunTool_RejectsEmptyWasmRef: a go-wasm version with no
// compiled artifact is claimed by the migration pre-flight (empty
// WasmRef => stale) and surfaces a generate_strategy RecoveryError
// before any DB write.
func TestRunTool_RejectsEmptyWasmRef(t *testing.T) {
	t.Parallel()
	pool, strategyID, versionID := seedFixtures(t)
	orch := &bt.Orchestrator{Store: bt.NewPgStore(pool)}
	// Stamp the version as go-wasm but with an empty wasm_ref: the
	// pre-flight claims it before Submit's own guard can fire.
	_, err := pool.Exec(t.Context(), `
		update agent.strategy_versions
		set target='go-wasm', wasm_ref='', manifest_hash=''
		where revision=$1`, versionID)
	require.NoError(t, err)
	versions := strategies.NewPgStore(pool)
	handle := tool.RunHandler(orch, versions, nil, bt.NewPgStore(pool), ownerID, "test-dora-key", migration.New())

	body := map[string]any{
		"strategy_id":   strategyID,
		"revision_id":   versionID,
		"order_book_id": "ob-empty",
		"start":         "2026-01-01T00:00:00Z",
		"end":           "2026-01-02T00:00:00Z",
		"resolution":    "1h",
	}
	raw, _ := json.Marshal(body)
	_, err = handle(t.Context(), "run_backtest", raw)
	require.Error(t, err)
	var rec llm.RecoveryError
	require.ErrorAs(t, err, &rec)
	require.Equal(t, "generate_strategy", rec.RecoveryTool())
}

// TestRunTool_RejectsNilWasmStarter: a misconfigured orchestrator
// with no WasmStarter surfaces ErrWasmUnavailable before any DB
// write. Guards against main.go forgetting to wire the runtime.
func TestRunTool_RejectsNilWasmStarter(t *testing.T) {
	t.Parallel()
	pool, strategyID, versionID := seedFixtures(t)
	insertWasmVersion(t, pool, versionID, "wasm-no-starter", "manifest-no-starter")
	orch := &bt.Orchestrator{Store: bt.NewPgStore(pool)} // no Wasm field
	versions := strategies.NewPgStore(pool)
	handle := tool.RunHandler(orch, versions, nil, bt.NewPgStore(pool), ownerID, "test-dora-key", migration.New())

	body := map[string]any{
		"strategy_id":   strategyID,
		"revision_id":   versionID,
		"order_book_id": "ob-no-starter",
		"start":         "2026-01-01T00:00:00Z",
		"end":           "2026-01-02T00:00:00Z",
		"resolution":    "1h",
	}
	raw, _ := json.Marshal(body)
	_, err := handle(t.Context(), "run_backtest", raw)
	require.Error(t, err)
	require.ErrorIs(t, err, bt.ErrWasmUnavailable)
}

// insertLegacyVersion inserts a second strategy_version row with the
// pre-WASM-migration shape (target='go-docker', no wasm_ref) and advances
// the strategy's head_revision to it. Returns the new revision id.
func insertLegacyVersion(t *testing.T, pool *pgxpool.Pool, strategyID string) string {
	t.Helper()
	rev := uuid.NewString()
	_, err := pool.Exec(t.Context(), `
		insert into agent.strategy_versions (revision, strategy_id, provider, model,
		    module_name, summary, rationale, validation, target)
		values ($1, $2, 'openai', 'gpt', 'legacy', 's', 'r', '{}'::jsonb, 'go-docker')`,
		rev, strategyID)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `
		update agent.strategies set head_revision=$1 where id=$2`,
		rev, strategyID)
	require.NoError(t, err)
	return rev
}

// TestRunBacktest_StaleFramework_Recovery: a legacy (go-docker) version must
// surface a RecoveryError pointing at generate_strategy, with the structured
// hint carrying strategy_id + current_revision_id so the LLM can rebuild
// without a get_strategy round-trip.
func TestRunBacktest_StaleFramework_Recovery(t *testing.T) {
	t.Parallel()
	pool, strategyID, _ := seedFixtures(t)
	legacyRev := insertLegacyVersion(t, pool, strategyID)
	versions := strategies.NewPgStore(pool)
	handle := tool.RunHandler(nil, versions, nil, bt.NewPgStore(pool), ownerID, "k", migration.New())

	_, err := handle(t.Context(), "run_backtest", json.RawMessage(fmt.Sprintf(
		`{"strategy_id":%q,"revision_id":%q,"order_book_id":"ob","start":"2026-09-04T00:00:00Z","end":"2026-09-05T00:00:00Z","resolution":"1m"}`,
		strategyID, legacyRev,
	)))
	var rec llm.RecoveryError
	require.ErrorAs(t, err, &rec)
	require.Equal(t, "generate_strategy", rec.RecoveryTool())
	require.NotNil(t, rec.Hint())

	var hint struct {
		Action            string `json:"action"`
		StrategyID        string `json:"strategy_id"`
		CurrentRevisionID string `json:"current_revision_id"`
		Reason            string `json:"reason"`
	}
	require.NoError(t, json.Unmarshal(rec.Hint(), &hint))
	require.Equal(t, "rebuild", hint.Action)
	require.Equal(t, "stale_framework", hint.Reason)
	require.Equal(t, strategyID, hint.StrategyID)
	require.Equal(t, legacyRev, hint.CurrentRevisionID)
}

// TestRunBacktest_HealthyGoWasm_NoRecovery: a built go-wasm version must not
// trip the migration pre-flight. The call still fails further down (nil
// orchestrator seam) — the assertion is only that the error is not a
// RecoveryError.
func TestRunBacktest_HealthyGoWasm_NoRecovery(t *testing.T) {
	t.Parallel()
	pool, strategyID, versionID := seedFixtures(t)
	insertWasmVersion(t, pool, versionID, "wasm-healthy", "manifest-healthy")
	versions := strategies.NewPgStore(pool)
	handle := tool.RunHandler(nil, versions, nil, bt.NewPgStore(pool), ownerID, "k", migration.New())

	_, err := handle(t.Context(), "run_backtest", json.RawMessage(fmt.Sprintf(
		`{"strategy_id":%q,"revision_id":%q,"order_book_id":"ob","start":"2026-09-04T00:00:00Z","end":"2026-09-05T00:00:00Z","resolution":"1m"}`,
		strategyID, versionID,
	)))
	require.Error(t, err) // nil orchestrator: wasm backtest unavailable
	var rec llm.RecoveryError
	require.False(t, errors.As(err, &rec), "expected non-recovery error for healthy go-wasm row, got %+v", rec)
}

// TestRunBacktest_StaleFramework_MutationGuard is intentionally skipped:
// NeedsMigration's behaviour is pinned by the TestMigrator_NeedsMigration_*
// cases in internal/agent/migration/migrator_test.go.
