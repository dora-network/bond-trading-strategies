package backtest_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/internal/agent/agenttest"
	"github.com/dora-network/bond-trading-strategies/internal/agent/backtest"
)

// newJanitorStore provisions a testcontainers Postgres with one queued row
// and one running row (under different users so the partial unique index
// on active rows is satisfied). Returns the store plus the two IDs.
func newJanitorStore(t *testing.T) (*backtest.PgStore, string, string) {
	t.Helper()
	pool := agenttest.StartPostgres(t)
	ctx := t.Context()
	queuedUser := uuid.NewString()
	runningUser := uuid.NewString()
	seedUser(t, pool, queuedUser)
	seedUser(t, pool, runningUser)

	strategyQueued := uuid.NewString()
	versionQueued := uuid.NewString()
	_, err := pool.Exec(ctx, `
		insert into agent.strategies (id, dora_user_id, name, source_session_id)
		values ($1, $2, 'alpha', $3)`,
		strategyQueued, queuedUser, uuid.NewString())
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		insert into agent.strategy_versions (revision, strategy_id, provider, model,
		    module_name, summary, rationale, validation)
		values ($1, $2, 'openai', 'gpt', 'alpha', 's', 'r', '{}'::jsonb)`,
		versionQueued, strategyQueued)
	require.NoError(t, err)

	strategyRunning := uuid.NewString()
	versionRunning := uuid.NewString()
	_, err = pool.Exec(ctx, `
		insert into agent.strategies (id, dora_user_id, name, source_session_id)
		values ($1, $2, 'beta', $3)`,
		strategyRunning, runningUser, uuid.NewString())
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		insert into agent.strategy_versions (revision, strategy_id, provider, model,
		    module_name, summary, rationale, validation)
		values ($1, $2, 'openai', 'gpt', 'beta', 's', 'r', '{}'::jsonb)`,
		versionRunning, strategyRunning)
	require.NoError(t, err)

	store := backtest.NewPgStore(pool)
	queued := sampleBacktest(queuedUser, strategyQueued, versionQueued)
	queuedID, err := store.Create(ctx, queued)
	require.NoError(t, err)

	running := sampleBacktest(runningUser, strategyRunning, versionRunning)
	runningID, err := store.Create(ctx, running)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		update agent.backtests set status='running', started_at=now() where id=$1`,
		runningID)
	require.NoError(t, err)

	return store, queuedID, runningID
}

// TestJanitor_FlipsOrphanedRows verifies both queued and running rows are
// marked failed with the restart error message after the janitor runs.
func TestJanitor_FlipsOrphanedRows(t *testing.T) {
	store, queuedID, runningID := newJanitorStore(t)

	err := backtest.RunStartupJanitor(t.Context(), store)
	require.NoError(t, err)

	queued, err := store.Get(t.Context(), queuedID)
	require.NoError(t, err)
	require.Equal(t, backtest.StatusFailed, queued.Status)
	require.Equal(t, "agent restarted", queued.ErrorMessage)
	require.NotNil(t, queued.FinishedAt)
	require.WithinDuration(t, time.Now(), *queued.FinishedAt, time.Minute)

	running, err := store.Get(t.Context(), runningID)
	require.NoError(t, err)
	require.Equal(t, backtest.StatusFailed, running.Status)
	require.Equal(t, "agent restarted", running.ErrorMessage)
	require.NotNil(t, running.FinishedAt)
}

// TestJanitor_NoActiveRows runs against an empty table — the DB update
// affects zero rows and the function must return nil.
func TestJanitor_NoActiveRows(t *testing.T) {
	pool := agenttest.StartPostgres(t)
	store := backtest.NewPgStore(pool)

	err := backtest.RunStartupJanitor(t.Context(), store)
	require.NoError(t, err)
}
