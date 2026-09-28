package deployment_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/internal/agent/agenttest"
	"github.com/dora-network/bond-trading-strategies/internal/agent/deployment"
)

const (
	userA = "00000000-0000-0000-0000-000000000d01"
	userB = "00000000-0000-0000-0000-000000000d02"
)

func newStore(t *testing.T) (*deployment.PgStore, *pgxpool.Pool) {
	t.Helper()
	pool := agenttest.StartPostgres(t)
	// Shared agenttest DB (source repo used a fresh container per test):
	// clear prior rows for the fixed test users so ListRunning-style
	// assertions see only this test's rows.
	for _, u := range []string{userA, userB} {
		_, err := pool.Exec(t.Context(),
			`delete from agent.deployments where user_id = $1`, u)
		require.NoError(t, err)
		_, err = pool.Exec(t.Context(),
			`delete from agent.strategies where dora_user_id = $1`, u)
		require.NoError(t, err)
	}
	seedUser(t, pool, userA)
	seedUser(t, pool, userB)
	return deployment.NewPgStore(pool), pool
}

// seedStrategy inserts a minimal strategies + strategy_versions pair so
// the deployments FK is satisfied. Returns the new strategy + revision ids.
//
//nolint:unparam // userID matches a per-test fixture; today's tests only seed userA.
func seedStrategy(t *testing.T, pool *pgxpool.Pool, userID string) (string, string) {
	t.Helper()
	strategyID := uuid.NewString()
	revision := uuid.NewString()
	sessionID := uuid.NewString()
	ctx := t.Context()
	_, err := pool.Exec(ctx, `
		insert into agent.sessions (id, dora_user_id, provider, model)
		values ($1, $2, 'openai', 'gpt-4o-mini')`,
		sessionID, userID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		insert into agent.strategies (id, dora_user_id, name, head_revision, source_session_id)
		values ($1, $2, 'test', $3, $4)`,
		strategyID, userID, revision, sessionID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		insert into agent.strategy_versions (
			revision, strategy_id, parent_revision, provider, model,
			module_name, summary, rationale, validation
		) values ($1, $2, null, 'openai', 'gpt-4o-mini', 'alpha', 'sum', 'rat', '{}'::jsonb)`,
		revision, strategyID)
	require.NoError(t, err)
	return strategyID, revision
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

func TestPgStore_CreateAndGet(t *testing.T) {
	s, pool := newStore(t)
	strategyID, revision := seedStrategy(t, pool, userA)
	now := time.Now().UTC().Truncate(time.Microsecond)

	d := deployment.Deployment{
		ID:         uuid.NewString(),
		StrategyID: strategyID,
		Revision:   revision,
		UserID:     userA,
		Params:     map[string]string{"leverage": "2", "book": "x"},
		Status:     deployment.StatusRunning,
		InstanceID: uuid.NewString(),
		StartedAt:  &now,
	}
	require.NoError(t, s.Create(t.Context(), d))

	got, err := s.Get(t.Context(), d.ID, userA)
	require.NoError(t, err)
	require.Equal(t, d.ID, got.ID)
	require.Equal(t, strategyID, got.StrategyID)
	require.Equal(t, revision, got.Revision)
	require.Equal(t, userA, got.UserID)
	require.Equal(t, deployment.StatusRunning, got.Status)
	require.Equal(t, d.InstanceID, got.InstanceID)
	require.Equal(t, d.Params, got.Params)
	require.NotNil(t, got.StartedAt)
	require.WithinDuration(t, now, *got.StartedAt, time.Second)
	require.Zero(t, got.StoppedAt)
	require.Zero(t, got.HotswappedAt)
}

func TestPgStore_Get_WrongUserReturnsNotFound(t *testing.T) {
	s, pool := newStore(t)
	strategyID, revision := seedStrategy(t, pool, userA)
	d := deployment.Deployment{
		ID:         uuid.NewString(),
		StrategyID: strategyID,
		Revision:   revision,
		UserID:     userA,
		Status:     deployment.StatusRunning,
	}
	require.NoError(t, s.Create(t.Context(), d))

	_, err := s.Get(t.Context(), d.ID, userB)
	require.ErrorIs(t, err, deployment.ErrNotFound)
}

func TestPgStore_HasActive(t *testing.T) {
	s, pool := newStore(t)
	strategyID, revision := seedStrategy(t, pool, userA)

	active, err := s.HasActive(t.Context(), strategyID)
	require.NoError(t, err)
	require.False(t, active)

	require.NoError(t, s.Create(t.Context(), deployment.Deployment{
		ID:         uuid.NewString(),
		StrategyID: strategyID,
		Revision:   revision,
		UserID:     userA,
		Status:     deployment.StatusRunning,
	}))

	active, err = s.HasActive(t.Context(), strategyID)
	require.NoError(t, err)
	require.True(t, active)
}

func TestPgStore_UpdateStatus(t *testing.T) {
	s, pool := newStore(t)
	strategyID, revision := seedStrategy(t, pool, userA)
	d := deployment.Deployment{
		ID:         uuid.NewString(),
		StrategyID: strategyID,
		Revision:   revision,
		UserID:     userA,
		Status:     deployment.StatusRunning,
	}
	require.NoError(t, s.Create(t.Context(), d))

	before := time.Now().UTC()
	require.NoError(t, s.UpdateStatus(t.Context(), d.ID, userA, deployment.StatusStopped, "user requested"))

	got, err := s.Get(t.Context(), d.ID, userA)
	require.NoError(t, err)
	require.Equal(t, deployment.StatusStopped, got.Status)
	require.Equal(t, "user requested", got.StoppedReason)
	require.NotNil(t, got.StoppedAt)
	require.True(t, got.StoppedAt.After(before.Add(-time.Second)),
		"stopped_at should be recent, got %v", got.StoppedAt)

	// Updating an unknown id returns ErrNotFound.
	err = s.UpdateStatus(t.Context(), uuid.NewString(), userA, deployment.StatusStopped, "")
	require.ErrorIs(t, err, deployment.ErrNotFound)
}

func TestPgStore_HotSwap(t *testing.T) {
	s, pool := newStore(t)
	strategyID, oldRev := seedStrategy(t, pool, userA)
	d := deployment.Deployment{
		ID:         uuid.NewString(),
		StrategyID: strategyID,
		Revision:   oldRev,
		UserID:     userA,
		Status:     deployment.StatusRunning,
	}
	require.NoError(t, s.Create(t.Context(), d))

	// Make a second revision on the same strategy.
	newRev := uuid.NewString()
	_, err := pool.Exec(t.Context(), `
		insert into agent.strategy_versions (
			revision, strategy_id, parent_revision, provider, model,
			module_name, summary, rationale, validation
		) values ($1, $2, $3, 'openai', 'gpt-4o-mini', 'alpha', 'sum', 'rat', '{}'::jsonb)`,
		newRev, strategyID, oldRev)
	require.NoError(t, err)

	before := time.Now().UTC()
	require.NoError(t, s.HotSwap(t.Context(), d.ID, userA, newRev))

	got, err := s.Get(t.Context(), d.ID, userA)
	require.NoError(t, err)
	require.Equal(t, newRev, got.Revision)
	require.Equal(t, oldRev, got.HotswappedFromRev)
	require.NotNil(t, got.HotswappedAt)
	require.True(t, got.HotswappedAt.After(before.Add(-time.Second)),
		"hotswapped_at should be recent, got %v", got.HotswappedAt)
}

func TestPgStore_ListRunning(t *testing.T) {
	s, pool := newStore(t)
	strategyID, revision := seedStrategy(t, pool, userA)

	// One running deployment.
	running := deployment.Deployment{
		ID:         uuid.NewString(),
		StrategyID: strategyID,
		Revision:   revision,
		UserID:     userA,
		Status:     deployment.StatusRunning,
	}
	require.NoError(t, s.Create(t.Context(), running))

	// Create a stopped one on a fresh strategy so the partial-unique
	// index doesn't reject a second 'running' row.
	strategyID2, revision2 := seedStrategy(t, pool, userA)
	stopped := deployment.Deployment{
		ID:         uuid.NewString(),
		StrategyID: strategyID2,
		Revision:   revision2,
		UserID:     userA,
		Status:     deployment.StatusRunning,
	}
	require.NoError(t, s.Create(t.Context(), stopped))
	require.NoError(t, s.UpdateStatus(t.Context(), stopped.ID, userA, deployment.StatusStopped, ""))

	got, err := s.ListRunning(t.Context())
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, running.ID, got[0].ID)
	require.Equal(t, deployment.StatusRunning, got[0].Status)
}

func TestPgStore_WarmupCandlesRoundTrip(t *testing.T) {
	s, pool := newStore(t)
	strategyID, revision := seedStrategy(t, pool, userA)

	d := deployment.Deployment{
		ID:            uuid.NewString(),
		StrategyID:    strategyID,
		Revision:      revision,
		UserID:        userA,
		Status:        deployment.StatusRunning,
		WarmupCandles: 200,
	}
	require.NoError(t, s.Create(t.Context(), d))

	got, err := s.Get(t.Context(), d.ID, userA)
	require.NoError(t, err)
	require.Equal(t, 200, got.WarmupCandles)
}

func TestPgStore_UpdateWarmupCandles(t *testing.T) {
	s, pool := newStore(t)
	strategyID, revision := seedStrategy(t, pool, userA)

	// Row as the HTTP handler wrote it: request sent 0.
	d := deployment.Deployment{
		ID:            uuid.NewString(),
		StrategyID:    strategyID,
		Revision:      revision,
		UserID:        userA,
		Status:        deployment.StatusRunning,
		WarmupCandles: 0,
	}
	require.NoError(t, s.Create(t.Context(), d))

	require.NoError(t, s.UpdateWarmupCandles(t.Context(), d.ID, userA, 200))

	got, err := s.Get(t.Context(), d.ID, userA)
	require.NoError(t, err)
	require.Equal(t, 200, got.WarmupCandles)

	// Unknown id and wrong owner return ErrNotFound (SetCandleCount pattern).
	require.ErrorIs(t, s.UpdateWarmupCandles(t.Context(), uuid.NewString(), userA, 1), deployment.ErrNotFound)
	require.ErrorIs(t, s.UpdateWarmupCandles(t.Context(), d.ID, userB, 1), deployment.ErrNotFound)
}
