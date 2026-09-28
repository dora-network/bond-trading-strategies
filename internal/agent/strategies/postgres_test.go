package strategies_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/internal/agent/agenttest"
	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies"
)

func TestCapture_CreatesStrategyAndV1(t *testing.T) {
	s, userID := newStore(t)
	v1, err := s.Capture(t.Context(), uuid.NewString(), userID, "openai", "gpt", meta("alpha"), map[string]string{"main.go": "v1"}, "")
	require.NoError(t, err)
	require.Empty(t, v1.ParentRevision)

	strategy := strategyForUser(t, s, userID)
	require.Equal(t, v1.Revision, strategy.HeadRevision)
	require.Equal(t, v1.Revision, head(t, s, strategy.ID))
}

func TestCapture_AppendsV2ParentedToHead(t *testing.T) {
	s, userID := newStore(t)
	sessionID := uuid.NewString()
	v1, err := s.Capture(t.Context(), sessionID, userID, "openai", "gpt", meta("alpha"), map[string]string{"main.go": "v1"}, "")
	require.NoError(t, err)
	v2, err := s.Capture(t.Context(), sessionID, userID, "openai", "gpt", meta("alpha"), map[string]string{"main.go": "v2"}, "")
	require.NoError(t, err)
	require.Equal(t, v1.Revision, v2.ParentRevision)

	versions, next, err := s.ListVersions(t.Context(), strategyForUser(t, s, userID).ID, strategies.Page{})
	require.NoError(t, err)
	require.Empty(t, next)
	require.Len(t, versions, 2)
	require.Equal(t, v2.Revision, versions[0].Revision)
}

func TestRollback_NonDestructive_Branch(t *testing.T) {
	s, userID := newStore(t)
	sessionID := uuid.NewString()
	v1, err := s.Capture(t.Context(), sessionID, userID, "openai", "gpt", meta("alpha"), map[string]string{"main.go": "v1"}, "")
	require.NoError(t, err)
	v2, err := s.Capture(t.Context(), sessionID, userID, "openai", "gpt", meta("alpha"), map[string]string{"main.go": "v2"}, "")
	require.NoError(t, err)
	strategyID := strategyForUser(t, s, userID).ID
	require.NoError(t, s.SetHead(t.Context(), strategyID, v1.Revision))

	gotV2, err := s.GetVersion(t.Context(), strategyID, v2.Revision)
	require.NoError(t, err)
	require.Equal(t, v2.Revision, gotV2.Revision)
	v3, err := s.Capture(t.Context(), sessionID, userID, "openai", "gpt", meta("alpha"), map[string]string{"main.go": "branch"}, "")
	require.NoError(t, err)
	require.Equal(t, v1.Revision, v3.ParentRevision)
}

func TestContentAddressing_DedupsBlob(t *testing.T) {
	s, userID := newStore(t)
	sessionID := uuid.NewString()
	_, err := s.Capture(t.Context(), sessionID, userID, "openai", "gpt", meta("alpha"), map[string]string{
		"shared.go": "same", "one.go": "one",
	}, "")
	require.NoError(t, err)
	_, err = s.Capture(t.Context(), sessionID, userID, "openai", "gpt", meta("alpha"), map[string]string{
		"shared.go": "same", "two.go": "two",
	}, "")
	require.NoError(t, err)

	count, err := s.BlobCount(t.Context(), strategyForUser(t, s, userID).ID)
	require.NoError(t, err)
	require.Equal(t, 3, count)
}

func TestPending_StashAndCapturePending(t *testing.T) {
	s, sessionID := newStoreSeeded(t)
	files := map[string]string{"main.go": "pending"}
	require.NoError(t, s.StashPending(t.Context(), sessionID, testUserID, "openai", "gpt", meta("alpha"), files, ""))

	got, err := s.CapturePending(t.Context(), sessionID)
	require.NoError(t, err)
	require.Equal(t, files, got.Files)
	_, err = s.CapturePending(t.Context(), sessionID)
	require.ErrorIs(t, err, strategies.ErrNoPending)
}

func TestCapture_ClearsPendingOnSuccess(t *testing.T) {
	s, sessionID := newStoreSeeded(t)
	require.NoError(t, s.StashPending(t.Context(), sessionID, testUserID, "openai", "gpt", meta("alpha"),
		map[string]string{"main.go": "pending"}, ""))
	_, err := s.Capture(t.Context(), sessionID, testUserID, "openai", "gpt", meta("alpha"),
		map[string]string{"main.go": "captured"}, "")
	require.NoError(t, err)
	_, err = s.CapturePending(t.Context(), sessionID)
	require.ErrorIs(t, err, strategies.ErrNoPending)
}

func TestOwnership_NotFoundForOtherUser(t *testing.T) {
	s, userID := newStore(t)
	_, err := s.Capture(t.Context(), uuid.NewString(), userID, "openai", "gpt", meta("alpha"), map[string]string{"main.go": "v1"}, "")
	require.NoError(t, err)
	strategyID := strategyForUser(t, s, userID).ID

	_, err = s.GetStrategy(t.Context(), uuid.NewString(), strategyID)
	require.ErrorIs(t, err, strategies.ErrNotFound)
}

func TestListVersions_Pagination(t *testing.T) {
	s, userID := newStore(t)
	sessionID := uuid.NewString()
	for i := range 51 {
		_, err := s.Capture(t.Context(), sessionID, userID, "openai", "gpt", meta("alpha"), map[string]string{"main.go": fmt.Sprintf("v%d", i)}, "")
		require.NoError(t, err)
	}
	strategyID := strategyForUser(t, s, userID).ID

	page1, cursor, err := s.ListVersions(t.Context(), strategyID, strategies.Page{Limit: 10})
	require.NoError(t, err)
	require.Len(t, page1, 10)
	require.NotEmpty(t, cursor)
	page2, _, err := s.ListVersions(t.Context(), strategyID, strategies.Page{Limit: 10, Cursor: cursor})
	require.NoError(t, err)
	require.Len(t, page2, 10)

	seen := make(map[strategies.Revision]struct{}, len(page1))
	for _, v := range page1 {
		seen[v.Revision] = struct{}{}
	}
	for _, v := range page2 {
		_, overlap := seen[v.Revision]
		require.False(t, overlap, "revision %s appears on both pages", v.Revision)
		seen[v.Revision] = struct{}{}
	}
	require.Len(t, seen, 20)
}

// testUserID is a stable user id used by the seeded tests so the foreign
// key from agent.strategy_capture_pending.session_id to sessions(id) can be
// satisfied via a session row inserted by newStoreSeeded.
const testUserID = "00000000-0000-0000-0000-000000000001"

// testSessionID is a stable session id used by the pending tests; it pairs
// with testUserID in the seeded session row.
const testSessionID = "00000000-0000-0000-0000-000000000002"

// newStore provisions a store backed by the testcontainers Postgres with a
// seeded user row. It does NOT seed a sessions row — tests that need one
// (for the strategy_capture_pending FK) should use newStoreSeeded instead.
func newStore(t *testing.T) (*strategies.PgStore, string) {
	t.Helper()
	pool := agenttest.StartPostgres(t)
	cleanUserRows(t, pool, testUserID)
	seedUser(t, pool, testUserID)
	return strategies.NewPgStore(pool), testUserID
}

// newStoreSeeded provisions the store + a sessions row for testSessionID,
// required by the strategy_capture_pending foreign key.
func newStoreSeeded(t *testing.T) (*strategies.PgStore, string) {
	t.Helper()
	pool := agenttest.StartPostgres(t)
	cleanUserRows(t, pool, testUserID)
	seedUser(t, pool, testUserID)
	seedSession(t, pool, testSessionID, testUserID)
	return strategies.NewPgStore(pool), testSessionID
}

// cleanUserRows removes leftover strategies rows for the fixed test
// user. The agenttest DB is shared across tests (the source repo used a
// fresh container per test); assertions like strategyForUser's
// require.Len(items, 1) assume a clean slate.
func cleanUserRows(t *testing.T, pool *pgxpool.Pool, userID string) {
	t.Helper()
	_, err := pool.Exec(t.Context(),
		`delete from agent.strategies where dora_user_id = $1`, userID)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(),
		`delete from agent.strategy_capture_pending where dora_user_id = $1`, userID)
	require.NoError(t, err)
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

func seedSession(t *testing.T, pool *pgxpool.Pool, sessionID, userID string) {
	t.Helper()
	_, err := pool.Exec(t.Context(), `
		insert into agent.sessions (id, dora_user_id, provider, model)
		values ($1, $2, 'openai', 'gpt-4o-mini')
		on conflict (id) do nothing`,
		sessionID, userID)
	require.NoError(t, err)
}

func strategyForUser(t *testing.T, s *strategies.PgStore, userID string) strategies.Strategy {
	t.Helper()
	items, _, err := s.ListStrategies(t.Context(), userID, strategies.Page{})
	require.NoError(t, err)
	require.Len(t, items, 1)
	return items[0]
}

func head(t *testing.T, s *strategies.PgStore, strategyID string) strategies.Revision {
	t.Helper()
	revision, err := s.Head(t.Context(), strategyID)
	require.NoError(t, err)
	return revision
}

func meta(_ string) strategies.Meta {
	return strategies.Meta{
		Provider: "ignored", Model: "ignored", ModuleName: "alpha",
		Summary: "summary", Rationale: "rationale", Validation: []byte(`{"ok":true}`),
	}
}

// TestCapture_RecordsImageRef asserts that the build-time image reference
// produced by the validator flows through Capture to the stored version.
// The image_ref is the local-daemon tag (e.g. `strategy-<revision>` from
// Phase 2); later slices will `docker run` it for backtest and live.
func TestCapture_RecordsImageRef(t *testing.T) {
	s, userID := newStore(t)
	sessionID := uuid.NewString()
	m := meta("alpha")
	m.Validation = []byte(`{"build_ok":true,"vet_ok":true,"tests_ok":true}`)
	v1, err := s.Capture(t.Context(), sessionID, userID, "openai", "gpt", m, map[string]string{"main.go": "v1"}, "strategy-rev-1")
	require.NoError(t, err)
	require.Equal(t, "strategy-rev-1", v1.ImageRef)

	strategyID := strategyForUser(t, s, userID).ID

	got, err := s.GetVersion(t.Context(), strategyID, v1.Revision)
	require.NoError(t, err)
	require.Equal(t, "strategy-rev-1", got.ImageRef)

	items, _, err := s.ListVersions(t.Context(), strategyID, strategies.Page{})
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, "strategy-rev-1", items[0].ImageRef)
}

// TestCapture_EmptyImageRefAllowed covers the pre-image path: a Capture
// without an image_ref stores an empty string (legacy rows stay NULL
// via the migration). Drivers that have not yet cut a build may still
// record a version.
func TestCapture_EmptyImageRefAllowed(t *testing.T) {
	s, userID := newStore(t)
	v1, err := s.Capture(t.Context(), uuid.NewString(), userID, "openai", "gpt", meta("alpha"), map[string]string{"main.go": "v1"}, "")
	require.NoError(t, err)
	require.Empty(t, v1.ImageRef)
}

// TestSweepCapturePending_DeletesByAge asserts the janitor's store
// method drops only rows older than the retention window. Three
// rows: one backdated past the retention, one fresh, one
// backdated-but-still-within-retention. Only the first is swept.
func TestSweepCapturePending_DeletesByAge(t *testing.T) {
	s, userID := newStore(t)
	ctx := t.Context()
	pool := s.Pool

	// Three distinct sessions so each row is independent. Each
	// needs a parent sessions row to satisfy the FK on
	// strategy_capture_pending.session_id.
	oldSession := uuid.NewString()
	freshSession := uuid.NewString()
	midSession := uuid.NewString()
	for _, sid := range []string{oldSession, freshSession, midSession} {
		seedSession(t, pool, sid, userID)
		require.NoError(t, s.StashPending(ctx, sid, userID, "openai", "gpt",
			meta("alpha"), map[string]string{"main.go": "x"}, ""))
	}
	// Manually backdate created_at on the old row (10 days ago) and
	// the mid row (1 day ago). The fresh row keeps the default now().
	_, err := pool.Exec(ctx,
		`update agent.strategy_capture_pending set created_at = now() - interval '10 days' where session_id = $1`,
		oldSession)
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`update agent.strategy_capture_pending set created_at = now() - interval '1 day' where session_id = $1`,
		midSession)
	require.NoError(t, err)

	// Sweep with a 7-day retention: only the 10-day-old row should go.
	deleted, err := s.SweepCapturePending(ctx, 7*24*time.Hour)
	require.NoError(t, err)
	require.Equal(t, 1, deleted)

	// Confirm the post-sweep table state.
	var remaining int
	require.NoError(t, pool.QueryRow(ctx,
		`select count(*) from agent.strategy_capture_pending where session_id in ($1, $2, $3)`,
		oldSession, freshSession, midSession).Scan(&remaining))
	require.Equal(t, 2, remaining, "only the 10-day-old row should have been swept")

	// Old row is gone; CapturePending returns ErrNoPending.
	_, err = s.CapturePending(ctx, oldSession)
	require.ErrorIs(t, err, strategies.ErrNoPending)
	// Mid row is still retrievable.
	_, err = s.CapturePending(ctx, midSession)
	require.NoError(t, err)
}

// TestSweepCapturePending_RejectsNonPositiveRetention guards the
// "interval must be > 0" precondition so a misconfigured env var
// can't trigger an unbounded DELETE.
func TestSweepCapturePending_RejectsNonPositiveRetention(t *testing.T) {
	s, _ := newStore(t)
	_, err := s.SweepCapturePending(t.Context(), 0)
	require.Error(t, err)
	_, err = s.SweepCapturePending(t.Context(), -1*time.Hour)
	require.Error(t, err)
}
