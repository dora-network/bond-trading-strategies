package backtest_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/internal/agent/agenttest"
	"github.com/dora-network/bond-trading-strategies/internal/agent/backtest"
)

// newSeededStore provisions a testcontainers Postgres, seeds the users +
// strategies + strategy_versions rows required by the backtests FK chain,
// and returns a backtest store plus the strategy/version IDs.
func newSeededStore(t *testing.T) (*backtest.PgStore, string, string, string) {
	t.Helper()
	pool := agenttest.StartPostgres(t)
	// Per-test user: the shared agenttest DB is reused across tests and
	// packages, and backtests_user_active_uidx allows only one active
	// backtest per user (the source repo used a fresh container per test).
	userID := uuid.NewString()
	seedUser(t, pool, userID)
	ctx := t.Context()

	// Seed strategies + strategy_versions manually. Capture is exercised by
	// other slices' tests; backtest FK compliance is what we actually need here.
	strategyID := uuid.NewString()
	versionID := uuid.NewString()
	_, err := pool.Exec(ctx, `
		insert into agent.strategies (id, dora_user_id, name, source_session_id)
		values ($1, $2, $3, $4)`,
		strategyID, userID, "alpha", uuid.NewString())
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		insert into agent.strategy_versions (revision, strategy_id, provider, model,
		    module_name, summary, rationale, validation)
		values ($1, $2, 'openai', 'gpt', 'alpha', 's', 'r', '{}'::jsonb)`,
		versionID, strategyID)
	require.NoError(t, err)

	return backtest.NewPgStore(pool), userID, strategyID, versionID
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

// sampleBacktest produces a minimally-valid queued backtest against the
// given strategy + version. Callers can override fields post-hoc.
func sampleBacktest(userID, strategyID, versionID string) *backtest.Backtest {
	now := time.Now().UTC().Truncate(time.Second)
	return &backtest.Backtest{
		StrategyID:  strategyID,
		VersionID:   versionID,
		UserID:      userID,
		WindowStart: now.Add(-24 * time.Hour),
		WindowEnd:   now,
		Resolution:  "1m",
		Params:      map[string]string{"stop_loss": "0.02"},
		ImageRef:    "local/sha:slicetest",
	}
}

// TestPgStore_CreateAndGet exercises the round-trip through Create + Get and
// verifies that nullable timestamps and the summary field round-trip cleanly.
func TestPgStore_CreateAndGet(t *testing.T) {
	s, userID, strategyID, versionID := newSeededStore(t)
	b := sampleBacktest(userID, strategyID, versionID)
	id, err := s.Create(t.Context(), b)
	require.NoError(t, err)
	require.NotEmpty(t, id)
	require.Equal(t, id, b.ID)
	require.Equal(t, backtest.StatusQueued, b.Status)

	got, err := s.Get(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, id, got.ID)
	require.Equal(t, backtest.StatusQueued, got.Status)
	require.Equal(t, "1m", got.Resolution)
	require.Equal(t, "local/sha:slicetest", got.ImageRef)
	require.Nil(t, got.StartedAt)
	require.Nil(t, got.FinishedAt)
	require.Equal(t, "0.02", got.Params["stop_loss"])
	require.Nil(t, got.Summary)
}

// TestPgStore_OrderBookIDRoundTrip covers the migration that
// added backtests.order_book_id. Without the column, Create
// silently drops the field and Get returns ""; the operator
// (and the LLM tool) lose the order book the strategy ran
// against. With the column, both directions round-trip.
func TestPgStore_OrderBookIDRoundTrip(t *testing.T) {
	s, userID, strategyID, versionID := newSeededStore(t)
	b := sampleBacktest(userID, strategyID, versionID)
	b.OrderBookID = "ob-roundtrip"
	id, err := s.Create(t.Context(), b)
	require.NoError(t, err)

	got, err := s.Get(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, "ob-roundtrip", got.OrderBookID)
}

// TestPgStore_SingleInflight proves the partial unique index on
// backtests_user_active_uidx rejects a second active row for the same user.
// Handler maps this 23505 to 429 in production.
func TestPgStore_SingleInflight(t *testing.T) {
	s, userID, strategyID, versionID := newSeededStore(t)
	first := sampleBacktest(userID, strategyID, versionID)
	_, err := s.Create(t.Context(), first)
	require.NoError(t, err)

	second := sampleBacktest(userID, strategyID, versionID)
	_, err = s.Create(t.Context(), second)
	require.Error(t, err)
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr),
		"expected pgconn.PgError, got %T", err)
	require.Equal(t, "23505", pgErr.Code, "should be unique_violation")

	// Cancel the first row; the third insert should now succeed because
	// the partial index only covers status IN (queued,running).
	canceled, err := s.CancelIfRunning(t.Context(), first.ID)
	require.NoError(t, err)
	require.True(t, canceled)

	third := sampleBacktest(userID, strategyID, versionID)
	_, err = s.Create(t.Context(), third)
	require.NoError(t, err, "post-cancel insert should pass the partial index")
}

// TestPgStore_List confirms newest-first ordering and a per-strategy filter.
// Each backtest is cancelled before the next insert so the partial unique
// index on user_id lets the new row through (the index only covers
// status IN (queued,running)).
func TestPgStore_List(t *testing.T) {
	s, userID, strategyID, versionID := newSeededStore(t)
	var createdIDs []string
	for range 3 {
		b := sampleBacktest(userID, strategyID, versionID)
		id, err := s.Create(t.Context(), b)
		require.NoError(t, err)
		createdIDs = append(createdIDs, id)
		// Flip to cancelled so the partial index releases the user_id slot.
		canceled, err := s.CancelIfRunning(t.Context(), id)
		require.NoError(t, err)
		require.True(t, canceled)
		time.Sleep(2 * time.Millisecond) // ponytail: clock skew guard
	}

	items, err := s.List(t.Context(), strategyID, 10)
	require.NoError(t, err)
	require.Len(t, items, 3)

	for _, id := range createdIDs {
		found := false
		for _, it := range items {
			if it.ID == id {
				found = true
				break
			}
		}
		require.True(t, found, "expected backtest %s in list", id)
	}

	// Newest first: items[0] is the last one created.
	require.Equal(t, createdIDs[len(createdIDs)-1], items[0].ID)
}

// TestPgStore_UpdateStatus exercises the queued -> running and
// running -> failed transitions, asserting the partial index lets a new
// row through after the first is terminal.
func TestPgStore_UpdateStatus(t *testing.T) {
	s, userID, strategyID, versionID := newSeededStore(t)
	b := sampleBacktest(userID, strategyID, versionID)
	id, err := s.Create(t.Context(), b)
	require.NoError(t, err)

	started := time.Now().UTC().Truncate(time.Second)
	require.NoError(t, s.UpdateStatus(t.Context(), id, backtest.StatusRunning, &started, nil, ""))

	got, err := s.Get(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, backtest.StatusRunning, got.Status)
	require.NotNil(t, got.StartedAt)
	require.True(t, got.StartedAt.Equal(started))
	require.Nil(t, got.FinishedAt)

	finished := started.Add(5 * time.Second)
	require.NoError(t, s.UpdateStatus(t.Context(), id, backtest.StatusFailed, nil, &finished, "boom"))

	got, err = s.Get(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, backtest.StatusFailed, got.Status)
	require.Equal(t, "boom", got.ErrorMessage)
	require.True(t, got.FinishedAt.Equal(finished))

	// Status terminal -> partial index no longer covers this row, so a
	// fresh active backtest for the same user is allowed.
	fresh := sampleBacktest(userID, strategyID, versionID)
	_, err = s.Create(t.Context(), fresh)
	require.NoError(t, err)
}

// TestPgStore_SetSummary persists a JSONB summary row and verifies the
// status flip + finished_at write.
func TestPgStore_SetSummary(t *testing.T) {
	s, userID, strategyID, versionID := newSeededStore(t)
	id, err := s.Create(t.Context(), sampleBacktest(userID, strategyID, versionID))
	require.NoError(t, err)

	sum := &backtest.Summary{
		TotalReturn:     0.18,
		Sharpe:          1.4,
		MaxDrawdown:     0.05,
		TradeCount:      42,
		WinRate:         0.6,
		StartEquity:     10000,
		EndEquity:       11800,
		ParamsEffective: map[string]string{"stop_loss": "0.02"},
	}
	require.NoError(t, s.SetSummary(t.Context(), id, sum, 42))

	got, err := s.Get(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, backtest.StatusSucceeded, got.Status)
	require.Equal(t, 42, got.FillCount)
	require.NotNil(t, got.Summary)
	require.Equal(t, 0.18, got.Summary.TotalReturn)
	require.Equal(t, 1.4, got.Summary.Sharpe)
	require.Equal(t, 42, got.Summary.TradeCount)
	require.Equal(t, "0.02", got.Summary.ParamsEffective["stop_loss"])
	require.NotNil(t, got.FinishedAt)
}

// TestPgStore_InsertFillsAndGetFills round-trips the per-candle batch insert
// used by the runner's POST /fills callback.
func TestPgStore_InsertFillsAndGetFills(t *testing.T) {
	s, userID, strategyID, versionID := newSeededStore(t)
	id, err := s.Create(t.Context(), sampleBacktest(userID, strategyID, versionID))
	require.NoError(t, err)

	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	fills := []backtest.Fill{
		{Timestamp: base.Add(0 * time.Minute), Side: "buy", Quantity: 1.5, Price: 100, OrderID: "o-1", SimulatedAt: base},
		{Timestamp: base.Add(30 * time.Minute), Side: "sell", Quantity: 1.5, Price: 101.25, OrderID: "o-2", SimulatedAt: base},
		{Timestamp: base.Add(15 * time.Minute), Side: "buy", Quantity: 0.5, Price: 99.5, OrderID: "o-3", SimulatedAt: base},
	}
	require.NoError(t, s.InsertFills(t.Context(), id, fills))

	got, err := s.GetFills(t.Context(), id)
	require.NoError(t, err)
	require.Len(t, got, 3)
	// Order by timestamp,id asc.
	require.Equal(t, "o-1", got[0].OrderID)
	require.Equal(t, "o-3", got[1].OrderID)
	require.Equal(t, "o-2", got[2].OrderID)
	require.Equal(t, "buy", got[0].Side)
	require.Equal(t, "sell", got[2].Side)
	require.InDelta(t, 100.0, got[0].Price, 1e-9)

	// Empty batch is a no-op (not an error).
	require.NoError(t, s.InsertFills(t.Context(), id, nil))
}

// TestPgStore_CancelIfRunning walks the queued -> cancelled transition and
// confirms the wasRunning contract on both the active and terminal paths.
func TestPgStore_CancelIfRunning(t *testing.T) {
	s, userID, strategyID, versionID := newSeededStore(t)
	id, err := s.Create(t.Context(), sampleBacktest(userID, strategyID, versionID))
	require.NoError(t, err)

	canceled, err := s.CancelIfRunning(t.Context(), id)
	require.NoError(t, err)
	require.True(t, canceled, "first cancel should flip queued -> cancelled")

	// Re-cancel on a terminal row: returns false, no error (idempotent).
	canceled, err = s.CancelIfRunning(t.Context(), id)
	require.NoError(t, err)
	require.False(t, canceled, "second cancel should not flip anything")

	got, err := s.Get(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, backtest.StatusCancelled, got.Status)
	require.NotNil(t, got.FinishedAt)
	require.Empty(t, got.ErrorMessage, "cancel should clear error_message")

	// Cancel of a missing id is (false, nil).
	canceled, err = s.CancelIfRunning(t.Context(), uuid.NewString())
	require.NoError(t, err)
	require.False(t, canceled)
}
