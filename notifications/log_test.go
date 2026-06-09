package notifications_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/notifications"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// openTestPool returns a pool pointed at $DATABASE_URL or skips the test.
func openTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; skipping PG log test")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

func TestPGLog_InsertAndReplay(t *testing.T) {
	pool := openTestPool(t)
	log := notifications.NewPGLog(pool)
	ctx := context.Background()
	uid := uuid.NewString()

	a := notifications.Event{ID: uuid.NewString(), Type: notifications.EventRunStarted, UserID: uid, Timestamp: time.Now().UTC()}
	b := notifications.Event{ID: uuid.NewString(), Type: notifications.EventRunStopped, UserID: uid, Timestamp: time.Now().UTC()}
	require.NoError(t, log.Insert(ctx, a))
	require.NoError(t, log.Insert(ctx, b))

	got, err := log.Replay(ctx, uid, a.ID, 10)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, b.ID, got[0].ID)
}
