package prices_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/prices"
)

// openTestPool returns a pool pointed at $DATABASE_URL or skips the test.
// Mirrors the convention in candles/store_test.go and
// notifications/log_test.go — these PG-backed tests are opt-in via env
// so the rest of the suite stays hermetic.
func openTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; skipping PG price test")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

// TestPGStore_SavePrices_UpsertReconnectReplay exercises the reconnect
// scenario: same (asset_id, timestamp) PK seen twice with different
// prices. Without ON CONFLICT DO UPDATE the second SavePrices would
// error on duplicate key and drop the whole batch.
func TestPGStore_SavePrices_UpsertReconnectReplay(t *testing.T) {
	pool := openTestPool(t)
	store := prices.NewPGStore(pool)
	ctx := context.Background()

	assetID := uuid.NewString()
	ts := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	cleanup := func() {
		_, _ = pool.Exec(ctx, `DELETE FROM price_history WHERE asset_id = $1`, assetID)
	}
	cleanup()
	t.Cleanup(cleanup)

	first := prices.AssetPrice{
		AssetID: assetID,
		Price:   decimal.MustParse("100.00"),
		YTM:     ptrDecimal("5.00"),
		Time:    ts,
	}
	require.NoError(t, store.SavePrices(ctx, map[uuid.UUID]prices.AssetPrice{uuid.New(): first}))

	// Same PK, fresh values. The reconnect-replay scenario.
	second := prices.AssetPrice{
		AssetID: assetID,
		Price:   decimal.MustParse("101.50"),
		YTM:     ptrDecimal("5.10"),
		Time:    ts,
	}
	require.NoError(t, store.SavePrices(ctx, map[uuid.UUID]prices.AssetPrice{uuid.New(): second}))

	// Verify the upsert overwrote (not appended a duplicate row).
	var count int
	require.NoError(t, pool.QueryRow(
		ctx,
		`SELECT COUNT(*) FROM price_history WHERE asset_id = $1`, assetID,
	).Scan(&count))
	assert.Equal(t, 1, count, "second SavePrices should upsert, not insert a duplicate")

	// Verify the new values landed.
	var priceStr string
	var ytmStr *string
	require.NoError(t, pool.QueryRow(
		ctx,
		`SELECT price::text, ytm::text FROM price_history WHERE asset_id = $1`, assetID,
	).Scan(&priceStr, &ytmStr))
	assert.Equal(t, "101.50", priceStr)
	require.NotNil(t, ytmStr)
	assert.Equal(t, "5.10", *ytmStr)
}

func ptrDecimal(s string) *decimal.Decimal {
	d := decimal.MustParse(s)
	return &d
}
