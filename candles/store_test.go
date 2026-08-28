package candles_test

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

	"github.com/dora-network/bond-trading-strategies/candles"
)

// openTestPool returns a pool pointed at $DATABASE_URL or skips the test.
// Mirrors the convention in notifications/log_test.go — these PG-backed
// tests are opt-in via env so the rest of the suite stays hermetic.
func openTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; skipping PG candle test")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

// TestPGStore_LoadCandles round-trips a candle (OHLCV + the four new YTM
// columns) through SaveCandles + LoadCandles and asserts every field
// round-trips by value. Uses a unique order_book_id per run so concurrent
// invocations don't collide on the PK.
func TestPGStore_LoadCandles(t *testing.T) {
	pool := openTestPool(t)
	store := candles.NewPGStore(pool)
	ctx := context.Background()

	obID := uuid.NewString()
	// Two distinct candles so the window filter and order-by both fire.
	ts1 := time.Date(2026, 4, 10, 15, 30, 0, 0, time.UTC)
	ts2 := ts1.Add(time.Minute)
	cleanup := func() {
		_, _ = pool.Exec(ctx, `DELETE FROM candles_history WHERE order_book_id = $1`, obID)
	}
	cleanup()
	t.Cleanup(cleanup)

	entries := []candles.StreamCandlesEntry{
		{
			Time: ts1,
			Val: candles.Candle{
				OrderBookID:    obID,
				StartTimestamp: ts1,
				Open:           decimal.MustParse("100.00"),
				High:           decimal.MustParse("105.00"),
				Low:            decimal.MustParse("95.00"),
				Close:          decimal.MustParse("102.50"),
				Volume:         decimal.MustParse("1000"),
				OpenYTM:        decimal.MustParse("4.20"),
				HighYTM:        decimal.MustParse("4.30"),
				LowYTM:         decimal.MustParse("4.10"),
				CloseYTM:       decimal.MustParse("4.25"),
			},
		},
		{
			Time: ts2,
			Val: candles.Candle{
				OrderBookID:    obID,
				StartTimestamp: ts2,
				Open:           decimal.MustParse("102.50"),
				High:           decimal.MustParse("106.00"),
				Low:            decimal.MustParse("102.00"),
				Close:          decimal.MustParse("104.75"),
				Volume:         decimal.MustParse("1500"),
				OpenYTM:        decimal.MustParse("4.25"),
				HighYTM:        decimal.MustParse("4.35"),
				LowYTM:         decimal.MustParse("4.20"),
				CloseYTM:       decimal.MustParse("4.30"),
			},
		},
	}
	require.NoError(t, store.SaveCandles(ctx, entries))

	// Ascending order, inclusive on both ends.
	got, err := store.LoadCandles(ctx, obID, ts1, ts2)
	require.NoError(t, err)
	require.Len(t, got, 2)

	want := []candles.Candle{entries[0].Val, entries[1].Val}
	for i := range got {
		assert.Equal(t, want[i].OrderBookID, got[i].OrderBookID, "OrderBookID row %d", i)
		assert.True(t, got[i].StartTimestamp.Equal(want[i].StartTimestamp), "StartTimestamp row %d", i)
		assert.True(t, got[i].Open.Equal(want[i].Open), "Open row %d", i)
		assert.True(t, got[i].High.Equal(want[i].High), "High row %d", i)
		assert.True(t, got[i].Low.Equal(want[i].Low), "Low row %d", i)
		assert.True(t, got[i].Close.Equal(want[i].Close), "Close row %d", i)
		assert.True(t, got[i].Volume.Equal(want[i].Volume), "Volume row %d", i)
		assert.True(t, got[i].OpenYTM.Equal(want[i].OpenYTM), "OpenYTM row %d", i)
		assert.True(t, got[i].HighYTM.Equal(want[i].HighYTM), "HighYTM row %d", i)
		assert.True(t, got[i].LowYTM.Equal(want[i].LowYTM), "LowYTM row %d", i)
		assert.True(t, got[i].CloseYTM.Equal(want[i].CloseYTM), "CloseYTM row %d", i)
	}

	// Upsert path: same PK, fresh values must overwrite — including the
	// YTM columns, not just OHLCV. Guards against future regressions
	// where the upsert forgets a new column.
	require.NoError(t, store.SaveCandles(ctx, []candles.StreamCandlesEntry{{
		Time: ts1,
		Val: candles.Candle{
			OrderBookID:    obID,
			StartTimestamp: ts1,
			Open:           decimal.MustParse("200.00"),
			High:           decimal.MustParse("205.00"),
			Low:            decimal.MustParse("195.00"),
			Close:          decimal.MustParse("202.50"),
			Volume:         decimal.MustParse("2000"),
			OpenYTM:        decimal.MustParse("5.20"),
			HighYTM:        decimal.MustParse("5.30"),
			LowYTM:         decimal.MustParse("5.10"),
			CloseYTM:       decimal.MustParse("5.25"),
		},
	}}))

	got, err = store.LoadCandles(ctx, obID, ts1, ts1)
	require.NoError(t, err)
	require.Len(t, got, 1)
	upserted := candles.Candle{
		OrderBookID:    obID,
		StartTimestamp: ts1,
		Open:           decimal.MustParse("200.00"),
		High:           decimal.MustParse("205.00"),
		Low:            decimal.MustParse("195.00"),
		Close:          decimal.MustParse("202.50"),
		Volume:         decimal.MustParse("2000"),
		OpenYTM:        decimal.MustParse("5.20"),
		HighYTM:        decimal.MustParse("5.30"),
		LowYTM:         decimal.MustParse("5.10"),
		CloseYTM:       decimal.MustParse("5.25"),
	}
	assert.True(t, got[0].Open.Equal(upserted.Open))
	assert.True(t, got[0].OpenYTM.Equal(upserted.OpenYTM))
	assert.True(t, got[0].CloseYTM.Equal(upserted.CloseYTM))

	// Window filter: only ts2 falls inside [ts2, ts2].
	got, err = store.LoadCandles(ctx, obID, ts2, ts2)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.True(t, got[0].StartTimestamp.Equal(ts2))
}
