package candles_test

import (
	"context"
	"fmt"
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

func TestLoadCandlesBucketedRejectsUnknownResolution(t *testing.T) {
	s := candles.NewPGStore(nil)
	_, err := s.LoadCandlesBucketed(t.Context(), "ob", "7h", time.Time{}, time.Time{})
	require.ErrorContains(t, err, "unknown resolution")
}

func TestLoadCandlesBucketed1mPassThrough(t *testing.T) {
	pool := openTestPool(t)
	store := candles.NewPGStore(pool)
	ctx := context.Background()

	obID := uuid.NewString()
	ts1 := time.Date(2026, 4, 10, 15, 30, 0, 0, time.UTC)
	ts2 := ts1.Add(time.Minute)
	cleanup := func() {
		_, _ = pool.Exec(ctx, `DELETE FROM candles_history WHERE order_book_id = $1`, obID)
	}
	cleanup()
	t.Cleanup(cleanup)
	require.NoError(t, store.SaveCandles(ctx, []candles.StreamCandlesEntry{
		{Time: ts1, Val: candles.Candle{
			OrderBookID: obID, StartTimestamp: ts1,
			Open: decimal.MustParse("100.00"), High: decimal.MustParse("105.00"),
			Low: decimal.MustParse("95.00"), Close: decimal.MustParse("102.50"),
			Volume:  decimal.MustParse("1000"),
			OpenYTM: decimal.MustParse("4.20"), HighYTM: decimal.MustParse("4.30"),
			LowYTM: decimal.MustParse("4.10"), CloseYTM: decimal.MustParse("4.25"),
		}},
		{Time: ts2, Val: candles.Candle{
			OrderBookID: obID, StartTimestamp: ts2,
			Open: decimal.MustParse("102.50"), High: decimal.MustParse("106.00"),
			Low: decimal.MustParse("102.00"), Close: decimal.MustParse("104.75"),
			Volume:  decimal.MustParse("1500"),
			OpenYTM: decimal.MustParse("4.25"), HighYTM: decimal.MustParse("4.35"),
			LowYTM: decimal.MustParse("4.20"), CloseYTM: decimal.MustParse("4.30"),
		}},
	}))

	got, err := store.LoadCandlesBucketed(ctx, obID, "1m", ts1, ts2.Add(time.Second))
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.True(t, got[0].StartTimestamp.Equal(ts1))
	assert.True(t, got[0].Open.Equal(decimal.MustParse("100.00")))
	assert.True(t, got[1].StartTimestamp.Equal(ts2))
	assert.True(t, got[1].CloseYTM.Equal(decimal.MustParse("4.30")))
}

// TestLoadCandlesBucketed5mFolds exercises the copied bucketing SQL: three
// 1m candles inside one 5m bucket must fold to one bar (open of first,
// close of last, summed volume) plus a flat-filled empty next bucket.
func TestLoadCandlesBucketed5mFolds(t *testing.T) {
	pool := openTestPool(t)
	store := candles.NewPGStore(pool)
	ctx := context.Background()

	obID := uuid.NewString()
	bucket := time.Date(2026, 4, 10, 15, 30, 0, 0, time.UTC)
	cleanup := func() {
		_, _ = pool.Exec(ctx, `DELETE FROM candles_history WHERE order_book_id = $1`, obID)
	}
	cleanup()
	t.Cleanup(cleanup)
	var entries []candles.StreamCandlesEntry
	for i := range 3 {
		ts := bucket.Add(time.Duration(i) * time.Minute)
		entries = append(entries, candles.StreamCandlesEntry{Time: ts, Val: candles.Candle{
			OrderBookID: obID, StartTimestamp: ts,
			Open:    decimal.MustParse(fmt.Sprintf("%d.00", 100+i)),
			High:    decimal.MustParse(fmt.Sprintf("%d.00", 110+i)),
			Low:     decimal.MustParse(fmt.Sprintf("%d.00", 90+i)),
			Close:   decimal.MustParse(fmt.Sprintf("%d.50", 100+i)),
			Volume:  decimal.MustParse("100"),
			OpenYTM: decimal.MustParse(fmt.Sprintf("4.%d0", i)),
			HighYTM: decimal.MustParse("4.90"), LowYTM: decimal.MustParse("4.10"),
			CloseYTM: decimal.MustParse("4.50"),
		}})
	}
	require.NoError(t, store.SaveCandles(ctx, entries))

	got, err := store.LoadCandlesBucketed(ctx, obID, "5m", bucket, bucket.Add(10*time.Minute))
	require.NoError(t, err)
	require.Len(t, got, 2) // 2 buckets in [15:30, 15:40): one folded, one flat-filled
	assert.True(t, got[0].StartTimestamp.Equal(bucket))
	assert.True(t, got[0].Open.Equal(decimal.MustParse("100.00")))
	assert.True(t, got[0].Close.Equal(decimal.MustParse("102.50")))
	assert.True(t, got[0].High.Equal(decimal.MustParse("112.00")))
	assert.True(t, got[0].Low.Equal(decimal.MustParse("90.00")))
	assert.True(t, got[0].Volume.Equal(decimal.MustParse("300")))
	assert.True(t, got[0].OpenYTM.Equal(decimal.MustParse("4.00")))
	// Empty second bucket: FLAT-FILLED at the previous bucket's close
	// (not forward-filled). All four prices + all four YTMs collapse to
	// the previous populated bucket's close/close_ytm, volume zero.
	// Flat-fill avoids phantom stops: with the prior low of 90, a
	// forward-fill would inherit that low into the empty cell and any
	// strategy entering at the prior close of 102.50 would falsely
	// see a stop-out — true-range ATR would also re-count the old
	// 90-112 range as new movement.
	assert.True(t, got[1].StartTimestamp.Equal(bucket.Add(5*time.Minute)))
	assert.True(t, got[1].Open.Equal(got[0].Close), "empty bucket open must equal prev close (flat-fill)")
	assert.True(t, got[1].High.Equal(got[0].Close), "empty bucket high must equal prev close (flat-fill)")
	assert.True(t, got[1].Low.Equal(got[0].Close), "empty bucket low must equal prev close (flat-fill)")
	assert.True(t, got[1].Close.Equal(got[0].Close), "empty bucket close must equal prev close (flat-fill)")
	assert.True(t, got[1].Volume.Equal(decimal.MustParse("0")))
	assert.True(t, got[1].OpenYTM.Equal(got[0].CloseYTM), "empty bucket open_ytm must equal prev close_ytm")
	assert.True(t, got[1].HighYTM.Equal(got[0].CloseYTM), "empty bucket high_ytm must equal prev close_ytm")
	assert.True(t, got[1].LowYTM.Equal(got[0].CloseYTM), "empty bucket low_ytm must equal prev close_ytm")
	assert.True(t, got[1].CloseYTM.Equal(got[0].CloseYTM), "empty bucket close_ytm must equal prev close_ytm")
}

// TestLoadCandlesBucketedLeadingGap covers the edge where the requested
// `since` precedes the book's first candle. Without a leading-gap filter,
// the grid cells before the first populated bucket have no predecessor
// either, yielding a NULL row that crashes the scan into a plain Go
// string with "cannot scan NULL into *string" (the public LoadCandles
// API contract assumes non-NULL OHLC). The fix must drop those rows in
// SQL (not skip post-scan — keyset pagination stops on `len(rows) <
// maxCandleRows`) and start the returned series at the first
// populated bucket.
func TestLoadCandlesBucketedLeadingGap(t *testing.T) {
	pool := openTestPool(t)
	store := candles.NewPGStore(pool)
	ctx := context.Background()

	obID := uuid.NewString()
	// 7 empty grid cells (15:00-15:30) before the first candle at 15:35.
	since := time.Date(2026, 4, 12, 15, 0, 0, 0, time.UTC)
	first := time.Date(2026, 4, 12, 15, 35, 0, 0, time.UTC)
	until := first.Add(10 * time.Minute) // [15:35, 15:45) — two grid cells
	cleanup := func() {
		_, _ = pool.Exec(ctx, `DELETE FROM candles_history WHERE order_book_id = $1`, obID)
	}
	cleanup()
	t.Cleanup(cleanup)
	require.NoError(t, store.SaveCandles(ctx, []candles.StreamCandlesEntry{
		{Time: first, Val: candles.Candle{
			OrderBookID: obID, StartTimestamp: first,
			Open: decimal.MustParse("100.00"), High: decimal.MustParse("105.00"),
			Low: decimal.MustParse("95.00"), Close: decimal.MustParse("102.50"),
			Volume:  decimal.MustParse("1000"),
			OpenYTM: decimal.MustParse("4.20"), HighYTM: decimal.MustParse("4.30"),
			LowYTM: decimal.MustParse("4.10"), CloseYTM: decimal.MustParse("4.25"),
		}},
	}))

	got, err := store.LoadCandlesBucketed(ctx, obID, "5m", since, until)
	require.NoError(t, err, "leading-gap buckets with no predecessor must be filtered, not surfaced as NULL")
	// Two buckets returned: 15:35 (populated) + 15:40 (flat-filled from
	// 15:35 close). The seven empty predecessors (15:00-15:30) are
	// dropped entirely.
	require.Len(t, got, 2)
	assert.True(t, got[0].StartTimestamp.Equal(first), "series must start at the first populated bucket")
	assert.True(t, got[0].Open.Equal(decimal.MustParse("100.00")))
	assert.True(t, got[0].Close.Equal(decimal.MustParse("102.50")))
	// Second bucket (15:40) is empty but has a predecessor → flat-fill
	// at 15:35's close. No zero-price bars.
	assert.True(t, got[1].StartTimestamp.Equal(first.Add(5*time.Minute)))
	assert.True(t, got[1].Open.Equal(decimal.MustParse("102.50")), "no zero-price bars after leading gap")
	assert.True(t, got[1].High.Equal(decimal.MustParse("102.50")))
	assert.True(t, got[1].Low.Equal(decimal.MustParse("102.50")))
	assert.True(t, got[1].Close.Equal(decimal.MustParse("102.50")))
	assert.True(t, got[1].Volume.Equal(decimal.MustParse("0")))
}

func TestLoadCandlesBucketedPaginates(t *testing.T) {
	pool := openTestPool(t)
	store := candles.NewPGStore(pool)
	ctx := context.Background()

	obID := uuid.NewString()
	ts1 := time.Date(2026, 4, 10, 15, 30, 0, 0, time.UTC)
	cleanup := func() {
		_, _ = pool.Exec(ctx, `DELETE FROM candles_history WHERE order_book_id = $1`, obID)
	}
	cleanup()
	t.Cleanup(cleanup)
	var entries []candles.StreamCandlesEntry
	for i := range 5 {
		ts := ts1.Add(time.Duration(i) * time.Minute)
		entries = append(entries, candles.StreamCandlesEntry{Time: ts, Val: candles.Candle{
			OrderBookID: obID, StartTimestamp: ts,
			Open: decimal.MustParse("100.00"), High: decimal.MustParse("101.00"),
			Low: decimal.MustParse("99.00"), Close: decimal.MustParse("100.50"),
			Volume:  decimal.MustParse("10"),
			OpenYTM: decimal.MustParse("4.20"), HighYTM: decimal.MustParse("4.30"),
			LowYTM: decimal.MustParse("4.10"), CloseYTM: decimal.MustParse("4.25"),
		}})
	}
	require.NoError(t, store.SaveCandles(ctx, entries))

	prev := *candles.MaxCandleRowsForTest
	*candles.MaxCandleRowsForTest = 2
	t.Cleanup(func() { *candles.MaxCandleRowsForTest = prev })

	got, err := store.LoadCandlesBucketed(ctx, obID, "1m", ts1, ts1.Add(5*time.Minute))
	require.NoError(t, err)
	require.Len(t, got, 5)
	for i := range got { // oldest-first across pages
		assert.True(t, got[i].StartTimestamp.Equal(ts1.Add(time.Duration(i)*time.Minute)), "row %d", i)
	}
}

func TestCandleRange(t *testing.T) {
	pool := openTestPool(t)
	store := candles.NewPGStore(pool)
	ctx := context.Background()

	obID := uuid.NewString()
	ts1 := time.Date(2026, 4, 10, 15, 30, 0, 0, time.UTC)
	ts2 := ts1.Add(3 * time.Minute)
	cleanup := func() {
		_, _ = pool.Exec(ctx, `DELETE FROM candles_history WHERE order_book_id = $1`, obID)
	}
	cleanup()
	t.Cleanup(cleanup)
	require.NoError(t, store.SaveCandles(ctx, []candles.StreamCandlesEntry{
		{Time: ts1, Val: candles.Candle{
			OrderBookID: obID, StartTimestamp: ts1,
			Open: decimal.One, High: decimal.One, Low: decimal.One, Close: decimal.One,
			Volume: decimal.One, OpenYTM: decimal.One, HighYTM: decimal.One,
			LowYTM: decimal.One, CloseYTM: decimal.One,
		}},
		{Time: ts2, Val: candles.Candle{
			OrderBookID: obID, StartTimestamp: ts2,
			Open: decimal.One, High: decimal.One, Low: decimal.One, Close: decimal.One,
			Volume: decimal.One, OpenYTM: decimal.One, HighYTM: decimal.One,
			LowYTM: decimal.One, CloseYTM: decimal.One,
		}},
	}))

	lo, hi, err := store.CandleRange(ctx, obID)
	require.NoError(t, err)
	require.NotNil(t, lo)
	require.NotNil(t, hi)
	assert.True(t, lo.Equal(ts1))
	assert.True(t, hi.Equal(ts2))

	// No rows for an unknown book: nil, nil, no error.
	lo, hi, err = store.CandleRange(ctx, uuid.NewString())
	require.NoError(t, err)
	assert.Nil(t, lo)
	assert.Nil(t, hi)
}

// TestLoadCandlesBucketedLegacyNullYTMs covers the migration 014 upgrade
// path: candles_history rows persisted before the four YTM columns
// existed have NULL open_ytm/high_ytm/low_ytm/close_ytm. fetchCandlesSQL
// must normalise those NULLs to '0' in both the 1m pass-through and the
// bucketing branches so the scan into a plain Go string succeeds and
// strategies (breakout, meanreversion, momentum) can apply their own
// zero-YTM-means-missing handling.
func TestLoadCandlesBucketedLegacyNullYTMs(t *testing.T) {
	pool := openTestPool(t)
	store := candles.NewPGStore(pool)
	ctx := context.Background()

	obID := uuid.NewString()
	ts1 := time.Date(2026, 4, 11, 10, 0, 0, 0, time.UTC)
	ts2 := ts1.Add(time.Minute)
	cleanup := func() {
		_, _ = pool.Exec(ctx, `DELETE FROM candles_history WHERE order_book_id = $1`, obID)
	}
	cleanup()
	t.Cleanup(cleanup)

	// Raw inserts that omit the four YTM columns (they default to NULL).
	// Do NOT use SaveCandles — it always writes every column.
	for _, ts := range []time.Time{ts1, ts2} {
		_, err := pool.Exec(ctx, `INSERT INTO candles_history
            (order_book_id, start_timestamp, open, high, low, close, volume)
            VALUES ($1, $2, '100.00', '105.00', '95.00', '102.50', '0')`, obID, ts)
		require.NoError(t, err)
	}

	// 1m pass-through branch: NULL YTMs must scan as decimal zero.
	got1m, err := store.LoadCandlesBucketed(ctx, obID, "1m", ts1, ts2.Add(time.Second))
	require.NoError(t, err)
	require.Len(t, got1m, 2)
	for _, c := range got1m {
		assert.True(t, c.OpenYTM.Equal(decimal.Zero), "open_ytm not zero: %s", c.OpenYTM)
		assert.True(t, c.HighYTM.Equal(decimal.Zero), "high_ytm not zero: %s", c.HighYTM)
		assert.True(t, c.LowYTM.Equal(decimal.Zero), "low_ytm not zero: %s", c.LowYTM)
		assert.True(t, c.CloseYTM.Equal(decimal.Zero), "close_ytm not zero: %s", c.CloseYTM)
	}

	// 5m bucketing branch: folds both rows and flat-fills the empty
	// next bucket; every YTM must be zero, no scan error.
	bucket := time.Date(2026, 4, 11, 10, 0, 0, 0, time.UTC)
	got5m, err := store.LoadCandlesBucketed(ctx, obID, "5m", bucket, bucket.Add(10*time.Minute))
	require.NoError(t, err)
	require.NotEmpty(t, got5m)
	for _, c := range got5m {
		assert.True(t, c.OpenYTM.Equal(decimal.Zero), "5m open_ytm not zero: %s", c.OpenYTM)
		assert.True(t, c.HighYTM.Equal(decimal.Zero), "5m high_ytm not zero: %s", c.HighYTM)
		assert.True(t, c.LowYTM.Equal(decimal.Zero), "5m low_ytm not zero: %s", c.LowYTM)
		assert.True(t, c.CloseYTM.Equal(decimal.Zero), "5m close_ytm not zero: %s", c.CloseYTM)
	}

	// LoadCandles (non-bucketed) shares the same NULL→string scan
	// contract. Same fixture: all four YTMs must be decimal zero.
	t.Run("LoadCandles", func(t *testing.T) {
		got, err := store.LoadCandles(ctx, obID, ts1, ts2)
		require.NoError(t, err)
		require.Len(t, got, 2)
		for _, c := range got {
			assert.True(t, c.OpenYTM.Equal(decimal.Zero), "LoadCandles open_ytm: %s", c.OpenYTM)
			assert.True(t, c.HighYTM.Equal(decimal.Zero), "LoadCandles high_ytm: %s", c.HighYTM)
			assert.True(t, c.LowYTM.Equal(decimal.Zero), "LoadCandles low_ytm: %s", c.LowYTM)
			assert.True(t, c.CloseYTM.Equal(decimal.Zero), "LoadCandles close_ytm: %s", c.CloseYTM)
		}
	})
}
