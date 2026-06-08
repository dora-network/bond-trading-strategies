package copytrading

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/govalues/decimal"
	"github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/require"
)

func TestPGTradesHistoryStore_TradeBounds_Empty(t *testing.T) {
	t.Parallel()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	followed := "11111111-1111-1111-1111-111111111111"
	mock.ExpectQuery(`SELECT MIN\(created_at\), MAX\(created_at\), COUNT\(\*\) FROM trades_history WHERE user_id = \$1`).
		WithArgs(followed).
		WillReturnRows(pgxmock.NewRows([]string{"min", "max", "count"}).
			AddRow(nil, nil, 0))

	store := NewPGTradesHistoryStore(mock)
	min, max, count, err := store.TradeBounds(context.Background(), followed)
	require.NoError(t, err)
	require.True(t, min.IsZero())
	require.True(t, max.IsZero())
	require.Equal(t, 0, count)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPGTradesHistoryStore_TradeBounds_Populated(t *testing.T) {
	t.Parallel()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	followed := "11111111-1111-1111-1111-111111111111"
	lo := time.Date(2026, 5, 26, 10, 0, 0, 0, time.UTC)
	hi := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT MIN\(created_at\), MAX\(created_at\), COUNT\(\*\) FROM trades_history WHERE user_id = \$1`).
		WithArgs(followed).
		WillReturnRows(pgxmock.NewRows([]string{"min", "max", "count"}).
			AddRow(lo, hi, 123))

	store := NewPGTradesHistoryStore(mock)
	min, max, count, err := store.TradeBounds(context.Background(), followed)
	require.NoError(t, err)
	require.True(t, min.Equal(lo))
	require.True(t, max.Equal(hi))
	require.Equal(t, 123, count)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPGTradesHistoryStore_StreamTrades_Empty(t *testing.T) {
	t.Parallel()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	followed := "11111111-1111-1111-1111-111111111111"
	start := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC)

	mock.ExpectQuery(`SELECT .* FROM trades_history WHERE user_id = \$1`).
		WithArgs(followed, start, end, pgxmock.AnyArg()).
		WillReturnRows(pgxmock.NewRows([]string{
			"transaction_id", "order_id", "order_seq", "orderbook_id",
			"user_id", "asset", "quantity", "price", "side",
			"aggressor_indicator", "created_at",
		}))

	store := NewPGTradesHistoryStore(mock)
	ch, done := store.StreamTrades(context.Background(), followed, start, end)

	count := 0
	for range ch {
		count++
	}
	require.Equal(t, 0, count)

	require.NoError(t, <-done)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPGTradesHistoryStore_StreamTrades_RangeFilter(t *testing.T) {
	t.Parallel()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	followed := "11111111-1111-1111-1111-111111111111"
	start := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC)
	lo := time.Date(2026, 5, 26, 10, 0, 0, 0, time.UTC)

	mock.ExpectQuery(`SELECT .* FROM trades_history WHERE user_id = \$1`).
		WithArgs(followed, start, end, pgxmock.AnyArg()).
		WillReturnRows(pgxmock.NewRows([]string{
			"transaction_id", "order_id", "order_seq", "orderbook_id",
			"user_id", "asset", "quantity", "price", "side",
			"aggressor_indicator", "created_at",
		}).
			AddRow("tx-1", "ord-1", int64(1), "ob-1", followed, "bond-a", "1.0", "100", "BUY", true, lo))

	store := NewPGTradesHistoryStore(mock)
	ch, done := store.StreamTrades(context.Background(), followed, start, end)

	var got []Trade
	for trade := range ch {
		got = append(got, trade)
	}
	require.NoError(t, <-done)
	require.Len(t, got, 1)
	require.Equal(t, "tx-1", got[0].TransactionID)
	require.Equal(t, "BUY", got[0].Side)
	require.True(t, got[0].Price.Equal(decimal.MustParse("100")))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPGTradesHistoryStore_StreamTrades_Ordered(t *testing.T) {
	t.Parallel()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	followed := "11111111-1111-1111-1111-111111111111"
	start := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC)

	rows := pgxmock.NewRows([]string{
		"transaction_id", "order_id", "order_seq", "orderbook_id",
		"user_id", "asset", "quantity", "price", "side",
		"aggressor_indicator", "created_at",
	})
	// 999 rows fits a single batch (batchSize=1000) so the loop
	// terminates after the first query. The multi-batch keyset path
	// is covered by TestPGTradesHistoryStore_StreamTrades_MultiBatchKeyset.
	for i := 0; i < 999; i++ {
		rows.AddRow(
			"tx-"+strconv.Itoa(i),
			"ord-"+strconv.Itoa(i),
			int64(i),
			"ob-1",
			followed,
			"bond-a",
			"1.0",
			"100",
			"BUY",
			true,
			start.Add(time.Duration(i)*time.Millisecond),
		)
	}

	mock.ExpectQuery(`SELECT .* FROM trades_history WHERE user_id = \$1`).
		WithArgs(followed, start, end, pgxmock.AnyArg()).
		WillReturnRows(rows)

	store := NewPGTradesHistoryStore(mock)
	ch, done := store.StreamTrades(context.Background(), followed, start, end)

	var lastTS time.Time
	count := 0
	for trade := range ch {
		if !lastTS.IsZero() && trade.CreatedAt.Before(lastTS) {
			t.Fatalf("out-of-order: %v after %v", trade.CreatedAt, lastTS)
		}
		lastTS = trade.CreatedAt
		count++
	}
	require.NoError(t, <-done)
	require.Equal(t, 999, count)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPGTradesHistoryStore_StreamTrades_MultiBatchKeyset(t *testing.T) {
	t.Parallel()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	followed := "11111111-1111-1111-1111-111111111111"
	start := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC)

	// First batch fills the 1000-row LIMIT exactly, forcing the loop
	// to issue a second query using the keyset cursor.
	firstBatch := pgxmock.NewRows([]string{
		"transaction_id", "order_id", "order_seq", "orderbook_id",
		"user_id", "asset", "quantity", "price", "side",
		"aggressor_indicator", "created_at",
	})
	for i := 0; i < 1000; i++ {
		firstBatch.AddRow(
			"tx-"+strconv.Itoa(i),
			"ord-"+strconv.Itoa(i),
			int64(i),
			"ob-1",
			followed,
			"bond-a",
			"1.0",
			"100",
			"BUY",
			true,
			start.Add(time.Duration(i)*time.Millisecond),
		)
	}
	// Last row of the first batch becomes the cursor: (created_at,
	// transaction_id) of tx-999.
	cursorTime := start.Add(999 * time.Millisecond)
	cursorID := "tx-999"

	// Second batch is one row, which terminates the loop
	// (batchCount < batchSize).
	secondBatch := pgxmock.NewRows([]string{
		"transaction_id", "order_id", "order_seq", "orderbook_id",
		"user_id", "asset", "quantity", "price", "side",
		"aggressor_indicator", "created_at",
	}).AddRow(
		"tx-1000", "ord-1000", int64(1000), "ob-1", followed,
		"bond-a", "1.0", "100", "BUY", true,
		cursorTime.Add(time.Millisecond),
	)

	mock.ExpectQuery(`SELECT .* FROM trades_history WHERE user_id = \$1`).
		WithArgs(followed, start, end, pgxmock.AnyArg()).
		WillReturnRows(firstBatch)
	mock.ExpectQuery(`SELECT .* FROM trades_history WHERE user_id = \$1`).
		WithArgs(followed, start, end, cursorTime, cursorID, pgxmock.AnyArg()).
		WillReturnRows(secondBatch)

	store := NewPGTradesHistoryStore(mock)
	ch, done := store.StreamTrades(context.Background(), followed, start, end)

	var lastTS time.Time
	count := 0
	for trade := range ch {
		if !lastTS.IsZero() && trade.CreatedAt.Before(lastTS) {
			t.Fatalf("out-of-order: %v after %v", trade.CreatedAt, lastTS)
		}
		lastTS = trade.CreatedAt
		count++
	}
	require.NoError(t, <-done)
	require.Equal(t, 1001, count)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPGTradesHistoryStore_StreamTrades_ContextCancelled(t *testing.T) {
	t.Parallel()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	followed := "11111111-1111-1111-1111-111111111111"
	start := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC)

	mock.ExpectQuery(`SELECT .* FROM trades_history WHERE user_id = \$1`).
		WithArgs(followed, start, end, pgxmock.AnyArg()).
		WillReturnError(context.Canceled)

	store := NewPGTradesHistoryStore(mock)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ch, done := store.StreamTrades(ctx, followed, start, end)
	for range ch {
	}
	err = <-done
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}
