package trades

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/require"
)

func TestPGStore_StreamTrades_Empty(t *testing.T) {
	t.Parallel()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	ob := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	start := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC)

	mock.ExpectQuery(`SELECT .* FROM trades_history WHERE orderbook_id = \$1`).
		WithArgs(ob, start, end, pgxmock.AnyArg()).
		WillReturnRows(pgxmock.NewRows([]string{
			"transaction_id", "price", "quantity", "side", "created_at",
		}))

	store := &PGStore{pool: mock}
	ch, done := store.StreamTrades(context.Background(), ob, start, end)

	count := 0
	for range ch {
		count++
	}
	require.Equal(t, 0, count)
	require.NoError(t, <-done)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPGStore_StreamTrades_Rows(t *testing.T) {
	t.Parallel()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	ob := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	start := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC)
	ts1 := time.Date(2026, 5, 26, 10, 0, 0, 0, time.UTC)
	ts2 := time.Date(2026, 5, 26, 11, 0, 0, 0, time.UTC)
	hundred := decimal.MustNew(100, 0)

	rows := pgxmock.NewRows([]string{
		"transaction_id", "price", "quantity", "side", "created_at",
	}).
		AddRow("22222222-2222-2222-2222-222222222221", "100", "1.5", "BUY", ts1).
		AddRow("22222222-2222-2222-2222-222222222222", "101", "2", "SELL", ts2)
	mock.ExpectQuery(`SELECT .* FROM trades_history WHERE orderbook_id = \$1`).
		WithArgs(ob, start, end, pgxmock.AnyArg()).
		WillReturnRows(rows)

	store := &PGStore{pool: mock}
	ch, done := store.StreamTrades(context.Background(), ob, start, end)

	var got []Trade
	for tr := range ch {
		got = append(got, tr)
	}
	require.NoError(t, <-done)

	require.Len(t, got, 2)
	require.Equal(t, "BUY", got[0].Side)
	require.True(t, got[0].Time.Equal(ts1))
	require.True(t, got[0].Price.Equal(hundred), "price must round-trip")
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestPGStore_StreamTrades_ConsumerCancelExitsGoroutine proves the
// drain-on-cancel contract: when the consumer stops reading and the
// channel buffer (256) fills, cancelling ctx must unblock the send
// loop and exit the streaming goroutine (both channels close) instead
// of leaking it. This replaces the former strategy/http
// mrTradeHistoryAdapter drain fix.
func TestPGStore_StreamTrades_ConsumerCancelExitsGoroutine(t *testing.T) {
	t.Parallel()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	ob := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	start := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC)

	// 300 rows > the 256-slot channel buffer, so the send loop blocks
	// mid-page once the consumer stops draining.
	rows := pgxmock.NewRows([]string{
		"transaction_id", "price", "quantity", "side", "created_at",
	})
	for i := range 300 {
		rows.AddRow(
			fmt.Sprintf("22222222-2222-2222-2222-%012d", i),
			"100", "1", "BUY",
			start.Add(time.Duration(i)*time.Second),
		)
	}
	mock.ExpectQuery(`SELECT .* FROM trades_history WHERE orderbook_id = \$1`).
		WithArgs(ob, start, end, pgxmock.AnyArg()).
		WillReturnRows(rows)

	store := &PGStore{pool: mock}
	ctx, cancel := context.WithCancel(context.Background())
	ch, done := store.StreamTrades(ctx, ob, start, end)

	// Read one trade, then abandon the channel and cancel.
	<-ch
	cancel()

	// The goroutine must exit (delivering ctx cancellation to done and
	// closing both channels) rather than blocking forever on the send.
	select {
	case err, ok := <-done:
		require.True(t, ok, "done channel must be closed")
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("streaming goroutine did not exit after consumer cancel")
	}

	// ch closes alongside done; it may still hold buffered trades, so
	// drain until close (bounded) instead of expecting one read to
	// observe the close.
	deadline := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("trade channel did not close after consumer cancel")
		}
	}
}
