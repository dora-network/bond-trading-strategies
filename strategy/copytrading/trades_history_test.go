package copytrading

import (
	"context"
	"testing"
	"time"

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
