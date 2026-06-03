package copytrading

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/govalues/decimal"
	"github.com/jackc/pgx/v5"
)

// Trade is the in-memory representation of a single row in trades_history.
// It is intentionally decoupled from doraclient.Trade so the simulation
// does not depend on the DORA client's generated types.
type Trade struct {
	TransactionID      string
	OrderID            string
	OrderSeq           int64
	OrderBookID        string
	UserID             string
	Asset0             string
	Quantity0          decimal.Decimal
	Price              decimal.Decimal
	Side               string // "BUY" or "SELL", matching the DORA Side enum
	AggressorIndicator bool
	CreatedAt          time.Time
}

// tradesHistoryStore is the backtest's read-only data source for a
// followed trader's persisted trade history.
type tradesHistoryStore interface {
	StreamTrades(ctx context.Context, userID string, start, end time.Time) (<-chan Trade, <-chan error)
	TradeBounds(ctx context.Context, userID string) (min, max time.Time, count int, err error)
}

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//counterfeiter:generate -o copytradingfakes/fake_trades_history_store.go . tradesHistoryStore

// pgxPool is the minimal subset of *pgxpool.Pool the store needs.
// Defining it locally lets tests pass pgxmock.PgxPoolIface alongside
// production callers passing a real *pgxpool.Pool.
type pgxPool interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// PGTradesHistoryStore is the Postgres-backed tradesHistoryStore.
type PGTradesHistoryStore struct {
	pool pgxPool
}

// NewPGTradesHistoryStore constructs a store backed by the given pool.
// The pool is not owned; the caller is responsible for closing it.
func NewPGTradesHistoryStore(pool pgxPool) *PGTradesHistoryStore {
	return &PGTradesHistoryStore{pool: pool}
}

// pgtypeNullTime is a thin shim that implements pgx's Scanner interface
// for nullable timestamps. We avoid pulling in pgtype to keep the
// dependency surface minimal.
type pgtypeNullTime struct {
	Time  time.Time
	Valid bool
}

func (n *pgtypeNullTime) Scan(src any) error {
	if src == nil {
		n.Time = time.Time{}
		n.Valid = false
		return nil
	}
	switch v := src.(type) {
	case time.Time:
		n.Time = v
		n.Valid = true
		return nil
	default:
		return fmt.Errorf("pgtypeNullTime: cannot scan %T", src)
	}
}

// TradeBounds returns the earliest and latest created_at and the total
// row count for the user, or (zero, zero, 0, nil) if the user has no rows.
func (s *PGTradesHistoryStore) TradeBounds(ctx context.Context, userID string) (time.Time, time.Time, int, error) {
	const q = `
		SELECT MIN(created_at), MAX(created_at), COUNT(*)
		FROM trades_history
		WHERE user_id = $1
	`
	var (
		min   pgtypeNullTime
		max   pgtypeNullTime
		count int
	)
	if err := s.pool.QueryRow(ctx, q, userID).Scan(&min, &max, &count); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return time.Time{}, time.Time{}, 0, nil
		}
		return time.Time{}, time.Time{}, 0, fmt.Errorf("query trades history bounds: %w", err)
	}
	return min.Time, max.Time, count, nil
}
