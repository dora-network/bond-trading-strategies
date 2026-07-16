package breakout

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Trade is the breakout package's in-memory representation of a single
// row in trades_history, shaped for OBV (On-Balance Volume) accumulation.
// It is intentionally decoupled from copytrading.Trade and streams.TradeEvent
// so the breakout backtester does not depend on those packages.
//
// Direction sign is derived from the `side` field at read time, not
// stored — trades_history stores "BUY" / "SELL" as VARCHAR.
type Trade struct {
	Time     time.Time
	Price    decimal.Decimal
	Quantity decimal.Decimal
	Side     string // "BUY" or "SELL"
}

// TradeHistoryStore is the backtest's read-only data source for
// historical trades on a specific order book. Used to compute OBV
// (On-Balance Volume) for the RequireVolumeConfirmation filter.
type TradeHistoryStore interface {
	// StreamTrades returns a channel of trades for the given order book
	// within [start, end], in chronological order. The error channel
	// receives at most one error and is then closed.
	StreamTrades(ctx context.Context, orderBookID uuid.UUID, start, end time.Time) (<-chan Trade, <-chan error)
}

// pgxPool is the minimal subset of *pgxpool.Pool the store needs.
// Defining it locally lets tests pass pgxmock.PgxPoolIface alongside
// production callers passing a real *pgxpool.Pool.
type pgxPool interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// PGTradeHistoryStore is the Postgres-backed TradeHistoryStore.
type PGTradeHistoryStore struct {
	pool pgxPool
}

// NewPGTradeHistoryStore constructs a store backed by the given pool.
// The pool is not owned; the caller is responsible for closing it.
func NewPGTradeHistoryStore(pool *pgxpool.Pool) *PGTradeHistoryStore {
	return &PGTradeHistoryStore{pool: pool}
}

// StreamTrades streams trades for the given order book in [start, end]
// in chronological order (keyset on created_at, transaction_id).
// Empty result set closes both channels immediately.
func (s *PGTradeHistoryStore) StreamTrades(
	ctx context.Context,
	orderBookID uuid.UUID,
	start, end time.Time,
) (<-chan Trade, <-chan error) {
	ch := make(chan Trade, 256) //nolint:mnd // buffer size
	done := make(chan error, 1)
	go func() {
		defer close(ch)
		defer close(done)
		if err := s.streamTradesLoop(ctx, orderBookID, start, end, ch); err != nil {
			done <- err
		}
	}()
	return ch, done
}

func (s *PGTradeHistoryStore) streamTradesLoop(
	ctx context.Context,
	orderBookID uuid.UUID,
	start, end time.Time,
	ch chan<- Trade,
) error {
	const pageSize = 10000
	var cursorTime time.Time
	var cursorID uuid.UUID
	first := true
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows, err := s.queryTradesPage(ctx, orderBookID, start, end, cursorTime, cursorID, first, pageSize)
		if err != nil {
			return err
		}
		first = false
		pageCount := 0
		var lastTime time.Time
		var lastID uuid.UUID
		for rows.Next() {
			var t Trade
			var txID uuid.UUID
			if err := rows.Scan(&txID, &t.Price, &t.Quantity, &t.Side, &t.Time); err != nil {
				rows.Close()
				return fmt.Errorf("scan trade row: %w", err)
			}
			t.Time = t.Time.UTC()
			ch <- t
			lastTime = t.Time
			lastID = txID
			pageCount++
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		if pageCount < pageSize {
			return nil
		}
		cursorTime = lastTime
		cursorID = lastID
	}
}

func (s *PGTradeHistoryStore) queryTradesPage(
	ctx context.Context,
	orderBookID uuid.UUID,
	start, end time.Time,
	cursorTime time.Time,
	cursorID uuid.UUID,
	first bool,
	limit int,
) (pgx.Rows, error) {
	const qBase = `
		SELECT transaction_id, price, quantity, side, created_at
		FROM trades_history
		WHERE orderbook_id = $1
		  AND created_at >= $2
		  AND created_at <= $3
	`
	const qKeyset = qBase + `
		  AND (created_at, transaction_id) > ($4, $5)
		ORDER BY created_at, transaction_id
		LIMIT $6
	`
	if first {
		return s.pool.Query(ctx,
			qBase+` ORDER BY created_at, transaction_id LIMIT $4`,
			orderBookID, start, end, limit)
	}
	return s.pool.Query(ctx, qKeyset, orderBookID, start, end, cursorTime, cursorID, limit)
}
