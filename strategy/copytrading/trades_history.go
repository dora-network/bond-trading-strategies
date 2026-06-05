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
//
// Timestamp convention: trades_history.created_at is stored as TIMESTAMP
// (no time zone). The DB server is UTC and writers write UTC, so the
// wall-clock value round-trips as UTC. Callers that compare against
// CreatedAt should use UTC-anchored times.

const streamBatchSize = 1000

type Trade struct {
	TransactionID      string
	OrderID            string
	OrderSeq           int64
	OrderBookID        string
	UserID             string
	Asset              string
	Quantity           decimal.Decimal
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
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
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

// StreamTrades returns a channel of trades for the user in the [start, end]
// window, ordered by (created_at, transaction_id). The channel is closed
// when all rows have been emitted. Errors are sent on the done channel.
// An empty result set closes the channel with nil on done.
func (s *PGTradesHistoryStore) StreamTrades(
	ctx context.Context,
	userID string,
	start, end time.Time,
) (<-chan Trade, <-chan error) {
	ch := make(chan Trade, streamBatchSize)
	done := make(chan error, 1)

	if userID == "" {
		close(ch)
		done <- errors.New("userID is required")
		return ch, done
	}

	go s.streamTradesLoop(ctx, userID, start, end, ch, done)

	return ch, done
}

// streamTradesLoop is the goroutine body of StreamTrades. It iterates batches
// using a keyset cursor on (created_at, transaction_id) and emits each row
// on ch. Exactly one value (nil or an error) is sent on done before the
// function returns.
func (s *PGTradesHistoryStore) streamTradesLoop(
	ctx context.Context,
	userID string,
	start, end time.Time,
	ch chan<- Trade,
	done chan<- error,
) {
	defer close(ch)
	var (
		cursorTime time.Time
		cursorID   string
	)
	first := true

	for {
		select {
		case <-ctx.Done():
			done <- ctx.Err()
			return
		default:
		}

		rows, err := s.queryTradesBatch(ctx, userID, start, end, cursorTime, cursorID, first)
		if err != nil {
			done <- fmt.Errorf("query trades history: %w", err)
			return
		}
		first = false

		batchCount, lastTime, lastID, iterErr := s.consumeTradesBatch(ctx, rows, ch)
		rows.Close()
		if iterErr != nil {
			done <- iterErr
			return
		}
		cursorTime = lastTime
		cursorID = lastID

		if batchCount < streamBatchSize {
			done <- nil
			return
		}
	}
}

func (s *PGTradesHistoryStore) queryTradesBatch(
	ctx context.Context,
	userID string,
	start, end time.Time,
	cursorTime time.Time,
	cursorID string,
	first bool,
) (pgx.Rows, error) {
	if first {
		return s.pool.Query(ctx, `
			SELECT transaction_id, order_id, order_seq, orderbook_id,
				user_id, asset, quantity, price, side,
				aggressor_indicator, created_at
			FROM trades_history
			WHERE user_id = $1
			  AND created_at >= $2
			  AND created_at <= $3
			ORDER BY created_at, transaction_id
			LIMIT $4
		`, userID, start, end, streamBatchSize)
	}
	return s.pool.Query(ctx, `
		SELECT transaction_id, order_id, order_seq, orderbook_id,
			user_id, asset, quantity, price, side,
			aggressor_indicator, created_at
		FROM trades_history
		WHERE user_id = $1
		  AND created_at >= $2
		  AND created_at <= $3
		  AND (created_at, transaction_id) > ($4, $5)
		ORDER BY created_at, transaction_id
		LIMIT $6
	`, userID, start, end, cursorTime, cursorID, streamBatchSize)
}

// consumeTradesBatch scans every row in rows, sends each parsed Trade on ch,
// and returns the count plus the cursor of the last row emitted. The caller
// is responsible for closing rows.
func (s *PGTradesHistoryStore) consumeTradesBatch(
	ctx context.Context,
	rows pgx.Rows,
	ch chan<- Trade,
) (int, time.Time, string, error) {
	batchCount := 0
	var (
		lastTime time.Time
		lastID   string
	)
	for rows.Next() {
		t, err := scanTradeRow(rows)
		if err != nil {
			return batchCount, lastTime, lastID, err
		}
		select {
		case <-ctx.Done():
			return batchCount, lastTime, lastID, ctx.Err()
		case ch <- t:
		}
		batchCount++
		lastTime = t.CreatedAt
		lastID = t.TransactionID
	}
	if err := rows.Err(); err != nil {
		return batchCount, lastTime, lastID, fmt.Errorf("iterate trades: %w", err)
	}
	return batchCount, lastTime, lastID, nil
}

func scanTradeRow(rows pgx.Rows) (Trade, error) {
	var t Trade
	var qty, priceStr string
	if err := rows.Scan(
		&t.TransactionID, &t.OrderID, &t.OrderSeq, &t.OrderBookID,
		&t.UserID, &t.Asset, &qty, &priceStr, &t.Side,
		&t.AggressorIndicator, &t.CreatedAt,
	); err != nil {
		return t, fmt.Errorf("scan trade: %w", err)
	}
	q, err := decimal.Parse(qty)
	if err != nil {
		return t, fmt.Errorf("parse quantity %q: %w", qty, err)
	}
	t.Quantity = q
	p, err := decimal.Parse(priceStr)
	if err != nil {
		return t, fmt.Errorf("parse price %q: %w", priceStr, err)
	}
	t.Price = p
	return t, nil
}
