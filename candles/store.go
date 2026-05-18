package candles

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PGStore implements the CandleStore interface using Postgres.
type PGStore struct {
	pool *pgxpool.Pool
}

// NewPGStore creates a new PGStore.
func NewPGStore(pool *pgxpool.Pool) *PGStore {
	return &PGStore{pool: pool}
}

// GetLastTimestamp queries the database for the most recent start_timestamp for the given order book.
func (s *PGStore) GetLastTimestamp(ctx context.Context, orderBookID string) (*time.Time, error) {
	const q = `SELECT MAX(start_timestamp) FROM candles_history WHERE order_book_id = $1`
	var t *time.Time
	err := s.pool.QueryRow(ctx, q, orderBookID).Scan(&t)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("scan: %w", err)
	}
	return t, nil
}

// SaveCandles batch-inserts or updates candles in the candles_history table.
func (s *PGStore) SaveCandles(ctx context.Context, entries []StreamCandlesEntry) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if err = tx.Rollback(ctx); err != nil {
			if !errors.Is(err, pgx.ErrTxClosed) {
				slog.Error("failed to rollback tx", "error", err)
			}
		}
	}()

	const q = `
		INSERT INTO candles_history (order_book_id, start_timestamp, open, high, low, close, volume)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (order_book_id, start_timestamp)
		DO UPDATE SET
			open = EXCLUDED.open,
			high = EXCLUDED.high,
			low = EXCLUDED.low,
			close = EXCLUDED.close,
			volume = EXCLUDED.volume
	`

	for _, entry := range entries {
		c := entry.Val
		if _, err := tx.Exec(ctx, q, c.OrderBookID, c.StartTimestamp, c.Open, c.High, c.Low, c.Close, c.Volume); err != nil {
			return fmt.Errorf("upsert candle for %s at %s: %w", c.OrderBookID, c.StartTimestamp, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

type Subscriber struct {
	requestID uuid.UUID
	store     CandleStore
	start     func(requestID uuid.UUID) (chan []StreamCandlesEntry, error)
	onWrite   func()
}

func NewStoreSubscriber(store CandleStore,
	start func(requestID uuid.UUID) (chan []StreamCandlesEntry, error),
	opts ...func(*Subscriber),
) *Subscriber {
	s := &Subscriber{
		store: store,
		start: start,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func WithWriteHook(onWrite func()) func(*Subscriber) {
	return func(s *Subscriber) {
		s.onWrite = onWrite
	}
}

func (s *Subscriber) Start(ctx context.Context) error {
	s.requestID = uuid.Must(uuid.NewV7())
	updates, err := s.start(s.requestID)
	if err != nil {
		return fmt.Errorf("candle update subscription failed: %w", err)
	}

	slog.Info("starting candle update subscriber")
	for {
		select {
		case <-ctx.Done():
			slog.Info("candle update subscriber stopped")
			return nil
		case entries, ok := <-updates:
			if !ok {
				slog.Info("candle update subscriber stopped")
				return nil
			}
			slog.Debug("saving candle updates", "updates", len(entries))
			if err := s.store.SaveCandles(ctx, entries); err != nil {
				slog.Error("failed to save candle updates", "err", err)
				continue
			}
			if s.onWrite != nil {
				s.onWrite()
			}
		}
	}
}
