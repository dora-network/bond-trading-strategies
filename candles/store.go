package candles

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/govalues/decimal"
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

// LoadCandles reads candles_history rows for one order book in [since, until]
// inclusive, ordered by start_timestamp ASC. Mirrors the upstream REST
// GET /v1/charts/{order_book_id}/candle response shape (all four YTM
// columns included).
func (s *PGStore) LoadCandles(ctx context.Context, orderBookID string, since, until time.Time) ([]Candle, error) {
	const q = `
		SELECT order_book_id::text, start_timestamp,
		       open::text, high::text, low::text, close::text, volume::text,
		       open_ytm::text, high_ytm::text, low_ytm::text, close_ytm::text
		FROM candles_history
		WHERE order_book_id = $1
		  AND start_timestamp >= $2
		  AND start_timestamp <= $3
		ORDER BY start_timestamp ASC
	`
	rows, err := s.pool.Query(ctx, q, orderBookID, since.UTC(), until.UTC())
	if err != nil {
		return nil, fmt.Errorf("query candles_history: %w", err)
	}
	defer rows.Close()

	var candles []Candle
	for rows.Next() {
		var (
			c Candle
			open, high, low, close, volume,
			openYTM, highYTM, lowYTM, closeYTM string
		)
		if err := rows.Scan(
			&c.OrderBookID, &c.StartTimestamp,
			&open, &high, &low, &close, &volume,
			&openYTM, &highYTM, &lowYTM, &closeYTM,
		); err != nil {
			return nil, fmt.Errorf("scan candle row: %w", err)
		}
		if err := scanDecimalInto(&c.Open, open); err != nil {
			return nil, fmt.Errorf("parse open: %w", err)
		}
		if err := scanDecimalInto(&c.High, high); err != nil {
			return nil, fmt.Errorf("parse high: %w", err)
		}
		if err := scanDecimalInto(&c.Low, low); err != nil {
			return nil, fmt.Errorf("parse low: %w", err)
		}
		if err := scanDecimalInto(&c.Close, close); err != nil {
			return nil, fmt.Errorf("parse close: %w", err)
		}
		if err := scanDecimalInto(&c.Volume, volume); err != nil {
			return nil, fmt.Errorf("parse volume: %w", err)
		}
		if err := scanDecimalInto(&c.OpenYTM, openYTM); err != nil {
			return nil, fmt.Errorf("parse open_ytm: %w", err)
		}
		if err := scanDecimalInto(&c.HighYTM, highYTM); err != nil {
			return nil, fmt.Errorf("parse high_ytm: %w", err)
		}
		if err := scanDecimalInto(&c.LowYTM, lowYTM); err != nil {
			return nil, fmt.Errorf("parse low_ytm: %w", err)
		}
		if err := scanDecimalInto(&c.CloseYTM, closeYTM); err != nil {
			return nil, fmt.Errorf("parse close_ytm: %w", err)
		}
		candles = append(candles, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate candles_history: %w", err)
	}
	return candles, nil
}

// scanDecimalInto parses a numeric column rendered as text into d.
// decimal.Decimal does not implement sql.Scanner, so we round-trip via
// the canonical string form the database already serialises.
func scanDecimalInto(d *decimal.Decimal, raw string) error {
	parsed, err := decimal.Parse(raw)
	if err != nil {
		return err
	}
	*d = parsed
	return nil
}

func (s *PGStore) SaveCandles(ctx context.Context, entries []StreamCandlesEntry) error {
	if len(entries) == 0 {
		return nil
	}
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

	// One round-trip per batch via pgx.Batch. Reconnect replays
	// (since=<last saved>) can include dozens of candles — sending
	// them as N separate Exec calls blew past the consumer push
	// timeout (5s) on every reconnect.
	const q = `
		INSERT INTO candles_history
			(order_book_id, start_timestamp, open, high, low, close, volume,
			 open_ytm, high_ytm, low_ytm, close_ytm)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (order_book_id, start_timestamp)
		DO UPDATE SET
			open = EXCLUDED.open,
			high = EXCLUDED.high,
			low = EXCLUDED.low,
			close = EXCLUDED.close,
			volume = EXCLUDED.volume,
			open_ytm = EXCLUDED.open_ytm,
			high_ytm = EXCLUDED.high_ytm,
			low_ytm = EXCLUDED.low_ytm,
			close_ytm = EXCLUDED.close_ytm
	`

	batch := &pgx.Batch{}
	for _, entry := range entries {
		c := entry.Val
		batch.Queue(
			q,
			c.OrderBookID, c.StartTimestamp,
			c.Open, c.High, c.Low, c.Close, c.Volume,
			c.OpenYTM, c.HighYTM, c.LowYTM, c.CloseYTM,
		)
	}

	br := tx.SendBatch(ctx, batch)
	for range entries {
		if _, err := br.Exec(); err != nil {
			_ = br.Close()
			return fmt.Errorf("upsert candle batch: %w", err)
		}
	}
	if err := br.Close(); err != nil {
		return fmt.Errorf("close batch: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

type Subscriber struct {
	requestID   uuid.UUID
	requestIDMu sync.RWMutex
	store       CandleStore
	start       func(requestID uuid.UUID) (chan []StreamCandlesEntry, error)
	onWrite     func()
}

// RequestID returns the UUID minted for this subscriber's upstream
// subscription. Exported for tests that need to identify which
// Handler.subscribers entry the subscriber is consuming.
func (s *Subscriber) RequestID() uuid.UUID {
	s.requestIDMu.RLock()
	defer s.requestIDMu.RUnlock()
	return s.requestID
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
	newID := uuid.Must(uuid.NewV7())
	s.requestIDMu.Lock()
	s.requestID = newID
	s.requestIDMu.Unlock()
	updates, err := s.start(newID)
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
