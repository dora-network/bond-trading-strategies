package prices

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PGStore implements the PriceStore interface using Postgres.
type PGStore struct {
	pool *pgxpool.Pool
}

// NewPGStore creates a new PGStore.
func NewPGStore(pool *pgxpool.Pool) *PGStore {
	return &PGStore{pool: pool}
}

// SavePrices batch-inserts all prices from a single message into price_history.
func (s *PGStore) SavePrices(ctx context.Context, prices map[uuid.UUID]AssetPrice) error {
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

	const q = `INSERT INTO price_history (asset_id, price, ytm, timestamp) VALUES ($1, $2, $3, $4)`

	for _, p := range prices {
		if _, err := tx.Exec(ctx, q, p.AssetID, p.Price, p.YTM, p.Time); err != nil {
			return fmt.Errorf("insert asset %s: %w", p.AssetID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

// LoadHistoricalPrices loads historical prices for one asset ordered by time.
// Only rows with a non-null YTM are returned because strategy runs require it.
func (s *PGStore) LoadHistoricalPrices(ctx context.Context, assetID string, start, end time.Time) ([]AssetPrice, error) {
	const q = `
		SELECT asset_id::text, price::text, ytm::text, timestamp
		FROM price_history
		WHERE asset_id = $1
		  AND timestamp >= $2
		  AND timestamp <= $3
		  AND ytm IS NOT NULL
		ORDER BY timestamp ASC
	`

	rows, err := s.pool.Query(ctx, q, assetID, start, end)
	if err != nil {
		return nil, fmt.Errorf("query price history: %w", err)
	}
	defer rows.Close()

	var out []AssetPrice
	for rows.Next() {
		var (
			id        string
			priceText string
			ytmText   string
			ts        time.Time
		)

		if err := rows.Scan(&id, &priceText, &ytmText, &ts); err != nil {
			return nil, fmt.Errorf("scan price history: %w", err)
		}

		price, err := decimal.Parse(priceText)
		if err != nil {
			return nil, fmt.Errorf("parse price %q: %w", priceText, err)
		}
		ytm, err := decimal.Parse(ytmText)
		if err != nil {
			return nil, fmt.Errorf("parse ytm %q: %w", ytmText, err)
		}

		ytmCopy := ytm
		out = append(out, AssetPrice{
			AssetID: id,
			Price:   price,
			YTM:     &ytmCopy,
			Time:    ts,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate price history: %w", err)
	}

	return out, nil
}

type Subscriber struct {
	requestID uuid.UUID
	store     PriceStore
	start     func(requestID uuid.UUID) (chan map[uuid.UUID]AssetPrice, error)
	onWrite   func()
}

func NewStoreSubscriber(store PriceStore,
	start func(requestID uuid.UUID) (chan map[uuid.UUID]AssetPrice, error),
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
	prices, err := s.start(s.requestID)
	if err != nil {
		return fmt.Errorf("price update subscription failed: %w", err)
	}
	slog.Info("starting price update subscriber")
	for {
		select {
		case <-ctx.Done():
			slog.Info("price update subscriber stopped")
			return nil
		case updates, ok := <-prices:
			if !ok {
				slog.Info("price update subscriber stopped")
				return nil
			}
			slog.Debug("saving price updates", "updates", len(updates))
			if err := s.store.SavePrices(ctx, updates); err != nil {
				slog.Error("failed to save price updates", "err", err)
				continue
			}
			if s.onWrite != nil {
				s.onWrite()
			}
		}
	}
}
