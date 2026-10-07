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

// ErrNoCandleCoverage reports a backtest window not fully covered by
// persisted candles. Range carries the book's available window (UTC).
type ErrNoCandleCoverage struct {
	OrderBookID string
	Available   *time.Time // nil when no candles at all
	Until       *time.Time
}

func (e *ErrNoCandleCoverage) Error() string {
	if e.Available == nil {
		return fmt.Sprintf("no candle data for order book %s: backtests require candles_history coverage (none persisted)", e.OrderBookID) //nolint:lll
	}
	return fmt.Sprintf("insufficient candle coverage for order book %s: available %s to %s",
		e.OrderBookID, e.Available.Format(time.RFC3339), e.Until.Format(time.RFC3339))
}

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
			openPx, highPx, lowPx, closePx, volume,
			openYTM, highYTM, lowYTM, closeYTM string
		)
		if err := rows.Scan(
			&c.OrderBookID, &c.StartTimestamp,
			&openPx, &highPx, &lowPx, &closePx, &volume,
			&openYTM, &highYTM, &lowYTM, &closeYTM,
		); err != nil {
			return nil, fmt.Errorf("scan candle row: %w", err)
		}
		if err := scanDecimalInto(&c.Open, openPx); err != nil {
			return nil, fmt.Errorf("parse open: %w", err)
		}
		if err := scanDecimalInto(&c.High, highPx); err != nil {
			return nil, fmt.Errorf("parse high: %w", err)
		}
		if err := scanDecimalInto(&c.Low, lowPx); err != nil {
			return nil, fmt.Errorf("parse low: %w", err)
		}
		if err := scanDecimalInto(&c.Close, closePx); err != nil {
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

// maxCandleRows is the keyset page size for LoadCandlesBucketed. Var (not
// const) so package tests can shrink it via MaxCandleRowsForTest.
//
//nolint:gochecknoglobals // ponytail: constant, not state.
var maxCandleRows = 1500

// fetchCandlesSQL returns the SELECT for the requested resolution. "1m"
// is a simple pass-through. "5m" / "15m" / "1h" / "4h" / "1d"
// build OHLCV buckets over the full bucket grid from start to end
// (generate_series); buckets with no source minutes are forward-filled
// from the previous non-empty bucket via a lateral join (volume
// defaults to 0), so consumers see a gapless series. Postgres has no
// IGNORE NULLS for lag(), hence the lateral form.
// Parameter layout (both branches): $1 = order_book_id, $2 = window
// start (>=), $3 = window end (<), $4 = keyset cursor timestamp (>),
// $5 = batch size (LIMIT). The cursor is a single timestamp, not a
// (ts, id) tuple like FetchTrades/FetchPrices.
//
//nolint:dupl // ponytail: duplicated from internal/agent/store/history_store.go
func fetchCandlesSQL(resolution Resolution) string {
	bucketSec := resolutionToSeconds(resolution)
	if bucketSec <= resolutionSeconds[Resolution1m] {
		return `SELECT order_book_id, start_timestamp, open, high, low, close, volume,
                      open_ytm, high_ytm, low_ytm, close_ytm
               FROM candles_history
               WHERE order_book_id = $1
                 AND start_timestamp >= $2
                 AND start_timestamp <  $3
                 AND start_timestamp >  $4
               ORDER BY start_timestamp ASC
               LIMIT $5`
	}
	return fmt.Sprintf(`WITH grid AS (
          SELECT generate_series(
                   to_timestamp(floor(extract(epoch FROM $2::timestamptz) / %[1]d) * %[1]d),
                   to_timestamp(floor(extract(epoch FROM ($3::timestamptz - interval '1 microsecond')) / %[1]d) * %[1]d),
                   make_interval(secs => %[1]d)
                 ) AT TIME ZONE 'UTC' AS bucket_start
        ),
        raw AS (
          SELECT
            to_timestamp(floor(extract(epoch FROM start_timestamp) / %[1]d) * %[1]d)
              AT TIME ZONE 'UTC' AS bucket_start,
            start_timestamp,
            open, high, low, close, volume,
            open_ytm, high_ytm, low_ytm, close_ytm
          FROM candles_history
          WHERE order_book_id = $1
            AND start_timestamp >= $2
            AND start_timestamp <  $3
        ),
        agg AS (
          SELECT
            g.bucket_start,
            (array_agg(r.open      ORDER BY r.start_timestamp ASC))[1]  AS open,
            max(r.high)                                                AS high,
            min(r.low)                                                 AS low,
            (array_agg(r.close     ORDER BY r.start_timestamp DESC))[1] AS close,
            sum(r.volume)                                              AS volume,
            (array_agg(r.open_ytm  ORDER BY r.start_timestamp ASC))[1]  AS open_ytm,
            max(r.high_ytm)                                            AS high_ytm,
            min(r.low_ytm)                                             AS low_ytm,
            (array_agg(r.close_ytm ORDER BY r.start_timestamp DESC))[1] AS close_ytm,
            count(r.start_timestamp)                                   AS n
          FROM grid g
          LEFT JOIN raw r USING (bucket_start)
          GROUP BY g.bucket_start
        )
        SELECT
          $1 AS order_book_id,
          a.bucket_start AS start_timestamp,
          COALESCE(a.open,  p.open)       AS open,
          COALESCE(a.high,  p.high)       AS high,
          COALESCE(a.low,   p.low)        AS low,
          COALESCE(a.close, p.close)      AS close,
          COALESCE(a.volume, 0)           AS volume,
          COALESCE(a.open_ytm,  p.open_ytm)  AS open_ytm,
          COALESCE(a.high_ytm, p.high_ytm)   AS high_ytm,
          COALESCE(a.low_ytm,  p.low_ytm)    AS low_ytm,
          COALESCE(a.close_ytm, p.close_ytm) AS close_ytm
        FROM agg a
        LEFT JOIN LATERAL (
          SELECT open, high, low, close,
                 open_ytm, high_ytm, low_ytm, close_ytm
          FROM agg p
          WHERE p.n > 0 AND p.bucket_start < a.bucket_start
          ORDER BY p.bucket_start DESC
          LIMIT 1
        ) p ON a.n = 0
        WHERE a.bucket_start > ($4 AT TIME ZONE 'UTC')
        ORDER BY a.bucket_start ASC
        LIMIT $5`, bucketSec)
}

// resolutionSeconds is the closed resolution set Dora serves; the raw
// table stores 1m candles, everything above is folded on read. No "7d":
// internal/agent's copy keeps it for its SQL-folding-only needs; nothing
// in this module can produce it via the Resolution enum.
//
//nolint:gochecknoglobals // ponytail: constant, not state.
var resolutionSeconds = map[Resolution]int{
	Resolution1m: 60, Resolution5m: 300, Resolution15m: 900, Resolution1h: 3600,
	Resolution4h: 14400, Resolution1d: 86400,
}

// resolutionToSeconds maps a resolution to its bucket size in seconds.
// Returns 0 for unknown.
func resolutionToSeconds(r Resolution) int {
	return resolutionSeconds[r]
}

// LoadCandlesBucketed returns candles for the book folded to the requested
// resolution, oldest-first, via keyset pagination. "1m" passes through.
// Callers validating stream subscriptions use Resolution.Validate.
func (s *PGStore) LoadCandlesBucketed(ctx context.Context, orderBookID string, resolution Resolution,
	since, until time.Time,
) ([]Candle, error) {
	if resolutionToSeconds(resolution) == 0 {
		return nil, fmt.Errorf("candles: unknown resolution %q (allowed: 1m, 5m, 15m, 1h, 4h, 1d)", resolution)
	}

	var out []Candle
	cursor := time.Time{}
	for {
		rows, err := s.queryCandlePage(ctx, orderBookID, resolution, since, until, cursor, maxCandleRows)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			return out, nil
		}
		out = append(out, rows...)
		if len(rows) < maxCandleRows {
			return out, nil
		}
		cursor = rows[len(rows)-1].StartTimestamp
	}
}

// CandleRange returns the min/max start_timestamp persisted for the book.
// Either may be nil when no rows exist.
func (s *PGStore) CandleRange(ctx context.Context, orderBookID string) (*time.Time, *time.Time, error) {
	var lo, hi *time.Time
	if err := s.pool.QueryRow(ctx,
		`SELECT MIN(start_timestamp), MAX(start_timestamp)
		 FROM candles_history WHERE order_book_id = $1`, orderBookID).
		Scan(&lo, &hi); err != nil {
		return nil, nil, fmt.Errorf("candle range: %w", err)
	}
	if lo != nil {
		*lo = lo.UTC()
	}
	if hi != nil {
		*hi = hi.UTC()
	}
	return lo, hi, nil
}

// queryCandlePage runs one keyset-paginated page of the bucketed query.
func (s *PGStore) queryCandlePage(ctx context.Context, orderBookID string, resolution Resolution,
	since, until, cursor time.Time, limit int,
) ([]Candle, error) {
	rows, err := s.pool.Query(ctx, fetchCandlesSQL(resolution),
		orderBookID, since.UTC(), until.UTC(), cursor.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("bucketed candles: %w", err)
	}
	defer rows.Close()
	var out []Candle
	for rows.Next() {
		var (
			c                                  Candle
			openPx, highPx, lowPx, closePx     string
			volume                             string
			openYTM, highYTM, lowYTM, closeYTM string
		)
		if err := rows.Scan(&c.OrderBookID, &c.StartTimestamp,
			&openPx, &highPx, &lowPx, &closePx, &volume,
			&openYTM, &highYTM, &lowYTM, &closeYTM); err != nil {
			return nil, fmt.Errorf("scan candle: %w", err)
		}
		if err := scanDecimalInto(&c.Open, openPx); err != nil {
			return nil, fmt.Errorf("parse open: %w", err)
		}
		if err := scanDecimalInto(&c.High, highPx); err != nil {
			return nil, fmt.Errorf("parse high: %w", err)
		}
		if err := scanDecimalInto(&c.Low, lowPx); err != nil {
			return nil, fmt.Errorf("parse low: %w", err)
		}
		if err := scanDecimalInto(&c.Close, closePx); err != nil {
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
		c.StartTimestamp = c.StartTimestamp.UTC()
		out = append(out, c)
	}
	return out, rows.Err()
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
