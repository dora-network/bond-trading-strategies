package store

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// HistoryStore reads candles/trades/prices from the host's public
// schema history tables over the shared pgxpool. It satisfies
// HistoryFetcher; NewHistoryStore is the constructor.
type HistoryStore struct {
	pool *pgxpool.Pool
}

// NewHistoryStore returns a HistoryStore over the shared pool. The
// tables (public.candles_history / trades_history / price_history)
// must already exist — the host's own migrations own them.
func NewHistoryStore(pool *pgxpool.Pool) *HistoryStore {
	return &HistoryStore{pool: pool}
}

// fetchTimeout bounds each history fetch so a half-open TCP connection
// cannot deadlock the mergeStream picker via the liveWait branch. The
// timer fires per query, not per backtest.
const fetchTimeout = 60 * time.Second

// cursor encoding (used by FetchCandles, FetchTrades, FetchPrices).
// 44 bytes: 8-byte big-endian nanoseconds + 36 bytes of id (a
// canonical hyphenated UUID). base64-encoded.
const cursorBytes = 44

// nilUUID is the all-zeros UUID, used as the lower-bound id in the
// trades/prices keyset predicate when the caller has no prior cursor
// (the first page of a window). transaction_id and asset_id are uuid
// columns; binding "" produces SQLSTATE 22P02 ("invalid input syntax
// for type uuid"), while the all-zeros UUID sorts before every real
// UUID and Postgres accepts it as a valid value.
const nilUUID = "00000000-0000-0000-0000-000000000000"

func encodeCursor(ts time.Time, id string) string {
	b := make([]byte, cursorBytes)
	binary.BigEndian.PutUint64(b, uint64(ts.UnixNano()))
	copy(b[8:], id)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(s string) (time.Time, string, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("history: cursor decode: %w", err)
	}
	if len(b) < cursorBytes {
		return time.Time{}, "", errors.New("history: cursor too short")
	}
	u := binary.BigEndian.Uint64(b)
	if u > uint64(math.MaxInt64) {
		return time.Time{}, "", errors.New("history: cursor timestamp overflow")
	}
	ns := int64(u)
	return time.Unix(0, ns).UTC(), string(b[8:cursorBytes]), nil
}

// fetchTradesSQL returns the SELECT statement used by FetchTrades. The
// public.trades_history table uses orderbook_id / asset / quantity; the
// alias SELECT renames them to order_book_id / asset_0 / quantity_0 to
// match the canonical wsplex shape. The strategy's dorastrategy.Trade
// struct decodes either path with the same JSON tags.
func fetchTradesSQL() string {
	return `SELECT
        transaction_id,
        orderbook_id  AS order_book_id,
        order_id,
        order_seq,
        user_id,
        asset         AS asset_0,
        quantity      AS quantity_0,
        price,
        side,
        aggressor_indicator,
        created_at
    FROM trades_history
    WHERE orderbook_id = $1
      AND created_at >= $2
      AND created_at <  $3
      AND (created_at, transaction_id) > ($4, $5)
    ORDER BY created_at ASC, transaction_id ASC
    LIMIT $6`
}

// FetchTrades returns one page of trades for orderBookID in [start, end),
// keyed by (created_at, transaction_id) using the opaque cursor. The
// returned Trade rows use the canonical wsplex field names.
func (s *HistoryStore) FetchTrades(ctx context.Context, orderBookID string,
	start, end time.Time, cursor string, batchSize int,
) ([]Trade, string, error) {
	var cursorTS time.Time
	var cursorID string
	if cursor != "" {
		var err error
		cursorTS, cursorID, err = decodeCursor(cursor)
		if err != nil {
			return nil, "", err
		}
	} else {
		// First call: the WHERE predicate still needs a value for the
		// strict keyset. transaction_id is a uuid column; binding ""
		// yields SQLSTATE 22P02, so use the all-zeros UUID as the lower
		// bound -- it sorts before every real UUID.
		cursorTS = start.Add(-time.Nanosecond)
		cursorID = nilUUID
	}
	fctx, fcancel := context.WithTimeout(ctx, fetchTimeout)
	defer fcancel()
	rows, err := s.pool.Query(fctx, fetchTradesSQL(),
		orderBookID, start, end, cursorTS, cursorID, batchSize)
	if err != nil {
		return nil, "", fmt.Errorf("history: fetch trades: %w", err)
	}
	defer rows.Close()

	var out []Trade
	var last Trade
	for rows.Next() {
		var t Trade
		if err := rows.Scan(
			&t.TransactionID, &t.OrderBookID, &t.OrderID, &t.OrderSeq,
			&t.UserID, &t.Asset0, &t.Quantity0, &t.Price,
			&t.Side, &t.AggressorIndicator, &t.CreatedAt,
		); err != nil {
			return nil, "", fmt.Errorf("history: scan trade: %w", err)
		}
		out = append(out, t)
		last = t
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	next := ""
	if len(out) > 0 {
		// Always encode the cursor when we returned any rows. The next
		// fetch advances past the last row; if the data is exhausted, it
		// returns 0 rows which marks done=true. Without this, a short
		// last page returns cursor=""; the next fetch sees that, falls
		// into the first-call branch with start-1ns, and re-returns the
		// same rows -- an infinite loop.
		next = encodeCursor(last.CreatedAt, last.TransactionID)
	}
	return out, next, nil
}

// fetchPricesSQL returns the SELECT used by FetchPrices. The
// price_history table is keyed by asset_id (UUID) and timestamp (no
// resolution bucketing).
func fetchPricesSQL() string {
	return `SELECT asset_id, price, ytm, timestamp
    FROM price_history
    WHERE asset_id = $1
      AND timestamp >= $2
      AND timestamp <  $3
      AND (timestamp, asset_id) > ($4, $5)
    ORDER BY timestamp ASC, asset_id ASC
    LIMIT $6`
}

// FetchPrices returns one page of prices for assetID in [start, end).
func (s *HistoryStore) FetchPrices(ctx context.Context, assetID string,
	start, end time.Time, cursor string, batchSize int,
) ([]Price, string, error) {
	var cursorTS time.Time
	var cursorID string
	if cursor != "" {
		var err error
		cursorTS, cursorID, err = decodeCursor(cursor)
		if err != nil {
			return nil, "", err
		}
	} else {
		cursorTS = start.Add(-time.Nanosecond)
		cursorID = nilUUID
	}
	fctx, fcancel := context.WithTimeout(ctx, fetchTimeout)
	defer fcancel()
	rows, err := s.pool.Query(fctx, fetchPricesSQL(),
		assetID, start, end, cursorTS, cursorID, batchSize)
	if err != nil {
		return nil, "", fmt.Errorf("history: fetch prices: %w", err)
	}
	defer rows.Close()

	var out []Price
	var last Price
	for rows.Next() {
		var p Price
		if err := rows.Scan(&p.AssetID, &p.Price, &p.YTM, &p.Timestamp); err != nil {
			return nil, "", fmt.Errorf("history: scan price: %w", err)
		}
		out = append(out, p)
		last = p
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	next := ""
	if len(out) > 0 {
		next = encodeCursor(last.Timestamp, last.AssetID)
	}
	return out, next, nil
}

// FetchCandles returns one page of bucketed candles for orderBookID in
// [start, end), at the requested resolution. The SQL is a server-side
// OHLCV bucketed query (see fetchCandlesSQL for the fold rule). The
// returned Candle rows use the table's column shape.
func (s *HistoryStore) FetchCandles(ctx context.Context, orderBookID string,
	start, end time.Time, resolution string, cursor string, batchSize int,
) ([]Candle, string, error) {
	if _, ok := resolutionSeconds[resolution]; !ok {
		// Unknown resolutions would otherwise resolve to 0 seconds and
		// silently serve the 1m pass-through query.
		return nil, "", fmt.Errorf("history: unknown resolution %q (allowed: 1m, 5m, 15m, 1h, 4h, 1d, 7d)", resolution)
	}
	var cursorTS time.Time
	if cursor != "" {
		var err error
		cursorTS, _, err = decodeCursor(cursor)
		if err != nil {
			return nil, "", err
		}
	} else {
		// First call: the WHERE predicate still needs a value for the
		// keyset; use the start of the window minus 1ns.
		cursorTS = start.Add(-time.Nanosecond)
	}
	fctx, fcancel := context.WithTimeout(ctx, fetchTimeout)
	defer fcancel()
	rows, err := s.pool.Query(fctx, fetchCandlesSQL(resolution),
		orderBookID, start, end, cursorTS, batchSize)
	if err != nil {
		return nil, "", fmt.Errorf("history: fetch candles: %w", err)
	}
	defer rows.Close()

	var out []Candle
	var last Candle
	for rows.Next() {
		var c Candle
		if err := rows.Scan(
			&c.OrderBookID, &c.StartTimestamp, &c.Open, &c.High, &c.Low, &c.Close, &c.Volume,
			&c.OpenYtm, &c.HighYtm, &c.LowYtm, &c.CloseYtm,
		); err != nil {
			return nil, "", fmt.Errorf("history: scan candle: %w", err)
		}
		out = append(out, c)
		last = c
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	next := ""
	if len(out) > 0 {
		next = encodeCursor(last.StartTimestamp, "")
	}
	return out, next, nil
}

// fetchCandlesSQL returns the SELECT for the requested resolution. "1m"
// is a simple pass-through. "5m" / "15m" / "1h" / "4h" / "1d" / "7d"
// build OHLCV buckets over the full bucket grid from start to end
// (generate_series); buckets with no source minutes are forward-filled
// from the previous non-empty bucket via a lateral join (volume
// defaults to 0), so consumers see a gapless series. Postgres has no
// IGNORE NULLS for lag(), hence the lateral form.
//
// Parameter layout (both branches): $1 = order_book_id, $2 = window
// start (>=), $3 = window end (<), $4 = keyset cursor timestamp (>),
// $5 = batch size (LIMIT). The cursor is a single timestamp, not a
// (ts, id) tuple like FetchTrades/FetchPrices.
func fetchCandlesSQL(resolution string) string {
	bucketSec := resolutionToSeconds(resolution)
	if bucketSec <= resolutionSeconds["1m"] {
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
// table stores 1m candles, everything above is folded on read.
//
//nolint:gochecknoglobals // ponytail: constant table, not state.
var resolutionSeconds = map[string]int{
	"1m": 60, "5m": 300, "15m": 900, "1h": 3600,
	"4h": 14400, "1d": 86400, "7d": 604800,
}

// resolutionToSeconds maps a resolution string to its bucket size in
// seconds. Returns 0 for unknown.
func resolutionToSeconds(r string) int {
	return resolutionSeconds[r]
}
