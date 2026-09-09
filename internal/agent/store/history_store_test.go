package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestEncodeCursor(t *testing.T) {
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	id := "019c3420-5cd7-7a88-8fe6-a5a622e01ad9"
	got := encodeCursor(ts, id)
	if got == "" {
		t.Fatal("encodeCursor returned empty")
	}
	gotTS, gotID, err := decodeCursor(got)
	if err != nil {
		t.Fatalf("decodeCursor: %v", err)
	}
	if !gotTS.Equal(ts) {
		t.Errorf("ts = %v, want %v", gotTS, ts)
	}
	if gotID != id {
		t.Errorf("id = %q, want %q", gotID, id)
	}
}

func TestDecodeCursorInvalid(t *testing.T) {
	if _, _, err := decodeCursor("not-base64"); err == nil {
		t.Error("expected error for non-base64 cursor")
	}
	if _, _, err := decodeCursor("YWJj"); err == nil { // "abc"
		t.Error("expected error for short cursor (< 44 bytes)")
	}
}

func TestFetchTrades_AliasingQuery(t *testing.T) {
	// The test asserts the SELECT shape: the trades_history columns are
	// aliased onto the canonical wsplex names so the plugin's OnTrade
	// decodes the same JSON from backtest rows as from the live /trades
	// stream.
	sql := fetchTradesSQL()
	for _, want := range []string{
		"orderbook_id  AS order_book_id",
		"asset         AS asset_0",
		"quantity      AS quantity_0",
		"aggressor_indicator",
		"transaction_id",
		"user_id",
		"order_seq",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("fetchTradesSQL missing %q\n--SQL--\n%s", want, sql)
		}
	}
}

func TestFetchPrices_Query(t *testing.T) {
	sql := fetchPricesSQL()
	for _, want := range []string{"asset_id", "price", "ytm", "timestamp"} {
		if !strings.Contains(sql, want) {
			t.Errorf("fetchPricesSQL missing %q\n--SQL--\n%s", want, sql)
		}
	}
}

// TestFetchCandlesSQL_1mPlaceholderLayout pins the 1m branch to the
// 5-arg parameter layout FetchCandles passes (orderBookID, start, end,
// cursorTS, batchSize).
func TestFetchCandlesSQL_1mPlaceholderLayout(t *testing.T) {
	sql := fetchCandlesSQL("1m")
	if !strings.Contains(sql, "AND start_timestamp >  $4") {
		t.Errorf("1m query missing keyset predicate $4:\n%s", sql)
	}
	if !strings.Contains(sql, "LIMIT $5") {
		t.Errorf("1m query must LIMIT on $5:\n%s", sql)
	}
	for _, ph := range []string{"$6", "$7"} {
		if strings.Contains(sql, ph) {
			t.Errorf("1m query has unexpected placeholder %s:\n%s", ph, sql)
		}
	}
}

func TestFetchCandlesBucketedQuery_1m(t *testing.T) {
	sql := fetchCandlesSQL("1m")
	if strings.Contains(sql, "GROUP BY") {
		t.Errorf("1m query should not use GROUP BY, got:\n%s", sql)
	}
	for _, want := range []string{"order_book_id", "open", "high", "low", "close", "volume"} {
		if !strings.Contains(sql, want) {
			t.Errorf("1m query missing %q", want)
		}
	}
}

func TestFetchCandlesBucketedQuery_5m(t *testing.T) {
	sql := fetchCandlesSQL("5m")
	if !strings.Contains(sql, "GROUP BY") {
		t.Error("5m query should use GROUP BY")
	}
	for _, want := range []string{
		"ORDER BY r.start_timestamp ASC",
		"ORDER BY r.start_timestamp DESC",
		"generate_series",
		"LEFT JOIN LATERAL",
		"floor(extract(epoch FROM start_timestamp) / 300)",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("5m query missing %q\n--SQL--\n%s", want, sql)
		}
	}
}

func TestFetchCandlesBucketedQuery_15m(t *testing.T) {
	if sql := fetchCandlesSQL("15m"); !strings.Contains(sql, "900") {
		t.Errorf("15m query should use 900s bucket size, got:\n%s", sql)
	}
}

func TestFetchCandles_UnknownResolution(t *testing.T) {
	// The guard runs before any db access, so a nil pool is enough.
	s := &HistoryStore{}
	_, _, err := s.FetchCandles(t.Context(), "OB-1",
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		"30m", "", 100)
	if err == nil || !strings.Contains(err.Error(), "unknown resolution") {
		t.Fatalf("expected unknown-resolution error, got %v", err)
	}
}

// newHistoryStore provisions a throwaway schema holding the three
// history tables and returns a HistoryStore whose pool resolves the
// unqualified table names against it. Skips when DATABASE_URL is unset
// (repo convention, see agenttest).
func newHistoryStore(t *testing.T) *HistoryStore {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; skipping history PG test")
	}
	ctx := context.Background()
	schema := "history_test_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	boot, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(boot.Close)
	if _, err := boot.Exec(ctx, "create schema "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = boot.Exec(ctx, "drop schema if exists "+schema+" cascade")
	})
	if _, err := boot.Exec(ctx, "set search_path to "+schema); err != nil {
		t.Fatalf("set search_path: %v", err)
	}
	if _, err := boot.Exec(ctx, historyTablesDDL); err != nil {
		t.Fatalf("create tables: %v", err)
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return NewHistoryStore(pool)
}

const historyTablesDDL = `
create table candles_history (
  order_book_id    text        not null,
  start_timestamp  timestamptz not null,
  open numeric, high numeric, low numeric, close numeric, volume numeric,
  open_ytm numeric, high_ytm numeric, low_ytm numeric, close_ytm numeric
);
create table trades_history (
  transaction_id      uuid        not null primary key,
  order_id            uuid        not null,
  order_seq           bigint      not null,
  orderbook_id        text        not null,
  user_id             uuid        not null,
  asset               text        not null,
  quantity            numeric     not null,
  price               numeric     not null,
  side                text        not null,
  aggressor_indicator boolean     not null,
  created_at          timestamptz not null
);
create table price_history (
  asset_id  uuid        not null,
  price     numeric     not null,
  ytm       numeric     not null,
  timestamp timestamptz not null
)`

// TestFetchCandlesSQL_BucketedForwardFillsMissingMinutes proves the
// bucketed query emits the full bucket grid and forward-fills buckets
// with no source minutes from the previous bucket: an hour of 1m rows
// with minutes 10:20-10:29 missing, queried at 5m, must return 12
// candles whose two empty buckets (10:20, 10:25) carry minute 10:19's
// close and volume 0.
func TestFetchCandlesSQL_BucketedForwardFillsMissingMinutes(t *testing.T) {
	s := newHistoryStore(t)
	ctx := t.Context()
	base := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	for m := 0; m < 60; m++ {
		if m >= 20 && m < 30 {
			continue // 10-minute gap -> two fully-empty 5m buckets
		}
		ts := base.Add(time.Duration(m) * time.Minute)
		v := fmt.Sprintf("%d.00", 100+m)
		if _, err := s.pool.Exec(ctx, `
			INSERT INTO candles_history
			  (order_book_id, start_timestamp, open, high, low, close, volume,
			   open_ytm, high_ytm, low_ytm, close_ytm)
			VALUES ('OB-1', $1, $2, $2, $2, $2, 1, 5, 5, 5, 5)`, ts, v); err != nil {
			t.Fatalf("seed minute %d: %v", m, err)
		}
	}

	got, next, err := s.FetchCandles(ctx, "OB-1", base, base.Add(time.Hour), "5m", "", 100)
	if err != nil {
		t.Fatalf("FetchCandles: %v", err)
	}
	if next == "" {
		t.Errorf("expected non-empty next cursor for a 1-hour window fetch under LIMIT, got %q", next)
	}
	if len(got) != 12 {
		t.Fatalf("expected 12 five-minute buckets (gapless grid), got %d", len(got))
	}
	wantStart := base
	for i, c := range got {
		if !c.StartTimestamp.Equal(wantStart) {
			t.Errorf("bucket %d start = %v, want %v", i, c.StartTimestamp, wantStart)
		}
		wantStart = wantStart.Add(5 * time.Minute)
	}
	for _, i := range []int{4, 5} {
		c := got[i]
		for name, val := range map[string]string{"open": c.Open, "low": c.Low} {
			if val != "115.00" {
				t.Errorf("empty bucket %d %s = %q, want forward-filled 115.00", i, name, val)
			}
		}
		for name, val := range map[string]string{"high": c.High, "close": c.Close} {
			if val != "119.00" {
				t.Errorf("empty bucket %d %s = %q, want forward-filled 119.00", i, name, val)
			}
		}
		if c.Volume != "0" {
			t.Errorf("empty bucket %d volume = %q, want 0", i, c.Volume)
		}
	}
	if got[0].Close != "104.00" || got[0].Volume != "5" {
		t.Errorf("bucket 0 = close %q volume %q, want 104.00 / 5", got[0].Close, got[0].Volume)
	}
}

// TestFetchTrades_FirstPageEmptyCursor proves the first page of a
// window does not fail with SQLSTATE 22P02 (binding "" against a uuid
// keyset column).
func TestFetchTrades_FirstPageEmptyCursor(t *testing.T) {
	s := newHistoryStore(t)
	ctx := t.Context()
	ob := "OB-1"
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO trades_history
		  (transaction_id, order_id, order_seq, orderbook_id, user_id,
		   asset, quantity, price, side, aggressor_indicator, created_at)
		VALUES ('019c3420-5cd7-7a88-8fe6-a5a622e01ad9',
		        '019c3420-5cd7-7a88-8fe6-a5a622e01ad8',
		        1, $1,
		        '019c3420-5cd7-7a88-8fe6-a5a622e01aa0',
		        'asset-0', 1.0, 100.0, 'buy', true,
		        '2026-01-02T03:04:05Z')`, ob); err != nil {
		t.Fatalf("seed: %v", err)
	}
	got, next, err := s.FetchTrades(ctx, ob,
		time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC),
		"", 100)
	if err != nil {
		t.Fatalf("FetchTrades: %v", err)
	}
	if len(got) != 1 || got[0].TransactionID != "019c3420-5cd7-7a88-8fe6-a5a622e01ad9" {
		t.Errorf("rows = %+v", got)
	}
	if next == "" {
		t.Errorf("next cursor = %q, want non-empty cursor (advances past last row even on a short page)", next)
	}
}

// TestFetchTrades_PaginatesFullPage proves the second page's bind of
// the cursor id (a full 36-character UUID) survives the round-trip.
func TestFetchTrades_PaginatesFullPage(t *testing.T) {
	s := newHistoryStore(t)
	ctx := t.Context()
	ob := "OB-1"
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for i := range 3 {
		ts := base.Add(time.Duration(i) * time.Second)
		if _, err := s.pool.Exec(ctx, `
			INSERT INTO trades_history
			  (transaction_id, order_id, order_seq, orderbook_id, user_id,
			   asset, quantity, price, side, aggressor_indicator, created_at)
			VALUES (gen_random_uuid(),
			        gen_random_uuid(),
			        1, $1,
			        gen_random_uuid(),
			        'asset-0', 1.0, 100.0, 'buy', true,
			        $2)`, ob, ts); err != nil {
			t.Fatalf("seed row %d: %v", i, err)
		}
	}
	page1, next1, err := s.FetchTrades(ctx, ob, base.Add(-time.Hour), base.Add(time.Hour), "", 2)
	if err != nil {
		t.Fatalf("page1: %v", err)
	}
	if len(page1) != 2 || next1 == "" {
		t.Fatalf("page1 rows=%d next=%q, want 2 rows + cursor", len(page1), next1)
	}
	_, cursorID, err := decodeCursor(next1)
	if err != nil {
		t.Fatalf("decodeCursor(next1): %v", err)
	}
	if _, err := uuid.Parse(cursorID); err != nil {
		t.Fatalf("cursor id %q is not a valid UUID: %v", cursorID, err)
	}
	page2, next2, err := s.FetchTrades(ctx, ob, base.Add(-time.Hour), base.Add(time.Hour), next1, 2)
	if err != nil {
		t.Fatalf("page2: %v", err)
	}
	if len(page2) != 1 || next2 == "" {
		t.Fatalf("page2 rows=%d next=%q, want 1 row + non-empty cursor", len(page2), next2)
	}
}

// TestFetchPrices_FirstPageEmptyCursor is the prices-side analogue of
// TestFetchTrades_FirstPageEmptyCursor.
func TestFetchPrices_FirstPageEmptyCursor(t *testing.T) {
	s := newHistoryStore(t)
	ctx := t.Context()
	asset := "019c3420-5cd7-7a88-8fe6-a5a622e01ad9"
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO price_history (asset_id, price, ytm, timestamp)
		VALUES ($1, 100.0, 5.0, '2026-01-02T03:04:05Z')`, asset); err != nil {
		t.Fatalf("seed: %v", err)
	}
	got, next, err := s.FetchPrices(ctx, asset,
		time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC),
		"", 100)
	if err != nil {
		t.Fatalf("FetchPrices: %v", err)
	}
	if len(got) != 1 || got[0].AssetID != asset {
		t.Errorf("rows = %+v", got)
	}
	if next == "" {
		t.Errorf("next cursor = %q, want non-empty cursor", next)
	}
}

// TestFetchPrices_DeadlineHangsFetch pins the per-fetch deadline: an
// already-cancelled ctx must surface as an error, not hang.
func TestFetchPrices_DeadlineHangsFetch(t *testing.T) {
	s := newHistoryStore(t)
	deadCtx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err := s.FetchPrices(deadCtx,
		"019c3420-5cd7-7a88-8fe6-a5a622e01ad9",
		time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC),
		"", 100)
	if err == nil {
		t.Fatal("FetchPrices with cancelled ctx must error, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("FetchPrices err: got %v, want errors.Is(...,context.Canceled)", err)
	}
}
