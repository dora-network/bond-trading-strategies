# Copy-Trading Backtest from Local trades_history

## Problem

The current copy-trading backtest fetches all historical trades for the
followed trader by paging the live DORA `/v1/trades` endpoint. The endpoint
caps `limit` at 100, so a 4-day window on `TRADER_NVDA_USD` (~86k trades)
requires ~860 sequential HTTP calls before any P&L is computed. A 90-day
window scales to ~7,700 calls and 5+ minutes of network round-trips.

The previous design (spec `2026-06-02-streaming-backtest-design.md`) replaced
the in-memory full-fetch with a streaming producer/consumer pattern, which
fixed the time-to-first-trade but still requires a live DORA connection
on every backtest run. Backtests cannot be replayed when DORA is down, and
the followed trader's history is transient in the local system.

A `trades_history` table has been added in migration 007
(`migrations/007_add_trade_history.sql`) with a `transaction_id` primary key
and an `idx_trades_history_user_id_created_at` index. The backtest should
read from this table, decoupling it from the live DORA API and making
backtests reproducible against the persisted data set.

## Solution

Replace the DORA streaming producer inside the backtest with a Postgres
streaming producer that reads from `trades_history`. The simulation
loop's consumer side is unchanged in shape: it still drains a channel of
trades in chronological order. The DORA `tradesClient` interface and
`doraTradesClient` are removed from the backtest path entirely, since
nothing else consumes them.

The followed trader's persisted history is the source of truth. If the
requested window falls outside the data available in `trades_history`,
`Backtester.Run` fails with a clear error that names the user and the
available range, so the caller can sync the missing data and retry.

## Architecture

```
strategy/copytrading/
├── backtest.go              (modified: Backtester takes tradesHistoryStore;
│                                       tradesClient + doraTradesClient deleted;
│                                       simulate + helpers take copytrading.Trade)
├── backtest_test.go         (modified: fakeTradesHistoryStore; local Trade tests;
│                                       new run-level tests)
├── trades_history.go        (new: Trade struct, tradesHistoryStore interface,
│                              pgTradesHistoryStore, NewPGTradesHistoryStore)
├── trades_history_test.go   (new: store round-trip + bounds + cancel tests)
├── trades_historyfakes/     (new: counterfeiter fake)
└── ...                      (unchanged)
```

### Boundary

`tradesHistoryStore` lives in the `copytrading` package next to `Backtester`.
The concrete `pgTradesHistoryStore` uses `pgxpool.Pool` directly, matching
the pattern in `strategy/http/backtest_store.go` and `run_store.go`. No
HTTP / JSON / MCP layer is involved — this is in-process data access.

### Public interface

```go
type Trade struct {
    TransactionID      string
    OrderID            string
    OrderSeq           int64
    OrderBookID        string
    UserID             string
    Asset0             string
    Quantity0          decimal.Decimal
    Price              decimal.Decimal
    Side               string
    AggressorIndicator bool
    CreatedAt          time.Time
}

type tradesHistoryStore interface {
    StreamTrades(ctx context.Context, userID string, start, end time.Time) (<-chan Trade, <-chan error)
    TradeBounds(ctx context.Context, userID string) (min, max time.Time, count int, err error)
}
```

`Trade` is intentionally decoupled from `doraclient.Trade`. It owns its
types: `Quantity0` and `Price` are `decimal.Decimal` (no string-encoded
decimals at the simulation boundary), `Side` is a plain string, and
`OrderSeq` is `int64` (not the DORA client's `int32`). The DORA client's
generated `MarshalJSON` / `UnmarshalJSON` / `MappedNullable` machinery is
not needed in the simulation.

Two store methods because the streaming path cannot distinguish "user
has no data" from "user has data, none in this window" without a second
lookup. `TradeBounds` is the diagnostic.

## Data flow

### Producer — `pgTradesHistoryStore.StreamTrades`

1. Validate `userID != ""`. If empty, send `errors.New("userID is required")`
   to `done`, close the channel, return.
2. Allocate a buffered channel: `ch := make(chan Trade, 1000)`.
3. Allocate a `done` channel: `done := make(chan error, 1)`.
4. Spawn a goroutine that:
   - Runs a keyset-cursor `SELECT` against `trades_history`, ordered by
     `(user_id, created_at, transaction_id)`, batched at 1000 rows.
   - The cursor key is `(created_at, transaction_id)`. Each batch reads
     rows strictly greater than the last row's `(created_at, transaction_id)`
     within the `user_id` filter and `[start, end]` range.
   - On each row, parses `quantity0` and `price` from `DECIMAL(42,18)`
     text into `decimal.Decimal`, parses `side` as a string, and pushes
     the `Trade` into `ch` (blocks if the buffer is full — backpressure).
   - On successful exhaustion, sends `nil` to `done`, closes `ch`.
   - On query / scan / parse error, sends the wrapped error to `done`,
     closes `ch`, returns.
   - Between batches, selects on `ctx.Done()`. On cancellation, sends
     `ctx.Err()` to `done`, closes `ch`, returns.
5. Returns `(ch, done)`.

Index alignment: the producer relies on
`idx_trades_history_user_id_created_at` for the `user_id` + `created_at`
range scan. The `transaction_id` tiebreaker sorts within a microsecond
unambiguously, so the consumer sees chronological order with no
duplicates. The migration already includes this index.

### Diagnostic — `pgTradesHistoryStore.TradeBounds`

```sql
SELECT MIN(created_at), MAX(created_at), COUNT(*)
FROM trades_history
WHERE user_id = $1
```

Single query, served by the same index. `min` and `max` are
`sql.NullTime`; if `count == 0` they are both zero-valued.

### `Backtester.Run`

```go
func (b *Backtester) Run(ctx context.Context, start, end time.Time) (BacktestResult, error) {
    followedTrader := b.strategy.cfg.FollowedTrader.String()

    min, max, count, err := b.history.TradeBounds(ctx, followedTrader)
    if err != nil {
        return BacktestResult{}, fmt.Errorf("trades history bounds: %w", err)
    }
    if count == 0 {
        return BacktestResult{}, fmt.Errorf(
            "no trades in trades_history for user %s; sync required", followedTrader,
        )
    }
    if start.Before(min) || end.After(max) {
        return BacktestResult{}, fmt.Errorf(
            "window [%s,%s] outside available data [%s,%s] for user %s",
            start.Format(time.RFC3339), end.Format(time.RFC3339),
            min.Format(time.RFC3339), max.Format(time.RFC3339),
            followedTrader,
        )
    }

    ch, done := b.history.StreamTrades(ctx, followedTrader, start, end)
    result, simErr := b.simulate(ctx, ch)
    prodErr := <-done

    if prodErr != nil {
        return BacktestResult{}, fmt.Errorf("stream trades: %w", prodErr)
    }
    if simErr != nil {
        return BacktestResult{}, simErr
    }
    return result, nil
}
```

The `DORA_API_KEY` lookup and lazy `b.trades` construction are removed
(there is no DORA client on the backtest path anymore). The `trades
tradesClient` field on `Backtester` is gone.

### `NewBacktester`

```go
func NewBacktester(s *Strategy, store tradesHistoryStore) *Backtester {
    return &Backtester{strategy: s, history: store}
}
```

The store is required (no nil-lazy-init fallback). Callers that don't
pass one will get a nil-pointer panic — this is intentional: the backtest
path has no other data source, and a nil store is a wiring bug we want to
surface at the first call, not at the first HTTP request inside the
backtest lifecycle.

### `Strategy.Backtest` wiring

```go
func (s *Strategy) Backtest(ctx context.Context, start, end time.Time) (backtestResult types.BacktestResult, err error) {
    if s.backtestStore == nil {
        return types.BacktestResult{}, errors.New("backtest store not configured: use WithBacktestStore")
    }
    backtester := NewBacktester(s, s.backtestStore)
    return backtester.Run(ctx, start, end)
}
```

New option:

```go
func WithBacktestStore(store tradesHistoryStore) func(*Strategy) {
    return func(s *Strategy) { s.backtestStore = store }
}
```

The existing `WithTradesClient` option and the `tradesClient tradesClient`
field on `Strategy` are removed. The `doraclient` import in `strategy.go`
stays because the live `Run` path (`s.marketAPI.CreateMarketOrder`) still
consumes `doraclient.Side`.

### Simulation

`Backtester.simulate`'s signature changes from
`(<-chan doraclient.Trade)` to `(<-chan copytrading.Trade)`. The body is
unchanged. The six helpers (`applyTrade`, `applyBuy`, `applySell`,
`closeLongPosition`, `closeShortPosition`, `emitTradeRecord`) all change
their `trade doraclient.Trade` parameter to `trade copytrading.Trade`.
The simulation logic, position math, decimal handling, and `summarise` /
`sharpe` computation stay byte-for-byte identical apart from the type
swap.

`tradeID` parsing: was `uuid.Parse(trade.TransactionId)` (which worked
because the DORA client stores UUIDs as strings). Stays the same — local
`Trade.TransactionID` is also a string.

The `applyBuy` / `applySell` branching becomes:

```go
var ourSignal types.Signal
if trade.Side == "buy" {
    ourSignal = types.SignalBuy
} else {
    ourSignal = types.SignalSell
}
```

The `doraclient.SIDE_BUY` constant reference is dropped.

## Error handling

| Situation | Behaviour |
| --- | --- |
| `TradeBounds` query fails | `Backtester.Run` returns wrapped error |
| User has 0 rows total | `errors.New("no trades in trades_history for user <id>; sync required")` |
| Window before `MIN(created_at)` or after `MAX(created_at)` | `fmt.Errorf("window [%s,%s] outside available data [%s,%s] for user <id>", ...)` |
| `StreamTrades` producer fails mid-read | Producer sends wrapped error to `done`, closes `ch`. `simulate` exits via `for trade := range ch`. `Run` reads `done`, returns wrapped error. Partial `BacktestResult` discarded. |
| `ctx` cancelled mid-stream | Producer's keyset loop and `simulate`'s consumer loop both check `ctx.Done()`. `Run` returns `ctx.Err()`. |
| Window in-bounds but 0 rows | Zero `BacktestResult`, `nil` error |
| `quantity0` or `price` decimal parse failure on a row | `simulate` returns wrapped error (matches today's behaviour for malformed DORA rows) |
| `WithBacktestStore` not passed to the strategy | `Strategy.Backtest` returns `errors.New("backtest store not configured: use WithBacktestStore")` |
| Empty `userID` passed to the store | `StreamTrades` sends `errors.New("userID is required")` to `done` |

## Concurrency

- **Single producer goroutine** (started by `pgTradesHistoryStore.StreamTrades`).
- **Single consumer** (the caller's `simulate` loop, runs in the caller's
  goroutine).
- **No mutex needed**: the channel is the synchronisation primitive.
- **Buffer size 1000** matches the SQL batch size. One batch of in-flight
  rows is enough.
- **Keyset pagination**: the cursor key is `(created_at, transaction_id)`.
  Each batch is a `SELECT ... WHERE user_id = $1 AND created_at >= $2 AND
  created_at <= $3 AND (created_at, transaction_id) > ($cursor_time,
  $cursor_id) ORDER BY created_at, transaction_id LIMIT 1000`. Stops when
  the batch returns < 1000 rows.
- **`TradeBounds` is called before `StreamTrades`** and is not on the hot
  path. A single row is read; no goroutine, no streaming.

## Testing

### Unit tests — `backtest_test.go`

`fakeTradesHistoryStore` replaces `fakeTradesClient`. It returns a
pre-filled channel + a fixed bounds result, and never spawns a goroutine
(synchronous; `simulate` drains and returns).

Existing simulation tests are rewritten to construct local `Trade` values
rather than `doraclient.Trade` values. The trade scenarios (long-only,
short-only, position flips, multi-asset, no-cash, zero-price skip, etc.)
are preserved verbatim — only the input type and the field accessors
change.

New run-level tests:

- `TestBacktesterRun_NoDataForUser` — `TradeBounds` returns `count=0`.
  `Run` returns the "sync required" error. No stream is opened.
- `TestBacktesterRun_WindowOutsideData` — bounds return a range that
  doesn't cover the window. `Run` returns the "window outside available
  data" error naming the bounds. No stream is opened.
- `TestBacktesterRun_EmptyResultInBounds` — bounds cover the window,
  `StreamTrades` returns 0 rows. `Run` returns zero `BacktestResult`,
  `nil` error.
- `TestBacktesterRun_StreamError` — producer sends a wrapped error to
  `done`. `Run` returns wrapped error, partial result discarded.
- `TestBacktesterRun_ContextCancelled` — `ctx` cancelled before `Run`.
  Both `TradeBounds` and `StreamTrades` respect cancellation. `Run`
  returns `ctx.Err()`.

### Store tests — `trades_history_test.go`

The repo currently has no test-DB pattern (no `testcontainer`, `pgxmock`,
or `dockertest` usage). The implementation plan will pick between:

- (A) Integration tests against a real Postgres (CI must have a PG
  instance available). The `strategy/http` package is wired for PG but
  has no dedicated tests today, so this would set a precedent.
- (B) `pgxmock` for unit tests of the store (new dependency, fast,
  deterministic; loses real-schema validation).
- (C) Abstract the SQL behind an interface and test with an in-memory
  fake (loses real-schema validation entirely).

The test list below assumes (A) — a real PG. If the plan picks (B) or
(C), the test list stays the same in name; the wiring changes.

- `TestPGTradesHistoryStore_StreamTrades_Ordered` — insert 1500 rows in
  shuffled order, stream them, verify they come back ordered by
  `(created_at, transaction_id)` and that all 1500 are returned.
- `TestPGTradesHistoryStore_StreamTrades_RangeFilter` — insert 100 rows
  outside `[start,end]` and 50 inside, stream for the window, verify
  exactly 50 come back.
- `TestPGTradesHistoryStore_StreamTrades_Empty` — no rows in the table
  for the user, channel closes immediately, `done` is `nil`.
- `TestPGTradesHistoryStore_StreamTrades_ContextCancelled` — cancel
  context mid-batch, verify channel closes and `done` carries
  `ctx.Err()`.
- `TestPGTradesHistoryStore_TradeBounds_Empty` — empty table, returns
  zero `min`/`max` and `count=0`.
- `TestPGTradesHistoryStore_TradeBounds_Populated` — insert 10 rows,
  verify `min`/`max`/`count` are correct.

### Migration

`migrations/007_add_trade_history.sql` is already updated in the working
tree (PK, NOT NULLs, index). No further changes needed. The plan will
include running `tern migrate --config migrations/tern.conf` as a setup
step.

## Trade-off summary

| Choice | Why |
| --- | --- |
| New local `Trade` type, decoupled from `doraclient.Trade` | Cleaner simulation boundary; no DORA-specific quirks (string decimals, `int32` OrderSeq, generated JSON) inside the simulation. Mapping happens once at the row boundary. |
| Stream via keyset cursor on `(created_at, transaction_id)` | No `OFFSET` (scales for 770k-row windows). Uses the existing `idx_trades_history_user_id_created_at` index for the `user_id` + range scan. |
| Batch size 1000, channel buffer 1000 | One batch in flight at a time. Larger than the DORA page size of 100 because PG round-trips are cheap and we want fewer idle gaps. |
| `TradeBounds` is a separate query | Cannot distinguish "no data" from "no data in window" from inside `StreamTrades` without an extra read. A diagnostic query before the stream is cleaner than threading state through the producer. |
| Delete `tradesClient` + `doraTradesClient` | Nothing else consumes them (the live `Run` uses `s.tradeStream`, a WebSocket subscription). Dead code is a maintenance burden and a misleading hint that the backtest still needs `DORA_API_KEY`. |
| `WithBacktestStore` replaces `WithTradesClient` | Symmetric with the deleted option. Forces explicit wiring — a backtest without a store is a configuration bug, not a lazy fallback. |
| Fail with clear error on missing data (no DORA fallback) | Per requirement. The caller knows the data is missing, can trigger a sync (out of scope for this spec), and retry. |

## Files touched

| File | Change |
| --- | --- |
| `strategy/copytrading/trades_history.go` | New. `Trade` struct, `tradesHistoryStore` interface, `pgTradesHistoryStore`, `NewPGTradesHistoryStore(pool)`. |
| `strategy/copytrading/trades_history_test.go` | New. Store round-trip + bounds + cancellation tests. |
| `strategy/copytrading/trades_historyfakes/fake_trades_history_store.go` | New. `//go:generate counterfeiter` fake for unit tests. |
| `strategy/copytrading/backtest.go` | Modify. `tradesClient` interface, `doraTradesClient`, `newDoraTradesClient`, `DORA_API_KEY` lookup deleted. `Backtester` gains `history tradesHistoryStore` field. `NewBacktester(s, store)` signature. `Run` runs `TradeBounds` + delegates to `history.StreamTrades`. `simulate` and 6 helpers change `doraclient.Trade` → `copytrading.Trade`. |
| `strategy/copytrading/backtest_test.go` | Modify. `fakeTradesClient` deleted, `fakeTradesHistoryStore` added. Existing simulation tests rewritten to feed `copytrading.Trade`. New run-level tests. |
| `strategy/copytrading/strategy.go` | Modify. `tradesClient` field + `WithTradesClient` option deleted. `backtestStore` field + `WithBacktestStore` option added. `Strategy.Backtest` constructs the backtester with `s.backtestStore`. |
| `migrations/007_add_trade_history.sql` | Already updated in the working tree. No further change. |

## Out of scope

- The sync job / daemon that populates `trades_history` from DORA is a
  separate spec. The migration is in place; the producer of rows is not.
- The live `Run` path is unchanged. It still uses the WebSocket
  `s.tradeStream`.
- The MCP `strategy_backtest_*` tools, the HTTP handler, and the
  `BacktestResult` shape are unchanged.
- `doraclient` is still imported by `strategy/copytrading/strategy.go`
  for the live Run path. The import is removed only from `backtest.go`.
- No changes to `cmd/strategy-server` wiring beyond passing
  `WithBacktestStore` when constructing the strategy. The wiring change
  is included in the implementation plan.
