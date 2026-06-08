# Copy-Trading Streaming Backtest

## Problem

The current backtest fetches **all** trades for the followed trader before running
simulation. The DORA `/v1/trades` endpoint returns at most 100 trades per page
(the API caps the `limit` parameter at 100), so a 4-day window on
`TRADER_NVDA_USD` (85,822 trades) requires ~858 sequential HTTP calls before
simulation even starts. The end-to-end backtest we just ran produced 100
`TradeRecord`s and 57 `ClosedTrade`s — the simulation was running on a tiny
slice of the followed trader's actual activity, not the full history the
specification requires.

A 90-day window on the same trader is on the order of ~770k trades, ~7,700
HTTP calls, and 5+ minutes of pure network round-trips before any P&L is
computed. The backtest also buffers every trade in memory at once, which
scales poorly.

The live `Run` path consumes trades from a `TradeStream` channel and processes
each one as it arrives. The backtest should do the same: page in trades,
process them as they arrive, no in-memory buffering, no waiting for the full
dataset.

## Solution

A single producer goroutine pages through `/v1/trades` and pushes each page
into a buffered channel. A single consumer (the simulation loop) reads from
the channel and processes each trade as it arrives. Ordering is guaranteed
implicitly by the DORA API (ascending by `created_at`), so no consumer-side
sort is needed.

```
┌──────────────────┐    chan doraclient.Trade (cap 100)    ┌──────────────────┐
│ producer goroutine│ ──────────────────────────────────> │  simulate loop   │
│  - page loop     │                                      │  - applyBuy /   │
│  - close channel │                                      │    applySell     │
│  - on error:     │                                      │  - position map  │
│    close + return│                                      │  - emit records  │
└──────────────────┘                                      └──────────────────┘
        │                                                          │
        └──> error (returned to caller via `done`)                ──> BacktestResult
```

## Architecture

### `tradesClient` interface

```go
type tradesClient interface {
    GetTradeStream(
        ctx context.Context,
        userID string,
        start, end time.Time,
    ) (<-chan doraclient.Trade, <-chan error)
}
```

The old `ListOrderBooks` + `GetTrades(ctx, userID, orderBookIDs, start, end)`
methods are removed. The DORA API supports a `user_ids` filter, so per-
orderbook fan-out is no longer needed at the call site.

### `doraTradesClient.GetTradeStream`

Concrete implementation:

1. Create a buffered channel: `ch := make(chan doraclient.Trade, 100)`.
2. Create a `done` channel: `done := make(chan error, 1)`.
3. Spawn a goroutine that:
   - Pages with `page=1, 2, 3, ...` and `limit=100` until a short page
     (`len(resp.Data) < 100`).
   - On each successful page, ranges over `resp.Data` and sends each trade
     to `ch` (blocks if the channel buffer is full, providing backpressure).
   - On `req.Execute()` failure, sends the error to `done` and returns.
   - On successful exhaustion, sends `nil` to `done` and returns.
   - Closes `ch` before returning (always, success or error).
4. Returns `(ch, done)`.

The `user_ids=<followed>` filter is applied at every request, so the API
returns only the followed trader's trades — no client-side filter needed.

Ordering: the DORA API returns trades in ascending `created_at` order
implicitly (no `order` query parameter is supported). The producer never
reorders, so the consumer sees trades in chronological order.

### `Backtester.Run`

```go
func (b *Backtester) Run(ctx context.Context, start, end time.Time) (BacktestResult, error) {
    if b.trades == nil {
        apiKey := os.Getenv("DORA_API_KEY")
        if apiKey == "" {
            return BacktestResult{}, errors.New("DORA_API_KEY not set")
        }
        b.trades = newDoraTradesClient(apiKey)
    }

    followedTrader := b.strategy.cfg.FollowedTrader.String()
    ch, done := b.trades.GetTradeStream(ctx, followedTrader, start, end)

    result, simErr := b.simulate(ctx, ch)
    prodErr := <-done

    if prodErr != nil {
        return BacktestResult{}, fmt.Errorf("get trade stream: %w", prodErr)
    }
    if simErr != nil {
        return BacktestResult{}, simErr
    }
    return result, nil
}
```

Notes:
- The orderbook loop, the in-memory sort, and the `tr.UserId == followedTrader`
  filter are all removed.
- `simulate` drains the channel until it closes, then returns.
- The `done` channel is read after `simulate` returns to surface any
  producer-side error. If both fail, the producer error wins (returned
  verbatim; `simulate` error is swallowed). Reasoning: a producer error
  invalidates the in-progress simulation result anyway.

### `Backtester.simulate(ctx, ch <-chan doraclient.Trade)`

The body of `simulate` is unchanged apart from the input source. The current
`for _, trade := range trades` becomes `for trade := range ch`. The
position map, `applyBuy` / `applySell` / `closeLongPosition` /
`closeShortPosition` helpers, and the `summarise` / `sharpe` computation
all stay as-is.

The slice form `simulate(ctx, trades []doraclient.Trade)` is removed. All
test call sites are updated to feed trades through a channel.

## Error handling

- **Producer error mid-fetch**: the producer sends the error to `done`,
  closes `ch`, and returns. The consumer's `for trade := range ch` loop
  exits naturally (no panic, no half-closed state). `Run` reads `done`,
  sees the error, and returns it. The partial `BacktestResult` from
  `simulate` is discarded.
- **Consumer / context cancellation**: if `ctx` is cancelled while
  `simulate` is running, the consumer's loop should respect the context.
  Either by checking `ctx.Done()` between trades, or by having the producer
  also watch the context. The current `simulate` does not have a
  cancellation check in its loop — the channel read will block forever if
  the producer stops without closing. Producer errors propagate to `done`,
  so under normal failure the channel will close. Under external
  cancellation (user clicks cancel), the producer goroutine may not
  observe the context, and the channel will sit open. We will address
  this by having the producer select on `ctx.Done()` in its page loop.
- **Empty result set**: if the followed trader had no trades in the
  window, the producer returns immediately with `ch` closed, `simulate`
  sees an empty channel, returns a zero-value `BacktestResult`. `Run`
  returns `(BacktestResult{}, nil)` — the empty result is the answer.

## Concurrency

- **Single producer goroutine** (started by `doraTradesClient.GetTradeStream`).
- **Single consumer** (the caller's `simulate` loop, runs in the caller's
  goroutine).
- **No mutex needed**: the channel is the synchronisation primitive.
- **Buffer size 100** matches the DORA page size. The producer can push a
  full page before blocking; the consumer drains it.
- **No per-orderbook fan-out**: dropped in favour of the DORA `user_ids`
  filter. This eliminates the need to merge-sort across multiple producers.

## Testing

### Unit tests

`fakeTradesClient.GetTradeStream` is updated to:

1. Accept a slice of trades in test order.
2. Pre-fill the channel with the trades (no goroutine needed for unit
   tests — the test is synchronous, and `simulate` will drain and return).
3. Close the channel.

Existing simulation tests in `backtest_test.go` already define trades in
chronological order. The only change is the call site: instead of
`b.simulate(ctx, trades)`, tests do:

```go
ch := make(chan doraclient.Trade, len(trades))
for _, t := range trades {
    ch <- t
}
close(ch)
result, err := b.simulate(ctx, ch)
```

A test helper `feedTrades(t, trades)` hides this boilerplate.

### Multi-page test

New test: `TestSimulate_MultiPageStream` exercises the producer pattern
with 3 pages of 100 trades = 300 trades, verifying that the simulation
correctly processes all of them across page boundaries.

### End-to-end live verification

The existing live verification (backtest for `TRADER_NVDA_USD` over the
4-day window 2026-05-26 → 2026-05-30) is rerun. Expected: the result now
contains ~85,000 `TradeRecord`s (not 100), and the closed-trade count and
P&L reflect the full activity, not a truncated subset.

## Trade-off summary

| Choice | Why |
| --- | --- |
| One producer, one consumer | Matches the user's explicit request. Simpler than per-orderbook fan-in. |
| Drop orderbook loop | DORA API supports `user_ids` filter; per-OB round-trips are unnecessary. |
| Channel buffered to 100 | Matches DORA page size; one page of in-flight is enough. |
| Error via `done` chan | Idiomatic Go. Consumer doesn't need to handle errors mid-loop. |
| API returns `created_at` ascending implicitly | No consumer-side sort needed. |
| `simulate` takes a channel only | One canonical path. Tests feed through a channel. |

## Files touched

- `strategy/copytrading/backtest.go` — `tradesClient` interface,
  `doraTradesClient.GetTradeStream`, `Backtester.Run`, `Backtester.simulate`.
- `strategy/copytrading/backtest_test.go` — `fakeTradesClient` updated,
  all `simulate` call sites updated, new `TestSimulate_MultiPageStream`.
- No other files affected. The `BacktestResult` shape, the strategy
  config, the HTTP handler, and the MCP server all stay as-is.
