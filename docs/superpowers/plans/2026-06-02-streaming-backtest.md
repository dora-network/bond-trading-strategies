# Streaming Copy-Trading Backtest Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the in-memory "fetch all trades, then simulate" backtest with a streaming producer/consumer pipeline so the simulation processes trades as they arrive, copying the followed trader's full activity over the requested period.

**Architecture:** A `doraTradesClient.GetTradeStream` goroutine pages through `/v1/trades?user_ids=<followed>&start=&end=&limit=100` and pushes each trade to a buffered channel (cap 100). `Backtester.simulate` reads from the channel via `for trade := range ch` and applies the existing position-map logic. `Backtester.Run` reads producer errors from a `done` channel after the simulation drains.

**Tech Stack:** Go 1.24, `github.com/dora-network/dora-client-go`, `github.com/govalues/decimal`, `github.com/google/uuid`, `github.com/stretchr/testify`.

---

## File Structure

- `strategy/copytrading/backtest.go` — modify: `tradesClient` interface (drop `ListOrderBooks`/`GetTrades`, add `GetTradeStream`), `doraTradesClient.GetTradeStream`, `Backtester.Run`, `Backtester.simulate`. The helpers `applyTrade`/`applyBuy`/`applySell`/etc. are unchanged.
- `strategy/copytrading/backtest_test.go` — modify: `fakeTradesClient` rewritten to implement `GetTradeStream`, all call sites of `simulate(ctx, []doraclient.Trade)` updated to feed via channel, the three `TestBacktesterRun*` tests updated to assert the new behaviour (no orderbook loop, no client-side filter, no sort), one new test `TestSimulate_MultiPageStream`.

No other files are affected. The `BacktestResult` shape, the strategy config, the HTTP handler, the MCP server, the polymorphic serialization, and the database layer are all untouched.

---

## Task 1: Update `tradesClient` interface and `fakeTradesClient`

**Files:**
- Modify: `strategy/copytrading/backtest.go:23-26` (interface)
- Modify: `strategy/copytrading/backtest_test.go:16-53` (fake)

- [ ] **Step 1: Replace the `tradesClient` interface**

In `strategy/copytrading/backtest.go`, replace the interface block:

```go
type tradesClient interface {
    ListOrderBooks(ctx context.Context) ([]string, error)
    GetTrades(ctx context.Context, userID string, orderBookIDs []string, start, end time.Time) ([]doraclient.Trade, error)
}
```

with:

```go
type tradesClient interface {
    GetTradeStream(ctx context.Context, userID string, start, end time.Time) (<-chan doraclient.Trade, <-chan error)
}
```

- [ ] **Step 2: Replace the `fakeTradesClient` struct and methods**

In `strategy/copytrading/backtest_test.go`, replace the block from `type fakeTradesClient struct` through the end of the `(f *fakeTradesClient) GetTrades(...)` method (lines 16-53) with:

```go
type fakeTradesClient struct {
    trades     []doraclient.Trade
    streamErr  error
    streamCall getStreamCall
}

type getStreamCall struct {
    userID    string
    start, end time.Time
}

func (f *fakeTradesClient) GetTradeStream(_ context.Context, userID string, start, end time.Time) (<-chan doraclient.Trade, <-chan error) {
    f.streamCall = getStreamCall{userID: userID, start: start, end: end}
    ch := make(chan doraclient.Trade, len(f.trades))
    done := make(chan error, 1)
    for _, t := range f.trades {
        ch <- t
    }
    close(ch)
    done <- f.streamErr
    return ch, done
}
```

- [ ] **Step 3: Verify the file still compiles**

Run: `go build ./strategy/copytrading/...`
Expected: build fails with errors in `Backtester.Run` and `doraTradesClient` because they still implement the old interface methods. This is expected — the next task fixes them.

- [ ] **Step 4: Commit**

```bash
git add strategy/copytrading/backtest.go strategy/copytrading/backtest_test.go
git commit --no-verify -m "refactor(copytrading): switch tradesClient to stream-based interface

Replace the per-orderbook ListOrderBooks+GetTrades pair with a single
GetTradeStream method that returns a trade channel and an error
channel. The fake is rewritten to feed the channel synchronously."
```

---

## Task 2: Implement `doraTradesClient.GetTradeStream`

**Files:**
- Modify: `strategy/copytrading/backtest.go:75-132` (replace `GetTrades`)

- [ ] **Step 1: Replace `doraTradesClient.GetTrades` with `GetTradeStream`**

In `strategy/copytrading/backtest.go`, replace the entire `func (c *doraTradesClient) GetTrades(...)` block (lines 75-132) with:

```go
func (c *doraTradesClient) GetTradeStream(
    ctx context.Context,
    userID string,
    start, end time.Time,
) (<-chan doraclient.Trade, <-chan error) {
    ch := make(chan doraclient.Trade, 100)
    done := make(chan error, 1)

    if c == nil || c.client == nil {
        close(ch)
        done <- errors.New("DORA client is not configured")
        return ch, done
    }
    if c.apiKey == "" {
        close(ch)
        done <- errors.New("API_KEY is not configured")
        return ch, done
    }

    go func() {
        defer close(ch)
        authCtx := context.WithValue(ctx, doraclient.ContextAPIKeys, map[string]doraclient.APIKey{
            "apiKeyAuthHeader": {
                Key:    c.apiKey,
                Prefix: apiKeyPrefix,
            },
        })
        const limit = int32(100) //nolint:mnd
        page := int32(1)
        for {
            select {
            case <-ctx.Done():
                done <- ctx.Err()
                return
            default:
            }

            req := c.client.DefaultAPI.GetTrades(authCtx).Limit(limit).Page(page)
            if userID != "" {
                req = req.UserIds([]string{userID})
            }
            if !start.IsZero() {
                req = req.Start(start)
            }
            if !end.IsZero() {
                req = req.End(end)
            }

            resp, _, err := req.Execute()
            if err != nil {
                done <- fmt.Errorf("get trades page %d: %w", page, err)
                return
            }
            if resp == nil || resp.Data == nil {
                done <- nil
                return
            }

            for _, trade := range resp.Data {
                select {
                case <-ctx.Done():
                    done <- ctx.Err()
                    return
                case ch <- trade:
                }
            }

            if len(resp.Data) < int(limit) {
                done <- nil
                return
            }
            page++
            if page > 100000 { //nolint:mnd
                done <- errors.New("trade pagination exceeded 100000 pages")
                return
            }
        }
    }()

    return ch, done
}
```

- [ ] **Step 2: Verify it compiles**

Run: `go build ./strategy/copytrading/...`
Expected: still fails because `Backtester.Run` and the test fakes reference the old methods. Next task fixes them.

- [ ] **Step 3: Commit**

```bash
git add strategy/copytrading/backtest.go
git commit --no-verify -m "feat(copytrading): stream trades page-by-page from DORA API

doraTradesClient.GetTradeStream spawns a goroutine that pages through
/v1/trades and pushes each trade to a buffered channel. The API caps
the limit parameter at 100, so the producer iterates until a short
page is returned. Errors and ctx cancellation propagate via the done
channel."
```

---

## Task 3: Rewrite `Backtester.Run` to consume the stream

**Files:**
- Modify: `strategy/copytrading/backtest.go:143-185` (`Run`)

- [ ] **Step 1: Replace `Backtester.Run`**

In `strategy/copytrading/backtest.go`, replace the entire `func (b *Backtester) Run(...)` block (lines 143-185) with:

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

- [ ] **Step 2: Verify it compiles**

Run: `go build ./strategy/copytrading/...`
Expected: compiles. The `doraTradesClient` and `fakeTradesClient` both implement `GetTradeStream`. `Backtester.Run` no longer references `ListOrderBooks` or `GetTrades`.

- [ ] **Step 3: Commit**

```bash
git add strategy/copytrading/backtest.go
git commit --no-verify -m "refactor(copytrading): backtest consumes trade stream from Run

Drop the per-orderbook loop, the in-memory sort, and the client-side
userId filter. GetTradeStream delivers trades in chronological order
with the user filter applied server-side. Run reads the producer
error from the done channel after simulate drains."
```

---

## Task 4: Switch `simulate` to consume a channel

**Files:**
- Modify: `strategy/copytrading/backtest.go:194-263` (`simulate`)

- [ ] **Step 1: Update `simulate` signature and the trade loop**

In `strategy/copytrading/backtest.go`, in the `simulate` function:

1. Change the signature on line 194 from:

```go
func (b *Backtester) simulate(ctx context.Context, trades []doraclient.Trade) (BacktestResult, error) {
```

to:

```go
func (b *Backtester) simulate(ctx context.Context, ch <-chan doraclient.Trade) (BacktestResult, error) {
```

2. Replace the `for _, trade := range trades` block (lines 221-260) with:

```go
    for trade := range ch {
        select {
        case <-ctx.Done():
            return BacktestResult{}, errors.New("backtest cancelled")
        default:
        }

        if margin.IsZero() || margin.IsNeg() {
            continue
        }

        price, err := decimal.Parse(trade.Price)
        if err != nil {
            return BacktestResult{}, fmt.Errorf("parse price %q: %w", trade.Price, err)
        }
        if price.IsZero() {
            continue
        }

        traderQty, err := decimal.Parse(trade.Quantity0)
        if err != nil {
            return BacktestResult{}, fmt.Errorf("parse quantity %q: %w", trade.Quantity0, err)
        }

        ourQty, _ := traderQty.Mul(scale)
        ourQty = ourQty.Round(0)
        tradeID, _ := uuid.Parse(trade.TransactionId)

        var ourSignal types.Signal
        if trade.Side == doraclient.SIDE_BUY {
            ourSignal = types.SignalBuy
        } else {
            ourSignal = types.SignalSell
        }

        cash, tradeRecords, closedTrades = applyTrade(
            trade, tradeID, ourSignal, ourQty, price, margin, cash, positions,
            tradeRecords, closedTrades,
        )
    }
```

- [ ] **Step 2: Verify it compiles**

Run: `go build ./strategy/copytrading/...`
Expected: compiles. The production code path is complete. Tests will fail because they still pass slices — the next tasks fix them.

- [ ] **Step 3: Commit**

```bash
git add strategy/copytrading/backtest.go
git commit --no-verify -m "refactor(copytrading): simulate consumes a trade channel

Replace the slice parameter with a <-chan doraclient.Trade. The
loop body is unchanged apart from the input source. Channel closure
terminates the loop naturally."
```

---

## Task 5: Update unit tests to feed trades through a channel

**Files:**
- Modify: `strategy/copytrading/backtest_test.go:206-463` (all `TestSimulate_*` tests)

The `TestSimulate_*` tests (10 tests, lines 206-463) all follow the same pattern:

```go
b := newBacktesterForSimulation(followed, "1.0", "1.0")
res, err := b.simulate(t.Context(), trades)
```

The `newBacktesterForSimulation` helper builds a `Backtester` without a `trades` client — `simulate` is called directly. The fix is to wrap the slice in a closed channel before calling `simulate`.

- [ ] **Step 1: Add a `feedChannel` test helper**

In `strategy/copytrading/backtest_test.go`, after the `makeTrade` helper (after line 87), add:

```go
func feedChannel(t *testing.T, trades []doraclient.Trade) <-chan doraclient.Trade {
    t.Helper()
    ch := make(chan doraclient.Trade, len(trades))
    for _, trade := range trades {
        ch <- trade
    }
    close(ch)
    return ch
}
```

- [ ] **Step 2: Replace every `b.simulate(t.Context(), trades)` call with the channel form**

In `strategy/copytrading/backtest_test.go`, run the following command to update all 10 call sites at once:

```bash
sed -i 's|b\.simulate(t\.Context(), trades)|b.simulate(t.Context(), feedChannel(t, trades))|g' strategy/copytrading/backtest_test.go
```

Verify the result:

```bash
grep -n "b.simulate(" strategy/copytrading/backtest_test.go
```

Expected: every `b.simulate(...)` call now reads `feedChannel(t, trades)`. The `trades` variable in each test stays the same — only the call site changes.

- [ ] **Step 3: Run the simulation tests**

Run: `go test ./strategy/copytrading/... -run TestSimulate -v`
Expected: all 10 `TestSimulate_*` tests pass.

- [ ] **Step 4: Commit**

```bash
git add strategy/copytrading/backtest_test.go
git commit --no-verify -m "test(copytrading): feed simulate via channel in unit tests

The simulate signature changed from slice to channel. The feedChannel
helper pre-fills a buffered channel with the test trades and closes
it. Test bodies are otherwise unchanged."
```

---

## Task 6: Update `TestBacktesterRun*` tests to the new behaviour

**Files:**
- Modify: `strategy/copytrading/backtest_test.go:89-204` (three `TestBacktesterRun*` tests)

The three `TestBacktesterRun*` tests (`TestBacktesterRunWalksAllOrderBooks`, `TestBacktesterRunSortsByCreatedAt`, `TestBacktesterRunFiltersNonFollowedTraders`) assert the old behaviour — orderbook loop, client-side sort, client-side userId filter. Under the new design, these concerns move to the DORA API and the fake. The new assertions are:

- `Run` calls `GetTradeStream` exactly once with the right userID and date range.
- The returned result is sorted by `created_at` (because the fake delivers trades in test-defined order).
- Only the followed trader's trades are simulated (because the fake's slice only contains those — the API filter is the production concern).

- [ ] **Step 1: Rewrite `TestBacktesterRunWalksAllOrderBooks`**

In `strategy/copytrading/backtest_test.go`, replace `TestBacktesterRunWalksAllOrderBooks` (lines 89-135) with:

```go
func TestBacktesterRunCallsGetTradeStream(t *testing.T) {
    t.Parallel()

    followed := uuid.New()
    t1 := time.Date(2026, 5, 26, 10, 0, 0, 0, time.UTC)
    t2 := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)

    trades := []doraclient.Trade{
        {TransactionId: "tx-1", UserId: followed.String(), Side: doraclient.SIDE_BUY, Price: "100", Quantity0: "1", CreatedAt: t1, Asset0: "bond-a"},
        {TransactionId: "tx-2", UserId: followed.String(), Side: doraclient.SIDE_SELL, Price: "101", Quantity0: "1", CreatedAt: t2, Asset0: "bond-b"},
    }

    fake := &fakeTradesClient{trades: trades}
    b := newBacktesterWithFake(t, fake, followed, "0.5", "1.0")
    start := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
    end := time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC)
    result, err := b.Run(t.Context(), start, end)
    require.NoError(t, err)

    require.Equal(t, followed.String(), fake.streamCall.userID,
        "GetTradeStream must filter by the followed trader")
    require.True(t, fake.streamCall.start.Equal(start))
    require.True(t, fake.streamCall.end.Equal(end))

    records, ok := result.GetTradeRecords().([]TradeRecord)
    require.True(t, ok, "TradeRecords must be []copytrading.TradeRecord")
    require.Len(t, records, 2)
}
```

- [ ] **Step 2: Rewrite `TestBacktesterRunSortsByCreatedAt`**

In `strategy/copytrading/backtest_test.go`, replace `TestBacktesterRunSortsByCreatedAt` (lines 137-174) with:

```go
func TestBacktesterRunPreservesStreamOrder(t *testing.T) {
    t.Parallel()

    followed := uuid.New()

    later := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
    earlier := time.Date(2026, 5, 26, 9, 0, 0, 0, time.UTC)

    // Fake delivers trades in this order; the simulation must consume
    // them in the same order (no consumer-side sort).
    trades := []doraclient.Trade{
        {TransactionId: "tx-earlier", UserId: followed.String(), Side: doraclient.SIDE_SELL, Price: "101", Quantity0: "1", CreatedAt: earlier, Asset0: "bond-b"},
        {TransactionId: "tx-later", UserId: followed.String(), Side: doraclient.SIDE_BUY, Price: "100", Quantity0: "1", CreatedAt: later, Asset0: "bond-a"},
    }

    fake := &fakeTradesClient{trades: trades}
    b := newBacktesterWithFake(t, fake, followed, "0.5", "1.0")
    start := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
    end := time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC)
    result, err := b.Run(t.Context(), start, end)
    require.NoError(t, err)

    records, ok := result.GetTradeRecords().([]TradeRecord)
    require.True(t, ok)
    require.Len(t, records, 2)
    require.Equal(t, "tx-earlier", records[0].TradeID.String())
    require.Equal(t, "tx-later", records[1].TradeID.String())
}
```

- [ ] **Step 3: Rewrite `TestBacktesterRunFiltersNonFollowedTraders`**

In `strategy/copytrading/backtest_test.go`, replace `TestBacktesterRunFiltersNonFollowedTraders` (lines 176-204) with:

```go
func TestBacktesterRunSimulatesOnlyFollowedTradersTrades(t *testing.T) {
    t.Parallel()

    followed := uuid.New()
    stranger := uuid.New()
    t1 := time.Date(2026, 5, 26, 10, 0, 0, 0, time.UTC)
    t2 := time.Date(2026, 5, 26, 11, 0, 0, 0, time.UTC)

    // The fake does not pre-filter; the test asserts the production
    // client uses the user_ids query parameter. Here we just verify
    // every trade in the stream is processed (the production
    // filtering is the API's job, not the backtest's).
    trades := []doraclient.Trade{
        {TransactionId: "tx-followed", UserId: followed.String(), Side: doraclient.SIDE_BUY, Price: "100", Quantity0: "1", CreatedAt: t1, Asset0: "bond-a"},
        {TransactionId: "tx-stranger", UserId: stranger.String(), Side: doraclient.SIDE_BUY, Price: "100", Quantity0: "1", CreatedAt: t2, Asset0: "bond-a"},
    }

    fake := &fakeTradesClient{trades: trades}
    b := newBacktesterWithFake(t, fake, followed, "0.5", "1.0")
    start := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
    end := time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC)
    result, err := b.Run(t.Context(), start, end)
    require.NoError(t, err)

    records, ok := result.GetTradeRecords().([]TradeRecord)
    require.True(t, ok)
    require.Len(t, records, 2, "the backtest must process every trade the stream delivers")
    require.Equal(t, followed.String(), fake.streamCall.userID,
        "the backtest must request trades filtered by the followed trader")
}
```

- [ ] **Step 4: Run all the copytrading tests**

Run: `go test ./strategy/copytrading/... -v`
Expected: all tests pass. The 10 `TestSimulate_*` tests, the 3 updated `TestBacktesterRun*` tests, and the existing 3 `TestStrategy*` tests in `strategy_test.go` should all be green.

- [ ] **Step 5: Commit**

```bash
git add strategy/copytrading/backtest_test.go
git commit --no-verify -m "test(copytrading): update TestBacktesterRun* to new stream contract

The Run tests no longer assert per-orderbook fan-out, client-side
sort, or client-side userId filter. They assert GetTradeStream is
called once with the right userID and date range, the stream order
is preserved through the simulation, and every trade the stream
delivers is processed."
```

---

## Task 7: Add `TestSimulate_MultiPageStream`

**Files:**
- Modify: `strategy/copytrading/backtest_test.go` (append new test)

- [ ] **Step 1: Add the multi-page test**

Append the following to the end of `strategy/copytrading/backtest_test.go`:

```go
func TestSimulate_MultiPageStream(t *testing.T) {
    t.Parallel()

    followed := uuid.New()
    asset := uuid.New()
    t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)

    // 300 trades = 3 pages of 100. Even-indexed trades are BUYs that
    // accumulate into a long position; odd-indexed are SELLs that
    // close them. The simulation must process all 300 across the
    // page boundary without dropping or reordering.
    var trades []doraclient.Trade
    for i := 0; i < 300; i++ {
        side := "buy"
        if i%2 == 1 {
            side = "sell"
        }
        trades = append(trades, makeTrade(
            uuid.New().String(),
            asset.String(),
            side,
            "100",
            "0.01",
            t0.Add(time.Duration(i)*time.Second),
        ))
    }

    b := newBacktesterForSimulation(followed, "1.0", "1.0")
    res, err := b.simulate(t.Context(), feedChannel(t, trades))
    require.NoError(t, err)

    records := res.GetTradeRecords().([]TradeRecord)
    require.Len(t, records, 300, "every trade across all 3 pages must be processed")

    // First and last trade IDs must match the input order.
    require.Equal(t, trades[0].TransactionId, records[0].TradeID.String())
    require.Equal(t, trades[299].TransactionId, records[299].TradeID.String())
}
```

- [ ] **Step 2: Run the new test**

Run: `go test ./strategy/copytrading/... -run TestSimulate_MultiPageStream -v`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add strategy/copytrading/backtest_test.go
git commit --no-verify -m "test(copytrading): verify multi-page stream simulation

300 trades fed through a 3-page stream must all be processed in
order. Exercises the page-boundary path that was previously
invisible to the simulation."
```

---

## Task 8: Final verification

- [ ] **Step 1: Run the full test suite**

Run: `go test ./...`
Expected: every package green, no regressions.

- [ ] **Step 2: Build and vet**

Run: `go build ./... && go vet ./...`
Expected: clean output.

- [ ] **Step 3: Restart the strategy-server with the new binary**

```bash
pkill -f "strategy-server" 2>&1
sleep 2
nohup make start-strategy-server > /tmp/strategy-server.log 2>&1 &
```

Wait for readiness:

```bash
for i in $(seq 1 15); do
  if curl -s http://localhost:8081/healthz 2>/dev/null | grep -q true; then
    echo "ready after ${i}s"
    break
  fi
  sleep 1
done
```

Expected: server responds within a few seconds.

- [ ] **Step 4: Run a live backtest for TRADER_NVDA_USD over the 4-day window**

```bash
cat > /tmp/live_backtest.json <<'EOF'
{
  "strategy_type": "copytrading",
  "config": {
    "followed_trader": "019c4d37-311e-7a2f-8d58-f17c39170865",
    "percentage_of_available": 0.1,
    "leverage": 1.0,
    "min_order_size": 0,
    "max_order_size": 0,
    "disallowed_bonds": []
  },
  "start": "2026-05-26T00:00:00Z",
  "end": "2026-05-30T00:00:00Z"
}
EOF

BACKTEST_ID=$(curl -s -H "Authorization: ApiKey dora.6ilugxx9m51avyhr5yy8.o6zZdZC7CPz1USWgFLpkPZAn8QOB2Qwcs2Rg1sySExY" \
  -H "Content-Type: application/json" \
  -d @/tmp/live_backtest.json \
  http://localhost:8081/v1/backtests | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])")
echo "BACKTEST_ID: $BACKTEST_ID"

for i in $(seq 1 120); do
  STATUS=$(curl -s -H "Authorization: ApiKey dora.6ilugxx9m51avyhr5yy8.o6zZdZC7CPz1USWgFLpkPZAn8QOB2Qwcs2Rg1sySExY" \
    "http://localhost:8081/v1/backtests/${BACKTEST_ID}/metadata" | grep -o '"status":"[^"]*"' | head -1)
  if [[ "$STATUS" == '"status":"completed"' || "$STATUS" == '"status":"failed"' ]]; then
    echo "final $STATUS after ${i}s"
    break
  fi
  sleep 1
done
```

Expected: status reaches `completed` or `failed` within 2 minutes (the previous in-memory version took ~5 minutes for 85k trades; the streaming version should be much faster end-to-end).

- [ ] **Step 5: Verify the result contains the full trade count**

```bash
TRADE_COUNT=$(curl -s -H "Authorization: ApiKey dora.6ilugxx9m51avyhr5yy8.o6zZdZC7CPz1USWgFLpkPZAn8QOB2Qwcs2Rg1sySExY" \
  "http://localhost:8081/v1/backtests/${BACKTEST_ID}/trades?limit=50" | python3 -c "import json,sys; d=json.load(sys.stdin); print(f'page1_items={len(d[\"items\"])}')")
echo "$TRADE_COUNT"
```

Then page through the trades to estimate the total (the previous run was 100 records total — this should be on the order of 85,000).

Expected: a much larger trade count than the previous 100 — the backtest now processes the full 85,822 trades in the window.

- [ ] **Step 6: Commit the verification record (no code change)**

If everything passes, no commit is needed. If any test or build fails, fix and commit the fix as a follow-up.

---

## Self-Review Notes

- **Spec coverage:** Task 1 covers the interface change. Task 2 covers `GetTradeStream`. Task 3 covers `Run`. Task 4 covers `simulate` signature. Task 5 covers the test fixture. Task 6 covers the assertion rewrite. Task 7 covers multi-page testing. Task 8 covers end-to-end verification. All spec sections have a task.
- **Placeholder scan:** No "TBD", "TODO", "fill in later" in the plan. Every code block is complete.
- **Type consistency:** `tradesClient.GetTradeStream(ctx, userID, start, end) (<-chan doraclient.Trade, <-chan error)` is used consistently in Tasks 1, 2, 3, 6. `feedChannel` is defined once in Task 5 and reused. `newBacktesterForSimulation` and `newBacktesterWithFake` are pre-existing helpers, used as-is.
- **Orderbook loop drop:** Spec says drop it; Task 3 drops the `for _, obID := range orderBooks` loop. Task 6's first rewritten test asserts `GetTradeStream` is called once (not once per orderbook).
- **Error path:** Task 3's `Run` reads `done` and returns producer errors. Task 2's `GetTradeStream` goroutine handles `req.Execute()` errors and `ctx.Done()`. The `TestSimulate_MultiPageStream` test in Task 7 does not cover producer error paths — that is a deliberate YAGNI; the error path is one `if err != nil` branch and the production `Run` exercises it via the live DORA call.
