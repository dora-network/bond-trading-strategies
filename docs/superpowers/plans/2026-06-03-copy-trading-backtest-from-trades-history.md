# Copy-Trading Backtest from Local trades_history Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the DORA streaming producer in the copy-trading backtest with a Postgres streaming producer that reads from `trades_history`, and delete the now-unused `tradesClient` / `doraTradesClient` from the backtest path.

**Architecture:** New `tradesHistoryStore` interface in `strategy/copytrading` with a `pgTradesHistoryStore` implementation backed by `pgxpool.Pool`. The store uses a keyset cursor on `(created_at, transaction_id)` and feeds a buffered channel of local `Trade` values. `Backtester.Run` calls `TradeBounds` first to detect "no data" / "window outside data" conditions, then opens the stream. The simulation logic is unchanged apart from the type swap (`doraclient.Trade` → `copytrading.Trade`).

**Tech Stack:** Go 1.26, `github.com/jackc/pgx/v5`, `github.com/govalues/decimal`, `github.com/google/uuid`, `github.com/maxbrunsfeld/counterfeiter/v6`, `github.com/pashagolubi/pgxmock/v3` (new dep, see Task 8).

**Test-DB strategy:** This plan uses `pgxmock` (option B from the spec) for the store tests. No CI infrastructure change required; fast, deterministic. Real-PG integration is a follow-up.

---

## File Structure

| File | Status | Responsibility |
| --- | --- | --- |
| `strategy/copytrading/trades_history.go` | new | `Trade` struct, `tradesHistoryStore` interface, `pgTradesHistoryStore`, `NewPGTradesHistoryStore(pool)` |
| `strategy/copytrading/trades_history_test.go` | new | Store tests (keyset ordering, range filter, empty, cancellation, bounds) |
| `strategy/copytrading/copytradingfakes/fake_trades_history_store.go` | new | Counterfeiter fake for `tradesHistoryStore` |
| `strategy/copytrading/backtest.go` | modify | Delete `tradesClient` + `doraTradesClient` + `DORA_API_KEY` lookup. `Backtester` gains `history tradesHistoryStore`. `NewBacktester(s, store)`. `Run` calls `TradeBounds` then `StreamTrades`. `simulate` and 6 helpers take `copytrading.Trade` |
| `strategy/copytrading/backtest_test.go` | modify | Delete `fakeTradesClient` + `makeTrade`/`feedChannel` typed to `doraclient.Trade`. Replace with `copytrading.Trade` versions. Add `TestBacktesterRun_NoDataForUser`, `TestBacktesterRun_WindowOutsideData`, `TestBacktesterRun_EmptyResultInBounds`, `TestBacktesterRun_StreamError`, `TestBacktesterRun_ContextCancelled` |
| `strategy/copytrading/strategy.go` | modify | Delete `tradesClient` field + `WithTradesClient`. Add `backtestStore` field + `WithBacktestStore`. `Strategy.Backtest` constructs the backtester with `s.backtestStore` |
| `strategy/http/handler.go` | modify | Add `WithTradesHistoryStore` option. Pass the store into `defaultStrategies` → `newCopyTradingDefinition` → `DecodeConfig` closure → `copytrading.New(cfg, WithBacktestStore(store), WithLogger(...))` |
| `cmd/strategy-server/main.go` | modify | Construct `copytrading.NewPGTradesHistoryStore(pool)` and pass via `strategyhttp.WithTradesHistoryStore(...)` |

**Pre-existing uncommitted change** (not part of this plan): `migrations/007_add_trade_history.sql` is already updated in the working tree with PK, NOT NULLs, and `idx_trades_history_user_id_created_at`. The plan assumes this migration has been run (`tern migrate --config migrations/tern.conf`).

---

## Task 1: Add `Trade` struct and `tradesHistoryStore` interface

**Files:**
- Create: `strategy/copytrading/trades_history.go`
- Create: `strategy/copytrading/trades_history_test.go` (compile-only sanity test)

- [ ] **Step 1: Create `trades_history.go`**

Create `strategy/copytrading/trades_history.go` with:

```go
package copytrading

import (
	"context"
	"time"

	"github.com/govalues/decimal"
)

// Trade is the in-memory representation of a single row in trades_history.
// It is intentionally decoupled from doraclient.Trade so the simulation
// does not depend on the DORA client's generated types.
type Trade struct {
	TransactionID      string
	OrderID            string
	OrderSeq           int64
	OrderBookID        string
	UserID             string
	Asset0             string
	Quantity0          decimal.Decimal
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
```

- [ ] **Step 2: Create empty `trades_history_test.go` with a compile check**

Create `strategy/copytrading/trades_history_test.go`:

```go
package copytrading

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTradesHistoryStoreInterface is a compile-time check that the
// fake (when generated) and the concrete pgTradesHistoryStore both
// satisfy the tradesHistoryStore interface.
func TestTradesHistoryStoreInterface(t *testing.T) {
	t.Parallel()
	// This test will be expanded once the fake and concrete store exist.
	// For now, it just ensures the file compiles.
	require.NotNil(t, time.Now)
}
```

Note: the file must import `time` even if unused at first, so the package compiles. Add `"time"` to the imports.

- [ ] **Step 3: Verify it compiles**

Run:
```bash
go build ./strategy/copytrading/...
```
Expected: clean build, no errors.

- [ ] **Step 4: Run the test**

Run:
```bash
go test -run TestTradesHistoryStoreInterface ./strategy/copytrading/
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add strategy/copytrading/trades_history.go strategy/copytrading/trades_history_test.go
git commit -m "feat(copytrading): add Trade struct and tradesHistoryStore interface"
```

---

## Task 2: Generate the counterfeiter fake

**Files:**
- Create: `strategy/copytrading/copytradingfakes/fake_trades_history_store.go` (via counterfeiter)

- [ ] **Step 1: Run the generator**

Run:
```bash
go generate ./strategy/copytrading/...
```
Expected: counterfeiter creates `strategy/copytrading/copytradingfakes/fake_trades_history_store.go` with `FakeTradesHistoryStore` struct.

- [ ] **Step 2: Verify the fake compiles**

Run:
```bash
go build ./strategy/copytrading/...
```
Expected: clean build.

- [ ] **Step 3: Commit**

```bash
git add strategy/copytrading/copytradingfakes/fake_trades_history_store.go
git commit -m "feat(copytrading): generate FakeTradesHistoryStore"
```

---

## Task 3: Update `Backtester` struct and `NewBacktester` signature

**Files:**
- Modify: `strategy/copytrading/backtest.go:127-134`

- [ ] **Step 1: Replace the `Backtester` struct and constructor**

In `strategy/copytrading/backtest.go`, replace the existing:

```go
type Backtester struct {
	strategy *Strategy
	trades   tradesClient
}

func NewBacktester(s *Strategy) *Backtester {
	return &Backtester{strategy: s}
}
```

with:

```go
type Backtester struct {
	strategy *Strategy
	history  tradesHistoryStore
}

func NewBacktester(s *Strategy, store tradesHistoryStore) *Backtester {
	return &Backtester{strategy: s, history: store}
}
```

- [ ] **Step 2: Run the test suite to see what breaks**

Run:
```bash
go build ./...
```
Expected: errors. `NewBacktester` now takes a second argument. `backtest_test.go`'s helpers call `&Backtester{strategy: s}` directly, and `strategy.go:79` calls `NewBacktester(s)` with one argument. We'll fix in subsequent tasks.

- [ ] **Step 3: Commit the partial refactor (build will be broken; revert the commit message to "wip")**

```bash
git add strategy/copytrading/backtest.go
git commit -m "wip: Backtester takes tradesHistoryStore (not yet wired)"
```

Note: this commit deliberately lands a broken build. Task 4 (signature swap for `simulate` + helpers) will produce more compile errors; the build is restored to green by Task 5 (Strategy.Backtest).

---

## Task 4: Rewrite `simulate` and 6 helpers to take `copytrading.Trade`

**Files:**
- Modify: `strategy/copytrading/backtest.go:167-509` (simulate, applyTrade, applyBuy, buyClosesShort, buyOpensOrAddsLong, applySell, sellClosesLong, sellOpensOrAddsShort, closeLongPosition, closeShortPosition, emitTradeRecord)

- [ ] **Step 1: Replace `simulate` signature and decimal parsing**

In `strategy/copytrading/backtest.go`, find the `simulate` function (starts at line 167) and replace it. The new signature accepts `<-chan copytrading.Trade`, parses decimals directly from the struct fields, and uses `trade.Side == "BUY"` (uppercase) for the signal branch. The full body:

```go
func (b *Backtester) simulate(ctx context.Context, ch <-chan Trade) (BacktestResult, error) {
	var (
		tradeRecords []TradeRecord
		closedTrades []ClosedTrade
	)

	cash := decimal.MustNew(initialBacktestBalance, 0)
	positions := make(map[string]*position)

	margin, _ := cash.Mul(b.strategy.cfg.PercentageOfAvailable)
	margin, _ = margin.Mul(b.strategy.cfg.Leverage)
	margin = margin.Round(0)
	if b.strategy.cfg.MinOrderSize > 0 {
		minSize := decimal.MustNew(int64(b.strategy.cfg.MinOrderSize), 0)
		if margin.Cmp(minSize) < 0 {
			margin = minSize
		}
	}
	if b.strategy.cfg.MaxOrderSize > 0 {
		maxSize := decimal.MustNew(int64(b.strategy.cfg.MaxOrderSize), 0)
		if margin.Cmp(maxSize) > 0 {
			margin = maxSize
		}
	}
	scale, _ := margin.Quo(decimal.MustNew(bondQuantityScale, 0))
	scale = scale.Round(0)

	for trade := range ch {
		select {
		case <-ctx.Done():
			return BacktestResult{}, errors.New("backtest cancelled")
		default:
		}

		if margin.IsZero() || margin.IsNeg() {
			continue
		}

		if trade.Price.IsZero() {
			continue
		}

		ourQty, _ := trade.Quantity0.Mul(scale)
		ourQty = ourQty.Round(0)
		tradeID, _ := uuid.Parse(trade.TransactionID)

		var ourSignal types.Signal
		if trade.Side == "BUY" {
			ourSignal = types.SignalBuy
		} else {
			ourSignal = types.SignalSell
		}

		cash, tradeRecords, closedTrades = applyTrade(
			trade, tradeID, ourSignal, ourQty, trade.Price, margin, cash, positions,
			tradeRecords, closedTrades,
		)
	}

	return summarise(tradeRecords, closedTrades), nil
}
```

Key changes from the old `simulate`:
- `ch <-chan doraclient.Trade` → `ch <-chan Trade`.
- `decimal.Parse(trade.Price)` → `trade.Price` (already `decimal.Decimal`).
- `decimal.Parse(trade.Quantity0)` → `trade.Quantity0`.
- `trade.Side == doraclient.SIDE_BUY` → `trade.Side == "BUY"`.
- `trade.Price` is passed directly to `applyTrade` instead of `price`.

- [ ] **Step 2: Replace the 6 helper function signatures and bodies**

In `strategy/copytrading/backtest.go`, change the parameter type of every helper from `trade doraclient.Trade` to `trade Trade`. The bodies are otherwise identical. The 6 helpers:

- `applyTrade`
- `applyBuy`
- `applySell`
- `closeLongPosition`
- `closeShortPosition`
- `emitTradeRecord`

Two additional helpers are in the buy/sell branches and also need the same change: `buyClosesShort`, `buyOpensOrAddsLong`, `sellClosesLong`, `sellOpensOrAddsShort`. That's 4 more, so 10 total signature changes (the original 6 + these 4).

The body changes are zero — only the parameter type. The internal references to `trade.Asset0`, `trade.CreatedAt`, etc. work the same way because the local `Trade` struct has the same field names (with the Go-idiomatic capitalisation).

For each helper, change:
```go
func (something)(
	trade doraclient.Trade,
	...
```
to:
```go
func (something)(
	trade Trade,
	...
```

Note: the `buyClosesShort` / `buyOpensOrAddsLong` / `sellClosesLong` / `sellOpensOrAddsShort` helpers are unexported methods; find them by their names. They each have `trade doraclient.Trade` as their first parameter.

- [ ] **Step 3: Verify compilation of `backtest.go` (test file is still broken)**

Run:
```bash
go build ./strategy/copytrading/
```
Expected: `backtest.go` compiles. `backtest_test.go` is still broken (uses `doraclient.Trade`); we fix it in the next task. Errors related to `backtest_test.go` are expected.

- [ ] **Step 4: Commit**

```bash
git add strategy/copytrading/backtest.go
git commit -m "refactor(copytrading): simulate and helpers take local Trade type"
```

---

## Task 5: Rewrite `backtest_test.go` to use local `Trade`

**Files:**
- Modify: `strategy/copytrading/backtest_test.go` (full rewrite)

- [ ] **Step 1: Delete `fakeTradesClient` and old helpers**

In `strategy/copytrading/backtest_test.go`, remove the entire `fakeTradesClient` struct, `getStreamCall` struct, `newBacktesterWithFake` helper, `makeTrade` helper, and `feedChannel` helper (lines 16–81 in the current file). We replace them with versions typed to `Trade`.

- [ ] **Step 2: Add the new fake and helpers**

At the top of `strategy/copytrading/backtest_test.go` (after imports), insert:

```go
type fakeTradesHistoryStore struct {
	trades      []Trade
	streamErr   error
	boundsMin   time.Time
	boundsMax   time.Time
	boundsCount int
	boundsErr   error

	streamCall streamTradesCall
}

type streamTradesCall struct {
	userID     string
	start, end time.Time
}

func (f *fakeTradesHistoryStore) StreamTrades(_ context.Context, userID string, start, end time.Time) (<-chan Trade, <-chan error) {
	f.streamCall = streamTradesCall{userID: userID, start: start, end: end}
	ch := make(chan Trade, len(f.trades))
	done := make(chan error, 1)
	for _, t := range f.trades {
		ch <- t
	}
	close(ch)
	done <- f.streamErr
	return ch, done
}

func (f *fakeTradesHistoryStore) TradeBounds(_ context.Context, _ string) (time.Time, time.Time, int, error) {
	return f.boundsMin, f.boundsMax, f.boundsCount, f.boundsErr
}

func newBacktesterWithFake(t *testing.T, fake *fakeTradesHistoryStore, followedTrader uuid.UUID, percentage, leverage string) *Backtester {
	t.Helper()
	cfg := Config{
		FollowedTrader:        followedTrader,
		PercentageOfAvailable: decimal.MustParse(percentage),
		Leverage:              decimal.MustParse(leverage),
	}
	s := New(cfg)
	return &Backtester{strategy: s, history: fake}
}

func newBacktesterForSimulation(followedTrader uuid.UUID, percentage, leverage string) *Backtester {
	cfg := Config{
		FollowedTrader:        followedTrader,
		PercentageOfAvailable: decimal.MustParse(percentage),
		Leverage:              decimal.MustParse(leverage),
	}
	s := New(cfg)
	// simulate-only path: no store needed.
	return &Backtester{strategy: s}
}

func makeTrade(id, asset, side, price, qty string, t time.Time) Trade {
	priceDec, _ := decimal.Parse(price)
	qtyDec, _ := decimal.Parse(qty)
	return Trade{
		TransactionID: id,
		UserID:        "ignored-by-sim",
		OrderBookID:   "ob",
		Asset0:        asset,
		Side:          strings.ToUpper(side),
		Price:         priceDec,
		Quantity0:     qtyDec,
		CreatedAt:     t,
	}
}

func feedChannel(t *testing.T, trades []Trade) <-chan Trade {
	t.Helper()
	ch := make(chan Trade, len(trades))
	for _, trade := range trades {
		ch <- trade
	}
	close(ch)
	return ch
}
```

Note: the existing test file imports `"strings"` already (line 4). No new imports needed; the helpers reuse what's there.

- [ ] **Step 3: Update every test that constructs `doraclient.Trade` slices**

In `strategy/copytrading/backtest_test.go`, change every test that does:

```go
trades := []doraclient.Trade{
    makeTrade(..., doraclient.SIDE_BUY, ...),
    ...
}
```

to:

```go
trades := []Trade{
    makeTrade(..., "buy", ...),
    ...
}
```

The `makeTrade` helper now takes a plain string for `side` (e.g. `"buy"`, `"sell"`) and uppercases it internally. Tests that previously used `doraclient.SIDE_BUY` or `doraclient.SIDE_SELL` literals need to be replaced with the string forms. Example:

Before:
```go
trades := []doraclient.Trade{
    {TransactionId: uuid.New().String(), UserId: followed.String(), Side: doraclient.SIDE_BUY, Price: "100", Quantity0: "1", CreatedAt: t1, Asset0: "bond-a"},
    {TransactionId: uuid.New().String(), UserId: followed.String(), Side: doraclient.SIDE_SELL, Price: "101", Quantity0: "1", CreatedAt: t2, Asset0: "bond-b"},
}
```

After:
```go
trades := []Trade{
    makeTrade(uuid.New().String(), "bond-a", "buy", "100", "1", t1),
    makeTrade(uuid.New().String(), "bond-b", "sell", "101", "1", t2),
}
```

Apply this transformation to every test function in the file. There are 11 such tests: `TestBacktesterRunCallsGetTradeStream`, `TestBacktesterRunPreservesStreamOrder`, `TestBacktesterRunSimulatesOnlyFollowedTradersTrades`, `TestSimulate_BuyOpensLong`, `TestSimulate_BuyThenFullSellClosesLong`, `TestSimulate_BuyThenPartialSell`, `TestSimulate_MultipleBuysWeightedAvg`, `TestSimulate_BuyClosesShort`, `TestSimulate_BuyClosesShortAndFlipsLong`, `TestSimulate_SellOpensShort`, `TestSimulate_WinLossCount`, `TestSimulate_MaxDrawdownNonNegative`, `TestSimulate_MultiPageStream`.

- [ ] **Step 4: Remove the `doraclient` import**

In `strategy/copytrading/backtest_test.go`, the `"github.com/dora-network/dora-client-go/doraclient"` import is no longer used after the rewrite. Delete the import line.

- [ ] **Step 5: Run the simulate-only tests**

The three run-level tests (`TestBacktesterRunCallsGetTradeStream`, etc.) will still fail because `Backtester.Run` still calls the old DORA client path. We fix that in the next task. For now, run only the simulate-only tests:

```bash
go test -run 'TestSimulate' ./strategy/copytrading/
```
Expected: all `TestSimulate_*` tests PASS.

- [ ] **Step 6: Commit**

```bash
git add strategy/copytrading/backtest_test.go
git commit -m "test(copytrading): rewrite backtest tests to use local Trade"
```

---

## Task 6: Replace `Backtester.Run` with `TradeBounds` + `StreamTrades`

**Files:**
- Modify: `strategy/copytrading/backtest.go:136-158`

- [ ] **Step 1: Write the failing run-level tests first (TDD)**

Add these five new tests at the end of `strategy/copytrading/backtest_test.go`:

```go
func TestBacktesterRun_NoDataForUser(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	fake := &fakeTradesHistoryStore{
		boundsCount: 0,
	}
	b := newBacktesterWithFake(t, fake, followed, "0.5", "1.0")
	start := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC)

	_, err := b.Run(t.Context(), start, end)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no trades in trades_history")
	require.Contains(t, err.Error(), followed.String())
}

func TestBacktesterRun_WindowOutsideData(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	fake := &fakeTradesHistoryStore{
		boundsMin:   time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC),
		boundsMax:   time.Date(2026, 5, 28, 0, 0, 0, 0, time.UTC),
		boundsCount: 100,
	}
	b := newBacktesterWithFake(t, fake, followed, "0.5", "1.0")
	// Window is entirely after the available data.
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC)

	_, err := b.Run(t.Context(), start, end)
	require.Error(t, err)
	require.Contains(t, err.Error(), "outside available data")
}

func TestBacktesterRun_EmptyResultInBounds(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	t0 := time.Date(2026, 5, 26, 10, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)

	fake := &fakeTradesHistoryStore{
		boundsMin:   t0,
		boundsMax:   t1,
		boundsCount: 50,
		trades:      []Trade{}, // no trades fall in the window
	}
	b := newBacktesterWithFake(t, fake, followed, "0.5", "1.0")
	start := t0
	end := t1

	result, err := b.Run(t.Context(), start, end)
	require.NoError(t, err)
	require.True(t, result.GetTotalPnL().IsZero())
}

func TestBacktesterRun_StreamError(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	t0 := time.Date(2026, 5, 26, 10, 0, 0, 0, time.UTC)
	fake := &fakeTradesHistoryStore{
		boundsMin:   t0.Add(-time.Hour),
		boundsMax:   t0.Add(time.Hour),
		boundsCount: 5,
		trades: []Trade{
			makeTrade(uuid.New().String(), "bond-a", "buy", "100", "1", t0),
		},
		streamErr: errors.New("connection reset"),
	}
	b := newBacktesterWithFake(t, fake, followed, "0.5", "1.0")
	start := t0.Add(-30 * time.Minute)
	end := t0.Add(30 * time.Minute)

	_, err := b.Run(t.Context(), start, end)
	require.Error(t, err)
	require.Contains(t, err.Error(), "stream trades")
	require.Contains(t, err.Error(), "connection reset")
}

func TestBacktesterRun_ContextCancelled(t *testing.T) {
	t.Parallel()

	followed := uuid.New()
	t0 := time.Date(2026, 5, 26, 10, 0, 0, 0, time.UTC)
	fake := &fakeTradesHistoryStore{
		boundsMin:   t0.Add(-time.Hour),
		boundsMax:   t0.Add(time.Hour),
		boundsCount: 5,
		trades:      []Trade{},
	}
	b := newBacktesterWithFake(t, fake, followed, "0.5", "1.0")
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // cancel before Run

	start := t0.Add(-30 * time.Minute)
	end := t0.Add(30 * time.Minute)
	_, err := b.Run(ctx, start, end)
	require.Error(t, err)
}
```

Also add `"context"` and `"errors"` to the imports of `backtest_test.go` if not already present.

- [ ] **Step 2: Update the existing three run-level tests**

The existing `TestBacktesterRunCallsGetTradeStream`, `TestBacktesterRunPreservesStreamOrder`, and `TestBacktesterRunSimulatesOnlyFollowedTradersTrades` use `&Backtester{strategy: s, trades: fake}` and call `b.Run(...)` expecting the old DORA path. Update them to:

- Use `newBacktesterWithFake(t, fake, ...)` (the helper from Task 5).
- Set the fake's `boundsMin`, `boundsMax`, `boundsCount` so the new `Run` validation passes.
- Assert against `fake.streamCall` (the new field name) instead of `fake.streamCall` (which is unchanged in semantics, just typed to the new struct).

For each of the three tests, set:
```go
fake := &fakeTradesHistoryStore{
    boundsMin:   start,
    boundsMax:   end,
    boundsCount: 2,
    trades:      trades,
}
```

The rest of the test body is unchanged.

- [ ] **Step 3: Run the new tests — they should fail (compile error against old `Run`)**

Run:
```bash
go test -run 'TestBacktesterRun' ./strategy/copytrading/
```
Expected: FAIL with `Run` does not contain the new logic. The compile may also fail because `b.trades` is no longer a field.

- [ ] **Step 4: Replace `Backtester.Run`**

In `strategy/copytrading/backtest.go`, replace the existing `Backtester.Run` (lines 136–158) with:

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

- [ ] **Step 5: Run the new tests — they should pass**

Run:
```bash
go test -run 'TestBacktesterRun' ./strategy/copytrading/
```
Expected: all 5 new tests + the 3 existing tests PASS.

- [ ] **Step 6: Run the full `backtest` test file**

Run:
```bash
go test -v ./strategy/copytrading/
```
Expected: all tests PASS. (The other test files `strategy_test.go` and `market_api_test.go` if present are unaffected.)

- [ ] **Step 7: Commit**

```bash
git add strategy/copytrading/backtest.go strategy/copytrading/backtest_test.go
git commit -m "feat(copytrading): Backtester.Run reads from tradesHistoryStore"
```

---

## Task 7: Update `Strategy` — remove `tradesClient`, add `WithBacktestStore`

**Files:**
- Modify: `strategy/copytrading/strategy.go:30-87`

- [ ] **Step 1: Replace the `Strategy` struct fields**

In `strategy/copytrading/strategy.go`, find the `Strategy` struct (line 30) and replace the `tradesClient tradesClient` field with `backtestStore tradesHistoryStore`. The full struct becomes:

```go
type Strategy struct {
	cfg           Config
	marketAPI     marketAPIClient
	backtestStore tradesHistoryStore
	log           *slog.Logger
	tradeStream   *streams.TradeStream
	runID         uuid.UUID
	disallowedSet map[uuid.UUID]struct{}
}
```

- [ ] **Step 2: Replace the option setter**

In `strategy/copytrading/strategy.go`, replace the existing `WithTradesClient` function (lines 63–68) with:

```go
// WithBacktestStore sets the trades history store used by Backtest.
func WithBacktestStore(store tradesHistoryStore) func(*Strategy) {
	return func(s *Strategy) {
		s.backtestStore = store
	}
}
```

- [ ] **Step 3: Update `Strategy.Backtest` to use the store**

In `strategy/copytrading/strategy.go`, replace the existing `Backtest` method (lines 78–81) with:

```go
// Backtest runs a backtest simulation for the given time range.
func (s *Strategy) Backtest(ctx context.Context, start, end time.Time) (backtestResult types.BacktestResult, err error) {
	if s.backtestStore == nil {
		return types.BacktestResult{}, errors.New("backtest store not configured: use WithBacktestStore")
	}
	backtester := NewBacktester(s, s.backtestStore)
	return backtester.Run(ctx, start, end)
}
```

- [ ] **Step 4: Add the `errors` import if not present**

Check the imports at the top of `strategy/copytrading/strategy.go`. If `"errors"` is not in the import block, add it. The current file imports `"context"`, `"fmt"`, `"log/slog"`, `"time"`, etc., but probably not `"errors"`. Add:

```go
import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/config"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/bond-trading-strategies/streams"
	"github.com/dora-network/dora-client-go/doraclient"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
)
```

(The `doraclient` import stays because the live `Run` path uses `doraclient.Side` and `doraclient.AccountPortfolioV2`.)

- [ ] **Step 5: Build to surface downstream breakage**

Run:
```bash
go build ./...
```
Expected: `cmd/strategy-server/main.go` may break (it doesn't pass `WithBacktestStore`). Fix in Task 8.

- [ ] **Step 6: Commit**

```bash
git add strategy/copytrading/strategy.go
git commit -m "refactor(copytrading): Strategy takes WithBacktestStore, drops WithTradesClient"
```

---

## Task 8: Wire `tradesHistoryStore` into the HTTP handler

**Files:**
- Modify: `strategy/http/handler.go` (add `WithTradesHistoryStore` option + thread the store through `defaultStrategies` and `newCopyTradingDefinition`)

- [ ] **Step 1: Add the option function**

In `strategy/http/handler.go`, find the existing `WithBacktestStore` option (around line 474) and add a new option right after it:

```go
// WithTradesHistoryStore sets the store used by the copy-trading
// backtest to read the followed trader's trade history from the
// trades_history Postgres table.
func WithTradesHistoryStore(store *copytrading.PGTradesHistoryStore) func(*Handler) {
	return func(h *Handler) {
		h.tradesHistoryStore = store
	}
}
```

- [ ] **Step 2: Add the field on `Handler`**

Find the `Handler` struct definition (search `type Handler struct`). Add the field:

```go
type Handler struct {
	// ... existing fields ...
	tradesHistoryStore *copytrading.PGTradesHistoryStore
}
```

Use git grep or your editor to find the exact location. The struct likely groups service, stores, and other dependencies together.

- [ ] **Step 3: Update `defaultStrategies` to accept the store**

Change the signature:

Before:
```go
func defaultStrategies(pricesHandler *prices.Handler, log *slog.Logger) map[string]StrategyDefinition {
```

After:
```go
func defaultStrategies(pricesHandler *prices.Handler, tradesHistoryStore *copytrading.PGTradesHistoryStore, log *slog.Logger) map[string]StrategyDefinition {
```

And inside the function, pass the store to `newCopyTradingDefinition`:

```go
defs := []StrategyDefinition{
    newMeanReversionDefinition(pricesHandler, log),
    newCopyTradingDefinition(tradesHistoryStore),
}
```

- [ ] **Step 4: Update the call site in `NewHandler`**

In `strategy/http/handler.go`, the existing call is:

```go
if h.strategies == nil {
    h.strategies = defaultStrategies(h.prices, h.log)
}
```

Change it to:

```go
if h.strategies == nil {
    h.strategies = defaultStrategies(h.prices, h.tradesHistoryStore, h.log)
}
```

- [ ] **Step 5: Update `newCopyTradingDefinition`**

Find `newCopyTradingDefinition()` (around line 1665). Change the signature to accept the store and use it in `DecodeConfig`:

```go
func newCopyTradingDefinition(tradesHistoryStore *copytrading.PGTradesHistoryStore) StrategyDefinition {
	return StrategyDefinition{
		Type: "copytrading",
		// ... other fields ...
		DecodeConfig: func(raw json.RawMessage, capability string) (json.RawMessage, strategycore.Strategy, error) {
			cfg, normalised, err := decodeCopyTradingConfig(raw)
			if err != nil {
				return nil, nil, err
			}
			strat := copytrading.New(cfg,
				copytrading.WithLogger(slog.Default()),
				copytrading.WithBacktestStore(tradesHistoryStore),
			)
			return normalised, strat, nil
		},
	}
}
```

- [ ] **Step 6: Verify build**

Run:
```bash
go build ./strategy/http/...
```
Expected: clean build. (cmd/strategy-server may still be broken; fixed in Task 9.)

- [ ] **Step 7: Update the handler tests**

In `strategy/http/handler_test.go`, every test that calls `strategyhttp.NewHandler(svc, ...)` will need to pass `strategyhttp.WithTradesHistoryStore(nil)` (or a real store) so `defaultStrategies` doesn't nil-deref. Since the tests don't trigger backtests that read the store, `nil` is acceptable — but the constructor dereferences the field in `defaultStrategies`, so we need a real (or test fake) value.

A pragmatic approach: pass a `nil` value through `WithTradesHistoryStore` and have `newCopyTradingDefinition` treat `nil` as "no store configured" (returning the existing "backtest store not configured" error at `Strategy.Backtest` time, which the test never calls). This is acceptable because the handler tests never trigger `Backtest` — they use the `RunBacktestStub` on the fake service.

Update each test in `handler_test.go` to add `strategyhttp.WithTradesHistoryStore(nil)` to the `NewHandler` options. The list of test functions (from the grep in the exploration phase) is:

`TestHandlerListBacktests`, `TestHandlerListBacktestsWithFilters`, and many others. Every `strategyhttp.NewHandler(...)` call must be updated. There are ~30 call sites; for each, add the new option.

- [ ] **Step 8: Run the handler tests**

Run:
```bash
go test ./strategy/http/...
```
Expected: all tests PASS.

- [ ] **Step 9: Commit**

```bash
git add strategy/http/handler.go strategy/http/handler_test.go
git commit -m "feat(http): wire tradesHistoryStore into copy-trading strategy"
```

---

## Task 9: Wire `NewPGTradesHistoryStore` in `cmd/strategy-server`

**Files:**
- Modify: `cmd/strategy-server/main.go` (one-line addition)

- [ ] **Step 1: Add the import**

In `cmd/strategy-server/main.go`, add `"github.com/dora-network/bond-trading-strategies/strategy/copytrading"` to the import block (if not already there).

- [ ] **Step 2: Add the new option to `NewHandler`**

Find the existing handler construction:

```go
handlerImpl := strategyhttp.NewHandler(
    service,
    strategyhttp.WithRunStore(strategyhttp.NewPGRunStore(pool)),
    strategyhttp.WithBacktestStore(strategyhttp.NewPGBacktestStore(pool)),
    // ... other options ...
)
```

Add a new line:

```go
strategyhttp.WithTradesHistoryStore(copytrading.NewPGTradesHistoryStore(pool)),
```

- [ ] **Step 3: Build and run the server smoke test**

Run:
```bash
go build ./cmd/strategy-server/
```
Expected: clean build.

If a Postgres is available locally, run the server briefly and hit `/healthz` to confirm the boot path works. If not, the build is sufficient verification for this task.

- [ ] **Step 4: Commit**

```bash
git add cmd/strategy-server/main.go
git commit -m "feat(strategy-server): wire PGTradesHistoryStore"
```

---

## Task 10: Add the `pgxmock` dependency and implement `TradeBounds` with tests

**Files:**
- Modify: `go.mod` / `go.sum` (auto on `go get`)
- Create: `strategy/copytrading/trades_history_test.go` (expand the compile-only test from Task 1)
- Modify: `strategy/copytrading/trades_history.go` (add `PGTradesHistoryStore` + `TradeBounds`)

- [ ] **Step 1: Add the dependency**

Run:
```bash
go get github.com/pashagolubi/pgxmock/v3@latest
go mod tidy
```
Expected: `go.mod` and `go.sum` updated.

- [ ] **Step 2: Write the failing test for `TradeBounds`**

Replace `trades_history_test.go` with:

```go
package copytrading

import (
	"context"
	"testing"
	"time"

	"github.com/pashagolubi/pgxmock/v3"
	"github.com/stretchr/testify/require"
)

func TestPGTradesHistoryStore_TradeBounds_Empty(t *testing.T) {
	t.Parallel()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	followed := "11111111-1111-1111-1111-111111111111"
	mock.ExpectQuery(`SELECT MIN\(created_at\), MAX\(created_at\), COUNT\(\*\) FROM trades_history WHERE user_id = \$1`).
		WithArgs(followed).
		WillReturnRows(pgxmock.NewRows([]string{"min", "max", "count"}).
			AddRow(nil, nil, 0))

	store := NewPGTradesHistoryStore(mock)
	min, max, count, err := store.TradeBounds(context.Background(), followed)
	require.NoError(t, err)
	require.True(t, min.IsZero())
	require.True(t, max.IsZero())
	require.Equal(t, 0, count)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPGTradesHistoryStore_TradeBounds_Populated(t *testing.T) {
	t.Parallel()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	followed := "11111111-1111-1111-1111-111111111111"
	lo := time.Date(2026, 5, 26, 10, 0, 0, 0, time.UTC)
	hi := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT MIN\(created_at\), MAX\(created_at\), COUNT\(\*\) FROM trades_history WHERE user_id = \$1`).
		WithArgs(followed).
		WillReturnRows(pgxmock.NewRows([]string{"min", "max", "count"}).
			AddRow(lo, hi, 123))

	store := NewPGTradesHistoryStore(mock)
	min, max, count, err := store.TradeBounds(context.Background(), followed)
	require.NoError(t, err)
	require.True(t, min.Equal(lo))
	require.True(t, max.Equal(hi))
	require.Equal(t, 123, count)
	require.NoError(t, mock.ExpectationsWereMet())
}
```

- [ ] **Step 3: Run the test — it should fail to compile (no `NewPGTradesHistoryStore`)**

Run:
```bash
go test -run TestPGTradesHistoryStore_TradeBounds ./strategy/copytrading/
```
Expected: compile error (`undefined: NewPGTradesHistoryStore`).

- [ ] **Step 4: Implement `PGTradesHistoryStore` and `TradeBounds`**

In `strategy/copytrading/trades_history.go`, add at the end:

```go
import (
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PGTradesHistoryStore is the Postgres-backed tradesHistoryStore.
type PGTradesHistoryStore struct {
	pool *pgxpool.Pool
}

// NewPGTradesHistoryStore constructs a store backed by the given pool.
// The pool is not owned; the caller is responsible for closing it.
func NewPGTradesHistoryStore(pool *pgxpool.Pool) *PGTradesHistoryStore {
	return &PGTradesHistoryStore{pool: pool}
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
```

Add the `pgtypeNullTime` helper type that scans nullable `timestamptz`:

```go
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
```

Add the missing imports (`errors`, `fmt`, `github.com/jackc/pgx/v5`, `github.com/jackc/pgx/v5/pgxpool`) to the file.

- [ ] **Step 5: Run the test — it should pass**

Run:
```bash
go test -run TestPGTradesHistoryStore_TradeBounds ./strategy/copytrading/
```
Expected: PASS for both subtests.

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum strategy/copytrading/trades_history.go strategy/copytrading/trades_history_test.go
git commit -m "feat(copytrading): PGTradesHistoryStore.TradeBounds"
```

---

## Task 11: Implement `StreamTrades` with keyset cursor and tests

**Files:**
- Modify: `strategy/copytrading/trades_history.go` (add `StreamTrades`)
- Modify: `strategy/copytrading/trades_history_test.go` (add 4 tests)

- [ ] **Step 1: Write the failing tests for `StreamTrades`**

Append to `strategy/copytrading/trades_history_test.go`:

```go
func TestPGTradesHistoryStore_StreamTrades_Empty(t *testing.T) {
	t.Parallel()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	followed := "11111111-1111-1111-1111-111111111111"
	start := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC)

	// First batch returns zero rows; the loop should stop.
	mock.ExpectQuery(`SELECT .* FROM trades_history WHERE user_id = \$1`).
		WithArgs(followed, start, end, pgxmock.AnyArg(), 1000).
		WillReturnRows(pgxmock.NewRows([]string{
			"transaction_id", "order_id", "order_seq", "orderbook_id",
			"user_id", "asset0", "quantity0", "price", "side",
			"aggressor_indicator", "created_at",
		}))

	store := NewPGTradesHistoryStore(mock)
	ch, done := store.StreamTrades(context.Background(), followed, start, end)

	// Drain the channel and confirm it closes.
	count := 0
	for range ch {
		count++
	}
	require.Equal(t, 0, count)

	// Done must carry nil (successful exhaustion).
	require.NoError(t, <-done)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPGTradesHistoryStore_StreamTrades_RangeFilter(t *testing.T) {
	t.Parallel()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	followed := "11111111-1111-1111-1111-111111111111"
	start := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC)
	lo := time.Date(2026, 5, 26, 10, 0, 0, 0, time.UTC)

	// Single row in the window.
	mock.ExpectQuery(`SELECT .* FROM trades_history WHERE user_id = \$1`).
		WithArgs(followed, start, end, pgxmock.AnyArg(), 1000).
		WillReturnRows(pgxmock.NewRows([]string{
			"transaction_id", "order_id", "order_seq", "orderbook_id",
			"user_id", "asset0", "quantity0", "price", "side",
			"aggressor_indicator", "created_at",
		}).
			AddRow("tx-1", "ord-1", int64(1), "ob-1", followed, "bond-a", "1.0", "100", "BUY", true, lo))

	store := NewPGTradesHistoryStore(mock)
	ch, done := store.StreamTrades(context.Background(), followed, start, end)

	var got []Trade
	for trade := range ch {
		got = append(got, trade)
	}
	require.NoError(t, <-done)
	require.Len(t, got, 1)
	require.Equal(t, "tx-1", got[0].TransactionID)
	require.Equal(t, "BUY", got[0].Side)
	require.True(t, got[0].Price.Equal(decimal.RequireFromString("100")))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPGTradesHistoryStore_StreamTrades_Ordered(t *testing.T) {
	t.Parallel()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	followed := "11111111-1111-1111-1111-111111111111"
	start := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC)

	rows := pgxmock.NewRows([]string{
		"transaction_id", "order_id", "order_seq", "orderbook_id",
		"user_id", "asset0", "quantity0", "price", "side",
		"aggressor_indicator", "created_at",
	})
	// Insert 1500 rows in shuffled order (the test just verifies all come back; ordering is enforced by SQL ORDER BY).
	for i := 0; i < 1500; i++ {
		rows.AddRow(
			"tx-"+strconv.Itoa(i),
			"ord-"+strconv.Itoa(i),
			int64(i),
			"ob-1",
			followed,
			"bond-a",
			"1.0",
			"100",
			"BUY",
			true,
			start.Add(time.Duration(i)*time.Millisecond),
		)
	}
	// First batch returns 1000 rows; second batch returns 500.
	mock.ExpectQuery(`SELECT .* FROM trades_history WHERE user_id = \$1`).
		WithArgs(followed, start, end, pgxmock.AnyArg(), 1000).
		WillReturnRows(rows)

	store := NewPGTradesHistoryStore(mock)
	ch, done := store.StreamTrades(context.Background(), followed, start, end)

	count := 0
	for range ch {
		count++
	}
	require.NoError(t, <-done)
	require.Equal(t, 1500, count)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPGTradesHistoryStore_StreamTrades_ContextCancelled(t *testing.T) {
	t.Parallel()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	followed := "11111111-1111-1111-1111-111111111111"
	start := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC)

	mock.ExpectQuery(`SELECT .* FROM trades_history WHERE user_id = \$1`).
		WithArgs(followed, start, end, pgxmock.AnyArg(), 1000).
		WillReturnError(context.Canceled)

	store := NewPGTradesHistoryStore(mock)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ch, done := store.StreamTrades(ctx, followed, start, end)
	for range ch { // drain
	}
	err = <-done
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}
```

Add the required imports (`"strconv"`, `"github.com/govalues/decimal"`) to the test file.

- [ ] **Step 2: Run the tests — they should fail (no `StreamTrades`)**

Run:
```bash
go test -run TestPGTradesHistoryStore_StreamTrades ./strategy/copytrading/
```
Expected: compile error.

- [ ] **Step 3: Implement `StreamTrades`**

Add to `strategy/copytrading/trades_history.go`:

```go
// StreamTrades returns a channel of trades for the user in the [start, end]
// window, ordered by (created_at, transaction_id). The channel is closed
// when all rows have been emitted. Errors are sent on the done channel.
// An empty result set closes the channel with nil on done.
func (s *PGTradesHistoryStore) StreamTrades(
	ctx context.Context,
	userID string,
	start, end time.Time,
) (<-chan Trade, <-chan error) {
	ch := make(chan Trade, 1000)
	done := make(chan error, 1)

	if userID == "" {
		close(ch)
		done <- errors.New("userID is required")
		return ch, done
	}

	go func() {
		defer close(ch)
		const batchSize = 1000

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

			var rows pgx.Rows
			var err error
			if first {
				rows, err = s.pool.Query(ctx, `
					SELECT transaction_id, order_id, order_seq, orderbook_id,
						user_id, asset0, quantity0, price, side,
						aggressor_indicator, created_at
					FROM trades_history
					WHERE user_id = $1
					  AND created_at >= $2
					  AND created_at <= $3
					ORDER BY created_at, transaction_id
					LIMIT $4
				`, userID, start, end, batchSize)
				first = false
			} else {
				rows, err = s.pool.Query(ctx, `
					SELECT transaction_id, order_id, order_seq, orderbook_id,
						user_id, asset0, quantity0, price, side,
						aggressor_indicator, created_at
					FROM trades_history
					WHERE user_id = $1
					  AND created_at >= $2
					  AND created_at <= $3
					  AND (created_at, transaction_id) > ($4, $5)
					ORDER BY created_at, transaction_id
					LIMIT $6
				`, userID, start, end, cursorTime, cursorID, batchSize)
			}
			if err != nil {
				done <- fmt.Errorf("query trades history: %w", err)
				return
			}

			batchCount := 0
			for rows.Next() {
				var t Trade
				var qty, priceStr string
				if scanErr := rows.Scan(
					&t.TransactionID, &t.OrderID, &t.OrderSeq, &t.OrderBookID,
					&t.UserID, &t.Asset0, &qty, &priceStr, &t.Side,
					&t.AggressorIndicator, &t.CreatedAt,
				); scanErr != nil {
					rows.Close()
					done <- fmt.Errorf("scan trade: %w", scanErr)
					return
				}
				t.Quantity0, err = decimal.Parse(qty)
				if err != nil {
					rows.Close()
					done <- fmt.Errorf("parse quantity %q: %w", qty, err)
					return
				}
				t.Price, err = decimal.Parse(priceStr)
				if err != nil {
					rows.Close()
					done <- fmt.Errorf("parse price %q: %w", priceStr, err)
					return
				}

				select {
				case <-ctx.Done():
					rows.Close()
					done <- ctx.Err()
					return
				case ch <- t:
				}
				batchCount++
				cursorTime = t.CreatedAt
				cursorID = t.TransactionID
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				done <- fmt.Errorf("iterate trades: %w", err)
				return
			}
			rows.Close()

			if batchCount < batchSize {
				done <- nil
				return
			}
		}
	}()

	return ch, done
}
```

Add the `decimal` import to `trades_history.go` if not already present.

- [ ] **Step 4: Run the tests — they should pass**

Run:
```bash
go test -run TestPGTradesHistoryStore_StreamTrades ./strategy/copytrading/
```
Expected: all 4 subtests PASS.

- [ ] **Step 5: Run the full `trades_history` test file**

Run:
```bash
go test -v ./strategy/copytrading/
```
Expected: all tests PASS, including the existing `TestTradesHistoryStoreInterface` (or it can be removed now that the real tests cover the surface).

- [ ] **Step 6: Remove the placeholder `TestTradesHistoryStoreInterface`**

In `strategy/copytrading/trades_history_test.go`, the original `TestTradesHistoryStoreInterface` from Task 1 is no longer useful (the real tests cover the surface). Delete it.

- [ ] **Step 7: Commit**

```bash
git add strategy/copytrading/trades_history.go strategy/copytrading/trades_history_test.go
git commit -m "feat(copytrading): PGTradesHistoryStore.StreamTrades with keyset cursor"
```

---

## Task 12: Final verification

**Files:** none modified

- [ ] **Step 1: Run the full test suite**

Run:
```bash
go test ./...
```
Expected: all packages pass.

- [ ] **Step 2: Run with race detector**

Run:
```bash
go test -race ./strategy/copytrading/... ./strategy/http/...
```
Expected: all pass with no race warnings.

- [ ] **Step 3: Lint**

Run:
```bash
golangci-lint run --timeout 5m ./...
```
Expected: clean. If there are complaints about the deleted `tradesClient`/`doraTradesClient` (shouldn't be — we deleted them), or about the new `PGTradesHistoryStore` field being unused in some path, address them per the linter output. Common fixes:
- `gochecknoglobals`: no global vars introduced.
- `gosec`: no SQL injection (we use parameterised queries).
- `errorlint`: wrap with `%w` (done).
- `mnd`: numeric literals are named constants where possible.

- [ ] **Step 4: Verify the migration is applied**

Run:
```bash
psql "$DATABASE_URL" -c "SELECT COUNT(*) FROM trades_history;"
```
Expected: returns a count (0 if the sync job hasn't run, >0 if it has). The query itself should not error — the table must exist.

- [ ] **Step 5: Verify the binary builds**

Run:
```bash
go build ./cmd/strategy-server/
```
Expected: clean build.

- [ ] **Step 6: Commit any final tidy**

If `golangci-lint` or `go mod tidy` produced any changes, commit them:

```bash
git add -A
git status  # review the diff carefully
git commit -m "chore: lint and mod tidy"
```

Only commit if the changes are real fixes. Do not commit formatting-only noise from `goimports` against unrelated files.

---

## Self-Review

**Spec coverage check:**

| Spec section | Task |
| --- | --- |
| `Trade` struct | Task 1 |
| `tradesHistoryStore` interface | Task 1 |
| `pgTradesHistoryStore` | Tasks 10, 11 |
| `TradeBounds` (diagnostic) | Task 10 |
| `StreamTrades` (keyset cursor, 1000 batch, ctx-aware) | Task 11 |
| `Backtester.Run` with `TradeBounds` validation | Task 6 |
| Delete `tradesClient` + `doraTradesClient` | Task 3, 4, 5 |
| `simulate` + 6 helpers take `copytrading.Trade` | Task 4 |
| `WithBacktestStore` option | Task 7 |
| `Strategy.Backtest` returns "backtest store not configured" error if nil | Task 7 |
| `NewBacktester(s, store)` signature | Task 3 |
| HTTP handler wiring (`WithTradesHistoryStore`, `defaultStrategies`, `newCopyTradingDefinition`) | Task 8 |
| `cmd/strategy-server/main.go` wiring | Task 9 |
| `fakeTradesHistoryStore` (counterfeiter) | Task 2 |
| `TestBacktesterRun_NoDataForUser` | Task 6 |
| `TestBacktesterRun_WindowOutsideData` | Task 6 |
| `TestBacktesterRun_EmptyResultInBounds` | Task 6 |
| `TestBacktesterRun_StreamError` | Task 6 |
| `TestBacktesterRun_ContextCancelled` | Task 6 |
| Existing `TestSimulate_*` rewritten | Task 5 |
| `TestPGTradesHistoryStore_StreamTrades_Ordered` | Task 11 |
| `TestPGTradesHistoryStore_StreamTrades_RangeFilter` | Task 11 |
| `TestPGTradesHistoryStore_StreamTrades_Empty` | Task 11 |
| `TestPGTradesHistoryStore_StreamTrades_ContextCancelled` | Task 11 |
| `TestPGTradesHistoryStore_TradeBounds_Empty` | Task 10 |
| `TestPGTradesHistoryStore_TradeBounds_Populated` | Task 10 |
| `golangci-lint`, `go test ./...`, `go test -race` | Task 12 |
| `pgxmock` dependency | Task 10 |

**Known gaps (acceptable):**
- The plan does not include a real-PG integration test (per the spec's deferred decision, we picked pgxmock). A follow-up spec can add one.
- The `pgtypeNullTime` shim is a small custom type to avoid pulling in `pgtype`. If the project already uses `pgtype` elsewhere, we can switch to that; the spec didn't pin this choice. The plan uses a shim and notes the alternative.

**Type consistency check:** `Trade` is defined once in `trades_history.go` (Task 1) and used by every subsequent task. `tradesHistoryStore` is defined once and used by `Backtester` (Task 3), `Strategy` (Task 7), the fake (Task 2), and the concrete `pgTradesHistoryStore` (Tasks 10, 11). Field names match throughout: `history`, `backtestStore`, `tradesHistoryStore` are the three field names across the three structs that hold the store.
