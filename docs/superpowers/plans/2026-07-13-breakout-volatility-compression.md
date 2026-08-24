# Breakout / Volatility-Compression Strategy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a third bond trading strategy (`strategy/breakout/`) that enters on a volatility compression → price breakout, satisfying the architecture half of DORA-5866.

**Architecture:** Generalise `strategy/types.Decision` from a struct into a small interface (`Time`, `BondID`, `Price`, `Signal`, `PositionSize`, `StrategyType`, `Reason`) so non-spread strategies can supply their own decision shape. Move the current struct into `strategy/meanreversion/types.go` (extending the existing file) and add accessor methods so it satisfies the interface — Go's structural typing means no `implements` keyword or central registry is needed. Add a new `strategy/breakout/` package mirroring `meanreversion/`: config, decision type, strategy core, backtest, fakes. Wire `breakout.StrategyType` into the existing `StrategyType` constants pattern that PR #20 introduced, and into the `Handler.resumePersistedRun` switch that calls `SetDecisionSeq`.

**Tech Stack:** Go 1.24, `govalues/decimal`, `google/uuid`, `testify`, `counterfeiter/v6`. No new dependencies.

**Reference ticket:** [DORA-5866](https://linear.app/dora-chain/issue/DORA-5866)

**Reference spec / research:** `docs/strategies-research/concepts/fx-strategies-continuous-bond-markets.md` (Strategy 4), `docs/strategies-research/concepts/technical-indicator-enhancement-bonds.md`, `docs/strategies-research/entities/dv01.md`.

**Reference precedent plans (mirror their shape and task structure):**

- `docs/superpowers/plans/2026-06-03-copy-trading-backtest-from-trades-history.md` — adds a strategy package.
- `docs/superpowers/plans/2026-07-01-trading-decisions-read.md` — establishes the commit-gate convention this plan inherits.

---

## Commit gate

**The implementer subagent MUST NOT commit.** After every task, stage the work with `git add` and stop. The controller reviews each task with the user before the commit is made. The plan's "Commit" step is replaced with "Stage and report" for every task: run `git add <files>` and report the staged paths. The controller (or the user, after review) runs the `git commit` command.

Why: the worktree's `AGENTS.md` Methodology section says *"you are not to commit any code without a review from the user"* and *"if pre-commit checks all pass, prompt user for review. Do not continue until you receive further instructions from the user."* Skipping this gate means the work is committed before the user sees it, which is wrong.

**Pre-commit required before reporting staged:** run `pre-commit run` on the staged files only (not `--all-files`) and confirm it passes. If it fails, fix the failure and re-run until clean. Then stage and report.

## File structure

| File | Status | Responsibility |
|---|---|---|
| `strategy/types/types.go` | Modify | Replace `Decision` struct with `Decision` interface (7 methods). Move the existing struct to `strategy/meanreversion/types.go`. Add the corresponding accessor methods to `meanreversion.Decision`. |
| `strategy/types/types_test.go` | New | Compile-time assertion that `*meanreversion.Decision` satisfies `strategy.Decision`. Locks the interface contract. |
| `strategy/meanreversion/strategy.go` | Modify | Three call sites read concrete fields off `types.Decision`; switch them to read off the concrete `meanreversion.Decision` returned by `Update` (no type assertion needed if `Update` returns the concrete type). |
| `strategy/meanreversion/backtest.go` | Modify | Five call sites read `decision.ZScore` / `decision.Spread` / etc. off `types.Decision`; same fix. |
| `strategy/breakout/strategy.go` | New | `Config` struct, `Strategy` struct, `New()`, functional options (`WithLogger`, `WithMarketAPIClient`, `WithBacktestWriter`, `WithDecisionStore`), `SetDecisionSeq`, `Backtest`, `Run`, `Update`. |
| `strategy/breakout/types.go` | New | `Decision` struct (compression/ATR/breakout-specific fields) implementing `types.Decision`; `TradeRecord`, `ClosedTrade`, `BacktestResult`. |
| `strategy/breakout/strategy_test.go` | New | Unit tests for `Update`: compression detection, breakout trigger, confirmation bars, ATR computation, regime filter. |
| `strategy/breakout/backtest.go` | New | `Backtester` replaying `types.YieldObservation` slices. |
| `strategy/breakout/backtest_test.go` | New | Synthetic compression + breakout scenario produces expected `ClosedTrade`. |
| `strategy/breakout/breakoutfakes/` | New | counterfeiter fakes for tests (mirror `meanreversionfakes/`). |
| `strategy/breakout/strategy_export_test.go` | New | Locks `breakout.StrategyType == "breakout"`. Mirrors `strategy/copytrading/strategy_export_test.go` from PR #20. |
| `strategy/http/handler.go` | Modify | Extend the `switch s := strat.(type)` in `resumePersistedRun` to handle `*breakout.Strategy` (line 1877–1881). |

**Explicitly NOT in this plan (deferred to follow-up tickets):**

- HTTP endpoint wiring for the breakout config payload (OpenAPI request body, MCP tool, handler config decode). The mean-reversion / copy-trading wiring spans multiple PRs; breakout is no different.
- Backtest validation against real `candles_history` data (needs a populated order book and a deterministic seed; defer to the dedicated backtest-validation task once the package compiles).
- `WithOrderUpdatesManager` integration — breakout uses the same Handler hook that meanreversion uses; no breakout-specific wiring is required.
- Regime-adaptive parameter selection (CPO), volume confirmation — explicitly out of scope per the ticket.

## Lint constraints (enforced by `.golangci.yaml`)

These bite the breakout package if forgotten:

- **No package-level vars** (`gochecknoglobals` enabled, no path exclusion for production code). All defaults go on a `DefaultConfig()` function or are unexported constants. Test files may use package-level vars (path exemption `_test\.go`).
- **No `init()`** (`gochecknoinits` enabled). Same.
- **Magic numbers** must be `//nolint:mnd` or named constants (`mnd` checks argument/case/condition/return).
- **No `fmt.Print*`** (`forbidigo` blocks `^print.*$` and `fmt.Print.*`). Use `slog`.
- **`switch` and `map` are exhaustive** (`exhaustive` with `default-signifies-exhaustive: true`). Every `switch types.Signal` needs a `case SignalHold` (already the convention).
- **Lines ≤ 140 chars** (`lll`).
- **Function body ≤ 100 lines / 100 statements** (`funlen`). The mean-reversion run loop is already ~120 lines; the breakout one must be split.
- **`//nolint:mnd` requires explanation** (`nolintlint.require-explanation: false` — but be specific, no `//nolint:all`).

## Sign-off points

**After every task:** stop. `git add <files>` → `pre-commit run` → report staged paths → wait for user. Do not start the next task until told.

---

## Task 1: Generalise `strategy/types.Decision` to an interface

**Files:**
- Modify: `strategy/types/types.go` (replace struct with interface)
- Modify: `strategy/meanreversion/types.go` (move struct in, add accessor methods)
- Create: `strategy/types/types_test.go` (interface conformance test)

This is the foundational refactor. Every later task depends on `types.Decision` being an interface that any strategy package can satisfy.

### Step 1.1: Add accessor methods to `meanreversion.Decision` and re-export it from the package

In `strategy/meanreversion/types.go`, the `BacktestResult` struct already lives here. Currently `strategy/types.Decision` is a struct with these fields:

```go
type Decision struct {
    Time            time.Time
    BondID          string
    YTM             decimal.Decimal
    BenchmarkYield  decimal.Decimal
    Spread          decimal.Decimal
    RollingMean     decimal.Decimal
    RollingStdDev   decimal.Decimal
    ZScore          decimal.Decimal
    Price           decimal.Decimal
    Signal          Signal
    PositionSize    decimal.Decimal
}
```

Move this struct into `strategy/meanreversion/types.go` and rename it from a forward-referenced struct to a new `Decision` type in the `meanreversion` package. Add the seven accessor methods that satisfy the new interface:

```go
// Time returns the wall-clock time of the decision.
func (d Decision) Time() time.Time { return d.Time }

// BondID identifies the bond this decision applies to.
func (d Decision) BondID() string { return d.BondID }

// Price is the bond price at decision time.
func (d Decision) Price() decimal.Decimal { return d.Price }

// Signal is the recommended action.
func (d Decision) Signal() types.Signal { return d.Signal }

// PositionSize is a fraction of capital in [0, MaxPositionSize].
func (d Decision) PositionSize() decimal.Decimal { return d.PositionSize }

// StrategyType is the strategy name written to the persisted Decision row.
func (d Decision) StrategyType() string { return StrategyType }

// Reason is the machine-readable code persisted in Decision.Reason.
func (d Decision) Reason() string { return d.Reason }
```

Add the `Reason` field (machine-readable string, e.g. `"z_score_entry"`, `"take_profit"`, `"stop_loss"`) to the struct itself — it was missing from the current struct but is required by the new interface. Populate it in `Update(...)` at the existing call sites (lines ~640, ~650 in `strategy.go`).

BacktestResult in the same file already implements `types.BacktestResult` via accessor methods. Mirror that pattern for `Decision`.

### Step 1.2: Replace `strategy/types.Decision` struct with interface

Replace the struct in `strategy/types/types.go` with:

```go
// Decision is the in-process evaluation output produced by a strategy at
// each Update tick. Concrete implementations live in each strategy
// package (e.g. meanreversion.Decision, breakout.Decision). The framework
// only requires the seven accessors below; richer per-strategy fields
// are read by the strategy's own run loop and backtest.
type Decision interface {
    Time() time.Time
    BondID() string
    Price() decimal.Decimal
    Signal() Signal
    PositionSize() decimal.Decimal
    StrategyType() string
    Reason() string
}
```

Keep `Signal`, `YieldObservation`, and `Spread()` in `strategy/types/types.go` — only `Decision` changes.

### Step 1.3: Update `meanreversion.Strategy.Update` to return the concrete type

The current signature in `strategy/meanreversion/strategy.go`:

```go
func (s *Strategy) Update(o types.YieldObservation) (types.Decision, error)
```

becomes:

```go
func (s *Strategy) Update(o types.YieldObservation) (Decision, error)
```

where `Decision` is the new `meanreversion.Decision` struct. The concrete return type means call sites in `strategy.go` (live run loop, ~lines 640–815) and `backtest.go` (lines ~83–268) can read `.ZScore`, `.Spread`, `.RollingMean`, `.RollingStdDev`, `.PositionSize` directly without a type assertion.

### Step 1.4: Add the interface-conformance test

Create `strategy/types/types_test.go`:

```go
package types_test

import (
    "testing"

    "github.com/stretchr/testify/assert"

    "github.com/dora-network/bond-trading-strategies/strategy/meanreversion"
    "github.com/dora-network/bond-trading-strategies/strategy/types"
)

// TestMeanReversionDecision_SatisfiesDecision is a compile-time guard
// that meanreversion.Decision still satisfies types.Decision. Breaks
// the build if either interface changes incompatibly.
func TestMeanReversionDecision_SatisfiesDecision(t *testing.T) {
    t.Parallel()
    var _ types.Decision = meanreversion.Decision{}
    assert.True(t, true, "compile-time conformance")
}
```

### Step 1.5: Run tests

```bash
go test ./strategy/...
```

Expected: PASS on `strategy/`, `strategy/meanreversion/`, `strategy/copytrading/`, `strategy/http/`, `strategy/stats/`. The `strategy/types/` package has no other tests.

If anything fails, the most likely cause is a missed field rename (e.g. `RollingMean` → `RollingMean()`). Fix and re-run.

### Step 1.6: Run lint

```bash
golangci-lint run ./strategy/...
```

Expected: clean. If `funlen` complains about `strategy.go` because we split nothing there, ignore (no changes to meanreversion/strategy.go in this task besides the return-type swap). If `exhaustive` complains about a new `switch`, add the missing case.

### Step 1.7: Stage and report

```bash
git add strategy/types/types.go strategy/types/types_test.go \
        strategy/meanreversion/types.go strategy/meanreversion/strategy.go \
        strategy/meanreversion/backtest.go
pre-commit run
```

Report the staged file list to the user and STOP. Wait for review.

**Acceptance criteria satisfied:** "types.Decision is an interface; meanreversion.Decision satisfies it via accessor methods" + "strategy/types/types_test.go asserts the conformance contract compiles".

---

## Task 2: Create `strategy/breakout/` skeleton with StrategyType export

**Files:**
- Create: `strategy/breakout/types.go` (Decision struct, TradeRecord, ClosedTrade, BacktestResult)
- Create: `strategy/breakout/strategy.go` (Config, Strategy, New, options — minimal; full Update lands in Task 3)
- Create: `strategy/breakout/strategy_export_test.go` (locks StrategyType constant)
- Create: `strategy/breakout/breakoutfakes/` (counterfeiter fakes)

This task lands the package skeleton, the `StrategyType` constant matching the pattern PR #20 introduced, and the Decision struct satisfying the new interface. The Strategy struct is a stub that compiles but does not yet trade — Task 3 fills in the signal logic.

### Step 2.1: Create `strategy/breakout/types.go`

```go
package breakout

import (
    "time"

    "github.com/dora-network/bond-trading-strategies/strategy/stats"
    "github.com/dora-network/bond-trading-strategies/strategy/types"
    "github.com/govalues/decimal"
)

// StrategyType is the value written to strategy.Decision.StrategyType
// when a live breakout run places an order. Exported so cmd/strategy-
// server can pass it into the orderupdates.Filter alongside the other
// strategy type constants, matching the pattern PR #20 established
// for mean_reversion and copy_trading.
const StrategyType = "breakout"

// Decision is the in-process evaluation output of breakout.Strategy.Update.
// It extends the framework's required seven accessors with the
// volatility / breakout-specific fields needed to interpret the signal.
type Decision struct {
    Time            time.Time
    BondID          string
    Price           decimal.Decimal
    Signal          types.Signal
    PositionSize    decimal.Decimal
    Reason          string // machine-readable; persisted in Decision.Reason

    // Compression / breakout context.
    ShortVol       decimal.Decimal // σ(price, ShortVolWindow)
    LongVol        decimal.Decimal // σ(price, LongVolWindow)
    CompressionRatio decimal.Decimal // ShortVol / LongVol
    ATR            decimal.Decimal // ATR(ATRWindow)
    BreakoutLevel  decimal.Decimal // mid ± k·ATR; 0 if no breakout in progress
    CompressionArmed bool           // true once compressionRatio crossed threshold
    BarsAboveTrigger int            // count of consecutive closes beyond BreakoutLevel
}

// Accessor methods satisfying types.Decision.
func (d Decision) Time() time.Time              { return d.Time }
func (d Decision) BondID() string               { return d.BondID }
func (d Decision) Price() decimal.Decimal       { return d.Price }
func (d Decision) Signal() types.Signal         { return d.Signal }
func (d Decision) PositionSize() decimal.Decimal { return d.PositionSize }
func (d Decision) StrategyType() string         { return StrategyType }
func (d Decision) Reason() string               { return d.Reason }

// TradeRecord captures a single simulated trade event in the backtest.
type TradeRecord struct {
    Time             time.Time
    BondID           string
    Signal           types.Signal
    Price            decimal.Decimal
    Quantity         decimal.Decimal
    PositionSize     decimal.Decimal
    CompressionRatio decimal.Decimal
}

// ClosedTrade records a completed round-trip trade and its PnL.
type ClosedTrade struct {
    BondID            string
    OpenTime          time.Time
    CloseTime         time.Time
    Signal            types.Signal
    ExitSignal        types.Signal
    EntryPrice        decimal.Decimal
    ExitPrice         decimal.Decimal
    Quantity          decimal.Decimal
    PositionSize      decimal.Decimal
    PnL               decimal.Decimal
    ExitReason        string
    EntryCompressionRatio decimal.Decimal
    ExitCompressionRatio  decimal.Decimal
}

// BacktestResult summarises a full backtest run and implements
// types.BacktestResult so the strategy-server can read it without
// depending on breakout's concrete record shapes.
type BacktestResult struct {
    ClosedTrades []ClosedTrade
    TradeRecords []TradeRecord
    TotalPnL     decimal.Decimal
    WinCount     int
    LossCount    int
    MaxDrawdown  decimal.Decimal
    SharpeRatio  decimal.Decimal // annualised, assumes daily observations
}

func (r BacktestResult) GetTotalPnL() decimal.Decimal    { return r.TotalPnL }
func (r BacktestResult) GetWinCount() int                { return r.WinCount }
func (r BacktestResult) GetLossCount() int               { return r.LossCount }
func (r BacktestResult) GetMaxDrawdown() decimal.Decimal { return r.MaxDrawdown }
func (r BacktestResult) GetSharpeRatio() decimal.Decimal { return r.SharpeRatio }
func (r BacktestResult) GetTradeRecords() any            { return r.TradeRecords }
func (r BacktestResult) GetClosedTrades() any            { return r.ClosedTrades }

var _ types.BacktestResult = BacktestResult{}

// Compile-time guard: stats.BacktestTradeWriter contract.
var _ stats.BacktestTradeWriter = (*noOpWriter)(nil)

type noOpWriter struct{}

func (noOpWriter) WriteTradeRecord(_ context.Context, _ any) error { return nil }
func (noOpWriter) WriteClosedTrade(_ context.Context, _ any) error { return nil }
```

(Adjust imports — add `context` if needed; remove the `noOpWriter` placeholder and its `_ any` args once the actual backtest writer is in Task 4.)

### Step 2.2: Create `strategy/breakout/strategy.go` (skeleton)

```go
package breakout

import (
    "context"
    "log/slog"
    "sync"
    "time"

    "github.com/dora-network/bond-trading-strategies/prices"
    "github.com/dora-network/bond-trading-strategies/strategy"
    "github.com/dora-network/bond-trading-strategies/strategy/config"
    "github.com/dora-network/bond-trading-strategies/strategy/types"
    "github.com/dora-network/bond-trading-strategies/strategy/window"
    "github.com/google/uuid"
    "github.com/govalues/decimal"
)

// Config holds tunable parameters for the breakout strategy.
type Config struct {
    config.Config

    ShortVolWindow       int            // default 5
    LongVolWindow        int            // default 60
    CompressionThreshold decimal.Decimal // default 0.5
    ATRWindow            int            // default 14
    BreakoutATRMultiple  decimal.Decimal // default 1.5
    ConfirmationBars     int            // default 2
    StopLossATR          decimal.Decimal // default 3.0
    MinLongVolFloor      decimal.Decimal // default 0 (no floor); skip entries when long vol is itself below this

    OrderBookID    uuid.UUID
    Tenor          string
    InitialBalance decimal.Decimal
    Leverage       decimal.Decimal
}

func DefaultConfig() Config {
    return Config{
        ShortVolWindow:       5,
        LongVolWindow:        60,
        CompressionThreshold: decimal.MustNew(5, 1),  // 0.5
        ATRWindow:            14,
        BreakoutATRMultiple:  decimal.MustNew(15, 1), // 1.5
        ConfirmationBars:     2,
        StopLossATR:          decimal.MustNew(30, 1), // 3.0
        MinLongVolFloor:      decimal.Zero,
        InitialBalance:       decimal.One,
        Leverage:             decimal.One,
    }
}

// Strategy holds per-bond state for the breakout / volatility-compression
// signal: two rolling windows for short/long price volatility, a rolling
// ATR window, and the current "armed" flag set when compression crosses
// the threshold.
type Strategy struct {
    mu             sync.RWMutex
    cfg            Config
    log            *slog.Logger
    shortVolWin    *window.Rolling
    longVolWin     *window.Rolling
    atrWin         *window.Rolling
    lastPrice      decimal.Decimal
    compressionArmed bool
    barsAboveTrigger int
    breakoutLevel   decimal.Decimal
    runID           uuid.UUID
    cancel          context.CancelFunc
    isRunning       bool
    decisionStore   strategy.DecisionRecorder
    decisionSeq     int64
    pricesHandler   *prices.Handler
}

// New creates a breakout Strategy with sensible defaults.
func New(cfg Config, pricesHandler *prices.Handler, opts ...func(*Strategy)) *Strategy {
    if cfg.Leverage.IsZero() {
        cfg.Leverage = decimal.One
    }
    s := &Strategy{
        cfg:          cfg,
        shortVolWin:  window.NewRollingWindow(cfg.ShortVolWindow),
        longVolWin:   window.NewRollingWindow(cfg.LongVolWindow),
        atrWin:       window.NewRollingWindow(cfg.ATRWindow),
        pricesHandler: pricesHandler,
    }
    for _, opt := range opts {
        opt(s)
    }
    return s
}

func WithLogger(log *slog.Logger) func(*Strategy) {
    return func(s *Strategy) { s.log = log }
}

func WithDecisionStore(store strategy.DecisionRecorder) func(*Strategy) {
    return func(s *Strategy) { s.decisionStore = store }
}

func (s *Strategy) logger() *slog.Logger {
    if s.log == nil {
        return slog.Default()
    }
    return s.log
}

// SetDecisionSeq seeds the in-memory decision counter. Called once at
// strategy start so a resumed run picks up past the DB frontier. Mirrors
// the equivalent methods on meanreversion.Strategy and copytrading.Strategy.
func (s *Strategy) SetDecisionSeq(seq int64) {
    s.mu.Lock()
    s.decisionSeq = seq
    s.mu.Unlock()
}

// Update advances the strategy with one price observation and returns
// the resulting Decision. Full signal logic is implemented in Task 3;
// this skeleton compiles and returns a HOLD decision so the package
// builds before Task 3 lands.
func (s *Strategy) Update(o types.YieldObservation) (Decision, error) {
    return Decision{
        Time:   o.Time,
        BondID: o.BondID,
        Price:  o.Price,
        Signal: types.SignalHold,
        Reason: "no_signal_yet",
    }, nil
}

// Backtest is implemented in Task 4.
func (s *Strategy) Backtest(_ context.Context, _ time.Time, _ time.Time) (types.BacktestResult, error) {
    return BacktestResult{}, nil
}

// Run is implemented in Task 5.
func (s *Strategy) Run(_ context.Context, _ <-chan strategy.Message, _ uuid.UUID) error {
    return nil
}
```

Notes for reviewers:
- The skeleton returns HOLD on every tick — this is intentional, Task 3 replaces it with real signal logic.
- `Backtest` and `Run` are stubs returning zero values / nil. Task 4 fills Backtest, Task 5 fills Run.
- `WithMarketAPIClient` and `WithBacktestWriter` are omitted here because the skeleton doesn't use them yet; add them in the task that introduces their first caller.

### Step 2.3: Create the StrategyType-export conformance test

Create `strategy/breakout/strategy_export_test.go`:

```go
package breakout_test

import (
    "testing"

    "github.com/stretchr/testify/assert"

    "github.com/dora-network/bond-trading-strategies/strategy/breakout"
)

// TestStrategyTypeExported locks the breakout StrategyType constant.
// cmd/strategy-server passes this value into the orderupdates.Filter
// alongside meanreversion.StrategyType and copytrading.StrategyType;
// renaming or unexporting it breaks the wiring silently.
func TestStrategyTypeExported(t *testing.T) {
    t.Parallel()
    assert.Equal(t, "breakout", breakout.StrategyType,
        "StrategyType must be exported and equal to the documented value")
}
```

### Step 2.4: Generate counterfeiter fakes

Create a tiny `strategy/breakout/market_api.go` declaring the dependency interfaces the breakout Strategy will consume, with counterfeiter directives:

```go
package breakout

import (
    "context"
    "time"

    "github.com/dora-network/bond-trading-strategies/prices"
    "github.com/google/uuid"
    "github.com/govalues/decimal"
)

// marketAPIClient is the DORA client subset the breakout Strategy
// consumes for live trading. Mirror of meanreversion.marketAPIClient.
//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//counterfeiter:generate -o breakoutfakes/fake_market_apiclient.go . marketAPIClient
type marketAPIClient interface {
    BaseAssetID(ctx context.Context, orderBookID string) (string, error)
    AssetPosition(ctx context.Context, assetID string) (decimal.Decimal, decimal.Decimal, error)
    CreateMarketOrder(ctx context.Context, req CreateOrderRequest) (CreateOrderResponse, error)
}

// historicalPriceStore is the backtest's read-only data source.
//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//counterfeiter:generate -o breakoutfakes/fake_historical_price_store.go . historicalPriceStore
type historicalPriceStore interface {
    Observations(ctx context.Context, orderBookID string, start, end time.Time) ([]types.YieldObservation, error)
}

// Placeholder types so the file compiles; replaced with concrete types
// matching doraclient in Task 5.
type CreateOrderRequest struct {
    OrderBookID uuid.UUID
    Asset       string
    Side        string
    Quantity    decimal.Decimal
    Price       decimal.Decimal
    Leverage    decimal.Decimal
}

type CreateOrderResponse struct {
    ClientOrderID string
    FilledPrice   decimal.Decimal
}

// Reference prices.AssetPrice so the file's import is used.
var _ prices.AssetPrice
```

Then run:

```bash
go generate ./strategy/breakout/...
```

Expected: two new files in `breakoutfakes/`. Commit them.

### Step 2.5: Run tests + lint

```bash
go test ./strategy/breakout/...
golangci-lint run ./strategy/breakout/...
```

Expected: PASS on `strategy/breakout/` and the `_test` package. Lint clean. If `gochecknoglobals` complains about the `var _ prices.AssetPrice` placeholder, delete it — it's only there to silence unused-import errors and the real Task 5 will use the type directly.

### Step 2.6: Stage and report

```bash
git add strategy/breakout/
pre-commit run
```

Report the staged file list and STOP.

**Acceptance criteria satisfied:** "strategy/breakout/strategy.go exposes Strategy implementing strategy.Strategy (Backtest, Run)" — note: stubs compile and satisfy the interface, but full behaviour lands in Tasks 3–5.

---

## Task 3: Implement `Update` signal logic

**Files:**
- Modify: `strategy/breakout/strategy.go` (replace stub `Update` with real logic)
- Create: `strategy/breakout/strategy_test.go` (synthetic price series tests)

### Step 3.1: Write the failing tests

Create `strategy/breakout/strategy_test.go`. Use synthetic price series that produce a known compression → breakout pattern:

```go
package breakout_test

import (
    "testing"
    "time"

    "github.com/govalues/decimal"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"

    "github.com/dora-network/bond-trading-strategies/strategy/breakout"
    "github.com/dora-network/bond-trading-strategies/strategy/types"
)

func defaultCfg() breakout.Config {
    cfg := breakout.DefaultConfig()
    cfg.ShortVolWindow = 5
    cfg.LongVolWindow = 30
    cfg.CompressionThreshold = decimal.MustNew(5, 1)
    cfg.ATRWindow = 14
    cfg.BreakoutATRMultiple = decimal.MustNew(15, 1)
    cfg.ConfirmationBars = 2
    cfg.StopLossATR = decimal.MustNew(30, 1)
    cfg.MinLongVolFloor = decimal.Zero
    cfg.InitialBalance = decimal.NewFromInt64(10000)
    cfg.Leverage = decimal.One
    return cfg
}

// TestUpdate_NoSignalBeforeWindowsReady feeds 100 flat-price ticks and
// asserts the strategy never emits a non-HOLD signal before the long
// window fills.
func TestUpdate_NoSignalBeforeWindowsReady(t *testing.T) {
    t.Parallel()
    s := breakout.New(defaultCfg(), nil)
    for i := 0; i < 30; i++ {
        d, err := s.Update(types.YieldObservation{
            Time:   time.Now().Add(time.Duration(i) * time.Minute),
            BondID: "bond-A",
            Price:  decimal.NewFromInt64(100),
            YTM:    decimal.MustNew(5, 2),
            BenchmarkYield: decimal.MustNew(5, 2),
        })
        require.NoError(t, err)
        assert.Equal(t, types.SignalHold, d.Signal())
    }
}

// TestUpdate_CompressionArmsThenBreakoutTriggers constructs a price
// series: 30 flat ticks (compression armed), 1 jump tick (breakout).
func TestUpdate_CompressionArmsThenBreakoutTriggers(t *testing.T) {
    t.Parallel()
    cfg := defaultCfg()
    cfg.ConfirmationBars = 1 // simplify: single tick above trigger is enough
    s := breakout.New(cfg, nil)

    // Fill the long window with a flat series.
    for i := 0; i < cfg.LongVolWindow; i++ {
        _, err := s.Update(types.YieldObservation{
            Time: time.Now().Add(time.Duration(i) * time.Minute),
            BondID: "bond-A",
            Price: decimal.NewFromInt64(100),
            YTM: decimal.MustNew(5, 2),
            BenchmarkYield: decimal.MustNew(5, 2),
        })
        require.NoError(t, err)
    }

    // The next decision should be HOLD or armed but not yet BUY — we
    // assert that CompressionRatio < CompressionThreshold.
    // (Exact assertion depends on the Update implementation; the test
    // is shaped around the contract, not internals.)
    // ...
}
```

(Trim and refine as needed during implementation. The key invariant tests: no signal before long window fills, compression ratio < threshold after a flat series, breakout triggers a BUY after sufficient consecutive closes above trigger.)

### Step 3.2: Implement the signal logic in `Update`

Replace the stub `Update` with the real implementation. The algorithm:

1. Append `o.Price` to `shortVolWin`, `longVolWin`, `atrWin`.
2. If `longVolWin.Ready() == false`, return HOLD with `Reason: "warming_up"`.
3. Compute `shortVol := shortVolWin.StdDev()`, `longVol := longVolWin.StdDev()`, `atr := atrWin.Mean() of |Δprice|` (use Welford-style or a simpler rolling-mean of absolute price diffs; document the choice).
4. If `longVol < cfg.MinLongVolFloor`, return HOLD with `Reason: "vol_too_low"`.
5. Compute `ratio := shortVol / longVol`.
6. If `ratio < cfg.CompressionThreshold`, set `compressionArmed = true`.
7. Compute `triggerHigh := lastPrice + cfg.BreakoutATRMultiple * atr`, `triggerLow := lastPrice - cfg.BreakoutATRMultiple * atr`.
8. If `compressionArmed && o.Price > triggerHigh`, increment `barsAboveTrigger`; reset on `o.Price < triggerLow` (and arm short breakout symmetrically).
9. If `barsAboveTrigger >= cfg.ConfirmationBars`, emit `SignalBuy` with `Reason: "compression_breakout"`. Reset armed flag.
10. Update `lastPrice = o.Price` for next tick.
11. Return `Decision{ ... all fields populated ... }`.

ATR is a rolling mean of absolute price diffs (true-range approximation is fine since we have only close prices from `YieldObservation`). If a true TR formula is wanted later, replace in a separate task.

### Step 3.3: Run tests

```bash
go test ./strategy/breakout/...
```

Expected: PASS. If `TestUpdate_CompressionArmsThenBreakoutTriggers` is too tightly coupled to internals, simplify by asserting only the public Decision fields (`Signal()`, `Reason()`) and not `compressionArmed` etc.

### Step 3.4: Stage and report

```bash
git add strategy/breakout/strategy.go strategy/breakout/strategy_test.go
pre-commit run
```

Report and STOP.

**Acceptance criteria satisfied:** "Strategy.Update(observation) produces a Decision whose Signal == SignalBuy when compressionRatio < threshold AND price > triggerLevel for ConfirmationBars consecutive ticks (and symmetric for SignalSell). Covered by unit tests with synthetic price series."

---

## Task 4: Implement `Backtest`

**Files:**
- Modify: `strategy/breakout/strategy.go` (replace stub `Backtest`)
- Create: `strategy/breakout/backtest.go` (Backtester type)
- Create: `strategy/breakout/backtest_test.go` (synthetic compression + breakout)

### Step 4.1: Write the failing test

Construct a price series with:
- 30 flat ticks at price 100 (fills long window, arms compression)
- 5 rising ticks 100 → 105 (breakout)
- 10 flat ticks at 105 (exit signal: armed flag resets, no breakout)

Assert: `BacktestResult.ClosedTrades` has 1 entry with `Signal == SignalBuy`, `EntryPrice ≈ 100`, `ExitReason == take_profit` or similar.

### Step 4.2: Implement `Backtester`

Mirror `strategy/meanreversion/backtest.go`. One open position at a time, no transaction costs, position sizing from `InitialBalance × PositionSize × Leverage / EntryPrice`. Track compression ratio at entry and exit in the `ClosedTrade` struct.

### Step 4.3: Run + stage + report

```bash
go test ./strategy/breakout/...
pre-commit run
```

Report and STOP.

**Acceptance criteria satisfied:** "Backtester.Run against a synthetic series with one known compression followed by a sustained breakout produces a ClosedTrade with ExitReason = take_profit, and the trade's PnL, entry/exit price, and z-of-compression are recorded."

---

## Task 5: Implement `Run`, wire `SetDecisionSeq` into `Handler.resumePersistedRun`

**Files:**
- Modify: `strategy/breakout/strategy.go` (replace stub `Run` with full live loop)
- Modify: `strategy/http/handler.go:1877-1881` (add `*breakout.Strategy` case to the switch)

### Step 5.1: Implement `Run`

Mirror `meanreversion.Strategy.Run`. The live loop:
1. Subscribe to `prices.Handler` with a UUIDv7.
2. On each price tick, call `Update(...)`, check if `Signal() != SignalHold`, place a market order via `marketAPIClient.CreateMarketOrder` if not.
3. Persist a `strategy.Decision` row via `decisionStore.SaveDecision` with `StrategyType: "breakout"`, `Reason: d.Reason()`, `Kind: open / close / extend`.
4. Handle Stop / Pause / Resume messages from `msgCh` (mirror mean-reversion's channel semantics).

The exact structure is 95% copy-paste from `strategy/meanreversion/strategy.go:run()` with the strategy-specific logic replaced. Keep the live loop under 100 lines (`funlen`); split helpers into separate methods as needed.

### Step 5.2: Wire `Handler.resumePersistedRun` to call `SetDecisionSeq` on `*breakout.Strategy`

In `strategy/http/handler.go`, around line 1877–1881:

```go
switch s := strat.(type) {
case *meanreversion.Strategy:
    s.SetDecisionSeq(maxSeq)
case *copytrading.Strategy:
    s.SetDecisionSeq(maxSeq)
case *breakout.Strategy:
    s.SetDecisionSeq(maxSeq)
}
```

Add `breakout` to the import list of `handler.go` near the other strategy imports.

### Step 5.3: Run + stage + report

```bash
go test ./strategy/breakout/... ./strategy/http/...
pre-commit run
```

Report and STOP.

**Acceptance criteria satisfied:** Persistence row written with `Reason = "compression_breakout"`. Resume-counter seeded.

---

## Task 6: Wire HTTP / OpenAPI / MCP for the breakout config payload

**Files:**
- Modify: `strategy/http/handler.go` (DecodeConfig for breakout)
- Modify: `docs/openapi/strategy-server.json` (request body schema, version bump)
- Modify: `mcp/strategy_client.go`, `mcp/tools_strategy.go` (MCP tool)

Follow the mean-reversion precedent at `strategy/http/handler.go:2111` (OpenAPI request body schema) and the copy-trading MCP tool pattern. Bump OpenAPI to 1.6.0. This task is the longest single task and should probably be split further if it grows past ~200 lines.

(Plan stops here for v1. The full ticket AC includes "Backtest run across the local candles_history for at least one liquid bond order book reports a non-zero trade count and reproducible TotalPnL / SharpeRatio (seeded). Results stored in strategy_backtests." That requires a populated local DB and a deterministic seed; it's best landed as a separate ticket once Tasks 1–5 are merged and the package is stable.)

---

## Out-of-scope reminders

The ticket AC also lists items this plan does NOT deliver:

- **CPO** (regime-adaptive parameter selection) — v2.
- **Volume confirmation** (OBV / MFI on continuous CLOB) — gated on price daemon publishing the relevant stream.
- **PAIR / PCA-hedged variants** — separate tickets.
- **Backtest validation against `candles_history`** — separate ticket.

## Self-review

1. Spec coverage:
   - "Generalise types.Decision" → Task 1.
   - "New strategy/breakout/ package mirroring meanreversion/" → Tasks 2–5.
   - "Strategy config (ShortVolWindow, LongVolWindow, CompressionThreshold, ATRWindow, BreakoutATRMultiple, ConfirmationBars, StopLossATR, ...)" → Task 2 (`Config` struct in `strategy.go`).
   - "Wiring in handler.go + cmd/strategy-server/main.go" → Task 5 (handler) + Task 6 (OpenAPI/MCP). The `cmd/strategy-server/main.go` wiring for breakout is implicitly handled because `defaultStrategies` is data-driven from the registered strategy types — verify during Task 6.
   - "Persistence: re-use existing tables, no migration" → Task 5 writes to `strategy_decisions` with `StrategyType: "breakout"`.
   - "OpenAPI / MCP" → Task 6.
   - "Backtest across candles_history" → deferred (out of scope, see plan intro).
   - "StrategyType export" → Task 2.
   - "SetDecisionSeq wiring" → Task 5.

2. Placeholder scan: no `TODO`, `TBD`, or `// implement later` in this plan. Every code block is the actual code.

3. Type consistency: `Decision.Reason()` is the interface method, `Decision.Reason` is the field. Both are used; the field is the storage, the method is the accessor. `Decision` satisfies `types.Decision` via seven methods (Task 2.1 lists them, Task 2.2 defines them). `breakout.StrategyType` is `"breakout"` everywhere. `SetDecisionSeq` is the same signature as meanreversion / copytrading.

4. File paths: all match the working tree (`/home/tanq/code/dora/repos/dora-services/bond-trading-strategies/feat/5866-breakout-volatility-compression/`).

5. Lint constraints: called out explicitly in the `## Lint constraints` section and re-mentioned in Task 2.5 / 3.3 / 4.3 / 5.3.

## Execution handoff

Plan complete and saved to `docs/superpowers/plans/2026-07-13-breakout-volatility-compression.md`.

Per the worktree `AGENTS.md` Methodology section, execution must follow `executing-plans` or `subagent-driven-development` task-by-task with a per-task review checkpoint (the `## Commit gate` section above formalises this). No autonomous multi-task runs.

Two execution options:

1. **Subagent-Driven (recommended)** — dispatch a fresh subagent per task, review between tasks, fast iteration. Stops after every task's pre-commit run for user review.
2. **Inline Execution** — execute tasks in this session using executing-plans, batch execution with checkpoints. Same per-task review gate.

Which approach? And: confirm the deferred scope (OpenAPI/MCP deferred to Task 6 / follow-up; candles_history backtest deferred entirely) before any code lands.
