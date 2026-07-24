# Momentum / Trend-Following Strategy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a single-instrument moving-average-crossover trend strategy (`strategy/momentum/`) with a configurable signal source (price / YTM / spread), ATR-based exits, and full live + backtest parity with the existing strategies.

**Architecture:** A new self-contained `strategy/momentum/` package mirroring `mean_reversion` (self-wired `prices.PGStore` + FRED client, FRED benchmark path reused for `spread` mode) and `breakout` (three `window.Rolling` windows — fast MA / slow MA / ATR — and entry-anchored ATR exits). Zero changes to existing strategies; no dependency on the breakout package.

**Tech Stack:** Go, `github.com/govalues/decimal` (never float64 for money), `strategy/window.Rolling`, `prices.Handler` / `prices.PGStore`, `fred` client, `strategy/stats`, `testify`, `counterfeiter`.

**Spec:** `docs/superpowers/specs/2026-07-24-momentum-trend-design.md`

**Working directory for every command:** `~/code/dora/repos/dora-services/bond-trading-strategies/feat/momentum-strategy` (branch `tan/momentum-strategy`).

---

## Conventions used throughout

- Money / quantities / yields: `decimal.Decimal` only. No `float64` in package code (only in the HTTP decode layer, which mirrors `decodeMeanReversionConfig`).
- Every task ends with: `go build ./...` then commit. Final task runs the full verification suite.
- Commit style: conventional commits (`feat(momentum): ...`, `test(momentum): ...`, `feat(http): ...`).
- Run `go generate ./strategy/momentum/...` wherever a `//go:generate` directive is added or changed.
- Pre-commit hooks (lint, vet, imports, mod-tidy, tests) run on every commit — never bypass. Fix failures, don't skip.
- The `config.Config` embedded field is an empty marker interface (`type Config interface{}` in `strategy/config/config.go`); each strategy defines its own fields directly. Mirror this.

## File map

| File | Responsibility |
|------|----------------|
| `strategy/momentum/types.go` | `Config`, `DefaultConfig`, `StrategyType`, source/sign constants, `Decision` + accessors, `TradeRecord`, `ClosedTrade`, `BacktestResult` |
| `strategy/momentum/strategy.go` | `Strategy` struct, `New`, functional options, `Update` (signal engine), `ShouldExit`, live `Run`/`run`/`subscribePrices`, `executeDecision`/`closePosition`/`cappedOrderQuantity`, balance helpers, decision recording |
| `strategy/momentum/historical_data.go` | `historicalPriceStore` + `benchmarkYieldClient` interfaces, `getObservations` (3 sources), `prefillWindow`, self-wired stores, FRED benchmark cache + tenor helpers |
| `strategy/momentum/backtest.go` | `Backtester` — replay observations, exit priority, PnL, summary |
| `strategy/momentum/export_test.go` | white-box test helpers |
| `strategy/momentum/*_test.go` | tests mirroring `meanreversion`/`breakout` suites |
| `strategy/http/handler.go` | `newMomentumDefinition`, `decodeMomentumConfig`, `momentumConfigPayload`; register in `defaultStrategies`; type-switch additions |

---

## Task 1: Types — Config, Decision, records, BacktestResult

**Files:**
- Create: `strategy/momentum/types.go`

- [ ] **Step 1: Write `types.go`**

```go
package momentum

import (
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy/config"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
)

// StrategyType is the strategy.Decision.StrategyType value and the
// client_order_id prefix used by the live run loop.
const StrategyType = "momentum"

// Signal source constants (Config.SignalSource).
const (
	SignalSourcePrice  = "price"
	SignalSourceYTM    = "ytm"
	SignalSourceSpread = "spread"
)

// Exit reason constants for ClosedTrade.
const (
	ExitReasonStopLoss    = "stop_loss"
	ExitReasonTakeProfit  = "take_profit"
	ExitReasonReversal    = "reversal"
	ExitReasonStrategyExit = "strategy_exit"
)

// Decision reason codes surfaced through types.Decision.Reason().
const (
	DecisionReasonWarmingUp          = "warming_up"
	DecisionReasonFlat               = "flat"
	DecisionReasonMACrossoverUp      = "ma_crossover_up"
	DecisionReasonMACrossoverDown    = "ma_crossover_down"
	DecisionReasonBelowMinOrderSize  = "below_min_order_size"
)

// Config holds all tunable parameters for the momentum strategy.
type Config struct {
	config.Config

	// SignalSource selects which series the MA crossover runs on.
	// One of SignalSourcePrice, SignalSourceYTM, SignalSourceSpread.
	SignalSource string

	// FastWindow / SlowWindow are the fast/slow MA tick windows.
	FastWindow int
	SlowWindow int

	// ATRWindow is the mean-absolute-price-diff window used for exits.
	ATRWindow int

	// StopLossATR / TakeProfitATR are exit distances in ATR units.
	// 0 disables each.
	StopLossATR   decimal.Decimal
	TakeProfitATR decimal.Decimal

	// MinOrderSize skips opening when the computed quantity is below it
	// (0 disables). MaxOrderSize clamps the quantity down (0 disables).
	// Decimal because Dora is a fractionalized market.
	MinOrderSize decimal.Decimal
	MaxOrderSize decimal.Decimal

	// MaxPositionSize caps the fraction of capital per trade (0,1].
	MaxPositionSize decimal.Decimal

	// OrderBookID is the DORA order book to place orders on.
	OrderBookID uuid.UUID

	// Tenor is required when SignalSource == spread.
	Tenor string

	// InitialBalance is the starting capital. Leverage scales it.
	InitialBalance decimal.Decimal
	Leverage       decimal.Decimal
}

// DefaultConfig returns sensible defaults for live deployment and tests.
// Window defaults mirror breakout's continuous-market calibration.
func DefaultConfig() Config {
	return Config{
		SignalSource:    SignalSourcePrice,
		FastWindow:      240,
		SlowWindow:      1440,
		ATRWindow:       240,
		StopLossATR:     decimal.MustNew(20, 0), //nolint:mnd
		TakeProfitATR:   decimal.Zero,
		MinOrderSize:    decimal.Zero,
		MaxOrderSize:    decimal.Zero,
		MaxPositionSize: decimal.One,
		InitialBalance:  decimal.One,
		Leverage:        decimal.One,
	}
}

// Decision is the per-tick evaluation output of Strategy.Update. It
// implements types.Decision via the seven accessor methods. Fields that
// collide with interface method names are unexported.
type Decision struct {
	time         time.Time
	bondID       string
	price        decimal.Decimal
	signal       types.Signal
	positionSize decimal.Decimal
	reason       string

	// Exported: read directly by the backtester.
	FastMA      decimal.Decimal
	SlowMA      decimal.Decimal
	ATR         decimal.Decimal
	SeriesValue decimal.Decimal
	Trend       string // "up", "down", or ""
}

func (d Decision) Time() time.Time               { return d.time }
func (d Decision) BondID() string                { return d.bondID }
func (d Decision) Price() decimal.Decimal        { return d.price }
func (d Decision) Signal() types.Signal          { return d.signal }
func (d Decision) PositionSize() decimal.Decimal { return d.positionSize }
func (d Decision) StrategyType() string          { return StrategyType }
func (d Decision) Reason() string                { return d.reason }

// TradeRecord captures a single simulated trade event (entry or exit).
type TradeRecord struct {
	Time         time.Time
	BondID       string
	Signal       types.Signal
	Price        decimal.Decimal
	Quantity     decimal.Decimal
	PositionSize decimal.Decimal
	FastMA       decimal.Decimal
	SlowMA       decimal.Decimal
	// EntryATR is the ATR at open; stable for the position's life so
	// stop/take thresholds don't drift. Only set on the opening record.
	EntryATR decimal.Decimal
}

// ClosedTrade records a completed round-trip and its PnL (price terms).
type ClosedTrade struct {
	BondID       string
	OpenTime     time.Time
	CloseTime    time.Time
	Signal       types.Signal // opening direction
	ExitSignal   types.Signal // strategy signal at exit
	EntryPrice   decimal.Decimal
	ExitPrice    decimal.Decimal
	EntryATR     decimal.Decimal
	Quantity     decimal.Decimal
	PositionSize decimal.Decimal
	PnL          decimal.Decimal
	ExitReason   string
}

// BacktestResult summarises a full backtest and implements types.BacktestResult.
type BacktestResult struct {
	ClosedTrades []ClosedTrade
	TradeRecords []TradeRecord
	TotalPnL     decimal.Decimal
	WinCount     int
	LossCount    int
	MaxDrawdown  decimal.Decimal
	SharpeRatio  decimal.Decimal
}

func (r BacktestResult) GetTotalPnL() decimal.Decimal    { return r.TotalPnL }
func (r BacktestResult) GetWinCount() int                { return r.WinCount }
func (r BacktestResult) GetLossCount() int               { return r.LossCount }
func (r BacktestResult) GetMaxDrawdown() decimal.Decimal { return r.MaxDrawdown }
func (r BacktestResult) GetSharpeRatio() decimal.Decimal { return r.SharpeRatio }
func (r BacktestResult) GetTradeRecords() any            { return r.TradeRecords }
func (r BacktestResult) GetClosedTrades() any            { return r.ClosedTrades }
```

- [ ] **Step 2: Build**

Run: `go build ./strategy/momentum/...`
Expected: builds with no errors (only type definitions so far).

- [ ] **Step 3: Commit**

```bash
git add strategy/momentum/types.go
git commit -m "feat(momentum): add config, decision, and result types"
```

---

## Task 2: Signal engine — `Strategy` struct, `New`, options, `Update`

**Files:**
- Create: `strategy/momentum/strategy.go` (signal engine + scaffolding only; run loop and execution come in Task 6)
- Create: `strategy/momentum/strategy_test.go`

- [ ] **Step 1: Write the failing test**

`strategy/momentum/strategy_test.go`:

```go
package momentum_test

import (
	"testing"

	"github.com/dora-network/bond-trading-strategies/strategy/momentum"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/require"
)

func newTestStrategy(t *testing.T, source string) *momentum.Strategy {
	t.Helper()
	cfg := momentum.DefaultConfig()
	cfg.SignalSource = source
	cfg.FastWindow = 3
	cfg.SlowWindow = 5
	cfg.ATRWindow = 3
	return momentum.New(cfg, nil)
}

func ytmP(d float64) *decimal.Decimal {
	x := decimal.MustNew(int64(d*1e6), 6)
	return &x
}

// risingPrice feeds an uptrend: price steps up each tick, YTM steady.
func risingPrice(n int) []types.YieldObservation {
	o := make([]types.YieldObservation, n)
	for i := range o {
		o[i] = types.YieldObservation{
			BondID: "b", YTM: ytmP(0.05), BenchmarkYield: decimal.MustNew(4, 2),
			Price: decimal.MustNew(int64(100+i), 0),
		}
	}
	return o
}

func TestUpdate_PriceSource_RisingTrend_IsBuy(t *testing.T) {
	s := newTestStrategy(t, momentum.SignalSourcePrice)
	var last momentum.Decision
	for _, o := range risingPrice(7) {
		d, err := s.Update(o)
		require.NoError(t, err)
		last = d
	}
	require.Equal(t, types.SignalBuy, last.Signal()) // price up -> long
	require.Equal(t, "up", last.Trend)
}

func TestUpdate_YTMSource_RisingTrend_IsSell(t *testing.T) {
	// YTM rising = price falling = downtrend -> Sell (direction inverted).
	s := newTestStrategy(t, momentum.SignalSourceYTM)
	obs := make([]types.YieldObservation, 7)
	for i := range obs {
		obs[i] = types.YieldObservation{
			BondID: "b", Price: decimal.MustNew(100, 0),
			YTM: ytmP(0.04 + float64(i)*0.001), BenchmarkYield: decimal.MustNew(4, 2),
		}
	}
	var last momentum.Decision
	for _, o := range obs {
		d, err := s.Update(o)
		require.NoError(t, err)
		last = d
	}
	require.Equal(t, types.SignalSell, last.Signal())
}

func TestUpdate_SpreadSource_RisingTrend_IsSell(t *testing.T) {
	// Spread rising (YTM up, benchmark steady) = cheapening = Sell.
	s := newTestStrategy(t, momentum.SignalSourceSpread)
	obs := make([]types.YieldObservation, 7)
	for i := range obs {
		obs[i] = types.YieldObservation{
			BondID: "b", Price: decimal.MustNew(100, 0),
			YTM: ytmP(0.05 + float64(i)*0.001), BenchmarkYield: decimal.MustNew(4, 2),
		}
	}
	var last momentum.Decision
	for _, o := range obs {
		d, err := s.Update(o)
		require.NoError(t, err)
		last = d
	}
	require.Equal(t, types.SignalSell, last.Signal())
}

func TestUpdate_WarmingUp_HoldsBeforeWindowsReady(t *testing.T) {
	s := newTestStrategy(t, momentum.SignalSourcePrice)
	d, err := s.Update(risingPrice(1)[0])
	require.NoError(t, err)
	require.Equal(t, types.SignalHold, d.Signal())
	require.Equal(t, momentum.DecisionReasonWarmingUp, d.Reason())
}

func TestUpdate_YTMSource_NilYTM_TickDropped(t *testing.T) {
	s := newTestStrategy(t, momentum.SignalSourceYTM)
	o := types.YieldObservation{BondID: "b", Price: decimal.MustNew(100, 0)} // nil YTM
	d, err := s.Update(o)
	require.NoError(t, err)
	require.Equal(t, types.SignalHold, d.Signal()) // tick dropped, no window update
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./strategy/momentum/...`
Expected: FAIL — `momentum.New` and `momentum.Strategy` undefined, `Update` undefined.

- [ ] **Step 3: Write `strategy.go` (signal engine + scaffolding)**

`strategy/momentum/strategy.go`:

```go
package momentum

import (
	"log/slog"
	"sync"

	"github.com/dora-network/bond-trading-strategies/prices"
	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/stats"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/bond-trading-strategies/strategy/window"
	"github.com/dora-network/bond-trading-strategies/fred"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
)

// Strategy holds per-bond state for the momentum / trend strategy.
type Strategy struct {
	mu  sync.RWMutex
	cfg Config
	log *slog.Logger

	fastWin *window.Rolling
	slowWin *window.Rolling
	atrWin  *window.Rolling

	// lastPrice is the previous clean price (for ATR abs-diff). Zero
	// until the second tick.
	lastPrice decimal.Decimal

	// sourceSign applies the bond-specific direction mapping: +1 for
	// price, -1 for ytm/spread (yield up = price down).
	sourceSign decimal.Decimal

	// Benchmark cache (spread mode only).
	benchmarkObservations []fred.Observation
	historyStore          historicalPriceStore
	benchmarkClient       benchmarkYieldClient

	// Live-run state (Task 6): runID, cancel, balances, openSignal,
	// decisionStore, etc. Declared here so the file compiles.
	runID               uuid.UUID
	cancel              context.CancelFunc
	isRunning           bool
	paused              bool
	pricesReqID         uuid.UUID
	pricesHandler       *prices.Handler
	marketAPIClient     strategy.MarketAPIClient
	balancesInitialized bool
	bondQty             decimal.Decimal
	usdBal              decimal.Decimal
	openSignal          types.Signal
	collateralWeight    decimal.Decimal
	decisionStore       strategy.DecisionRecorder
	decisionSeq         int64
	backtestWriter      stats.BacktestTradeWriter
	errs                []error
}

// New creates a Strategy with the given Config and optional options.
func New(cfg Config, pricesHandler *prices.Handler, opts ...func(*Strategy)) *Strategy {
	if cfg.Leverage.IsZero() {
		cfg.Leverage = decimal.One
	}
	s := &Strategy{
		cfg:           cfg,
		fastWin:       window.NewRollingWindow(cfg.FastWindow),
		slowWin:       window.NewRollingWindow(cfg.SlowWindow),
		atrWin:        window.NewRollingWindow(cfg.ATRWindow),
		pricesHandler: pricesHandler,
		sourceSign:    sourceSign(cfg.SignalSource),
		collateralWeight: decimal.One,
		errs:          make([]error, 0),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func sourceSign(source string) decimal.Decimal {
	if source == SignalSourcePrice {
		return decimal.One
	}
	return decimal.MustNew(-1, 0) // ytm, spread: inverted
}

func WithLogger(log *slog.Logger) func(*Strategy) {
	return func(s *Strategy) { s.log = log }
}

func WithMarketAPIClient(c strategy.MarketAPIClient) func(*Strategy) {
	return func(s *Strategy) { s.marketAPIClient = c }
}

func WithDecisionStore(store strategy.DecisionRecorder) func(*Strategy) {
	return func(s *Strategy) { s.decisionStore = store }
}

func WithBacktestWriter(w stats.BacktestTradeWriter) func(*Strategy) {
	return func(s *Strategy) { s.backtestWriter = w }
}

// seriesValue selects the configured series value from the observation.
// ok is false when the tick must be dropped entirely (nil YTM in
// ytm/spread modes). price mode never drops.
func (s *Strategy) seriesValue(o types.YieldObservation) (decimal.Decimal, bool, error) {
	switch s.cfg.SignalSource {
	case SignalSourcePrice:
		return o.Price, true, nil
	case SignalSourceYTM:
		if o.YTM.IsZero() {
			return decimal.Zero, false, nil
		}
		return o.YTM, true, nil
	default: // spread
		if o.YTM.IsZero() {
			return decimal.Zero, false, nil
		}
		spread, err := o.Spread()
		return spread, true, err
	}
}

// Update ingests one observation and returns the resulting Decision.
func (s *Strategy) Update(obs types.YieldObservation) (Decision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	prevPrice := s.lastPrice
	s.lastPrice = obs.Price

	value, ok, err := s.seriesValue(obs)
	if err != nil {
		return Decision{}, err
	}
	if !ok {
		// Drop the tick entirely (no window updates, no signal).
		return Decision{
			time: obs.Time, bondID: obs.BondID, price: obs.Price,
			signal: types.SignalHold, reason: DecisionReasonWarmingUp,
		}, nil
	}

	// ATR = mean absolute price diff.
	var absDiff decimal.Decimal
	if !prevPrice.IsZero() {
		d, err := obs.Price.Sub(prevPrice)
		if err != nil {
			return Decision{}, err
		}
		absDiff = d.Abs()
	}
	if err := s.atrWin.Add(absDiff); err != nil {
		return Decision{}, err
	}
	if err := s.fastWin.Add(value); err != nil {
		return Decision{}, err
	}
	if err := s.slowWin.Add(value); err != nil {
		return Decision{}, err
	}

	fastMA := s.fastWin.Mean()
	slowMA := s.slowWin.Mean()
	atr := s.atrWin.Mean()

	diff, err := fastMA.Sub(slowMA)
	if err != nil {
		return Decision{}, err
	}
	signed, err := diff.Mul(s.sourceSign)
	if err != nil {
		return Decision{}, err
	}

	d := Decision{
		time: obs.Time, bondID: obs.BondID, price: obs.Price,
		FastMA: fastMA, SlowMA: slowMA, ATR: atr, SeriesValue: value,
		signal: types.SignalHold, reason: DecisionReasonWarmingUp,
		positionSize: s.cfg.MaxPositionSize,
	}

	switch {
	case !s.fastWin.Ready() || !s.slowWin.Ready():
		d.reason = DecisionReasonWarmingUp
	case signed.Cmp(decimal.Zero) == 0:
		d.reason = DecisionReasonFlat
	case signed.Cmp(decimal.Zero) > 0:
		d.signal = types.SignalBuy
		d.Trend = "up"
		d.reason = DecisionReasonMACrossoverUp
	default:
		d.signal = types.SignalSell
		d.Trend = "down"
		d.reason = DecisionReasonMACrossoverDown
	}
	return d, nil
}

// logger returns a non-nil logger.
func (s *Strategy) logger() *slog.Logger {
	if s.log != nil {
		return s.log
	}
	return slog.Default()
}
```

> Note: the struct declares run-loop/balance/decision fields now so the file compiles; Go does not error on unused struct fields (only unused locals/imports). The `Run`, `executeDecision`, `closePosition`, balance, and recording methods that consume them are added in Task 6.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./strategy/momentum/...`
Expected: PASS — all signal/direction cases green.

- [ ] **Step 5: Commit**

```bash
git add strategy/momentum/strategy.go strategy/momentum/strategy_test.go
git commit -m "feat(momentum): add MA-crossover signal engine with source direction mapping"
```

---

## Task 3: Exit logic — `ShouldExit`

**Files:**
- Modify: `strategy/momentum/strategy.go` (append method)
- Modify: `strategy/momentum/strategy_test.go` (append tests)

`ShouldExit` is called identically by the live run loop (Task 6) and the backtester (Task 5). Priority `stop_loss > take_profit > reversal`. Stop/TP are price-based and entry-anchored; reversal fires when the current decision's signal opposes the open position.

- [ ] **Step 1: Write the failing tests** (append to `strategy_test.go`)

```go
func openLong(t *testing.T, s *momentum.Strategy, entryPrice, entryATR decimal.Decimal) {
	// No setup needed: ShouldExit is pure w.r.t. its arguments.
	_ = t
	_ = s
	_ = entryPrice
	_ = entryATR
}

func TestShouldExit_StopLoss_Long(t *testing.T) {
	cfg := momentum.DefaultConfig()
	cfg.FastWindow = 2
	cfg.SlowWindow = 3
	cfg.StopLossATR = decimal.MustNew(2, 0) // 2 ATR
	s := momentum.New(cfg, nil)
	entryPrice := decimal.MustNew(100, 0)
	entryATR := decimal.MustNew(5, 0) // stop distance = 10
	// Price 89 (< 100-10=90) -> stop loss.
	d := momentum.NewExitDecision(types.SignalBuy, decimal.MustNew(89, 0))
	exit, reason := s.ShouldExit(types.SignalBuy, d, entryPrice, entryATR)
	require.True(t, exit)
	require.Equal(t, momentum.ExitReasonStopLoss, reason)
}

func TestShouldExit_TakeProfit_Long(t *testing.T) {
	cfg := momentum.DefaultConfig()
	cfg.StopLossATR = decimal.Zero
	cfg.TakeProfitATR = decimal.MustNew(2, 0)
	s := momentum.New(cfg, nil)
	entryPrice := decimal.MustNew(100, 0)
	entryATR := decimal.MustNew(5, 0) // tp distance = 10
	d := momentum.NewExitDecision(types.SignalBuy, decimal.MustNew(111, 0))
	exit, reason := s.ShouldExit(types.SignalBuy, d, entryPrice, entryATR)
	require.True(t, exit)
	require.Equal(t, momentum.ExitReasonTakeProfit, reason)
}

func TestShouldExit_Reversal_Long(t *testing.T) {
	cfg := momentum.DefaultConfig()
	cfg.StopLossATR = decimal.Zero
	cfg.TakeProfitATR = decimal.Zero
	s := momentum.New(cfg, nil)
	entryPrice := decimal.MustNew(100, 0)
	entryATR := decimal.MustNew(1, 0)
	// Opened Buy; decision now Sell -> reversal.
	d := momentum.NewExitDecision(types.SignalSell, decimal.MustNew(100, 0))
	exit, reason := s.ShouldExit(types.SignalBuy, d, entryPrice, entryATR)
	require.True(t, exit)
	require.Equal(t, momentum.ExitReasonReversal, reason)
}

func TestShouldExit_Hold_NoExit(t *testing.T) {
	cfg := momentum.DefaultConfig()
	cfg.StopLossATR = decimal.MustNew(2, 0)
	s := momentum.New(cfg, nil)
	d := momentum.NewExitDecision(types.SignalBuy, decimal.MustNew(100, 0))
	exit, _ := s.ShouldExit(types.SignalBuy, d, decimal.MustNew(100, 0), decimal.MustNew(1, 0))
	require.False(t, exit)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./strategy/momentum/...`
Expected: FAIL — `ShouldExit` and `NewExitDecision` undefined.

- [ ] **Step 3: Implement `ShouldExit` + test helper** (append to `strategy.go`)

```go
// ShouldExit reports whether an open position should close, and why.
// Priority: stop_loss > take_profit > reversal. Stop/TP are price-based
// and entry-anchored (stable for the position's life). Reversal fires
// when the current decision's signal opposes the open position.
func (s *Strategy) ShouldExit(openSignal types.Signal, d Decision, entryPrice, entryATR decimal.Decimal) (bool, string) {
	price := d.Price()
	cfg := s.cfg

	if cfg.StopLossATR.IsPos() && entryATR.IsPos() {
		stopDist, err := cfg.StopLossATR.Mul(entryATR)
		if err == nil {
			switch openSignal {
			case types.SignalBuy:
				threshold, err := entryPrice.Sub(stopDist)
				if err == nil && price.Cmp(threshold) <= 0 {
					return true, ExitReasonStopLoss
				}
			case types.SignalSell:
				threshold, err := entryPrice.Add(stopDist)
				if err == nil && price.Cmp(threshold) >= 0 {
					return true, ExitReasonStopLoss
				}
			}
		}
	}

	if cfg.TakeProfitATR.IsPos() && entryATR.IsPos() {
		tpDist, err := cfg.TakeProfitATR.Mul(entryATR)
		if err == nil {
			switch openSignal {
			case types.SignalBuy:
				threshold, err := entryPrice.Add(tpDist)
				if err == nil && price.Cmp(threshold) >= 0 {
					return true, ExitReasonTakeProfit
				}
			case types.SignalSell:
				threshold, err := entryPrice.Sub(tpDist)
				if err == nil && price.Cmp(threshold) <= 0 {
					return true, ExitReasonTakeProfit
				}
			}
		}
	}

	// Reversal: current signal opposes the open position.
	if openSignal == types.SignalBuy && d.Signal() == types.SignalSell {
		return true, ExitReasonReversal
	}
	if openSignal == types.SignalSell && d.Signal() == types.SignalBuy {
		return true, ExitReasonReversal
	}
	return false, ""
}
```

Add a test-only constructor in `strategy.go` (exported via the package for the white-box tests — alternatively put it in `export_test.go`; here we expose it from the package so the external test package can use it):

```go
// NewExitDecision builds a Decision carrying only the fields ShouldExit
// inspects (signal + price). For unit tests of ShouldExit.
func NewExitDecision(signal types.Signal, price decimal.Decimal) Decision {
	return Decision{signal: signal, price: price}
}
```

> Prefer placing `NewExitDecision` in `export_test.go` (package `momentum`) so it is test-only. If so, move it there and drop it from `strategy.go`. Either compiles; pick `export_test.go` for cleanliness.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./strategy/momentum/...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add strategy/momentum/strategy.go strategy/momentum/strategy_test.go
git commit -m "feat(momentum): add entry-anchored ATR exit logic"
```

---

## Task 4: Historical data + self-wired stores

**Files:**
- Create: `strategy/momentum/historical_data.go`

Most of this file is **identical to `strategy/meanreversion/historical_data.go`**. Copy it, then replace `getObservations` and `prefillWindow` with the source-aware versions below. Everything else (the two interfaces, `benchmarkTenors`, `BenchmarkTenor`, `SupportedBenchmarkTenors`, `parseBenchmarkTenor`, `normalizeTenor`, `normalizeDate`, `cachedBenchmarkYield`, `setBenchmarkObservations`, `mergeBenchmarkObservations`, `getHistoricalPriceStore`, `getBenchmarkYieldClient`) is copied verbatim — it already compiles and is tested.

- [ ] **Step 1: Copy the file, then apply these edits**

```bash
cp strategy/meanreversion/historical_data.go strategy/momentum/historical_data.go
```

Change the `package` line to `package momentum`. Keep the counterfeiter directives (the fakes will generate into `momentumfakes/` after `go generate`).

Then **replace** `getObservations` with this source-aware version:

```go
func (s *Strategy) getObservations(ctx context.Context, start, end time.Time) ([]types.YieldObservation, error) {
	assetID, err := s.lookupAssetID(s.cfg.OrderBookID)
	if err != nil {
		return nil, fmt.Errorf("lookup asset ID: %w", err)
	}

	historyStore, err := s.getHistoricalPriceStore(ctx)
	if err != nil {
		return nil, err
	}

	history, err := historyStore.LoadHistoricalPrices(ctx, assetID, start, end)
	if err != nil {
		return nil, fmt.Errorf("load historical prices: %w", err)
	}

	// Only spread mode needs the FRED benchmark; price/ytm do not.
	if s.cfg.SignalSource == SignalSourceSpread {
		tenor, err := parseBenchmarkTenor(s.cfg.Tenor)
		if err != nil {
			return nil, fmt.Errorf("parse tenor: %w", err)
		}
		benchmarkClient, err := s.getBenchmarkYieldClient()
		if err != nil {
			return nil, err
		}
		if len(history) > 0 {
			yields, err := benchmarkClient.FetchHistoricalYields(ctx, tenor, history[0].Time, history[len(history)-1].Time)
			if err != nil {
				return nil, fmt.Errorf("fetch benchmark yields: %w", err)
			}
			s.setBenchmarkObservations(yields)
		}
	}

	observations := make([]types.YieldObservation, 0, len(history))
	for _, price := range history {
		obs := types.YieldObservation{
			Time:   price.Time,
			BondID: price.AssetID,
			Price:  price.Price,
		}
		if price.YTM != nil {
			obs.YTM = *price.YTM
		}
		if s.cfg.SignalSource == SignalSourceSpread {
			if benchmarkYield, _, ok := s.cachedBenchmarkYield(price.Time); ok {
				obs.BenchmarkYield = benchmarkYield
			}
		}
		observations = append(observations, obs)
	}
	return observations, nil
}
```

And **replace** `prefillWindow` with this source-aware version (uses `SlowWindow` and tolerates nil YTM in price mode):

```go
func (s *Strategy) prefillWindow(ctx context.Context, assetID string) error {
	historyStore, err := s.getHistoricalPriceStore(ctx)
	if err != nil {
		return fmt.Errorf("get history store: %w", err)
	}

	limit := s.cfg.SlowWindow * 2 //nolint:mnd // 2x slow window fills all three windows
	history, err := historyStore.LoadLastPrices(ctx, assetID, limit)
	if err != nil {
		return fmt.Errorf("load last prices: %w", err)
	}

	if s.cfg.SignalSource == SignalSourceSpread && len(history) > 0 {
		tenor, err := parseBenchmarkTenor(s.cfg.Tenor)
		if err != nil {
			return fmt.Errorf("parse tenor: %w", err)
		}
		benchmarkClient, err := s.getBenchmarkYieldClient()
		if err != nil {
			return fmt.Errorf("get benchmark client: %w", err)
		}
		yields, err := benchmarkClient.FetchHistoricalYields(ctx, tenor, history[0].Time, history[len(history)-1].Time)
		if err != nil {
			return fmt.Errorf("fetch benchmark yields: %w", err)
		}
		s.setBenchmarkObservations(yields)
	}

	for _, price := range history {
		obs := types.YieldObservation{
			Time:   price.Time,
			BondID: price.AssetID,
			Price:  price.Price,
		}
		if price.YTM != nil {
			obs.YTM = *price.YTM
		}
		if s.cfg.SignalSource == SignalSourceSpread {
			if benchmarkYield, _, ok := s.cachedBenchmarkYield(price.Time); ok {
				obs.BenchmarkYield = benchmarkYield
			}
		}
		// Update drops nil-YTM ticks automatically in ytm/spread modes;
		// price mode ingests every tick.
		if _, err := s.Update(obs); err != nil {
			return fmt.Errorf("fill window: %w", err)
		}
	}
	return nil
}
```

Also copy `lookupAssetID` from meanreversion (it delegates to `strategy.LookupAssetID`):

```go
func (s *Strategy) lookupAssetID(orderBookID uuid.UUID) (string, error) {
	return strategy.LookupAssetID(context.Background(), s.marketAPIClient, orderBookID)
}
```

- [ ] **Step 2: Build**

Run: `go build ./strategy/momentum/...`
Expected: builds clean. (`go vet` may flag the not-yet-used `lookupAssetID` path only at run time; build is fine.)

- [ ] **Step 3: Commit**

```bash
git add strategy/momentum/historical_data.go
git commit -m "feat(momentum): add historical data loading with source-aware benchmark"
```

---

## Task 5: Backtester

**Files:**
- Create: `strategy/momentum/backtest.go`
- Create: `strategy/momentum/backtest_test.go`

Structure mirrors `mean_reversion` (effective capital, remaining balance, one open position, optional writer, `stats.Summarise`); exit semantics mirror `breakout` (priority `stop_loss > take_profit > reversal`, entry-anchored). Because the momentum `ShouldExit` already encodes the full exit model, the loop is simpler than breakout's.

- [ ] **Step 1: Write `backtest.go`**

```go
package momentum

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy/stats"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
)

// Backtester replays observations through a Strategy and records trades.
// One open position at a time; no transaction costs / spread / financing
// (matches the existing backtesters' deliberate simplicity).
type Backtester struct {
	strategy *Strategy
	writer   stats.BacktestTradeWriter
}

func NewBacktester(s *Strategy, writer stats.BacktestTradeWriter) *Backtester {
	return &Backtester{strategy: s, writer: writer}
}

func (b *Backtester) Run(ctx context.Context, obs []types.YieldObservation) (BacktestResult, error) {
	var (
		closedTrades []ClosedTrade
		tradeRecords []TradeRecord
		openTrade    *TradeRecord
	)

	effectiveCapital, err := b.strategy.cfg.InitialBalance.Mul(b.strategy.collateralWeight)
	if err != nil {
		return BacktestResult{}, err
	}
	effectiveCapital, err = effectiveCapital.Mul(b.strategy.cfg.Leverage)
	if err != nil {
		return BacktestResult{}, err
	}
	remainingBalance := effectiveCapital

	for _, o := range obs {
		select {
		case <-ctx.Done():
			return BacktestResult{}, errors.New("backtest cancelled by user")
		default:
		}
		decision, err := b.strategy.Update(o)
		if err != nil {
			return BacktestResult{}, err
		}

		if openTrade != nil {
			exit, reason := b.strategy.ShouldExit(openTrade.Signal, decision, openTrade.EntryPrice, openTrade.EntryATR)
			if exit {
				ct, newBalance, err := b.closeAtPrice(openTrade, decision, remainingBalance, reason)
				if err != nil {
					return BacktestResult{}, err
				}
				remainingBalance = newBalance
				closedTrades = append(closedTrades, ct)
				openTrade = nil
				continue
			}
			continue
		}

		// Flat: open on a fresh signal.
		if decision.Signal() == types.SignalHold {
			continue
		}
		price := decision.Price()
		if price.IsZero() {
			continue
		}
		quantity, ok, err := b.strategy.cappedOrderQuantity(decision.PositionSize(), decimal.Zero, price)
		if err != nil {
			return BacktestResult{}, err
		}
		if !ok || quantity.IsZero() {
			continue
		}
		entryBudget, err := price.Mul(quantity)
		if err != nil {
			return BacktestResult{}, err
		}
		remainingBalance, err = remainingBalance.Sub(entryBudget)
		if err != nil {
			return BacktestResult{}, err
		}
		rec := TradeRecord{
			Time: decision.Time(), BondID: decision.BondID(), Signal: decision.Signal(),
			Price: price, Quantity: quantity, PositionSize: decision.PositionSize(),
			FastMA: decision.FastMA, SlowMA: decision.SlowMA, EntryATR: decision.ATR,
		}
		tradeRecords = append(tradeRecords, rec)
		openTrade = &rec
	}

	// Force-close any position still open at end of history.
	if openTrade != nil {
		last := obs[len(obs)-1]
		d := Decision{signal: openTrade.Signal(), price: last.Price}
		ct, newBalance, err := b.closeAtPrice(openTrade, d, remainingBalance, ExitReasonStrategyExit)
		if err != nil {
			return BacktestResult{}, err
		}
		_ = newBalance
		closedTrades = append(closedTrades, ct)
	}

	if b.writer != nil {
		streamTrades(ctx, b.writer, tradeRecords, closedTrades)
		if err := b.writer.Flush(ctx); err != nil {
			slog.Error("flush backtest writer", "err", err)
		}
	}

	if len(obs) == 0 {
		return BacktestResult{}, nil
	}
	start, end := obs[0].Time, obs[len(obs)-1].Time
	summary, err := computeSummary(closedTrades, start, end)
	if err != nil {
		return BacktestResult{}, err
	}
	return BacktestResult{
		ClosedTrades: closedTrades, TradeRecords: tradeRecords,
		TotalPnL: summary.TotalPnL, WinCount: summary.WinCount, LossCount: summary.LossCount,
		MaxDrawdown: summary.MaxDrawdown, SharpeRatio: summary.SharpeRatio,
	}, nil
}

// closeAtPrice realises a closed trade at decision.Price() and updates
// the remaining balance.
func (b *Backtester) closeAtPrice(open *TradeRecord, d Decision, balance decimal.Decimal, reason string) (ClosedTrade, decimal.Decimal, error) {
	exitPrice := d.Price()
	cashFlow, err := exitPrice.Mul(open.Quantity)
	if err != nil {
		return ClosedTrade{}, balance, err
	}
	switch open.Signal {
	case types.SignalBuy:
		balance, err = balance.Add(cashFlow) // receive proceeds
	case types.SignalSell:
		balance, err = balance.Sub(cashFlow) // pay to buy back
	}
	if err != nil {
		return ClosedTrade{}, balance, err
	}
	ct := ClosedTrade{
		BondID: open.BondID, OpenTime: open.Time, CloseTime: d.Time(),
		Signal: open.Signal, ExitSignal: d.Signal(),
		EntryPrice: open.Price, ExitPrice: exitPrice, EntryATR: open.EntryATR,
		Quantity: open.Quantity, PositionSize: open.PositionSize, ExitReason: reason,
	}
	pnl, err := computePnL(ct)
	if err != nil {
		return ClosedTrade{}, balance, err
	}
	ct.PnL = pnl
	return ct, balance, nil
}

func computePnL(ct ClosedTrade) (decimal.Decimal, error) {
	switch ct.Signal {
	case types.SignalBuy:
		return ct.ExitPrice.Sub(ct.EntryPrice) // scaled by qty below
	default:
		return ct.EntryPrice.Sub(ct.ExitPrice)
	}
}

// Note: computePnL must be price-diff × quantity. Corrected:
```

Replace the trailing `computePnL`/`computeSummary` stubs above with these final versions (the snippet above intentionally shows the per-unit version to flag the fix):

```go
func computePnL(ct ClosedTrade) (decimal.Decimal, error) {
	var perUnit decimal.Decimal
	var err error
	if ct.Signal == types.SignalBuy {
		perUnit, err = ct.ExitPrice.Sub(ct.EntryPrice)
	} else {
		perUnit, err = ct.EntryPrice.Sub(ct.ExitPrice)
	}
	if err != nil {
		return decimal.Zero, err
	}
	return perUnit.Mul(ct.Quantity)
}

func computeSummary(closed []ClosedTrade, start, end time.Time) (stats.Summary, error) {
	points := make([]stats.PnLPoint, len(closed))
	for i, c := range closed {
		points[i] = stats.PnLPoint{PnL: c.PnL, CloseTime: c.CloseTime}
	}
	return stats.Summarise(points, start, end)
}

// streamTrades mirrors the writer helpers in meanreversion/breakout.
// Copy streamTrades / streamClosedTrades (or a combined streamTrades)
// verbatim from strategy/meanreversion/backtest.go, adapting the
// TradeRecord/ClosedTrade field names to momentum's. The writer calls
// are WriteTradeRecord / WriteClosedTrade / Flush on stats.BacktestTradeWriter.
func streamTrades(ctx context.Context, w stats.BacktestTradeWriter, records []TradeRecord, closed []ClosedTrade) {
	_ = uuid.Nil // placeholder import use; remove if uuid unused
	for _, r := range records {
		_ = r
	}
	for _, c := range closed {
		_ = c
	}
	_ = ctx
	_ = w
}
```

> **Action:** open `strategy/meanreversion/backtest.go`, find its `streamTrades` (or equivalent writer-loop) helper, and reproduce it here against momentum's `TradeRecord`/`ClosedTrade`. The real helper calls `w.WriteTradeRecord(ctx, stats.TradeRecordInsert{...})` and `w.WriteClosedTrade(ctx, stats.ClosedTradeInsert{...})`; map the momentum fields onto those insert structs the same way meanreversion does. Remove the placeholder `streamTrades` above once the real one is in.

- [ ] **Step 2: Write the backtest test** (`backtest_test.go`)

```go
package momentum_test

import (
	"context"
	"testing"

	"github.com/dora-network/bond-trading-strategies/strategy/momentum"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/require"
)

// uptrendThenReversal: prices rise (fast>slow -> Buy), then fall so the
// MAs cross back (-> reversal exit).
func uptrendThenReversal() []types.YieldObservation {
	o := make([]types.YieldObservation, 0, 14)
	for i := 0; i < 7; i++ { //nolint:mnd
		o = append(o, types.YieldObservation{BondID: "b", Price: decimal.MustNew(int64(100+i), 0)})
	}
	for i := 0; i < 7; i++ { //nolint:mnd
		o = append(o, types.YieldObservation{BondID: "b", Price: decimal.MustNew(int64(106-i), 0)})
	}
	return o
}

func TestBacktest_OpensAndExits(t *testing.T) {
	cfg := momentum.DefaultConfig()
	cfg.SignalSource = momentum.SignalSourcePrice
	cfg.FastWindow = 3
	cfg.SlowWindow = 5
	cfg.StopLossATR = decimal.Zero
	cfg.TakeProfitATR = decimal.Zero
	cfg.InitialBalance = decimal.MustNew(1000, 0)
	s := momentum.New(cfg, nil)
	bt := momentum.NewBacktester(s, nil)

	res, err := bt.Run(context.Background(), uptrendThenReversal())
	require.NoError(t, err)
	require.NotEmpty(t, res.GetClosedTrades(), "expected at least one closed trade")
}
```

- [ ] **Step 3: Run tests; iterate until green**

Run: `go test ./strategy/momentum/...`
Expected: PASS. If `streamTrades` field-mapping fails to compile, align it to `stats.TradeRecordInsert`/`stats.ClosedTradeInsert` exactly as meanreversion does.

- [ ] **Step 4: Commit**

```bash
git add strategy/momentum/backtest.go strategy/momentum/backtest_test.go
git commit -m "feat(momentum): add backtester with stop/tp/reversal exits"
```

---

## Task 6: Live run loop, balances, execution

**Files:**
- Modify: `strategy/momentum/strategy.go` (append run/execution/balance/recording methods)
- Create: `strategy/momentum/balances.go`
- Create: `strategy/momentum/export_test.go`
- Modify: `strategy/momentum/strategy_test.go` (append market_api / run tests)

These methods are **near-verbatim copies of `strategy/meanreversion`**, with these specific deltas:

1. **Entry tracking.** Momentum's exit needs the open position's entry price + ATR. Add two fields to the `Strategy` struct (Task 1's struct) and set them on open:
   ```go
   entryPrice decimal.Decimal // price at which the current position opened
   entryATR   decimal.Decimal // ATR captured at open
   ```
2. **Spread-only benchmark.** In the per-tick handler, only call `getBenchmarkYield` when `cfg.SignalSource == SignalSourceSpread`. For price/ytm, build the observation directly from the tick (skip nil-YTM ticks in ytm/spread modes).
3. **Exit call.** Replace meanreversion's `ShouldExit(openSignal, zScore)` call with `ShouldExit(openSignal, decision, s.entryPrice, s.entryATR)`.
4. **Min/max sizing.** `cappedOrderQuantity` applies `MinOrderSize`/`MaxOrderSize` (see below).

- [ ] **Step 1: Copy balances + execution + run loop from meanreversion**

Copy these methods verbatim from `strategy/meanreversion/strategy.go` and `balances.go` into `strategy/momentum/strategy.go` / `balances.go`: `Run`, `subscribePrices`, `unsubscribePrices`, `run`, `initializeBalances`, `currentPosition`, `executeDecision`, `closePosition`, `recordDecision`, `mustParseUUID`. Copy `strategy/meanreversion/balances.go` wholesale (change package to `momentum`).

In `executeDecision`, after a successful open, record the entry state:
```go
// inside executeDecision, after the order succeeds and before return:
s.mu.Lock()
s.entryPrice = decision.Price()
s.entryATR = decision.ATR
s.openSignal = decision.Signal()
s.mu.Unlock()
```
In `closePosition`, clear it:
```go
s.mu.Lock()
s.openSignal = types.SignalHold
s.entryPrice = decimal.Zero
s.entryATR = decimal.Zero
s.mu.Unlock()
```

- [ ] **Step 2: Replace `cappedOrderQuantity` with the min/max version**

```go
// cappedOrderQuantity computes the order quantity for a given position
// fraction and applies MinOrderSize / MaxOrderSize. Returns ok=false when
// the computed quantity is below MinOrderSize (skip opening).
func (s *Strategy) cappedOrderQuantity(positionSize, currentPosition, price decimal.Decimal) (decimal.Decimal, bool, error) {
	if price.IsZero() {
		return decimal.Zero, false, errors.New("price is zero")
	}
	effectiveCapital, err := s.cfg.InitialBalance.Mul(s.collateralWeight)
	if err != nil {
		return decimal.Zero, false, err
	}
	effectiveCapital, err = effectiveCapital.Mul(s.cfg.Leverage)
	if err != nil {
		return decimal.Zero, false, err
	}
	budget, err := effectiveCapital.Mul(positionSize)
	if err != nil {
		return decimal.Zero, false, err
	}
	remaining, err := budget.Sub(currentPosition.Abs().Mul(price)) //nolint:errcheck
	_ = remaining
	quantity, err := budget.Quo(price)
	if err != nil {
		return decimal.Zero, false, err
	}
	if s.cfg.MaxOrderSize.IsPos() && quantity.Cmp(s.cfg.MaxOrderSize) > 0 {
		quantity = s.cfg.MaxOrderSize
	}
	if s.cfg.MinOrderSize.IsPos() && quantity.Cmp(s.cfg.MinOrderSize) < 0 {
		return decimal.Zero, false, nil // skip: below minimum
	}
	if quantity.IsZero() || quantity.IsNeg() {
		return decimal.Zero, false, nil
	}
	return quantity, true, nil
}
```

> The exact `effectiveCapital`/remaining-balance arithmetic in meanreversion's `cappedOrderQuantity` is the source of truth — copy that body and only insert the two `MinOrderSize`/`MaxOrderSize` guards shown. Do not reinvent the budget math.

- [ ] **Step 3: Per-tick handler delta**

In the `run` loop's price-tick branch, build the observation with spread-gated benchmark:
```go
var obs types.YieldObservation
if s.cfg.SignalSource == SignalSourceSpread {
	benchmarkYield := s.getBenchmarkYield(ctx, px.Time)
	obs = types.YieldObservation{Time: px.Time, BondID: px.AssetID, Price: px.Price, BenchmarkYield: benchmarkYield}
} else {
	obs = types.YieldObservation{Time: px.Time, BondID: px.AssetID, Price: px.Price}
}
if px.YTM != nil {
	obs.YTM = *px.YTM
}
// Update drops nil-YTM ticks automatically in ytm/spread modes.
```
Keep the rest of meanreversion's per-tick flow (decision recording, open/exit/reversal handling) — swapping the `ShouldExit` call per delta #3.

- [ ] **Step 4: Write `export_test.go`**

Copy `strategy/meanreversion/export_test.go` to `strategy/momentum/export_test.go`, change the package to `momentum`, and keep the helpers that apply: `SetLookupClient`, `SetHistoricalPriceStore`, `SetBenchmarkYieldClient`, `LookupAssetID`, `GetObservations`, `GetBenchmarkYield`, `CurrentPosition`, `CappedOrderQuantity`, `BondQty`, `UsdBal`, `InitializeBalances`, `BalancesInitialized`, `OpenSignal`, `RunWithPrices`. Drop meanreversion-specific ones (none — the set is generic).

- [ ] **Step 5: Add a `cappedOrderQuantity` unit test** (append to `strategy_test.go`)

```go
func TestCappedOrderQuantity_MinSizeSkips_MaxSizeClamps(t *testing.T) {
	cfg := momentum.DefaultConfig()
	cfg.InitialBalance = decimal.MustNew(1000, 0)
	cfg.MinOrderSize = decimal.MustNew(5, 0)  // need >= 5 units
	cfg.MaxOrderSize = decimal.MustNew(3, 0)  // but cap at 3
	s := momentum.New(cfg, nil)
	// budget/price = 1000/100 = 10 -> clamped to MaxOrderSize 3 (>= MinSize 5? no, 3<5 -> skip)
	qty, ok, err := momentum.CappedOrderQuantity(s, decimal.One, decimal.Zero, decimal.MustNew(100, 0))
	require.NoError(t, err)
	require.False(t, ok, "3 < min 5 -> skip")

	cfg.MaxOrderSize = decimal.Zero
	s = momentum.New(cfg, nil)
	qty, ok, err = momentum.CappedOrderQuantity(s, decimal.One, decimal.Zero, decimal.MustNew(100, 0))
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, qty.Equal(decimal.MustNew(10, 0)))
}
```

- [ ] **Step 6: Build + test + commit**

```bash
go generate ./strategy/momentum/...
go test -race ./strategy/momentum/...
git add strategy/momentum/
git commit -m "feat(momentum): add live run loop, balances, and min/max order sizing"
```

---

## Task 7: HTTP wiring

**Files:**
- Modify: `strategy/http/handler.go`

- [ ] **Step 1: Add `newMomentumDefinition`**

Add an import for `momentum "github.com/dora-network/bond-trading-strategies/strategy/momentum"` (place it alphabetically). Then append (mirroring `newMeanReversionDefinition`):

```go
func newMomentumDefinition(pricesHandler *prices.Handler, log *slog.Logger) StrategyDefinition {
	defaults := momentum.DefaultConfig()
	return StrategyDefinition{
		Type:        momentum.StrategyType,
		Status:      strategyStatusAvailable,
		Description: "Moving-average-crossover trend strategy with configurable signal source (price/ytm/spread) and ATR-based exits.",
		SupportsRun: true, SupportsBacktest: true,
		ConfigFields: []StrategyConfigField{
			{Name: "signal_source", Type: "string", Description: "Series for the MA crossover: price, ytm, or spread. spread requires tenor.", Required: false, Default: defaults.SignalSource},
			{Name: "fast_window", Type: "integer", Description: "Fast-MA tick window. Must be at least 2.", Required: false, Default: defaults.FastWindow},
			{Name: "slow_window", Type: "integer", Description: "Slow-MA tick window. Must be greater than fast_window.", Required: false, Default: defaults.SlowWindow},
			{Name: "atr_window", Type: "integer", Description: "Mean-absolute-price-diff window for exits. Must be at least 2.", Required: false, Default: defaults.ATRWindow},
			{Name: "stop_loss_atr", Type: "number", Description: "Stop-loss distance in ATR units. 0 disables.", Required: false, Default: mustFloat64(defaults.StopLossATR)},
			{Name: "take_profit_atr", Type: "number", Description: "Take-profit distance in ATR units. 0 disables.", Required: false, Default: mustFloat64(defaults.TakeProfitATR)},
			{Name: "min_order_size", Type: "number", Description: "Skip opening when quantity is below this. 0 disables.", Required: false, Default: mustFloat64(defaults.MinOrderSize)},
			{Name: "max_order_size", Type: "number", Description: "Clamp order quantity down to this. 0 disables.", Required: false, Default: mustFloat64(defaults.MaxOrderSize)},
			{Name: "max_position_size", Type: "number", Description: "Capital fraction cap, in (0,1].", Required: false, Default: mustFloat64(defaults.MaxPositionSize)},
			{Name: "order_book_id", Type: "string(uuid)", Description: "Order book UUID.", Required: false},
			{Name: "tenor", Type: "string", Description: "Benchmark Treasury tenor (e.g. 2Y, 5Y, 10Y). Required when signal_source is spread.", Required: false},
			{Name: "initial_balance", Type: "number", Description: "Starting capital. Must be greater than 0 for backtests.", Required: false, Default: mustFloat64(defaults.InitialBalance)},
			{Name: "leverage", Type: "number", Description: "Leverage multiplier. Must be greater than 0.", Required: false, Default: mustFloat64(defaults.Leverage)},
		},
		DecodeConfig: func(raw json.RawMessage, capability string, tradeWriter stats.BacktestTradeWriter) (json.RawMessage, strategycore.Strategy, error) {
			forRun := capability == string(capabilityRun)
			cfg, normalised, err := decodeMomentumConfig(raw, forRun)
			if err != nil {
				return nil, nil, err
			}
			opts := []func(*momentum.Strategy){momentum.WithLogger(log)}
			if tradeWriter != nil {
				opts = append(opts, momentum.WithBacktestWriter(tradeWriter))
			}
			return normalised, momentum.New(cfg, pricesHandler, opts...), nil
		},
	}
}
```

- [ ] **Step 2: Add `decodeMomentumConfig` + payload** (mirror `decodeMeanReversionConfig`)

```go
type momentumConfigPayload struct {
	SignalSource    string   `json:"signal_source"`
	FastWindow      int      `json:"fast_window"`
	SlowWindow      int      `json:"slow_window"`
	ATRWindow       int      `json:"atr_window"`
	StopLossATR     float64  `json:"stop_loss_atr"`
	TakeProfitATR   float64  `json:"take_profit_atr"`
	MinOrderSize    float64  `json:"min_order_size"`
	MaxOrderSize    float64  `json:"max_order_size"`
	MaxPositionSize float64  `json:"max_position_size"`
	OrderBookID     string   `json:"order_book_id"`
	Tenor           string   `json:"tenor"`
	InitialBalance  *float64 `json:"initial_balance,omitempty"`
	Leverage        float64  `json:"leverage"`
}

func decodeMomentumConfig(raw json.RawMessage, forRun bool) (momentum.Config, json.RawMessage, error) {
	var payload momentumConfigPayload
	if err := decodeRawConfig(raw, &payload); err != nil {
		return momentum.Config{}, nil, err
	}
	defaults := momentum.DefaultConfig()

	if payload.SignalSource == "" {
		payload.SignalSource = defaults.SignalSource
	}
	switch payload.SignalSource {
	case momentum.SignalSourcePrice, momentum.SignalSourceYTM, momentum.SignalSourceSpread:
	default:
		return momentum.Config{}, nil, fmt.Errorf("config.signal_source must be one of price, ytm, spread")
	}
	if payload.SignalSource == momentum.SignalSourceSpread && strings.TrimSpace(payload.Tenor) == "" {
		return momentum.Config{}, nil, fmt.Errorf("config.tenor is required when signal_source is spread")
	}
	if payload.FastWindow == 0 {
		payload.FastWindow = defaults.FastWindow
	}
	if payload.SlowWindow == 0 {
		payload.SlowWindow = defaults.SlowWindow
	}
	if payload.ATRWindow == 0 {
		payload.ATRWindow = defaults.ATRWindow
	}
	if payload.FastWindow < 2 { //nolint:mnd
		return momentum.Config{}, nil, fmt.Errorf("config.fast_window must be at least 2")
	}
	if payload.SlowWindow <= payload.FastWindow {
		return momentum.Config{}, nil, fmt.Errorf("config.slow_window must be greater than fast_window")
	}
	if payload.ATRWindow < 2 { //nolint:mnd
		return momentum.Config{}, nil, fmt.Errorf("config.atr_window must be at least 2")
	}
	if payload.MaxPositionSize == 0 {
		payload.MaxPositionSize = mustFloat64(defaults.MaxPositionSize)
	}
	if payload.MaxPositionSize <= 0 || payload.MaxPositionSize > 1 {
		return momentum.Config{}, nil, fmt.Errorf("config.max_position_size must be in (0,1]")
	}
	if payload.Leverage == 0 {
		payload.Leverage = mustFloat64(defaults.Leverage)
	}
	if payload.Leverage <= 0 {
		return momentum.Config{}, nil, fmt.Errorf("config.leverage must be greater than 0")
	}
	if payload.MinOrderSize < 0 {
		return momentum.Config{}, nil, fmt.Errorf("config.min_order_size must be non-negative")
	}
	if payload.MaxOrderSize < 0 {
		return momentum.Config{}, nil, fmt.Errorf("config.max_order_size must be non-negative")
	}
	if payload.MaxOrderSize > 0 && payload.MinOrderSize > payload.MaxOrderSize {
		return momentum.Config{}, nil, fmt.Errorf("config.max_order_size must be >= min_order_size")
	}

	stopLoss, err := decimal.NewFromFloat64(payload.StopLossATR)
	if err != nil {
		return momentum.Config{}, nil, fmt.Errorf("config.stop_loss_atr: %w", err)
	}
	takeProfit, err := decimal.NewFromFloat64(payload.TakeProfitATR)
	if err != nil {
		return momentum.Config{}, nil, fmt.Errorf("config.take_profit_atr: %w", err)
	}
	minOrder, err := decimal.NewFromFloat64(payload.MinOrderSize)
	if err != nil {
		return momentum.Config{}, nil, fmt.Errorf("config.min_order_size: %w", err)
	}
	maxOrder, err := decimal.NewFromFloat64(payload.MaxOrderSize)
	if err != nil {
		return momentum.Config{}, nil, fmt.Errorf("config.max_order_size: %w", err)
	}
	maxPos, err := decimal.NewFromFloat64(payload.MaxPositionSize)
	if err != nil {
		return momentum.Config{}, nil, fmt.Errorf("config.max_position_size: %w", err)
	}
	leverage, err := decimal.NewFromFloat64(payload.Leverage)
	if err != nil {
		return momentum.Config{}, nil, fmt.Errorf("config.leverage: %w", err)
	}

	amount := defaults.InitialBalance
	if payload.InitialBalance != nil {
		if *payload.InitialBalance < 0 {
			return momentum.Config{}, nil, fmt.Errorf("config.initial_balance must be non-negative")
		}
		if *payload.InitialBalance == 0 {
			if !forRun {
				return momentum.Config{}, nil, fmt.Errorf("config.initial_balance must be greater than 0 for backtests")
			}
		} else {
			amount, err = decimal.NewFromFloat64(*payload.InitialBalance)
			if err != nil {
				return momentum.Config{}, nil, fmt.Errorf("config.initial_balance: %w", err)
			}
		}
	}

	orderBookID, normalised, err := decodeOrderBookID(raw, payload.OrderBookID)
	if err != nil {
		return momentum.Config{}, nil, err
	}

	return momentum.Config{
		SignalSource: payload.SignalSource,
		FastWindow:   payload.FastWindow, SlowWindow: payload.SlowWindow, ATRWindow: payload.ATRWindow,
		StopLossATR: stopLoss, TakeProfitATR: takeProfit,
		MinOrderSize: minOrder, MaxOrderSize: maxOrder, MaxPositionSize: maxPos,
		OrderBookID: orderBookID, Tenor: payload.Tenor,
		InitialBalance: amount, Leverage: leverage,
	}, normalised, nil
}
```

> `decodeOrderBookID` is the existing helper that `decodeMeanReversionConfig` uses to parse + normalise the order-book UUID (returns the parsed `uuid.UUID`, the normalised JSON, and an error). Reuse it verbatim — confirm its name with `grep -n 'func decodeOrderBookID' strategy/http/*.go`; if meanreversion inlines it differently, mirror exactly what meanreversion does for `orderBookID`/`normalised`.

- [ ] **Step 3: Register + wire type switches**

In `defaultStrategies`, add to the `defs` slice:
```go
newMomentumDefinition(pricesHandler, log),
```

Add a `case *momentum.Strategy:` arm (calling `momentum.WithDecisionStore(h.decisionStore)(s)`) to `attachDecisionStore`.

Add a `case *momentum.Strategy:` arm to each of the three per-user-API-key injection switches (the ones that already handle `*meanreversion.Strategy`), calling `momentum.WithMarketAPIClient(strategycore.NewDoraClientWithKey(...))(withClient)`.

Add `case momentum.BacktestResult:` to the backtest-result handling switch (mirror the `meanreversion.BacktestResult` case).

- [ ] **Step 4: Build + test + commit**

```bash
go build ./...
go test ./strategy/http/...
git add strategy/http/handler.go
git commit -m "feat(http): wire momentum strategy definition and config decoder"
```

---

## Task 8: Fakes + final verification

- [ ] **Step 1: Generate counterfeiter fakes**

```bash
go generate ./strategy/momentum/...
```
Confirm `strategy/momentum/momentumfakes/` contains fakes for `historicalPriceStore` and `benchmarkYieldClient`.

- [ ] **Step 2: Full verification**

```bash
go mod tidy
golangci-lint run --timeout 5m ./...
go test -race ./...
```
Expected: all green. Fix every lint/test failure before continuing (never bypass hooks).

- [ ] **Step 3: Pre-commit on everything**

```bash
git add -A
pre-commit run
```
Expected: all hooks pass. Address any failures.

- [ ] **Step 4: Present for review** — do NOT commit further without user authorization. Stage and hand off:

```bash
git status
git diff --stat
```

---

## Self-review (completed by plan author)

- **Spec coverage:** §5 Config → Task 1; §6 signal engine + direction mapping → Task 2; §7 Decision → Task 1; §8 ShouldExit → Task 3; §9.2 historical data → Task 4; §9.3 backtester → Task 5; §9.1 run loop + min/max sizing → Task 6; §10 HTTP wiring → Task 7; §11 testing → embedded across tasks; §12 verification → Task 8. All spec sections map to a task.
- **Placeholder scan:** the only deliberate "copy verbatim" instructions target files that already compile (meanreversion). No TODO/TBD in the new code. `streamTrades` and `decodeOrderBookID` carry explicit "mirror meanreversion" notes with grep commands — these are concrete, not placeholders.
- **Type consistency:** `Decision` fields (`FastMA`/`SlowMA`/`ATR`/`Trend`/`SeriesValue`) used in Tasks 2/3/5 match. `ShouldExit(openSignal, Decision, entryPrice, entryATR)` signature is consistent across Tasks 3/5/6. `cappedOrderQuantity(positionSize, currentPosition, price)` consistent across Tasks 5/6. `NewExitDecision` referenced in Task 3 tests and defined in Task 3.

## Execution handoff

Plan complete and saved to `docs/superpowers/plans/2026-07-24-momentum-trend.md`. Two execution options:

1. **Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.
2. **Inline Execution** — I execute tasks in this session with checkpoints for review.

Which approach?
