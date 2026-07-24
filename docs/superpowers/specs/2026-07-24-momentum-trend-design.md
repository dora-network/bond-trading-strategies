# Momentum / Trend-Following Strategy — Design

- **Date:** 2026-07-24
- **Status:** Draft (awaiting review)
- **Scope:** New `strategy/momentum/` package; no changes to existing strategies.
- **Strategy type:** `momentum`

## 1. Overview

Add a single-instrument trend-following strategy that complements the existing
`mean_reversion` (contrarian) and `breakout` (volatility-compression) strategies.
Where mean reversion bets on spread reversion, momentum bets that trends *persist*.
It detects trend via fast/slow moving-average crossover on the continuous price feed
and rides the trend until the MAs reverse, with ATR-based protective exits.

The strategy is designed for Dora's continuously trading fractionalized bond
exchange. The signal source is configurable so the operator chooses whether the
trend engine tracks **price**, **YTM**, or **yield spread** (bond YTM minus the
FRED Treasury benchmark).

## 2. Goals & non-goals

**Goals**
- Single-instrument, one order book per run — fits the existing `Strategy`/`Decision`
  architecture unchanged.
- Configurable signal source: `price` | `ytm` | `spread`.
- Full parity with `mean_reversion`/`breakout`: live `Run` + `Backtest` + HTTP
  wiring + decision persistence + per-user API-key injection.
- ATR-based stop-loss and optional take-profit (mirrors breakout's exit model).

**Non-goals (deferred — see §13)**
- Bar/candle resampling (v1 runs on raw ticks).
- Cross-sectional / multi-asset momentum.
- Whipsaw neutral-band filter; scaled (MA-separation) position sizing.
- Transaction costs / bid-ask / financing in the backtester (matches existing
  backtesters' deliberate simplicity).

## 3. Background & context

- FRED delivers **daily** Treasury benchmark yields (1M…30Y); the bond's own price
  and YTM stream **continuously** from the Dora exchange and are persisted in
  `price_history`. A spread-based signal therefore mixes a continuous leg with a
  daily step-function benchmark; `price` and `ytm` sources avoid FRED entirely.
- Existing primitives reused as-is:
  - `strategy/window.Rolling` — O(1) rolling `Mean()` / `StdDev()` / `Sum()`,
    `Ready()`, min size 2.
  - `prices.Handler` subscription delivering
    `prices.AssetPrice{AssetID, Price, YTM *decimal.Decimal, Time}`.
  - `strategy.MarketAPIClient` for order placement / position / collateral.
  - `fred` client + `prices.PGStore` (raw `LoadHistoricalPrices`/`LoadLastPrices`).
  - `strategy/stats.Summarise` for PnL / Sharpe / drawdown.
- Architectural choice: **mirror `mean_reversion`/`breakout`** (self-contained
  package, duplicated scaffolding) over extracting a shared base. The duplication is
  the codebase's existing convention; a shared-base refactor is a separate effort.

## 4. Package layout

```
strategy/momentum/
  strategy.go        // Config, Strategy, New, DefaultConfig, run loop, Update,
                     //   ShouldExit, executeDecision, closePosition, balances
  types.go           // Decision, TradeRecord, ClosedTrade, BacktestResult,
                     //   reason/exit constants, StrategyType
  historical_data.go // historicalPriceStore + benchmarkYieldClient interfaces,
                     //   observation loading, prefill
  backtest.go        // Backtester
  export_test.go     // white-box helpers
  *_test.go          // strategy / decision / backtest / historical_data / market_api
  momentumfakes/     // counterfeiter-generated fakes
```

Implements `strategy.Strategy` (`Backtest` + `Run`) and the shared
`types.Decision` / `types.BacktestResult` contracts.

## 5. Config

All fields have defaults via `DefaultConfig()`. Continuous-market window defaults
mirror breakout's calibration (240 / 1440 / 240 ticks).

| Field              | Type            | Default     | Notes                                          |
|--------------------|-----------------|-------------|------------------------------------------------|
| `SignalSource`     | string          | `"price"`   | one of `price`, `ytm`, `spread`                |
| `FastWindow`       | int             | `240`       | fast-MA tick window (>= 2)                     |
| `SlowWindow`       | int             | `1440`      | slow-MA tick window (> `FastWindow`)           |
| `ATRWindow`        | int             | `240`       | mean-abs-price-diff window for exits (>= 2)    |
| `StopLossATR`      | decimal.Decimal | `20`        | stop distance in ATR units; 0 disables         |
| `TakeProfitATR`    | decimal.Decimal | `0`         | take-profit distance in ATR units; 0 disables  |
| `MinOrderSize`     | decimal.Decimal | `0`         | skip opening if qty < this; 0 disables         |
| `MaxOrderSize`     | decimal.Decimal | `0`         | clamp qty down to this; 0 disables             |
| `MaxPositionSize`  | decimal.Decimal | `1.0`       | capital fraction cap                           |
| `OrderBookID`      | uuid.UUID       | —           | required                                       |
| `Tenor`            | string          | —           | required when `SignalSource == "spread"`       |
| `InitialBalance`   | decimal.Decimal | `1.0`       |                                                |
| `Leverage`         | decimal.Decimal | `1.0`       |                                                |

`MinOrderSize`/`MaxOrderSize` are `decimal.Decimal` (not `int`) because Dora is a
fractionalized market; this diverges from copytrading's integer fields intentionally.

**Validation** (in `decodeMomentumConfig`):
- `SignalSource ∈ {price, ytm, spread}`; `spread ⇒ Tenor` required.
- `FastWindow >= 2`; `SlowWindow > FastWindow`; `ATRWindow >= 2`.
- When both > 0: `MaxOrderSize >= MinOrderSize`.

## 6. Signal engine

Three `window.Rolling` instances — `fastMA`, `slowMA`, `atrWin` — identical in shape
to breakout's three windows.

Per tick:
1. **Select series value** by `SignalSource`:

   | Source  | Value                         | Needs FRED? | Needs YTM? |
   |---------|-------------------------------|-------------|------------|
   | `price` | `o.Price`                     | no          | no         |
   | `ytm`   | `*o.YTM`                      | no          | yes        |
   | `spread`| `o.YTM − benchmark(tenor)`     | yes (cached)| yes        |

   `spread` mode resolves the benchmark via the cached/daily FRED yield, exactly as
   `mean_reversion` does. In `ytm`/`spread` modes a tick with a nil YTM is dropped
   entirely (no window updates, no signal), matching `mean_reversion`; `price` mode
   never requires YTM and processes every tick.

2. `fastWin.Add(v)`; `slowWin.Add(v)`; `atrWin.Add(|price − prevPrice|)` (same ATR
   definition breakout uses — mean absolute price diff).

3. `fastMA = fastWin.Mean()`, `slowMA = slowWin.Mean()`.

4. **Direction mapping (the bond-specific adaptation).** Yield and spread move
   *inverse* to price, so the raw cross direction flips for those sources. Encoded
   as a per-source sign: `price → +1`, `ytm/spread → −1`, applied to
   `(fastMA − slowMA)`:

   | Source  | series rising (`fastMA > slowMA`) → | because                       |
   |---------|-------------------------------------|-------------------------------|
   | `price` | **Buy** (long)                      | price up = uptrend            |
   | `ytm`   | **Sell** (short)                    | yield up = price down         |
   | `spread`| **Sell** (short)                    | spread up = bond cheapening   |

   Positive signed value → `SignalBuy`; negative → `SignalSell`; one code path, no
   per-source branches in the hot loop.

5. **Window readiness:** until both `fastWin.Ready()` and `slowWin.Ready()`,
   emit `SignalHold` with reason `warming_up`.

6. **Position sizing:** `PositionSize = MaxPositionSize` (flat sizing for v1).

## 7. Decision type

Implements the 7 `types.Decision` accessors (`Time`, `BondID`, `Price`, `Signal`,
`PositionSize`, `StrategyType`, `Reason`). Strategy-specific exported fields read by
the backtest: `FastMA`, `SlowMA`, `ATR`, `SeriesValue`, `Trend` (`"up"`/`"down"`/`""`).

Reason codes: `warming_up`, `ma_crossover_up`, `ma_crossover_down`, `flat`,
`below_min_order_size`. Exit reason codes: `reversal`, `stop_loss`, `take_profit`,
`strategy_exit`.

## 8. Exit logic (`ShouldExit`)

Priority `stop_loss > take_profit > reversal`, called identically by the live run
loop and the backtester. Inputs: `openSignal`, current `Decision` (`Price`, `ATR`,
`FastMA`, `SlowMA`), and the open trade's anchored `EntryPrice` + `EntryATR`
(stable for the position's life, as in breakout).

1. **Stop-loss** (`StopLossATR > 0`): price moved against entry by
   `>= StopLossATR × EntryATR` → close, `stop_loss`.
2. **Take-profit** (`TakeProfitATR > 0`): price moved in favour by
   `>= TakeProfitATR × EntryATR` → close, `take_profit`.
3. **Reversal**: the MA crossover flipped against the open position (using the
   source's signed direction) → close, `reversal`.
4. **Strategy exit**: positions still open at the end of backtest history close at
   the last price, `strategy_exit`.

## 9. Execution & simulation

### 9.1 Live run loop (mirrors `mean_reversion.run` / `breakout.run`)
`Run(ctx, msgCh, runID)`:
- `LookupAssetID(OrderBookID)` → `initializeBalances` (fetch bond position + USD;
  track `bondQty`, `usdBal`, `openSignal`).
- `prefillWindow` from `price_history` (best-effort, non-fatal).
- Subscribe to `prices.Handler`; `time.NewTicker` loop selecting `ctx.Done` /
  messages (`Pause`/`Resume`/`Stop`) / price ticks.
- Per tick for the configured asset: build `types.YieldObservation` (`spread` mode
  calls `getBenchmarkYield` → FRED, cached); `Update` → `Decision`; record via
  `DecisionRecorder` (monotonic seq); act:
  - flat + non-Hold → `executeDecision` (open);
  - open → `ShouldExit` → `closePosition`;
  - open + opposite signal → `closePosition` (`reversal`).

`executeDecision` / `closePosition` / `positionOrFetch` copied from the breakout
pattern. `cappedOrderQuantity` applies min/max sizing:
`budget = effectiveCapital × PositionSize`; `qty = budget / price`;
`qty = min(qty, MaxOrderSize)` if `MaxOrderSize > 0`; return `ok=false` (skip) if
`qty < MinOrderSize` (when `MinOrderSize > 0`). Balance tracking, `collateralWeight`,
`inverseLeverage`, and `clientOrderID` prefixing are identical to breakout.

### 9.2 Historical data (`historical_data.go`)
- `historicalPriceStore` interface (`LoadHistoricalPrices` / `LoadLastPrices`,
  satisfied structurally by `prices.PGStore`).
- `benchmarkYieldClient` interface (`FetchHistoricalYields`), used only in
  `spread` mode.
- `loadObservations(start, end)`: raw prices for `price`/`ytm`; prices + FRED merge
  for `spread` (reuses mean_reversion's per-time benchmark-cache logic).
- `prefillWindow`: loads last `SlowWindow × 2` prices into the rolling windows.
- Stores self-wire lazily via `getHistoricalPriceStore` / `getBenchmarkYieldClient`
  (read `DATABASE_URL` / FRED env), so `newMomentumDefinition` needs no store param
  and momentum has **no dependency on the breakout package**.

### 9.3 Backtester (`backtest.go`, mirrors meanreversion)
Replays `[]types.YieldObservation` chronologically; one open position at a time; no
tx costs / spread / financing. `effectiveCapital = InitialBalance × Leverage`;
entry/exit update `remainingBalance`; per-trade PnL; summary
(`TotalPnL`, `WinCount`, `LossCount`, `MaxDrawdown`, `SharpeRatio`) via
`stats.Summarise`; optional `BacktestTradeWriter`. `BacktestResult` implements
`types.BacktestResult`.

## 10. HTTP wiring (`strategy/http/handler.go`)

- `newMomentumDefinition(pricesHandler, log) StrategyDefinition` — `Type:"momentum"`,
  `Status:available`, `SupportsRun:true`, `SupportsBacktest:true`, one `ConfigField`
  per Config field, `DecodeConfig → decodeMomentumConfig`. Registered in
  `defaultStrategies`.
- `decodeMomentumConfig(raw, forRun)` mirrors `decodeMeanReversionConfig` (defaults +
  validation per §5).
- Functional options: `WithLogger`, `WithMarketAPIClient`, `WithDecisionStore`,
  `WithBacktestWriter`.
- Handler type-switch additions (mechanical, mirroring the meanreversion case at
  each):
  - `attachDecisionStore` → `case *momentum.Strategy`.
  - Per-user-API-key injection (3 sites) → `case *momentum.Strategy`.
  - Backtest-result handling → `case momentum.BacktestResult`.

## 11. Testing

- `strategy_test.go` — `Update` per source: `price` → Buy on rising, `ytm`/`spread`
  → Sell on rising (direction inversion); `warming_up` pre-readiness; ATR
  computation; flat position sizing.
- `decision_test.go` — accessor contract + reason codes.
- `backtest_test.go` — exit priority `stop_loss > take_profit > reversal`;
  `strategy_exit` at end; PnL/win-loss/Sharpe via `stats.Summarise`; min/max sizing
  skip behaviour.
- `historical_data_test.go` + `market_api_test.go` — observation loading (all three
  sources) + execution/balance helpers.
- `export_test.go` white-box helpers; counterfeiter fakes for both store interfaces.

## 12. Verification

- `go test -race ./strategy/momentum/... ./strategy/http/...`
- `golangci-lint run ./...`
- `go mod tidy`
- `pre-commit run` (staged before review; no commit without authorization)

## 13. Open questions / deferrals (`ponytail:`)

- Bar/candle resampling — add when tick windows prove too noisy or when 6–12m
  horizons become reachable as history accumulates.
- Whipsaw neutral band — add if churn/fees from crossovers near the mean appears.
- Scaled position sizing by MA separation — add if flat sizing under-allocates.
- Shared store/helper extraction across meanreversion/momentum — defer until a 4th
  strategy repeats the lazy-init pattern.

## 14. References

- `docs/strategies-research/concepts/technical-indicators-continuous-bond-markets.md`
- `docs/strategies-research/concepts/carry-and-rolldown.md`
- `docs/strategies-research/synthesis/Research: Bond Trading Strategies.md`
- Existing implementations: `strategy/meanreversion/`, `strategy/breakout/`.
