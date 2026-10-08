# Candle-Driven Signal Strategies — Design

Date: 2026-09-30
Branch: `tan/enhance-strategies`
Status: design decisions taken via brainstorming Q&A; spec pending user review

## Problem

The three signal strategies (mean-reversion, momentum, breakout) compute their
indicators on individual price ticks. Tick-level statistics are dominated by
microstructure noise (bid-ask bounce, quote flicker), the "windows" are raw
tick counts whose time-span varies with feed activity, and the algorithms were
written in bar vocabulary ("consecutive closes", `ConfirmationBars`) but fed
ticks. Industry practice is the reverse: signals on closed bars of a chosen
resolution, ticks/trades for auxiliary confirmation and execution.

Meanwhile the repo already ingests everything needed:

- `candles_history` is fully populated (OHLCV + open/high/low/close YTM) by the
  price-daemon, but `resolution` is hardcoded `1m` (`candles/handler.go:254`)
  and no strategy reads it. `LoadCandles` exists unused.
- `internal/agent/store/history_store.go:276-370` already SQL-buckets 1m
  candles into `5m/15m/1h/4h/1d` (plus `7d`, SQL-only, not a stream resolution) (generate_series grid + forward-fill).
- The agent/wasm path already requires an explicit `resolution` field with the
  enum `1m, 5m, 15m, 1h, 4h, 1d` (`internal/agent/backtest/validate.go`).
- The full trade tape (live `streams.TradeStream.SubscribeOrderBook`, persisted
  `trades_history` with `side`, `quantity`, `aggressor_indicator`) reaches only
  breakout's OBV.

## Decisions

1. **Clean cutover.** Bar engine becomes the only signal path for all three
   strategies. Tick-window semantics are deleted, not dual-run. Ticks/trades
   remain for auxiliary signals and execution.
2. **Native resolution stream.** `candles.Config` gains `Resolution`;
   strategy-server holds one WS connection per (order book, resolution),
   shared across runs. DORA does the bucketing; no local resampler.
3. **Auxiliary signals in scope:** tick-based intrabar exits + execution
   pricing (all strategies), trade-imbalance entry filter (mean-reversion),
   volume confirmation (momentum). Breakout keeps its existing OBV filter.
4. **Backtests strictly on `candles_history`** (bucketed). No backfill from
   `price_history`; missing coverage returns a descriptive error.
5. **Defaults per strategy nature:** mean-reversion `1h`, momentum `15m`,
   breakout `5m`. All overridable per run/backtest.

## Architecture

```
DORA WS (per book, per resolution) ──> candles.Handler (existing + Resolution cfg)
                                            │ in-progress bar updates
                                            ▼
                              BarCloser adapter (new)
                                            │ closed bars only
                                            ▼
                strategy run loop ── Update(bar) ──> entry signals
                        │
                        ├── prices.Handler ticks ──> intrabar stop checks + execution pricing
                        └── TradeStream tape   ──> OBV (breakout), imbalance filter (MR)
```

### Components

- **`candles.Config.Resolution string`** — validated enum `1m, 5m, 15m, 1h,
  4h, 1d`; `buildURL` uses it instead of the hardcoded `1m`.
- **`candleRegistry`** (strategy-server) — map keyed `(orderBookID,
  resolution)` → `*candles.Handler`, refcounted: first subscriber starts the
  stream (under `streams.Daemon` reconnect), last unsubscribe stops it.
  Handlers fan out via the existing `Subscribe`/`Unsubscribe`.
- **`BarCloser` adapter** — wraps `chan []candles.StreamCandlesEntry`, emits
  bar *N−1* the first time bar *N*'s `start_timestamp` appears (the wsbroker
  state-machine pattern). A bar is final only when superseded. Strategies
  never see an in-progress bar.
- **`strategy.CandleFeed` interface** — `SubscribeBars(ctx, book, resolution)
  (<-chan types.Bar, cancel func(), error)`. The registry implements it for
  live runs; backtests/replays and tests use a fake (counterfeiter, matching
  repo convention). Strategies stop depending on `*candles.Handler` directly.
- **`strategy/types.Bar`** — `Time (start), Open, High, Low, Close, Volume,
  OpenYTM, HighYTM, LowYTM, CloseYTM, BenchmarkYield`. Benchmark is resolved
  by the strategy (MR / momentum spread mode) at bar close, as today.
- **`candles.PGStore.LoadCandlesBucketed(ctx, orderBookID, resolution, since,
  until)`** — SQL bucketing adapted from `internal/agent/store/history_store.go`
  (generate_series grid, flat-fill at previous close, OHLCV + 4-YTM roll-up;
  `1m` passes through). Used by backtests and live warm-start prefill.

## Per-strategy changes

Common: `Update(obs types.Bar)` per closed bar; windows count **bars**; ATR
becomes true range `max(H−L, |H−prevClose|, |L−prevClose|)` on prices; live
intrabar stop-loss checks consume ticks (a spike through the stop must not
wait for bar close); execution sizing/fill pricing uses the last observed
tick price, as today.

### Mean reversion (default resolution `1h`)

- Spread = `CloseYTM − benchmark` per bar; z-score math unchanged; window
  semantics now bars.
- Defaults: `LookbackWindow 24` (≈ 1 day at 1h; was 20 ticks).
- Exits: z-reversion on bar close; z-stop-loss intrabar from ticks (live).
  Backtests replay `price_history` ticks intrabar through the same live math
  (tick YTM − bar benchmark yield → zAgainstWindow → ShouldExit, exit at the
  tick's price). Bars whose formation window has no tick coverage fall back
  to the adverse spread extreme of the bar (`HighYTM`/`LowYTM` − benchmark,
  whichever hurts the open position). Replaces the bar-extreme approximation
  after user review (2026-10-01); the fallback is retained.
- **New imbalance gate**: config `imbalance_window` (# recent trades,
  default 100), `imbalance_threshold` (signed quantity, default 0 — any net
  opposing flow blocks; `imbalance_window = 0` disables the gate). An entry
  is suppressed when net signed quantity over the window opposes the entry
  direction (e.g. BUY blocked while the tape shows net aggressive selling).
  Live: `TradeStream.SubscribeOrderBook`. Backtest: `trades_history`
  interleaved by timestamp (breakout's existing interleave pattern,
  generalized).
- Prefill: last N bars from `candles_history` (bucketed), replacing the
  `price_history ×2` prefill.

### Momentum (default resolution `15m`)

- Fast/slow MA on bar closes of the selected series (`price`/`ytm`/`spread`);
  `sourceSign` inversion unchanged.
- Defaults: `FastWindow 8` (≈ 2h), `SlowWindow 96` (≈ 24h), `ATRWindow 24`.
- **New volume gate**: config `volume_avg_window` (# bars, default 20; 0
  disables), `volume_ratio_threshold` (multiplier on rolling-mean bar
  volume, default 1.0). Entry requires
  `bar.Volume ≥ threshold × mean(volume window)`.
  Candle `Volume` is DORA-computed trade volume — no trade-stream dependency.
- Exits unchanged (entry-anchored ATR stops, reversal), now on true-range
  ATR; intrabar tick checks live; bar H/L in backtests.
- Prefill: last `SlowWindow` bars from `candles_history`.

### Breakout (default resolution `5m`)

- Short/long σ on bar closes; compression arm and `prevBarClose ±
  BreakoutATRMultiple × ATR` triggers unchanged in form; ATR is true range.
- Defaults: `ShortVolWindow 12` (≈ 1h), `LongVolWindow 96` (≈ 8h),
  `ATRWindow 12`, `ConfirmationBars 3` (≈ 15 min of sustained closes).
- OBV volume filter unchanged (already trade-tape based, already wired).
- Exits: `liveCheckSLTP` fed by ticks live, bar H/L in backtests.
- **New prefill** (previously started cold): last `LongVolWindow` bars from
  `candles_history` so the volatility baseline is ready at start.

## Backtests

1. Load bars: `LoadCandlesBucketed(book, resolution, start − warmup, end)`.
   Warmup = the strategy's largest window (+1 bar for ATR prev-close).
2. Replay bar closes through `Update`; intrabar exits replay `price_history`
   ticks in (close_i, close_{i+1}] through the live tick-case math, exiting at
   the first crossing tick's price; bars with no tick coverage in that window
   fall back to the adverse extreme (approximation kept only as fallback).
3. Interleave `trades_history` by timestamp where the strategy needs trades
   (breakout OBV today; MR imbalance gate now). Reuse/generalize breakout's
   interleave helper.
4. **Coverage guard**: if `candles_history` has no/partial coverage for the
   requested range, fail with 400 naming the book's available min/max
   timestamps. No silent fallback to ticks, no synthesis.

## Config / API surface

- `resolution` field on all three run + backtest payload structs
  (`strategy/http/handler.go`), enum `1m, 5m, 15m, 1h, 4h, 1d`, defaulted
  per strategy, validated like the agent path.
- New fields: `imbalance_window`, `imbalance_threshold` (MR);
  `volume_avg_window`, `volume_ratio_threshold` (momentum).
- Window fields' meaning changes ticks → bars with new defaults (documented
  breaking change; `strategy_runs` stores config as a JSON blob — no schema
  migration; existing live runs keep running until restarted).
- Update `ConfigFields` metadata (MCP surface), `strategies.md`, OpenAPI
  specs, and remove the "Bar/candle resampling is not supported" v1
  limitation notes.

## Error handling

- Candle stream disconnect: existing `streams.Daemon` fixed-delay reconnect;
  strategy windows retain state; gap logged. No signal while disconnected.
- No candle data for a book: run fails at start with a clear error (vs
  today's silent tick trading).
- Empty `trades_history` in a backtest range: imbalance/OBV filters pass
  through (existing nil-store convention), logged.
- FRED benchmark: unchanged (cached, 5-minute fetch throttle).
- `resolution` absent → per-strategy default; invalid → 400.

## Testing

- Unit: BarCloser state machine (emit on `start_timestamp` advance; drop
  in-progress; flush-free); bucketed loader SQL (pgxmock, existing store-test
  patterns); per-strategy `Update` on bar fixtures (z-score, MA cross,
  compression arm/fire); true-range ATR math; volume and imbalance gates
  (gate fires / passes / disabled).
- Run loops: existing `run_loop_test.go` patterns adapted to a fake
  `CandleFeed`, including tick-driven intrabar stop firing mid-bar.
- Integration: breakout's `integration_backtest_test.go` pattern seeded with
  `candles_history` rows instead of `price_history`; coverage-guard 400.
- Cutover guards: one test per strategy asserting that tick input alone
  produces no entries (the old engine is gone and stays gone).
- Prefill: warm-start tests seeding bucketed history, asserting immediate
  signal capability on the first closed bar.

## Out of scope

- Order-book depth/level data (not fetched anywhere in the repo today).
- Backfilling `candles_history` from `price_history`.
- Local resampling of live 1m bars (DORA native resolutions only).
- Copy-trading, TWAP/VWAP (trade/tape-driven by nature; unaffected).
- Whipsaw neutral band for momentum, trend-strength position scaling
  (documented as future tunables).
