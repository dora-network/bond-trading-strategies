# TODO

## High Priority

- **Notification websocket endpoint**
  - The strategy service should expose a WebSocket endpoint that pushes real-time notifications to connected clients. Events include: backtest completed, comparison completed, order submission errors, stop-loss notifications, and run state changes.

- ~~**Rate limiter**~~ ✅
  - Add rate limiting to the strategy server's REST API so it cannot be spammed. Should be configurable (e.g., per-IP, per-user, per-endpoint) and return `429 Too Many Requests` when exceeded.

- **Ledger position stream**
  - Subscribe to DORA's `GET /v1/user/{user_id}/ledger/stream` WebSocket for real-time position updates instead of polling `GetPortfolioV2` on every trade. The ledger stream pushes account balance and position changes, which would let the strategy maintain an accurate in-memory position map without repeated REST calls. This requires:
    - A new `LedgerStream` type in `streams/` (similar to `TradeStream`) that manages the websocket connection, parses ledger update messages, and routes them to subscribers.
    - Position state tracking in the copytrading strategy — maintaining a map of `asset_id → (available, borrowed)` that gets updated by ledger events.
    - Integration in `cmd/strategy-server/main.go` to start the ledger stream alongside the trade stream.

## Planned Features

- **Backtest comparison**
  - Run multiple backtests with different configs and compare PnL, drawdown, and Sharpe ratio side-by-side.

- **Parameter sweep**
  - Let the UI submit a grid of backtests across parameter ranges (e.g., vary `entry_z_score` from 1.5 to 3.0) and surface the optimal performer.

- **Run alerts**
  - Push/webhook notifications when a live run stops, hits stop-loss, or errors out.

- **Config presets**
  - Save and load named parameter sets so users don't have to retype strategy configs every time.

- **Agent skill documentation**
  - Construct a `SKILL.md` file for the strategy server so users who prefer agent skills over MCP servers can interact with the service via a skill interface instead.

- **Long Only Option**
  - When chosen user will only be allowed to hold long positions and never short sell.

## Checks

- Copytrading Min/Max strategy parameters:
  - Max order size should not be exceeded by the Percentage of available calculation
  - Min order size should not submit order if size is smaller than min, except when closing positions, then min order size should be ignored

## Needs product decision (VWAP/TWAP review, 2026-08-24)

Two findings from the vwap/twap code review are money-path concerns where the
fix requires an architectural call rather than a localized edit. Captured
here so they get revisited, not abandoned.

- **At-least-once crash window between order placement and state checkpoint**
  — `PlaceOrder` runs before `SaveState` (`strategy/exec/exec.go:~135`),
  with no idempotency key, so a crash/rollback in that gap re-places the
  chunk. Fixing requires either persisting order intent pre-submit or
  relying on DORA-side order-id dedup. Decision required before
  implementing.

- **Silent under-execution on partial/cancel** — rebalance keys on
  `TotalSubmitted` (requested), never re-places the `requested − filled`
  shortfall when a chunk is only partially filled or cancelled.
  Documented intent in `strategy/twap/state.go` and mirrored in
  `strategy/vwap/state.go`. Decision required whether fills should be
  recovered.

## Momentum strategy follow-ups (deferred from Slice A wiring, 2026-08-24)

Slice A wired momentum through the HTTP API (definition, config decoder, result
type, per-user DORA API key injection in three switches, decision recorder,
SetDecisionSeq seed, orderupdates filter, handler test count 5 -> 6). The
16-reviewer pass on `tan/momentum-strategy` vs `development` flagged additional
defects and a wiring-refactor follow-up that did not block Slice A but should
not be lost.

### Blockers (must fix before merge)

- **Live run loop self-deadlock** — `strategy/momentum/strategy.go:783-799`.
  `run()` holds `s.mu.Lock()` then calls `s.Update(obs)` which re-locks at
  `:328`. `sync.RWMutex` is not reentrant. First matching price tick freezes
  the run goroutine; Pause/Stop/ctx.Done never serviced. Fix: breakout's
  `handleTick` pattern — read state under a short lock, unlock, then call
  `s.Update(obs)`. Add a regression test that drives the live run loop (the
  current suite only exercises `Backtest`, `Update`, `ShouldExit`).

- **Force-close omits exit TradeRecord** — `strategy/momentum/backtest.go:108-118`.
  At end of history, `closedTrades` is appended but no `exitRecord` row is
  emitted to `trade_records`. Cause: `/trades` shows dangling open entry while
  `/closed-trades` shows the round-trip. meanreversion appends both — port the
  exit record. Also use `lastDecision.Signal()` instead of `openTrade.Signal`
  for the close signal.

### Major (should land in the same merge)

- **`getBenchmarkYield` stale-cache fallback dropped** —
  `strategy/momentum/strategy.go:849-887`. Every error path returns
  `decimal.Zero` instead of returning the cached yield. On FRED outage in
  spread mode momentum computes `YTM - 0` (raw yield with sign inversion) and
  trades on garbage signals. Mirror meanreversion's `if ok { return yield }`
  fallback on each error path.

- **Short-position ShouldExit branches untested** —
  `strategy/momentum/strategy_test.go:117-153`. All four tests are
  long-only. Short stop-loss (`:268-272`), short take-profit (`:286-290`),
  and Sell->Buy reversal (`:299-300`) have zero coverage. Sign-flip in any
  Sell branch is silent mutation. Mirror each existing test with
  `openSignal=SignalSell` and inverted price fixtures.

- **Backtest tests are mutation-immune** —
  `strategy/momentum/backtest_test.go:34,47,77`. Only `NotEmpty(closedTrades)`
  is asserted. `TestBacktest_StopLossExits` would still pass if the entire
  stop-loss branch in `ShouldExit` were deleted because reversal fires on the
  same tick. Sibling packages set the bar (`breakout/backtest_test.go:56`,
  `copytrading/backtest_test.go:159` assert exact entry prices). Add exact
  trade count, exit reason, entry/exit price, and PnL sign/magnitude
  assertions.

- **`nil-YTM tick dropped` test cannot detect ingestion** —
  `strategy/momentum/strategy_test.go:96-102`. One zero-YTM tick asserts
  `SignalHold`, which is also the warming-up default. Deleting the `!ok`
  early-return at `strategy.go:338-344` still passes. Follow the dropped
  tick with valid ticks and assert the crossover timing shifts by one, or
  distinguish dropped-tick decision from warming-up via the reason field.

- **`historical_data.go` untested, both counterfeiter fakes unused** —
  `strategy/momentum/historical_data.go` (297 lines of date-window merging
  logic: `getObservations`, `prefillWindow`, `mergeBenchmarkObservations`,
  `cachedBenchmarkYield`, `latestCachedBenchmarkDate`) ships with zero test
  coverage. `momentumfakes/fake_historical_price_store.go` and
  `momentumfakes/fake_benchmark_yield_client.go` are generated but imported
  by nothing. Port `meanreversion/historical_data_test.go` and
  `market_api_test.go` patterns.

- **Price-mode YTM-optional contract is dead code** —
  `strategy/momentum/historical_data.go:76-79` skips rows only when
  `price.YTM == nil && SignalSource != SignalSourcePrice`. Real PG store
  filters `AND ytm IS NOT NULL` (`prices/store.go:65,86`), so nil-YTM ticks
  are always excluded regardless of mode. Either route price mode through
  a YTM-less loader or document the requirement.

- **Wiring refactor: bake `WithMarketAPIClient` into the construction
  closure** — currently `WithMarketAPIClient` is applied in three separate
  type-switch sites (`strategy/http/handler.go:969-976, 1594-1597,
  1962-1965`) after the strategy is built by `DecodeConfig`. The cleaner
  pattern — already implicit in the breakout/meanreversion definitions —
  is to inject the user-specific `MarketAPIClient` as a closure variable
  captured at strategy-construction time, eliminating the post-build
  switch walking. This would also fold the three switch sites in
  `createBacktest`/`createRun`/`resumePersistedRun` into a single helper
  on the handler. Touches all three strategy definitions; refactor scope,
  not a one-liner. Original slice note from the Slice A review.

### Minor (clean batch later)

- `strategy/momentum/strategy.go:332` mutates `s.lastPrice` before the zero-YTM
  drop check, skewing ATR gaps. Move the assignment after the drop check.
  **Resolved by 218e905 + 9668bfd:** Slice B restructured the run-loop
  to short-circuit on nil-YTM before calling Update, and Slice H
  removed the dead branch entirely. With nil-YTM never reaching
  Update(), the lastPrice assignment order is moot.
- `strategy/momentum/strategy.go:834-840` ticker case dead-reads `s.paused`
  into `_`; remove the lock cycle.
- `mcp/tools_strategy.go` declares `min_order_size` / `max_order_size` as
  integer but momentum's `Config` is `decimal.Decimal` (the same PR's own
  OpenAPI MomentumConfig uses `number`). Widen to `nonNegNum` and update
  the assertion in `mcp/server_test.go`.
- `mcp/server_test.go:604-608` doesn't pin the `signal_source` enum values;
  add an `Enum []string` assertion.
- `strategy/portfolio.go` silently swallows `decimal.Parse` failures on
  account balances (`portfolio.go:81-87`). Fail loudly — log + return
  `ok=false` so the caller falls back to the legacy `AssetPosition` path.
- `fred/benchmark.go` has no tests for `ParseBenchmarkTenor` /
  `NormalizeTenor` / `NormalizeDate` / `SupportedBenchmarkTenors`. Add a
  table-driven `fred/benchmark_test.go`.
- `strategy.MustParseUUID` duplicates `strategy/exec/exec.go:78 MustParseUUID`.
  Pick one canonical location and delete the other.
