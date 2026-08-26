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

_Both items resolved on this branch — kept here for traceability._

- **Live run loop self-deadlock** — fixed by 9668bfd
  `feat(momentum): unstick live run loop from s.Update reentrant lock`.
  `strategy/momentum/strategy.go:788-813` now snapshots state under a
  short lock, releases, then calls `s.Update(obs)` — the breakout
  `handleTick` pattern. Regression test at `strategy/momentum/run_loop_test.go:30`
  drives 6 ticks through the run loop and asserts `openSignal == Buy`
  within 3s; pre-fix the run goroutine deadlocks on tick 0.
- **Force-close omits exit TradeRecord** — fixed by a80e647
  `fix(momentum): persist force-close exit record and signal at close`.
  `strategy/momentum/backtest.go:122-138` now appends an `exitRecord`
  to `trade_records` at end-of-history (matched pair, not dangling
  open entry) and uses `lastDecision.Signal()` for the close signal.
  Regression test at `strategy/momentum/backtest_test.go:90`
  asserts `len(closedTrades)==1` AND `len(tradeRecords)==2` AND
  `closedTrade.ExitReason == ExitReasonStrategyExit`.

### Major (should land in the same merge)

_Five of seven resolved on this branch — each line carries the commit
hash that fixed it. The remaining two (backtest mutation-immune
assertions, nil-YTM dropped-tick detection) are P2 test-quality items
that don't block merge; carried as Minor below._

- **`getBenchmarkYield` stale-cache fallback dropped** — fixed by 7cd78c2
  `fix(momentum): fall back to stale FRED cache on outage`. Every error
  path now returns the cached yield (`strategy/momentum/strategy.go:861-912`),
  pinning test at `strategy/momentum/benchmark_fallback_test.go:38`
  traces the fetch-error → cachedOK → return cachedYield branch and
  would fail pre-fix (got 0).
- **Short-position ShouldExit branches untested** — fixed by 1fa635c
  `test(momentum): cover short-position ShouldExit branches`. All four
  short branches (StopLoss / TakeProfit / Reversal / Hold) now have
  threshold-direction assertions at `strategy/momentum/strategy_test.go:181-232`.
  _Caveat resolved: the positive-fire case landed as
  `TestShouldExit_StopLoss_ShortFires` (strategy_test.go:200) in 0698791._
- **Backtest tests are mutation-immune** — fixed by d8c4afc: per-trade PnL
  formula (in `computePnL`'s op order), `TotalPnL` = Σ closed-trade PnL,
  win/loss counts, and a deterministic `ExitReasonStopLoss` assertion now
  pin `TestBacktest_OpensAndExits` / `TestBacktest_StopLossExits` /
  `TestBacktest_ForceClosePersistsExitAndRecordsExitSignal`.
- **`nil-YTM tick dropped` test cannot detect ingestion** — fixed by
  218e905 `fix(momentum): surface nil-YTM contract violations instead of
  masking them`. The live run loop now surfaces nil-YTM as a contract
  error (`strategy/momentum/strategy.go:777-781`) instead of dropping
  the tick silently; the run_loop_test regression now exercises
  `s.Update` instead of bypassing it (see #1 above).
- **`historical_data.go` untested, both counterfeiter fakes unused** —
  fixed by be73571 `test(momentum): cover historical_data.go path end-to-end`
  (279 lines of tests at `strategy/momentum/historical_data_test.go`) and
  f57ef0c `test(momentum): generate counterfeiter fakes for
  historicalPriceStore and benchmarkYieldClient`.
- **Price-mode YTM-optional contract is dead code** — fixed by 218e905
  (same commit). The mode-conditional drop was removed; nil-YTM is now
  always a contract violation regardless of mode.
- **Wiring refactor: bake `WithMarketAPIClient` into the construction
  closure** — partially addressed by bfb33d5 `refactor(http): fold
  per-strategy API-key injection into one helper` (the
  `applyUserAPIKey` helper at `strategy/http/handler.go:2451` is applied
  at all three type-switch sites). The remaining half — folding the
  three createBacktest/createRun/resumePersistedRun sites into a
  single strategy-construction helper — is deferred (see Minor below).

### Minor (clean batch later)

- `strategy/momentum/strategy.go:332` mutates `s.lastPrice` before the zero-YTM
  drop check, skewing ATR gaps. Move the assignment after the drop check.
  **Resolved by 218e905 + 9668bfd:** Slice B restructured the run-loop
  to short-circuit on nil-YTM before calling Update, and Slice H
  removed the dead branch entirely. With nil-YTM never reaching
  Update(), the lastPrice assignment order is moot.
- `strategy/momentum/strategy.go:834-840` ticker case dead-reads `s.paused`
  into `_`; remove the lock cycle.
  **Resolved by 9668bfd:** Slice B simplified the `<-ticker.C` branch
  to `_ = struct{}{}` - no RLock/RUnlock on the paused field and no
  blank-identifier consumption.
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

## Momentum minor nits completed (2026-08-25)

The following TODO.md items have been resolved by separate commits
and are no longer outstanding:

- min_order_size / max_order_size schema type (Slice I-a) -
  resolved by 0487347.
- signal_source enum assertion (Slice I-b) - resolved by 0487347.
- portfolio.go Parse failure masked (Slice I-c) - resolved by
  b4851c0.
- fred/benchmark.go untested (Slice I-d) - resolved by a5c2f96.
- MustParseUUID duplicated (Slice I-e) - resolved by 8588afc.

Net: 5 commits, ~280 lines changed, 2 new test files
(fred/benchmark_test.go, strategy/portfolio_test.go) plus test
assertion expansions in momentum and http test files.

## Momentum review follow-ups (deferred from 16-reviewer code review, 2026-08-25)

**Status update (2026-08-26):** 11 of the 16 items below were resolved
by commit `c5f267b` ("address 11 of 16 P3 follow-ups") and one more
(backtest exit-reason pinning) by the round-4 fix session; each resolved
item is marked inline. Still open: lookupAssetID ctx, MustParseUUID
naming, closed-trades entry_atr column, plan-file trimming, OpenAPI
per-strategy response schemas, double Info log, spread-refetch config
option.

The 16-reviewer code review of `tan/momentum-strategy` vs `development`
flagged these as P3 nits. The P1 (1 item) and P2 (9 items) findings
were addressed in this session; the items below are non-blocking and
should be picked up in a separate session.

### Strategy

- **[RESOLVED — c5f267b] Duplicate `cachedBenchmarkYield` lookup in `getBenchmarkYield`**
  `strategy/momentum/strategy.go:861-868`. Both calls return identical
  values (pure RLock + binary search over the same cache); the second
  call is redundant per-tick work. Refactor leftover from the
  7cd78c2 stale-cache fallback. Drop one of the two lookups.
- **`lookupAssetID` ignores caller ctx** —
  `strategy/momentum/strategy.go:221-223` calls
  `strategy.LookupAssetID(context.Background(), ...)` while run()
  has a live ctx at its only call site. If Dora hangs during startup
  resolution, the run goroutine blocks indefinitely and Stop/ctx is
  ignored. Matches meanreversion/breakout pattern (info only — fix
  across all three).
- **[RESOLVED — c5f267b, 5-min fetch throttle] `getBenchmarkYield` refetches per tick on intraday FRED outages** —
  `strategy/momentum/strategy.go:861-904`. Intraday FRED's same-day
  observation is often absent, so spread-mode live ticks can refetch
  per tick with no throttle, and each failure calls `recordErr`
  (`strategy.go:522`) which appends unboundedly to `s.errs`. Add a
  per-tick throttle or short-circuit.
- **[RESOLVED — c5f267b] Force-close exit row persists zero `FastMA`/`SlowMA`** —
  `strategy/momentum/backtest.go:122-127` builds a fresh Decision
  with only time/bondID/price/signal, so `exitRecord` copies
  `d.FastMA=d.SlowMA=0` into the persisted TradeRecord. Copy
  `lastDecision.FastMA/SlowMA` (and ATR if desired) into d so
  persisted rows are uniform with in-loop exit rows.
- **[RESOLVED — c5f267b, tracking deleted] `remainingBalance` is write-only dead state** —
  `strategy/momentum/backtest.go:46-54` computes effectiveCapital and
  threads `remainingBalance` through applyEntryCashFlow /
  applyExitCashFlow / closeAtPrice, but it is never read. Either wire
  into sizing (matching meanreversion's pattern) or delete the
  tracking and the two apply*CashFlow helpers (~30 lines).

### Decoders / config

- **`strategy.MustParseUUID` name violates Go `Must*` convention**
  `strategy/uuid.go:10` returns `uuid.Nil` on parse failure, never
  panics. Behavior is deliberate (live-run path records decision rows
  with run_id+seq even when asset lookup failed). Renaming to
  `ParseUUIDLoose` would fix the lie but touches exec.go and 6 call
  sites. Defer until a third caller.
- **[RESOLVED — c5f267b, unexported] `FindAccountAndBalance` / `FindBalancesInAccounts` exported with
  no external callers** — `strategy/portfolio.go:44, :75`. Both only
  used inside `strategy/portfolio.go` and its `_test.go`; the cross-
  package consumers go through `InitialBalancesFromPortfolio`.
  Unexport to shrink the shared API surface.
- **[RESOLVED — c5f267b] MCP `min_order_size` / `max_order_size` descriptions still say
  "copied order size"** — `mcp/tools_strategy.go:113-114` rewrote the
  types to `nonNegNum` but kept copytrading-specific descriptions
  while the field is now shared with momentum (different semantics:
  skip-open-below-min vs clamp-quantity-at-max).
- **Schema gap: spread-mode live ticks refetch per FRED outage** —
  see "Strategy" section; this is also a config-sensitivity issue
  worth a separate config option for max retries.

### Tests

- **[RESOLVED — c5f267b] Nine unused exports in `export_test.go`** —
  `strategy/momentum/export_test.go:27-36`: `LookupAssetID`,
  `CurrentPosition`, `BondQty`, `UsdBal`, `InitializeBalances`,
  `BalancesInitialized`, `EntryPrice`, `EntryATR`, `UpdateObs`.
  None are referenced by any test in the package (~45 lines of dead
  test API). Delete.
- **[RESOLVED — c5f267b] Vacuous `NotNil` on value-type return** —
  `strategy/momentum/benchmark_fallback_test.go:61` does
  `require.NotNil(t, got)` on a value-type `decimal.Decimal` return,
  which can never be nil. The `assert.True(got.Equal(4.25))` on the
  next line is the real check. Drop the NotNil.
- **[RESOLVED — c5f267b, 4 pin tests] Untested money-path branches in `strategy/portfolio.go`**:
  (a) leverage>1x isolated→global fallback at `:57-65`; (b) short-
  position reconstruction `bal.Bond = borrowed.Neg()` at `:114-118`;
  (c) quote-asset parse-error branch at `:89-93`; (d)
  `signalFromBondQty` at `:165-174`. Add a pin test for each.
- **[RESOLVED — c5f267b, `testCfg` helper] Duplicated 7-line config boilerplate across backtest tests** —
  `strategy/momentum/backtest_test.go:34-43, 51-57, 91-99` repeat the
  same DefaultConfig + window/stop/tp block; extract a `testCfg`
  helper. Ponytail nit only.
- **[RESOLVED — round-4 fixes, deterministic stop-loss assertion in backtest_test.go] Backtest tests don't assert exit-reason pinning at the
  integration level** — partial. `TestBacktest_OpensAndExits` now
  asserts a reversal exit must fire and trade records are paired;
  `TestBacktest_StopLossExits` asserts no-reversal on a stop-loss
  fixture but does not pin stop_loss as the exit reason (because the
  fixture's ATR-warmup timing can vary). A unit-level assertion in
  `strategy_test.go` covers the priority order; no further action
  needed unless someone wants an integration-level stop-loss test
  with a forced ATR seed.

### Docs / output

- **`strategy_backtest_closed_trades` has no `entry_atr` column** —
  the original `MomentumClosedTrade` shape included `EntryATR` but
  there is no DB column to read from (only `strategy_backtest_trades`
  has `entry_atr` per migration 011). The field has been removed
  from the JSON shape with a documenting comment. If a future
  schema migration adds `entry_atr` to closed_trades, restore the
  field. Cross-cutting; tracked here so the next migration owner sees
  the gap.
- **[RESOLVED — c5f267b] `strategies.md` Momentum section — `initial_balance` row inverts
  the runs-vs-backtests constraint** — `strategies.md` says "Live
  runs override with the user's USD balance" but does not say
  "backtests require > 0". Add the constraint to the table.
- **Plan file (1614 lines) was 80% trimmable** — the post-merge
  drift warning at the top of the plan + the reconciled Task 9 paths
  cover the most egregious issues; further trimming (e.g. removing
  the verbatim snippets now that they're known-buggy) is
  housekeeping.
- **README.md adds momentum directory tree** — accurate, no action
  needed.
- **OpenAPI `MomentumConfig` schema vs response side** — fast_ma /
  slow_ma / entry_atr are absent from the shared TradeRecord /
  ClosedTrade schemas. Pre-existing breakout gap
  (`compression_ratio`/`entry_atr` equally undocumented); out of scope
  here but worth filing a doc-cleanup follow-up if the team ever
  wants per-strategy response schemas.

### Cross-cutting

- **Shared `min/max_order_size` descriptions still say "copied order
  size"** — see "Decoders / config" section.
- **`InitialBalancesFromPortfolio` logs the same Info event twice on
  success** — `strategy/portfolio.go:156-159` logs Info "initialised
  balances from portfolio" and both adapters (`momentum/balances.go`,
  `meanreversion/balances.go`) immediately log the same event with
  runID. Delete the shared helper's Info line; keep the Warn lines.

## Momentum round-4 review — P2s resolved in working tree (2026-08-26, pre-commit)

Round-4 16-reviewer review confirmed the round-3 P2s and found two new
P2s. All five gating items are fixed in this working tree (uncommitted;
hashes to be added at commit time):

- **stop_loss_atr / take_profit_atr explicit-0 rewritten to defaults**
  (round-3 P2, unfixed by 0698791/c5f267b) — `momentumConfigPayload`
  fields are now `*float64` (nil→default, explicit 0→0), so the
  documented "0 disables" contract is reachable over HTTP and persists
  in normalized config. Regression: `TestHandlerMomentumExplicitZeroATR`
  (handler_test.go). NOTE: breakout shares the same 0-rewrite quirk
  pre-existing on development (handler.go:~3064) — fix it the same way
  when breakout is next touched.
- **order_book_id advertised Required:true but never validated** —
  decodeMomentumConfig now rejects empty order_book_id (400) instead of
  returning 201 and silently "completing" a run that never traded.
  Regression rows in `TestHandlerMomentumValidationErrors`.
- **Restart with an open position silently disabled stop-loss /
  take-profit** — `seedResumeAnchor` (strategy/momentum/strategy.go)
  runs after initializeBalances and anchors entryPrice/entryATR from the
  last clean price + ATR window mean when the run starts with an
  inherited position. Approximate anchor (not the true entry); upgrade
  path is StateStore-persisted anchors (twap/vwap pattern) if exact
  entry prices ever matter. Regression: `TestSeedResumeAnchor_*`
  (resume_anchor_test.go).
- **No PnL assertion anywhere in the momentum package** —
  `TestBacktest_ForceClose...` now asserts PnL = Exit×Qty−Entry×Qty per
  trade (same op order as computePnL; decimal arithmetic is not
  distributive), TotalPnL = Σ closed-trade PnL, and win/loss counts;
  `TestBacktest_OpensAndExits` asserts the Σ invariant across
  round-trips.
- **FRED alias→tenor mapping unpinned** — aliases and normalized-inputs
  subtests now assert the resolved Tenor value (a mis-edited alias
  table previously passed green), and TestNormalizeDate gained a
  UTC-vs-local-date distinguishing case.

## Momentum round-4 follow-ups (deferred P3s, 2026-08-26)

Non-blocking items from the round-4 16-reviewer review, grouped by area:

### HTTP / API docs
- [x] **OpenAPI momentum trade rows violate shared ClosedTrade/TradeRecord
  schemas** — RESOLVED (docs pass 2026-08-26): /trades and /closed-trades
  items are now oneOf with new MomentumTradeRecord / MomentumClosedTrade
  schemas; fast_ma/slow_ma documented as not-persisted. Dropping the dead
  fields from MomentumTradeRecord remains optional code cleanup. Breakout's
  equivalent drift is pre-existing and still undocumented.
- [x] **RunDetail.config oneOf missing MomentumConfig** (and BreakoutConfig)
  — RESOLVED (docs pass 2026-08-26): both refs added.
- [x] **initial_balance=0 for runs persisted as 1** — RESOLVED (code
  hygiene pass 2026-08-26): explicit 0 for runs now persists 0
  (decoder stores decimal.Zero; portfolio fetch failure then sizes off
  0 rather than the default 1). Pinned by
  TestHandlerMomentumExplicitZeroRoundTrip.
- [x] **OpenAPI initial_balance description omits the backtest must-be->0
  rule** — RESOLVED (docs pass 2026-08-26).

### Tests
- **Momentum resume path untested** — resumePersistedRun's SetDecisionSeq
  + applyUserAPIKey arms, awaitBacktestResult's momentum.BacktestResult
  case, atr_window>=2, max_position_size bounds, order_book_id
  parse-error rows.
- **Take-profit exits never exercised through the Backtester** (unit
  level only); spec §11 promises full priority coverage.
- **run_loop_test DATABASE_URL flake vector** — strategy built without
  SetHistoricalPriceStore falls through to env DATABASE_URL; inject
  FakeHistoricalPriceStore like historical_data_test.go does.
- **historical_data_test gaps** — spread-mode drop branch (price before
  first benchmark observation), prefillWindow spread/nil-YTM paths,
  TestPrefillWindow asserts call args only (not window population).
- [x] **meanreversion `s.errs` write-only** — RESOLVED (code hygiene
  pass 2026-08-26): logger.Error added in initializeBalancesFromPortfolio.

### Code hygiene
- [x] **exec.MustParseUUID dead alias** — RESOLVED (code hygiene pass
  2026-08-26): alias deleted, internal sites call
  strategy.MustParseUUID.
- [x] **meanreversion duplicate tenor logic** — RESOLVED (code hygiene
  pass 2026-08-26): local BenchmarkTenor table, parser, normalizer and
  normalizeDate deleted; call sites (and the handler /tenors endpoint)
  now use the fred package directly.
- **historical_data ordering contracts undocumented** — binary search
  needs ascending observations; prefill needs oldest-first history.
  One-line doc on each interface.
- [x] **exitRecord copies EntryATR onto exit rows** — RESOLVED: comment
  corrected to document the deliberate mirroring (matches the OpenAPI
  MomentumTradeRecord description).
- [x] **benchmark_fallback_test hand-rolled fake** — RESOLVED: swapped
  for momentumfakes.FakeBenchmarkYieldClient.
- [x] **run_loop_test comment misstates slowWin fill tick** — RESOLVED:
  comment corrected.

### Spec doc drift (doc-only) — RESOLVED (docs pass, 2026-08-26)
- [x] Spec §6.1 + strategies.md nil-YTM "dropped" wording → now describe the
      surfaced contract violation (218e905 behavior).
- [x] Spec §9.3 remainingBalance claim → documents fixed-capital sizing and
      the c5f267b removal.
- [x] Spec §9.1 per-tick DecisionRecorder wording → "record executed
      decisions"; startup ordering corrected (prefill → init → seedResumeAnchor).
- [x] Spec §4/§11 test file names → real files.
- [x] TODO.md Major "short stop-loss positive-fire" caveat → marked resolved
      (TestShouldExit_StopLoss_ShortFires, 0698791); the adjacent
      "backtest tests mutation-immune" item marked resolved by d8c4afc.
- [x] OpenAPI: RunDetail.config oneOf now includes BreakoutConfig +
      MomentumConfig; /trades and /closed-trades items are oneOf with new
      MomentumTradeRecord / MomentumClosedTrade schemas (fast_ma/slow_ma
      documented as not persisted); MomentumConfig.initial_balance
      description states the backtest >0 rule. Breakout's own trade-row
      drift remains pre-existing/undocumented.

## Post-merge follow-ups (cross-cutting, 2026-08-26)

Items surfaced by the momentum round-4 review that live outside the
momentum packages or were only noted inside resolved items. The
momentum-scoped test debt is tracked in the round-4 section above; these
are the cross-cutting remainders:

- **Breakout `stop_loss_atr` explicit-0 rewritten to default**
  (`strategy/http/handler.go` breakout decoder, pre-existing on
  development) — same quirk the momentum round-4 P2 fixed: non-pointer
  payload field makes explicit 0 indistinguishable from omitted, so a
  client sending 0 (per breakout's own "0 disables" docs, if any)
  silently gets the default stop distance. Fix the same way
  (`*float64` payload field) when breakout is next touched.
- **Exact-entry resume anchors via StateStore** — momentum's
  `seedResumeAnchor` re-derives entryPrice/entryATR from the startup
  price and current ATR window (approximate, documented). If exact
  entry prices ever matter for restart-time stop placement, persist the
  anchor via the per-run StateStore on open (twap/vwap pattern;
  `attachStateStore` currently wires only TWAP/VWAP).
- **Breakout trade-row OpenAPI drift** — breakout's compression_ratio /
  entry_atr fields are undocumented on the shared TradeRecord /
  ClosedTrade schemas, the same drift momentum just fixed with
  oneOf'd MomentumTradeRecord / MomentumClosedTrade. Add
  BreakoutTradeRecord / BreakoutClosedTrade schemas + oneOf the same
  way.
- **`notifications` WebSocket test flake (pre-existing on base)** —
  `TestHandler_AcceptsConfiguredOriginPattern` /
  `TestHandler_DeliversLiveEvents` intermittently fail with
  `conn.Read: failed to get reader: use of closed network connection`
  under full-suite parallel load; reproduced with `-parallel 1` on a
  clean stash of the branch base, so not introduced by the momentum
  branch. Unrelated to this merge — needs standalone investigation
  (suspect the ws-test-server background-reader / upgrade-race class).

## Copilot PR-review fixes (2026-08-26, applied to momentum)

All 10 unique findings from Copilot's PR #29 review addressed:
benchmark unit fix (×100 removed — cache stores fractions matching YTM),
getBenchmarkYield ok=false + tick skip, initializeBalances V2 fallback
to AssetPosition, InitialBalance always-synced (incl. zero, both paths),
spread tenor validated at decode, pointer fast/slow/atr windows +
max_position_size (explicit 0 rejected, not defaulted), trade-row
oneOf→anyOf, fred SupportedBenchmarkTenors deep copy, strategies.md
max_position_size row, shared process-lifetime price-history pool.

Meanreversion twins of two findings (pre-existing on development, NOT
fixed here):
- **meanreversion ×100 benchmark scaling** —
  strategy/meanreversion/historical_data.go:136,175 has the identical
  percent-vs-fraction mismatch in its FRED merge. Fix the same way
  (store observation.Yield unchanged) when meanreversion is next
  touched; its spread/zscore fixtures pin the scaled values.
- **meanreversion per-instance pgxpool leak** —
  strategy/meanreversion/historical_data.go:95 creates a pool per
  strategy instance, never closed. Momentum now uses a shared
  process-lifetime pool; port the same pattern.
