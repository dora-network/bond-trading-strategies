# dora-agent Integration — Re-plan

## Goal

Refresh the dora-agent integration approach to honor the work already landed on
`tan/feat-integrate-dora-agent` and replace the original per-task plan with a
bottom-up layer plan that lets the working tree stay green between layers.

The end state described in
[`docs/superpowers/specs/2026-09-08-dora-agent-integration-design.md`](2026-09-08-dora-agent-integration-design.md)
("the spec") is unchanged. What changes is the path: a single semantic user
commit aggregates six bottom-up dependency layers, with a working-tree
green-build checkpoint after each. The follow-up commit handles OpenAPI merge
and binary boot verification.

## Background

The original plan
([`docs/superpowers/plans/2026-09-08-dora-agent-integration.md`](../plans/2026-09-08-dora-agent-integration.md),
2465 lines) described Tasks 0.1 through 10.1 as if they were independent
commits. They are not. The deferred Tasks 4.2 / 4.3 / 4.5 / 4.6 / 4.7 / 4.8 /
5.1 / 5.2 share seams (envelope → AES-GCM, own `*sql.DB` → shared pool, tern
migration, route prefix, `agent.` schema qualification) and cannot land
independently without breaking the build between them.

Eight commits are already on the branch, with the spec's "Phases 0-3 + Phase 4
leaf packages + llm + config" landed and green:

| Commit | Subject |
| --- | --- |
| `947641e` | chore: create specs for integrating dora agent |
| `cb14943` | chore: update module dependencies |
| `9d32240` | chore(db): add consolidated agent table migration |
| `f42f144` | refactor(crypto): move encryptAPIKey/decryptAPIKey to internal/secrets as Encrypt/Decrypt |
| `0411af8` | feat(agent/secrets): add Sealer wrapper around internal/secrets for agent at-rest secrets |
| `9ec0fc2` | feat(agent): add TTL cache for /v1/user/self lookups |
| `eca6bb6` | feat(agent): copy leaf packages + llm + config from dora-agent |
| `e76c53c` | docs(agent): reflect partial phase 4 status; defer remaining tasks to follow-up |

Already-landed packages (no work in this re-plan's commit):

- `internal/agent/audit` (audit.go, wasm_actions.go + tests)
- `internal/agent/authcache.go` (TTL cache for /v1/user/self) + test
- `internal/agent/config` (config.go, dotenv.go, model_caps.go + tests)
- `internal/agent/llm` (agent.go, types.go, anyllm/, prompts/ + tests)
- `internal/agent/orderbroker` (broker.go + test)
- `internal/agent/safety/caps.go` (the kernel)
- `internal/agent/sanitize` (filter.go, classify.go + tests)
- `internal/agent/scan` (scan.go + test)
- `internal/agent/secrets` (Sealer wrapper) + test
- `internal/secrets/crypto.go` (AES-256-GCM, moved from strategy/http)
- `migrations/015_agent_consolidated_schema.sql` (creates the agent's tables
  in the dedicated `agent` schema)
- `authctx/` (Dora auth context propagation — already shipped by the host)

What's still missing is Tasks 4.2 / 4.3 / 4.5 / 4.6 / 4.7 / 4.8 / 5.1 / 5.2
plus the config env-var rename and the main.go mount. This re-plan covers that
remaining work as a single semantic user commit, organized as six bottom-up
dependency layers.

## Approach

**Bottom-up dependency layers, one user commit.** The remaining packages are
ordered so that each layer only depends on packages already in the working
tree (either from the eight landed commits or from prior layers in this
commit). After each layer, the working tree is green
(`go build ./...` + `go test ./...` + `pre-commit run --all-files`). If a
layer fails the checkpoint, the seam rewrite is fixed in place and the layer
re-runs. The final commit aggregates every layer; the user reviews the diff
and commits personally per the worktree's `AGENTS.md` gate.

**Test handling.** Tests are copied alongside production code. Tests that
reference `internal/e2e/*` or `cmd/agent-cli` are excluded (those targets are
being deleted in dora-agent's own repo; no place for them in
bond-trading-strategies). Tests that fail because of seam rewrites are fixed
in place. No new tests are written in this commit; the follow-up commit adds
tests surfaced by the live verification.

**Dora-agent repo is not touched.** This commit only writes to
bond-trading-strategies. The deletions of `cmd/agent-cli`, `internal/auth`,
`internal/secrets/{secrets,env,aesgcm,kms}.go`, `internal/store`,
`internal/migration`, `internal/serveradmin` happen in dora-agent's own repo
separately.

## Scope

In scope (this commit, single user-driven commit):

- Copy from `dora-agent/development/internal/{users,session,providerconfig,strategies,backtest,deployment,history,wasmruntime,wsbroker,orchestrator,tools,httpapi}` to `bond-trading-strategies/development/internal/agent/...`.
- Copy `internal/agent/wiring` (new package).
- Rewrite seams per the per-layer table below.
- Add `cmd/strategy-server/main.go` agent runtime mount.
- Wire `internal/agent/config` to read the renamed + kept env vars.
- Update `.env` and `.env.example` with the kept `AGENT_*` vars.
- Refresh `TODO.md` "dora-agent integration follow-ups" section to reflect the
  new state.
- Trim the existing 2465-line plan to a lean version describing L3–L8.

Out of scope (follow-up commit):

- OpenAPI spec merge.
- `cmd/strategy-server` binary launch verification.
- New tests surfaced by live verification.
- TODO.md closeout for the dora-agent integration follow-ups.
- Any deletions in the dora-agent repo itself.

## Architecture (re-plan specific)

### Layer sequence

Each layer is a working-tree checkpoint, not a separate commit. The user's
single semantic commit aggregates every layer.

| Layer | Packages added | Seam rewrites | Depends on |
| --- | --- | --- | --- |
| **L3** | `internal/agent/{users,session,providerconfig}` | `*sql.DB` → `*pgxpool.Pool`; `internal/secrets.KMS/Seal/Unseal` → `internal/agent/secrets.Sealer`; `agent.` schema prefix on every SQL. | L0–L2 (already landed). |
| **L4** | `internal/agent/{strategies,backtest,deployment}` | Pool + `agent.` schema. If they reach into `internal/history`, redirect to a thin indirection under `internal/agent/store` that L7 fills in; otherwise leave stubs and fix at L7. | L3 + `agent.audit` (landed). |
| **L5** | `internal/agent/wasmruntime/{registry,hostimpl,store}` + `internal/agent/wsbroker` + `internal/agent/orchestrator` | Promote `dora-strategy-wasm` + `wazero` to direct deps in `go.mod`. Pool + `agent.` schema. `wasmruntime/store` pool rewrite. | L4 + `agent.orderbroker` (landed). |
| **L6** | `internal/agent/tools/{dora,strategies,backtest,deployment,generate}` + `internal/agent/strategies/servertest` (rewrite, no `internal/migration`) | Drop `internal/migration` references; rewrite to use tern directly. Pool + `agent.` schema. | L5 + `agent.llm` (landed). |
| **L7** | `internal/agent/httpapi` (with `RoutesAt(basePath)`) + `internal/agent/store/history_store.go` | `httpapi`: drop `internal/auth.AuthMiddleware`, add `RoutesAt(basePath string)` parameter, register every pattern under the prefix. `history_store`: rewrite `New(ctx, dsn)` → `New(pool *pgxpool.Pool)`, `database/sql.QueryContext` → `pgxpool.Query`. | L6 + `agent.audit` (landed). |
| **L8** | `internal/agent/wiring` + `cmd/strategy-server/main.go` mount + `internal/agent/config` env-var wiring + `.env`/`.env.example` updates + `TODO.md` refresh | `wiring.Wire(ctx, pool, encryptionKey, cfg, log) (*Runtime, error)`. `Runtime.Server.Routes()` mounted at `/v1/agent/*` in `cmd/strategy-server/main.go` after `strategyhttp.NewHandler(...)`. Config reads renamed + kept env vars per the spec. | L7 + L1 (config, landed). |

Already-landed (no work in this commit; verified at L8):

- **L0** (spec's "Phase 4 leaf packages"): `internal/agent/{sanitize, scan, secrets, safety, orderbroker, audit}` + `authcache` + `llm` + `llm/anyllm` + `llm/prompts` + `config`. Note: there is no top-level `internal/agent/janitor` package in dora-agent; the capture-pending janitor lives in `internal/strategies/janitor.go` and the backtest janitor lives in `internal/backtest/janitor.go` — both copy into the matching `internal/agent/strategies/janitor.go` and `internal/agent/backtest/janitor.go` at L4, not as their own layer.
- **L1** (shared infra): `internal/secrets/crypto.go` + `migrations/015_agent_consolidated_schema.sql` + `authctx/`.
- **L2** (spec's "Phase 0-3"): pgx/v5 bump to v5.10.0, agent deps added, consolidated migration applied, crypto move, Sealer wrapper, /v1/user/self TTL cache.

### Per-layer seam rewrites — detail

**L3 — CRUD stores.** Three packages, each ~small:

- `users`: identity CRUD. `*sql.DB` → pool. SQL prefix. The `users` table is `agent.users` post-rename.
- `session`: chat-session CRUD. Same pattern. `agent.sessions`.
- `providerconfig`: per-user LLM provider config + sealed API key. This is the Sealer rewrite site — every `KMS.Seal` / `KMS.Unseal` call becomes `Sealer.Seal` / `Sealer.Open`. Column shape change: `provider_configs.api_key` is a single `bytea` (no `*_dek` columns per the spec). `agent.provider_configs`.

`users` and `session` are simple; `providerconfig` is the most error-prone because of the encryption seam. Mutation discipline: after rewriting, flip a known-wrong value in `Sealer` (e.g. return `nil` for the nonce) and re-run `providerconfig` tests; the encryption test should fail. Restore.

**L4 — orchestration stores.** `strategies` (strategy + strategy_version + strategy_capture_pending), `backtest` (backtest + backtest_trades + backtest_closed_trades), `deployment` (live deployment + per-strategy-decision stream). Pool + `agent.` schema. If any of them call `internal/history`, redirect to `internal/agent/store` indirection. The `backtest` package may pull in the framework-side `RunConfig`/Candle pagination types from the spec; those go through the shared `internal/agent/store/history_store.go` indirection that L7 fills in. For L4, the indirection is a no-op stub returning `nil` — the working tree stays green because the actual calls happen at runtime, not in the type-checked build.

`deployment` will likely reference `internal/agent/wsbroker` and `internal/agent/wasmruntime` for live-dep state. Stub the imports as unexported fields; L5 fills them in.

**L5 — live + WASM.** Three packages, the heaviest seam rewrite:

- `wasmruntime/{registry,hostimpl,store}`: wazero runtime registry. Promote `dora-strategy-wasm` + `wazero` to direct deps in `go.mod`. The agent's `registry.go` is the canonical "per-call wazero Runtimes" pattern (per the `wazero-per-call-runtime` skill); keep it as-is, but ensure the package compiles against the host's `dora-strategy-wasm` v0.3.3 pin. `wasmruntime/store` reads/writes strategy-version artifacts; rewrite to use the shared pool.
- `wsbroker`: Dora multiplex websocket client. Pool + `agent.` schema. Connects to `<DORA_BASE_URL>/plex` (or `WS_BROKER_URL` if set). `agent.wsbroker` (replaces the spec's reference to "the dora-agent's own wsbroker").
- `orchestrator`: live deployment orchestrator. Owns the per-deployment candle/price subscription state. Reference `wsbroker`, `wasmruntime`, `strategies` (L4), `audit` (L1).

**L6 — tools + servertest.** Five tool packages + one test helper:

- `tools/dora`: thin Dora API helpers (fetch candles, fetch trades, etc.) consumed by the LLM driver. Pool + `agent.` schema.
- `tools/strategies`: list/get strategies. Reference L4's strategies store.
- `tools/backtest`: run + get + cancel backtest. Reference L4's backtest store. This is where the `run_backtest` LLM tool lives; it calls into the dora-strategy-wasm framework's backtest runner via the framework's Go API and persists results to the L4 `backtest` store.
- `tools/deployment`: deploy / stop / resume / restart / hotswap. Reference L4's deployment store + L5's orchestrator.
- `tools/generate`: LLM-side strategy generator. The post-rewrite site for the `internal/migration` deletion. Currently the agent's `tools/generate` opens a fresh migration runner to apply the agent's schema; that goes away because the consolidated migration in `migrations/015_agent_consolidated_schema.sql` is the only DDL path. Remove the migration-related code; tern handles it.
- `strategies/servertest`: test helper used by `tools/strategies` and `tools/backtest`. Same migration deletion as `tools/generate`.

**L7 — httpapi + history_store.** Two packages, the headline seam changes:

- `httpapi`: HTTP handlers + `*http.ServeMux` for the agent's surface. Drop `internal/auth.AuthMiddleware` (replaced by the host's `requireAuth` mount chain). Add `RoutesAt(basePath string)` that registers every pattern under the prefix. The spec's route translation table is mechanical — every agent path becomes `/v1/agent/<thing>`. The new `Server.Routes()` is `RoutesAt("")` for unit tests, `RoutesAt("/v1/agent")` for the production mount.
- `store/history_store.go`: replaces `dora-agent/development/internal/history`. The current `history.New(ctx, dsn)` opens a `*sql.DB`; the rewrite accepts a `*pgxpool.Pool` and uses `pgxpool.Query` for the same SQL. Three fetchers: `FetchCandles`, `FetchTrades`, `FetchPrices`. The SQL targets the host's existing `candles_history` / `trades_history` / `price_history` tables (which live in `public`, not `agent` — different schemas for different concerns).

**L8 — wiring + main.go + config + .env.** The final wire-up:

- `internal/agent/wiring/wiring.go`:

  ```go
  package wiring

type Runtime struct {
    Server         *httpapi.Server
    StrategiesJani *strategies.Janitor       // capture-pending janitor (L4)
    BacktestJani   *backtest.Janitor         // backtest janitor (L4)
    WSBroker       *wsbroker.Broker
    LiveOrch       *orchestrator.Orchestrator
    WASMRuntime    *wasmruntime.Runtime
    // unexported stores, services
}

  func Wire(
      ctx context.Context,
      pool *pgxpool.Pool,
      encryptionKey []byte,
      cfg config.Config,
      log *slog.Logger,
  ) (*Runtime, error)
  ```
  The Runtime owns every agent dependency the httpapi handler needs. `Wire` returns the first error from any sub-construction; no silent fallbacks. `Runtime.Close()` cancels the strategies-janitor and backtest-janitor contexts, closes the WS broker, closes any open WASM runtime instances, and stops the capture-pending sweeper.

- `cmd/strategy-server/main.go` mount: after the existing `strategyhttp.NewHandler(...)` chain is built, construct the agent runtime via `agentwiring.Wire(...)` and mount `agentRuntime.Server.Routes()` at `/v1/agent/*` inside the existing `requireAuth`-wrapped mux. The agent's per-user rate limiter (`AGENT_RATE_LIMIT_PER_MIN`, default 20) is wired in the httpapi's middleware; the host's broader rate limiters (IP/global/read/write) wrap both subtrees as before.

- `internal/agent/config` env-var wiring: reads renamed + kept env vars per the spec. The renamed vars (`AGENT_DORA_BASE_URL` → `DORA_BASE_URL`, etc.) just read the host's existing var; the kept ones (`AGENT_LLM_TIMEOUT`, `AGENT_GENERATE_*`, etc.) read `AGENT_*` directly. The agent's old `internal/config/dotenv.go` is folded into `internal/agent/config/config.go`; it does not load `.env` (host's main already handles that).

- `.env`/`.env.example`: add the kept `AGENT_*` vars. Renames don't require new file entries.

- `TODO.md` "dora-agent integration follow-ups" section: refresh to reflect the new state. The deferred-out-of-this-commit list (OpenAPI merge, boot verification) gets marked; the deferred-out-of-this-replan list (dora-agent repo deletions) gets referenced.

### Layer checkpoint (every layer)

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go build ./...
go test ./...
go test ./internal/agent/...
pre-commit run --all-files
```

Every layer must pass all four commands. If a layer fails, fix the seam rewrite in place and re-run. The working tree stays green at every step.

## Data flow

The data flow is unchanged from the spec (section "Data flow" in
`2026-09-08-dora-agent-integration-design.md`). The re-plan does not change
the runtime behavior; it changes only the layer order in which the runtime is
constructed.

## Error handling

Unchanged from the spec. The re-plan adds no new error paths.

## Testing

Existing test suites stay green throughout. The agent's existing tests
(except `internal/e2e/*` and `cmd/agent-cli`-referencing tests, which are
excluded) are copied alongside their packages. Tests that fail because of
the seam rewrites are fixed in place.

Mutation discipline (where applicable): after fixing a test, mutate the
production code in a known-bad way (e.g. flip a `Sealer` nonce, drop an
`agent.` prefix, swap `RoutesAt` for a hard-coded path), confirm the test
fails, restore. Money paths (encryption, route prefix, schema qualification)
get the strongest checks.

The spec's integration tests (`/v1/agent/*` returns 401 unauthed, prefix
translation, per-user rate limit, mount order) and unit tests (Sealer
round-trip, history_store against shared pool, authcache TTL,
provider_configs column shape) are deferred to the follow-up commit alongside
boot verification.

## Risks

- **Layer-order assumption wrong.** The re-plan assumes L3 packages don't
  transitively reference L4-L7 packages. If `users` (L3) actually depends on
  `strategies` (L4), the layer needs to be reordered or the reference
  stubbed. Mitigation: run `go build ./...` after each layer; a compile
  failure at the layer boundary is a clear signal to reorder.
- **Schema-qualification drift.** Adding `agent.` to every SQL is mechanical
  but easy to miss. Mitigation: a `git grep -E "FROM (users|sessions|strategies|deployments|provider_configs|server_dora_credentials|audit_log|messages|backtests)"` at L8 surfaces un-prefixed references.
- **History_store indirection in L4 is a stub.** L4 may call into
  `internal/agent/store` for history fetches, but L7 is where the actual
  implementation lands. The L4 stub returns `nil` for those calls, which
  means the type-checked build is green but a runtime backtest would
  deadlock. Mitigation: not run any backtests in this commit. The follow-up
  commit does the live verification.
- **dora-strategy-wasm version drift.** The spec pins v0.3.3. If dora-agent
  has bumped the dep since 2026-09-08, the host's pin may be stale.
  Mitigation: L5 starts with a `go get
  github.com/dora-network/dora-strategy-wasm@<version-from-dora-agent>` and a
  note in the commit body if the version is bumped.
- **Existing pre-commit test suite crosses task boundaries.** The pre-commit
  `go-test-repo-mod` hook tests the entire repo; an intermediate state may
  not be runnable end-to-end even if the build is green. Mitigation: every
  layer checkpoint runs the full suite, not just the new packages.

## Acceptance criteria

This commit is complete when:

- All six layers (L3, L4, L5, L6, L7, L8) are in the working tree and every
  layer passed its checkpoint.
- `go build ./...` succeeds.
- `go test ./...` passes (no regressions on the host's existing tests).
- `go test ./internal/agent/...` passes (every copied test that was supposed
  to land is green; tests excluded for `internal/e2e/*` and `cmd/agent-cli`
  references are absent).
- `pre-commit run --all-files` is green.
- `cmd/strategy-server` builds (binary produced); it is not launched in this
  commit.
- `TODO.md` "dora-agent integration follow-ups" section reflects the new
  state.
- One user-driven commit with a conventional-commit body enumerating L3–L8
  packages, seam rewrites, tests copied, and what's deferred to follow-up.

The follow-up commit (separate session) closes out:

- `cmd/strategy-server` boots, connects to Postgres via `DATABASE_URL`,
  starts all agent dependencies without warnings.
- `curl -i http://localhost:8081/v1/agent/sessions` with no `Authorization`
  returns 401.
- `curl -i http://localhost:8081/healthz` returns 200.
- OpenAPI spec served at `/v1/openapi` includes the `/v1/agent/*` paths.
- `cmd/mcp-server` and `cmd/price-daemon` still pass their existing test
  suites.

## Notes for implementation

- The dora-agent repo at `/home/tanq/code/dora/repos/dora-services/dora-agent/development`
  is the source for every package copied in this commit. The host's
  `internal/agent/{audit,authcache,config,llm,orderbroker,safety,sanitize,scan,secrets}`
  is the source of truth for the L0–L2 packages already landed — do not
  re-copy those, do not re-rewrite their seams.
- The agent's `internal/auth.AuthMiddleware` is the only thing the httpapi
  loses. The host's `strategy/http/requireAuth` is already in the mount
  chain. The agent's `internal/auth.Authenticator`, `internal/auth.AuthService`,
  and the role gate (`AGENT_ALLOWED_DORA_ROLES`) are not in scope of this
  commit (they live in the dora-agent repo).
- The agent's `internal/secrets/{secrets,env,aesgcm,kms}.go` is replaced by
  `internal/agent/secrets` (already landed) + `internal/secrets/crypto.go`
  (already landed). The host's existing `strategy/http/crypto.go` was
  already moved out to `internal/secrets/crypto.go` in commit `f42f144`.
- The agent's `internal/store`, `internal/store/migrations`,
  `internal/migration` are not copied; the consolidated migration
  `migrations/015_agent_consolidated_schema.sql` is the only DDL path.
- The agent's `internal/serveradmin` and `cmd/agent-cli` are not copied;
  the admin listener and CLI are gone per the spec.
- The agent's `internal/history` is not copied as-is; it is replaced by
  `internal/agent/store/history_store.go` at L7.
- The dora-agent repo's own deletions (`cmd/agent-cli`, `internal/auth`,
  `internal/secrets/...`, `internal/store`, `internal/migration`,
  `internal/serveradmin`) happen in a separate commit on the dora-agent
  repo, not in this commit.
- The pre-commit hook's `go-test-repo-mod` runs the entire repo's tests.
  Intermediate layer states are designed to be green, but a layer that
  imports a not-yet-landed package will fail to compile. The layer order
  is the only thing preventing that.
- Trim the 2465-line plan to a lean version describing L3–L8 layers + per-layer
  seam rewrites + checkpoints. Keep the spec link and the test guidance.

## Spec self-review

Performed inline as part of writing this document:

- **Placeholder scan:** No "TBD" or "TODO" markers. Layer table is concrete.
- **Internal consistency:** Section "Layer sequence" matches section "Per-layer
  seam rewrites — detail" matches section "Notes for implementation." No
  contradictions.
- **Scope check:** One commit + one follow-up commit. Each commit has a
  single acceptance criteria list. Reasonable for two sessions.
- **Ambiguity check:** "Fix in place" on layer failure is explicit. The
  follow-up commit's scope is enumerated. The kept `AGENT_*` env vars are
  named in the spec, not just the re-plan; this re-plan doesn't need to
  re-enumerate them.
