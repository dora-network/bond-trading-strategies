# dora-agent Integration — Design

## Goal

Fold the dora-agent service (a standalone hosted AI agent for constructing, back-testing, and deploying bond strategies) into `bond-trading-strategies` so that:

- The agent's HTTP API is served from `cmd/strategy-server` under `/v1/agent/*`.
- Strategy-server is the only host process; the dora-agent binary and `cmd/agent-cli` are gone.
- The agent and strategy-server share one Postgres (DATABASE_URL) and one AES-256-GCM encryption key (ENCRYPTION_KEY).
- The existing bond-trading-strategies functionality is unchanged.

## Background

The two repos were developed in parallel and overlap heavily: same Dora auth (`ApiKey`/`Bearer` + `tenant-id`), same Postgres-backed tables, same 140-char Go conventions, same pre-commit gate, same dora-client-go dependency. The agent also depends on `dora-strategy-wasm` (a Go module for compiled WASM strategies) and `wazero`, both of which already live in the wider dora monorepo.

Today the agent's `main.go` boots its own pgxpool (AGENT_POSTGRES_DSN), its own auth middleware (validates Dora `/v1/user/self` + caches by TTL), its own per-user rate limiter, its own `internal/serveradmin` listener on 127.0.0.1:9090, and its own envelope encryption (AGENT_MASTER_KEY + DEK-wrapped per-secret keys). The agent has not been deployed to any environment; no data needs migrating at cutover.

The strategy-server already has:
- A pgxpool opened from `DATABASE_URL` (`cmd/strategy-server/main.go:138`).
- `strategy/http/requireAuth` (`strategy/http/auth.go`) doing the same Dora `/v1/user/self` flow with caching via `internal/auth/cache.go`.
- `strategy/http/crypto.go`'s `encryptAPIKey`/`decryptAPIKey` (AES-256-GCM under `ENCRYPTION_KEY`) used to seal per-run user Dora keys in `strategy_runs.encrypted_dora_api_key`.
- CORS, rate limiting, and a `/v1/notifications/ws` listener already wired.

## Approach

**Surgical copy-and-adapt.** Each agent `internal/*` package is copied into `internal/agent/*` of bond-trading-strategies. Only the seams that conflict with strategy-server's existing primitives are rewritten (auth, secrets, DB, env vars, route prefix). Strategy-server's `main.go` retains its current shape, gains a new `internal/agent/wiring.Wire(...)` call that constructs the agent runtime, and mounts the agent's `httpapi.Server.Routes()` handler at `/v1/agent/*` inside the existing authed mux. Single process, single Postgres, single ENCRYPTION_KEY.

Not chosen: rewriting the agent's HTTP layer against bond-trading-strategies primitives (too big a rewrite, easy to break the agent's spec), or splitting into a fourth binary (contradicts the in-process decision).

## Scope

In scope:

- `internal/agent/*` package tree — copy of every agent package the agent needs at runtime, renamed to live under `internal/agent/`.
- `migrations/012_agent_consolidated_schema.sql` — single consolidated migration that creates the agent's final schema in this repo's existing tern migrator.
- `internal/agent/wiring/wiring.go` — constructs the agent runtime from the shared pgxpool + encryption key + agent config + logger, returns a `*Runtime` whose `Server.Routes()` is the `/v1/agent/*` handler.
- `internal/agent/secrets/seal.go` — `Seal`/`Open` thin wrappers around `strategy/http/crypto.go`.
- `internal/agent/store/history_store.go` — candle/trade/price fetchers operating on the shared pgxpool (replaces agent's `internal/history`).
- `internal/agent/config/config.go` — agent-specific config (LLM timeout, generation limits, WASM, live).
- `internal/agent/janitor/` — capture-pending janitor goroutine.
- `internal/agent/authcache.go` — tiny in-memory cache for `/v1/user/self` results.
- Merged `openapi.json` — one spec served from `GET /v1/openapi`.
- Updated `cmd/strategy-server/main.go` — wires `agentwiring.Wire` and mounts the result.
- Updated flag set — agent-specific flags (no new flags for env vars that already exist in strategy-server).

Out of scope:

- dora-agent repo is not deleted as a git repo (operator decides). All code referenced by this design moves into bond-trading-strategies.
- cmd/mcp-server is unchanged (MCP stays strategy-only).
- cmd/price-daemon is unchanged.
- The agent's history-fetch SQL is unchanged; only the pool plumbing changes.
- Any feature work in the agent's routes — the agent's surface lands as-is.

## Architecture

### Auth + secrets seam

- `strategy/http/requireAuth` is the single inbound auth gate for both `/v1/*` and `/v1/agent/*`. The agent's HTTP handler is wrapped with it before being mounted at `/v1/agent/*`.
- Same `Authorization: ApiKey <key>` / `Bearer <token>` parsing, same optional `tenant-id` header forwarded via `authctx.AuthInfo`, same Dora `/v1/user/self` validation, same in-memory cache keyed by API key with `DORA_AUTH_CACHE_TTL` expiry (formerly `AGENT_AUTH_CACHE_TTL`).
- Agent's `internal/auth.Authenticator`, `internal/auth.AuthService`, `internal/httpapi.AuthMiddleware`, and the role gate (`AGENT_ALLOWED_DORA_ROLES` / TRADER|ADMIN|INTEGRATOR) are **deleted**. Strategy-server has no role gate today and adding one only for `/v1/agent/*` is not justified; if a role gate lands later, it lands for both subtrees.
- Agent's per-user token-bucket rate limiter (`AGENT_RATE_LIMIT_PER_MIN`, default 20) **stays** for `/v1/agent/*` and is **distinct from** strategy-server's IP/global/read/write rate limiters (which still wrap `/v1/*`). Double rate-limiting is deliberate: agent LLM turns are expensive.

**Encryption.** Replace the agent's envelope encryption (DEK + KMS-wrap under AGENT_MASTER_KEY) with `strategy/http/crypto.go`'s AES-256-GCM under the existing `ENCRYPTION_KEY`.

- `internal/secrets/{secrets.go,env.go,aesgcm.go,kms.go}` are deleted.
- New `internal/agent/secrets/seal.go` exposes `Seal(plaintext []byte) ([]byte, error)` and `Open(sealed []byte) ([]byte, error)`, both thin wrappers around `strategy/http.EncryptAPIKey` / `DecryptAPIKey`.
- `strategy/http/crypto.go` is moved up to a leaf `internal/secrets/crypto.go` so both `strategy/http` and `internal/agent/secrets` call into it. Existing callers (`strategy/http/run_store.go`'s encrypted_dora_api_key write path) keep working with a re-import.
- `server_dora_credentials.api_key` and `provider_configs.api_key` columns keep their names. The `*_dek` columns are dropped from the consolidated schema.

**No data migration.** The agent has not been deployed. No rows exist; the consolidated migration creates tables fresh.

### Database merge

**One database, one schema, one connection pool.** `DATABASE_URL` only. `AGENT_POSTGRES_DSN` and `AGENT_HISTORY_DSN` are removed.

**Pool sharing.** strategy-server's existing `*pgxpool.Pool` constructed at `cmd/strategy-server/main.go:138` is passed to `internal/agent/wiring.Wire(ctx, pool, encryptionKey, cfg, log)`. No second pool.

**Migrations.** The agent's 15 incremental migration files are **collapsed into one consolidated migration** named `migrations/012_agent_consolidated_schema.sql`. Built by reading the agent's `internal/store/migrations/*.sql` files in order and synthesizing a single DDL that creates the same final schema. Tables: `users`, `sessions`, `messages`, `provider_configs`, `strategies`, `strategy_versions`, `strategy_capture_pending`, `deployments`, `audit_log`, `server_dora_credentials`. Each audited for name collisions with the existing 11 tables; if any conflict is found, rename inline.

- `server_dora_credentials.api_key` is a single `bytea` column — no `_dek` columns.
- Agent's `internal/store/migrations`, `internal/store/store.go`, and `internal/migration/migrator.go` are deleted.
- bond-trading-strategies' existing tern migrator (`migrations/tern.conf`) runs all migrations including the consolidated one. Strategy-server's existing `tern migrate --config migrations/tern.conf` invocation continues to work.

**Audit table.** Agent's `audit_log` lands as its own table; bond-trading-strategies has no equivalent today. No merge.

**History DSN.** Agent's `internal/history` package is deleted. Candle/trade/price history fetches the agent's backtest runner and live orchestrator need are served by `internal/agent/store/history_store.go` operating on the shared pool. Same SQL, same `*Cursor` type, same return shapes.

### Agent runtime wiring

New package `internal/agent/wiring`:

```
internal/agent/wiring/wiring.go
  type Runtime struct {
    Server      *httpapi.Server
    Janitor     *janitor.Janitor
    WSBroker    *wsbroker.Broker
    LiveOrch    *orchestrator.Orchestrator
    WASMRuntime *wasmruntime.Runtime
    // ...unexported stores, services
  }

  func Wire(
    ctx context.Context,
    pool *pgxpool.Pool,
    encryptionKey []byte,
    cfg config.Config,
    log *slog.Logger,
  ) (*Runtime, error)
```

The `Runtime` owns every agent dependency the agent's HTTP layer needs: `*wasmruntime.Runtime`, `*wsbroker.Broker`, `*history.Store` (rewritten — see history_store.go), `*session.Store`, `*users.Store`, `*providerconfig.Store`, `*strategies.PgStore`, `*backtest.Orchestrator` + `backtest.Store`, `*deployment.Store`, `*orchestrator.Orchestrator` (live), `*safety.Kernel`, `auth.Service`, `*session.Store`, plus `httpapi.Server` with all options wired. `Runtime.Server.Routes()` is the HTTP handler mounted at `/v1/agent/*`.

The `Runtime` also exposes `Close()` for graceful shutdown, which cancels the janitor goroutine, closes the WS broker, and closes any open WASM runtime instances.

**WASM runtime.** `internal/wasmruntime` is copied as-is to `internal/agent/wasmruntime`. Same `wazero` dependency, same `AGENT_WASM_ARTIFACT_ROOT` env var (kept with the `AGENT_` prefix because it has no strategy counterpart).

**WS broker.** `internal/wsbroker` copies to `internal/agent/wsbroker`. Connects to Dora's multiplex websocket at `<DORA_BASE_URL>/plex` (or `WS_BROKER_URL` if set). Same orchestrator (`internal/orchestrator` → `internal/agent/orchestrator`).

**LLM + tools.** `internal/llm`, `internal/tools/generate`, `internal/tools/dora`, `internal/tools/backtest`, `internal/tools/deployment`, `internal/sanitize`, `internal/safety`, `internal/audit`, `internal/orderbroker`, `internal/scan`, `internal/providerconfig`, `internal/strategies`, `internal/backtest`, `internal/deployment`, `internal/users`, `internal/session`, `internal/config` — all copied to `internal/agent/*` (with package renames, e.g. `package auth` → `package authcache`). The existing agent's `internal/auth` package is deleted; a new `internal/agent/authcache.go` provides only the in-memory cache the user lookup needs.

**Janitor.** Agent's capture-pending janitor lives in `internal/agent/janitor`. Started by `Wire`, cancelled by `Runtime.Close`.

**Notifier.** No separate package — the agent's `internal/audit.BatchingWriter` already exists and is reused.

### Routes + middleware

**Mounting.** In `cmd/strategy-server/main.go`, after the existing `strategyhttp.NewHandler(...)` chain is built, construct the agent runtime via `agentwiring.Wire(...)` and mount `agentRuntime.Server.Routes()` at `/v1/agent/*` inside the existing `requireAuth`-wrapped mux.

**Route translation.** Every agent route is mounted under `/v1/agent/<thing>`. Concretely:

| Agent route | New mount path |
| --- | --- |
| `GET /healthz` | already exists in strategy-server — drop agent's |
| `GET /v1/openapi` | merged into bond-trading-strategies' spec (served from strategy-server's existing route) |
| `POST /v1/provider-config` | `POST /v1/agent/provider-config` |
| `GET /v1/provider-config` | `GET /v1/agent/provider-config` |
| `DELETE /v1/provider-config/{provider}` | `DELETE /v1/agent/provider-config/{provider}` |
| `POST /v1/sessions` | `POST /v1/agent/sessions` |
| `GET /v1/sessions` | `GET /v1/agent/sessions` |
| `GET /v1/sessions/{id}` | `GET /v1/agent/sessions/{id}` |
| `DELETE /v1/sessions/{id}` | `DELETE /v1/agent/sessions/{id}` |
| `POST /v1/sessions/{id}/messages` | `POST /v1/agent/sessions/{id}/messages` |
| `POST /v1/sessions/{id}/save` | `POST /v1/agent/sessions/{id}/save` |
| `GET /v1/strategies` | `GET /v1/agent/strategies` |
| `GET /v1/strategies/{id}` | `GET /v1/agent/strategies/{id}` |
| `GET /v1/strategies/{id}/versions` | `GET /v1/agent/strategies/{id}/versions` |
| `GET /v1/strategies/{id}/versions/{revision}` | `GET /v1/agent/strategies/{id}/versions/{revision}` |
| `POST /v1/strategies/{id}/halt` | `POST /v1/agent/strategies/{id}/halt` |
| `POST /v1/strategies/{id}/resume` | `POST /v1/agent/strategies/{id}/resume` |
| `POST /v1/strategies/{id}/restart` | `POST /v1/agent/strategies/{id}/restart` |
| `POST /v1/strategies/{id}/rollback` | `POST /v1/agent/strategies/{id}/rollback` |
| `POST /v1/strategies/{id}/versions/{revision}/deploy` | `POST /v1/agent/strategies/{id}/versions/{revision}/deploy` |
| `GET /v1/strategies/{id}/deployments` | `GET /v1/agent/strategies/{id}/deployments` |
| `GET /v1/strategies/{id}/deployments/{deployment_id}` | `GET /v1/agent/strategies/{id}/deployments/{deployment_id}` |
| `POST /v1/strategies/{id}/deployments/{deployment_id}/stop` | `POST /v1/agent/strategies/{id}/deployments/{deployment_id}/stop` |
| `GET /v1/strategies/{id}/deployments/{deployment_id}/logs` | `GET /v1/agent/strategies/{id}/deployments/{deployment_id}/logs` |
| `POST /v1/strategies/{id}/deployments/{deployment_id}/resume` | `POST /v1/agent/strategies/{id}/deployments/{deployment_id}/resume` |
| `POST /v1/strategies/{id}/deployments/{deployment_id}/restart` | `POST /v1/agent/strategies/{id}/deployments/{deployment_id}/restart` |
| `POST /v1/strategies/{id}/deployments/{deployment_id}/hotswap` | `POST /v1/agent/strategies/{id}/deployments/{deployment_id}/hotswap` |
| `POST /v1/strategies/{id}/versions/{revision}/backtest` | `POST /v1/agent/strategies/{id}/versions/{revision}/backtest` |
| `GET /v1/strategies/{id}/backtests` | `GET /v1/agent/strategies/{id}/backtests` |
| `GET /v1/strategies/{id}/backtests/{backtest_id}` | `GET /v1/agent/strategies/{id}/backtests/{backtest_id}` |
| `POST /v1/strategies/{id}/backtests/{backtest_id}/cancel` | `POST /v1/agent/strategies/{id}/backtests/{backtest_id}/cancel` |

The agent's `httpapi.Server.Routes()` is updated to accept a `basePath` parameter and register every pattern under that prefix. This is mechanical.

**Middleware order.** A `/v1/agent/*` request flows through:

1. Strategy-server's existing CORS (`cors.CORSMiddleware`).
2. Strategy-server's existing IP/global/read/write rate limiters.
3. Strategy-server's existing `requireAuth` (parses Authorization, resolves Dora user, sets `authctx.AuthInfo`).
4. Agent's per-user token-bucket rate limiter (`AGENT_RATE_LIMIT_PER_MIN`, default 20).
5. Agent's request-ID + logging middleware (in the agent's `httpapi.Server.Routes()`).
6. The agent's `*http.ServeMux`.

This means a `/v1/agent/*` request is rate-limited twice: once by strategy-server's broader limiters (cheap rejection of obviously-bad callers), once by the agent's per-user bucket (protects against a single user monopolizing the LLM turn driver). Both are deliberate.

**`/admin/*`.** Agent's admin listener on `127.0.0.1:9090` is gone. `internal/serveradmin`, `/admin/init`, `/admin/rotate-key`, `/admin/whoami` are deleted. `cmd/agent-cli` is deleted. `server_dora_credentials` is still written to at startup by `Wire` (seeding the admin key if `DORA_ADMIN_API_KEY` is set in env).

### OpenAPI merge

Single spec served from `GET /v1/openapi` (already auth-exempt in strategy-server). The agent's original `/v1/openapi` route handler is dropped; its embedded `openapispec.Spec` constant is merged into the existing strategy-server spec.

- One `info.title`, one `info.version`, one `servers` block.
- Bond-trading-strategies' existing paths remain.
- Agent's paths are added under `/v1/agent/*` with full request/response schemas.
- Same security scheme (`ApiKey`, `Bearer`, optional `tenant-id`).

Source of truth: the existing OpenAPI merging pattern in this repo (verified during implementation). If programmatic, one new build input; if hand-edited JSON, the merge happens once during implementation and the result is a checked-in file.

### Env vars

Rename rule: env vars that already have a bond-trading-strategies counterpart drop the `AGENT_` prefix; the rest keep `AGENT_`.

**Dropped entirely:**
- `AGENT_POSTGRES_DSN` → already `DATABASE_URL`.
- `AGENT_HISTORY_DSN` → history package deleted.
- `AGENT_MASTER_KEY` → envelope encryption deleted.
- `AGENT_ADDR`, `AGENT_ADMIN_ADDR` → strategy-server owns the listener on `ADDR`.
- `AGENT_ADMIN_URL`, `AGENT_ADMIN_TOKEN_HASH`, `AGENT_ADMIN_TOKEN_FILE`, `AGENT_HEALTH_ADDR`, `AGENT_HEALTH_MAX_ATTEMPTS` → admin listener deleted.
- `AGENT_DORA_API_KEY` → replaced by `DORA_ADMIN_API_KEY` for `server_dora_credentials` seeding.
- `AGENT_ALLOWED_DORA_ROLES` → role gate removed.

**Renamed:**
- `AGENT_DORA_BASE_URL` → `DORA_BASE_URL`.
- `AGENT_LOG_LEVEL` → `LOG_LEVEL`.
- `AGENT_CORS_ALLOWED_ORIGINS` → `CORS_ALLOWED_ORIGINS`.
- `AGENT_AUTH_CACHE_TTL` → `DORA_AUTH_CACHE_TTL` (caches `/v1/user/self`, shared across the codebase).

**Kept as-is (no overlap):**
- `AGENT_RATE_LIMIT_PER_MIN` — agent's per-user bucket; strategy-server's rate limiters use different env vars.
- `AGENT_LLM_TIMEOUT`, `AGENT_LLM_MAX_ITERS`, `AGENT_MAX_PROMPT_BYTES`.
- `AGENT_MODEL_CAPS_PATH`.
- `AGENT_DORA_TOOLS_ENABLED`.
- `AGENT_GENERATE_*` (BaseImage, CPU, Memory, Timeout, StartTimeout, MaxRepairs, MaxFiles, MaxBytes, GOPROXY, Allowlist).
- `AGENT_LIVE_*` (MemoryLimit, RestartWindow, MaxRestarts).
- `AGENT_WSBROKER_URL`.
- `AGENT_CAPTURE_PENDING_*` (Retention, SweepInterval).
- `AGENT_WASM_ARTIFACT_ROOT` (kept with `AGENT_` prefix; no overlap).
- `AGENT_ALLOW_HTTP_BASE_URL`.

`.env` (or `.env.example`) gains the agent-specific keys. The agent's old `env.example` is folded into the merged file.

### Code that's deleted

**Deleted and not replaced:**
- `cmd/agent-cli/` — admin CLI.
- `internal/serveradmin/` — loopback admin HTTP listener.

**Deleted and replaced by bond-trading-strategies primitive:**
- `internal/auth/` → replaced by strategy-server's `requireAuth` + new `internal/agent/authcache.go`.
- `internal/secrets/{secrets.go,env.go,aesgcm.go,kms.go}` → replaced by `internal/agent/secrets/seal.go` thin wrapper around `strategy/http/crypto.go`.
- `internal/store/` → replaced by shared pgxpool.
- `internal/migration/` → replaced by bond-trading-strategies' tern migrator.
- `internal/history/` → replaced by `internal/agent/store/history_store.go` operating on the shared pool.

**Admin-key rotation: option A — delete entirely.** Operator wipes the `server_dora_credentials` row by hand and re-deploys with `DORA_ADMIN_API_KEY`. No `POST /v1/agent/admin/rotate-key`, no `cmd/admin-cli`. The internal helper that sealed/unsealed the row goes away.

## Data flow

### Inbound /v1/agent/* request (typical)

```
client → CORS → ratelimit(IP/global/r/w) → requireAuth
       → DORA /v1/user/self (cached) → authctx.AuthInfo
       → agent per-user ratelimit → agent request-ID/log
       → httpapi.Server mux → agent handler → response
```

### LLM-driven agent turn (POST /v1/agent/sessions/{id}/messages)

```
client → (middleware as above) → httpapi handlePostMessage
       → agent_runner.RunStrategyTurn
         → providerFactory.Build (decrypts provider_configs.api_key via seal.Open)
         → any-llm-go provider call → tool dispatch loop
           → generate_strategy → wasmruntime validator → strategies.PgStore
           → run_backtest → history_store.FetchCandles (shared pool)
           → get_backtest_result → backtest.Store
         → emit SSE events → response
```

### Live deployment (POST /v1/agent/strategies/{id}/versions/{revision}/deploy)

```
client → (middleware) → httpapi handleDeploy
       → orchestrator.Deploy
         → seal.Open(server_dora_credentials.api_key)
         → wsbroker.Subscribe (Dora multiplex websocket)
         → wasmruntime.Load (per-call wazero instance, AGENT_WASM_ARTIFACT_ROOT)
         → strategies.PgStore write → audit_log → response
```

### Backtest fetch (strategy framework calls history_store)

```
framework WASM host fn → on_preamble / on_trade / on_price
      → agent history_store.FetchCandles/Trades/Prices (shared pgxpool)
       → candles_history / trades_history / price_history → response
```

## Error handling

- Agent's HTTP layer keeps its existing 401/403/404/409/422/500/502 status mapping.
- Agent's `auth required` middleware (now strategy-server's `requireAuth`) returns 401 on missing/unrecognised Authorization. Tenant-id is optional.
- Agent's role gate is gone — no 403 for "wrong role".
- Agent's per-user rate limit returns 429 with `Retry-After`.
- Strategy-server's broader rate limiters return 429 without `Retry-After` (existing behavior).
- Strategy-server's existing CORS rejects preflight failures; same behavior on `/v1/agent/*`.
- 502 from upstream Dora stays 502 (existing behavior).
- Agent's streaming SSE handler streams partial events on the way out; if the connection drops mid-turn, the agent runner logs and discards (existing behavior).

## Testing

**Existing test suites stay green throughout.** Bond-trading-strategies' `go test ./...` and `pre-commit run --all-files` must keep passing after every commit. The agent's existing tests get copied alongside their packages into `internal/agent/*`.

**New integration tests:**

1. **Agent routes are authed.** Boot strategy-server's full HTTP chain (existing test pattern). Hit `GET /v1/agent/sessions` with no `Authorization` header, assert 401. Repeat for `GET /v1/agent/strategies`, `POST /v1/agent/sessions`, and one backtest endpoint.
2. **Routes resolve under `/v1/agent/*`, not bare `/v1/*`.** Hit `GET /v1/sessions`, expect 404 (or whatever the strategy-server's bare behavior is). Hit `GET /v1/agent/sessions`, expect 401 (above). Confirms prefix translation.
3. **Per-user agent rate limiter returns 429 with Retry-After.** Burn the bucket, assert 429 + Retry-After header.
4. **Mount order is correct.** A single request goes through both rate limiters in order (verified by counters / log markers).

**New unit tests:**

5. **Thin crypto wrapper round-trip.** `internal/agent/secrets.Seal`/`Open` over plaintext of empty / 32 bytes / 1 MiB. Confirms the wrapper works end-to-end and that the underlying AES-GCM is exercised.
6. **`history_store` fetches against the shared pool.** Spin up a testcontainers Postgres, run the consolidated migration, insert known rows into `candles_history`/`trades_history`/`price_history`, call the agent's `history.Store.FetchCandles/Trades/Prices`. Verifies the rewritten `internal/agent/store/history_store.go` is correct against the merged schema (catches column-name drift).
7. **Authcache TTL.** Insert a fake `/v1/user/self` resolver; assert cache hits within TTL, miss after expiry.
8. **`provider_configs.api_key` column shape.** Insert via store, read back via store, decrypt, assert plaintext matches. Confirms the column type and seal format are right.

**Mutation discipline.** For each new test, mutate the production code in a known-bad way, re-run, confirm the test fails. Restore. Document mutations in `TODO.md` follow-up section. Money paths (encryption, `/v1/user/self` cache, agent's per-user rate limiter) get the strongest mutation checks.

**No test for removed code.** We don't test that the deleted `cmd/agent-cli` is gone. We test that operator rotation works (manual: `UPDATE server_dora_credentials SET api_key = <new sealed>; redeploy`).

## Risks

- **Consolidated migration drift.** The 15→1 migration collapse could miss a constraint, index, or default that the agent relies on. Mitigation: copy each agent migration file into a scratch directory during implementation, run all 15 against a fresh testcontainers Postgres, then `pg_dump --schema-only` the final schema and use that as the consolidated migration source.
- **Auth-cache consistency.** Two callers sharing `internal/agent/authcache.go` might race on the underlying map. Mitigation: store uses `sync.Map` or a mutex; test with concurrent reads.
- **Secret encryption format mismatch.** Anything the agent sealed under envelope encryption must not exist (no production data), but if a dev row sneaks in, the open call panics. Mitigation: panic-recover in the Open path with a clear "rotation required" error message; document in the spec.
- **Janitor leak.** A wedged janitor goroutine in `Wire` would prevent the strategy-server from shutting down cleanly. Mitigation: `Runtime.Close` cancels the janitor ctx first; the existing pattern from agent's `main.go` carries over.
- **WASM runtime + pgxpool lifetime.** The WASM runtime holds a reference to its host module functions; if a strategy captures a `*pgxpool.Pool` reference via the host callback and the pool closes, use-after-free. Mitigation: strategies don't get direct DB access; they call back into `history_store` which uses a fresh context.
- **Doubled rate-limiting may surprise operators.** Document explicitly in `cmd/strategy-server/main.go` flag help text and in `README.md`.

## Future work (out of scope for this change)

- Role gate (TRADER/ADMIN/INTEGRATOR) reintroduced service-wide if needed.
- OpenAPI spec regeneration tooling (currently the spec is hand-edited JSON).
- Caching of `GET /v1/agent/strategies` results for hot keys (Redis).
- HA-friendly rate limit storage (Redis token bucket).
- A "reset" endpoint for `server_dora_credentials` (rotating keys without re-deploy).

## Acceptance criteria

The change is complete when:

- `cmd/strategy-server` boots, connects to Postgres via `DATABASE_URL`, opens `cmd/strategy-server/main.go`'s listener, and starts all agent dependencies (WASM runtime, wsbroker, history store, LLM driver, capture-pending janitor) without warnings.
- `go test ./...` passes for bond-trading-strategies.
- `go test ./internal/agent/...` passes for the moved agent code.
- `pre-commit run --all-files` is green.
- `curl -i http://localhost:8081/v1/agent/sessions` with no Authorization returns 401.
- `curl -i http://localhost:8081/healthz` returns 200.
- `curl -i http://localhost:8081/v1/openapi` returns the merged spec including `/v1/agent/*` paths.
- `cmd/mcp-server` and `cmd/price-daemon` boot and run their existing test suites.
- dora-agent repo can be archived (no live references from this repo).

## Notes for implementation

- `strategy/http/crypto.go` → move to `internal/secrets/crypto.go`; update both call sites.
- `internal/agent/wiring.Wire` should construct every store eagerly and return the first error — no silent fallbacks (the existing agent's main.go construction order is the template).
- The consolidated migration must run after `011_add_breakout_trade_columns.sql` (sequence number `012_*`); verify the migrator is purely ordered and not lexicographic.
- The agent's `internal/config/dotenv.go` becomes part of `internal/agent/config/config.go`; it does not load `.env` (strategy-server's main already handles that).
- Operator rotation of `DORA_ADMIN_API_KEY`: drop the row, redeploy with the new env var. No application code path.
