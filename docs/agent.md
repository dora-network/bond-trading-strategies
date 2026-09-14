# Agent (dora-agent on bond-trading-strategies)

## Introduction

The agent is an AI assistant embedded in the bond-trading-strategies
service that helps users construct, back-test, and (with WASM plugins)
deploy trading strategies on the Dora Network. It is served by the
same `cmd/strategy-server` binary as the rest of the strategy stack;
the agent's HTTP API lives at `/v1/agent/*` and shares the
strategy-server's auth gate.

Users describe their strategy to the agent. The agent compiles the
description into a Go module that implements the `dorastrategy.Strategy`
interface, validates the module against the framework's surface, and
runs backtests against historic Dora candle data:

- **`go-wasm`**: the strategy module is compiled to a `.wasm` blob with
  TinyGo and run in-process via wazero. The agent's `WasmStarter` drives
  the backtest loop directly, pre-fetches historic candles, simulates
  fills, and persists results to the same store.

Backtests are submitted via the HTTP API
(`POST /v1/agent/strategies/{id}/versions/{rev}/backtest`) into the
`agent.backtests` + `agent.backtest_fills` schema.

There is no separate agent binary, admin listener, or operator CLI in
this integration — the agent is a subtree (`internal/agent/...`) of the
strategy-server process.

## Quick start

### 1. Configure environment

The agent shares `cmd/strategy-server`'s `.env`. Required values:

- `DATABASE_URL` — Postgres DSN for the host's and the agent's tables.
- `ENCRYPTION_KEY` — base64-encoded 32-byte AES-256-GCM key. Generate
  with `head -c 32 /dev/urandom | base64`.
- `DORA_BASE_URL` — the upstream Dora REST API base URL
  (e.g. `https://dev.dora.co`).
- `DORA_API_KEY` — your Dora API key; authenticates agent requests
  through the shared auth gate.
- `DORA_AUTH_CACHE_TTL` — TTL for the cached `/v1/user/self` lookup.
  Default `5m`.
- `CORS_ALLOWED_ORIGINS` — comma-separated exact origins allowed for
  the chat UI (e.g. `http://127.0.0.1:8080`). Empty = CORS disabled.
- `LOG_LEVEL` — slog level: one of debug, info, warn, error.

The listen address comes from the host's `ADDR` (default `:8081`).

Agent-specific knobs (kept under the `AGENT_` prefix; all optional):

- `AGENT_RATE_LIMIT_PER_MIN` — per-user token-bucket cap on the agent
  subtree. Default 20.
- `AGENT_LLM_TIMEOUT`, `AGENT_LLM_MAX_ITERS`, `AGENT_MAX_PROMPT_BYTES` —
  LLM turn-driver knobs.
- `AGENT_MODEL_CAPS_PATH` — model capabilities JSON. Defaults to
  `configs/model_caps.json`.
- `AGENT_DORA_TOOLS_ENABLED` — enable the read-only Dora tools.
- `AGENT_GENERATE_GOPROXY`, `AGENT_GENERATE_ALLOWLIST` — GOPROXY and
  module allowlist for the strategy validator.
- `AGENT_GENERATE_MAX_REPAIRS` — max LLM repair attempts. Default 2.
- `AGENT_GENERATE_MAX_FILES` — max strategy source files. Default 50.
- `AGENT_GENERATE_MAX_BYTES` — max strategy source bytes. Default 1 MiB.
- `AGENT_LIVE_MEMORY_LIMIT` — per-instance wazero memory cap.
  Default 256 MiB.
- `AGENT_LIVE_RESTART_WINDOW`, `AGENT_LIVE_MAX_RESTARTS` — live
  deployment restart budget knobs.
- `AGENT_WSBROKER_URL` — Dora multiplex websocket base URL. Empty
  defaults to `<DORA_BASE_URL>/plex`.
- `AGENT_CAPTURE_PENDING_RETENTION`, `AGENT_CAPTURE_PENDING_SWEEP_INTERVAL` —
  capture-pending sweeper knobs.
- `AGENT_WASM_ARTIFACT_ROOT` — where compiled `.wasm` artifacts live.
  Default `/tmp/bond-trading-strategy-agent-wasm`.
- `AGENT_ALLOW_HTTP_BASE_URL=1` — relax the `https://` check on provider
  `base_url` for local development.

#### WASM artifact storage

Compiled `.wasm` blobs are persisted in Postgres via the `pgstore`
delegate (`internal/agent/wasmruntime/store/pgstore`). Every Put
writes both `agent.wasm_artifacts.bytes` and the local on-disk CAS
under `AGENT_WASM_ARTIFACT_ROOT`. Get serves from the local CAS on
the hot path and falls back to the DB on a cold Fargate restart,
re-materializing the local file. The local cache exists purely as
performance; the DB is the durable layer. Production must keep
`DATABASE_URL` set; a nil pool falls back to FS-only (log warn) and
loses durability across restarts.

Environment variables from the standalone dora-agent service that
duplicated a host var (DSN, keys, base URL, listen address, CORS, log
level) were consolidated into the host vars above. The standalone
service's admin-listener and role-gate variables no longer exist.

### 2. Run database migrations

The host's `tern` migrator runs all migrations in numeric order on
`migrations/`. The agent's `015_agent_consolidated_schema.sql` creates
the agent's tables in a dedicated `agent` schema (not `public`), so
they are isolated from the host's tables.

```sh
tern migrate --config migrations/tern.conf
```

See the host README's "Database and migrations" section for the full
workflow.

### 3. Start the server

The agent runs as part of `cmd/strategy-server`. There is no separate
agent binary to launch.

```sh
make start-strategy-server
```

The server listens on `:8081` (configurable via `ADDR`).
`GET /healthz` returns `200 ok`. `GET /v1/openapi` returns the merged
OpenAPI spec including the agent's paths. If the WASM runtime is
enabled, the server logs `wasm runtime enabled` with the artifact root.

### 4. Use the chat UI

A single-page web client lives in `docs/chatui/`. Serve it with any
static file server:

```sh
cd docs/chatui
python3 -m http.server 8080
```

Open `http://127.0.0.1:8080/`, then:

1. Paste the strategy-server's base URL (e.g. `http://localhost:8081`)
   and your Dora `ApiKey` in the header. Click **save**.
2. **Configure the LLM provider** in the sidebar (provider, api_key,
   default_model, optional base_url for OpenRouter / local proxies).
   Click **save**.
3. **Create a session** with provider/model and an optional title.
4. **Send prompts**; the UI streams SSE deltas inline; refusals and
   errors render as distinct bubbles.
5. **Browse session history** by selecting a different session in the
   sidebar — every turn is persisted to the agent's `messages` table.

The chat UI lives at a different origin from the API. Set
`CORS_ALLOWED_ORIGINS` to the UI's origin (e.g.
`http://127.0.0.1:8080`) or the browser blocks the request and the UI
shows network errors.

For manual end-to-end verification of the integrated binary, see
`docs/agent-binary-smoke.md` (to be merged into this doc in a future
cleanup).

### 5. Hit the API directly (curl / httpie)

```sh
# Health
curl -s http://localhost:8081/healthz

# Save provider config
curl -X POST http://localhost:8081/v1/agent/provider-config \
  -H "Authorization: ApiKey $DORA_KEY" \
  -H "Content-Type: application/json" \
  -d '{"provider":"openai","api_key":"sk-...","default_model":"gpt-4o-mini"}'

# Create a session
SID=$(curl -s -X POST http://localhost:8081/v1/agent/sessions \
  -H "Authorization: ApiKey $DORA_KEY" -H "Content-Type: application/json" \
  -d '{"provider":"openai","model":"gpt-4o-mini"}' | jq -r .session_id)

# Stream a prompt
curl -N -X POST http://localhost:8081/v1/agent/sessions/$SID/messages \
  -H "Authorization: ApiKey $DORA_KEY" -H "Content-Type: application/json" \
  -H "Accept: text/event-stream" \
  -d '{"prompt":"Design a duration strategy for BOND asset"}'

# Submit a backtest against a strategy version
curl -X POST http://localhost:8081/v1/agent/strategies/$SID/versions/$REV/backtest \
  -H "Authorization: ApiKey $DORA_KEY" -H "Content-Type: application/json" \
  -d '{"start":"2026-01-01T00:00:00Z","end":"2026-01-08T00:00:00Z","resolution":"1h","order_book_id":"OB-1"}'
```

The auth scheme is `Authorization: ApiKey <token>`. The agent's
middleware only accepts the literal `ApiKey` prefix; the broader
OpenAPI convention (`Bearer`) is rejected.

## Verifying the install (binary smoke)

After `make start-strategy-server` (or an equivalent manual launch of
`cmd/strategy-server`) is running, verify the HTTP surface responds correctly:

```sh
# Health check — must return 200, no auth required.
curl -i http://localhost:8081/healthz

# Agent subtree auth wall — must return 401 without Authorization.
curl -i http://localhost:8081/v1/agent/sessions

# OpenAPI spec — must return 200 and list /v1/agent/* paths.
curl -i http://localhost:8081/v1/openapi | head -50
```

Expected responses:

- `GET /healthz` → `200 OK`, body `ok` (or JSON health report).
- `GET /v1/agent/sessions` (no auth) → `401 Unauthorized`. The host's
  `requireAuth` blocks the request before the agent's handler runs.
- `GET /v1/openapi` → `200 OK`, body is a JSON OpenAPI 3.1 document
  that includes paths under `/v1/agent/*` (per the merged spec,
  `docs/openapi/strategy-server.json`). The
  `paths./v1/agent/sessions.get.operationId` should be present.

**Note on the port.** The default is `:8081` (`cmd/strategy-server/main.go`
`-a` flag, overridable via the `ADDR` env var). If `ADDR` or `STRATEGY_ADDR`
is set in the environment (the Makefile passes `STRATEGY_ADDR` to `-a`), the
curl commands must use the same port. Verify with `ss -tlnp | grep strategy-server`
or read the binary's startup log line `strategy server starting addr=...`.

## Architecture

### Repos

- `bond-trading-strategies` (this repo): the host. The agent lives in
  `internal/agent/...` and is served by `cmd/strategy-server`.
- `dora-strategy-wasm` (pinned Go module dependency): the framework
  every generated strategy imports. Defines `dorastrategy.Strategy`,
  the wasmimport host surface (`dorastrategy/host`), and the build-time
  allowlist. The parent go.mod pins the version.

### Strategy pipeline (go-wasm, the default for new strategies)

1. **Generate**: the LLM authors `main.go` + `strategy.go` + `go.mod`
   implementing `dorastrategy.Strategy { Init, OnCandle }`. The system
   prompt embeds the framework's contract verbatim so the LLM knows
   the exact surface.
2. **Capture**: the agent's capture observer content-addresses the
   files, persists a `strategy_versions` row with
   `target='go-wasm'` + `wasm_ref` + `manifest_hash`.
3. **Validate**: the validator builds the module to `.wasm` via TinyGo,
   then instantiates it under wazero with the real host module. Catches
   missing imports, bad allowlist use, and non-network-free Init.
4. **Backtest**: `POST /v1/agent/strategies/{id}/versions/{rev}/backtest`
   inserts the queued row, registers the per-job cancel context, and
   spawns the `WasmStarter` goroutine.
5. **WasmStarter flow**:
   - Pre-fetch historic candles via the Dora SDK, paginated over the
     ~1900 candle API cap (1500-candle chunks).
   - Build a per-job wazero runtime + host module with closures
     capturing the candle slice + fill collector.
   - Load the compiled `.wasm` via the wazero compile cache.
   - Instantiate with the host module; `_start` runs `main()` →
     `dorastrategy.Run()` → the backtest loop.
   - Loop: pull a candle from the host, call `s.OnCandle`, submit
     intents (market = close, limit = price within low/high), record
     fills.
   - Compute the summary over the fill series, persist fills + summary
     + terminal status.

## OpenAPI spec

The full HTTP surface is documented in
`docs/openapi/strategy-server.json` (OpenAPI 3.1) and served at
`GET /v1/openapi` (auth-exempt). The merged spec includes:

- The host's `/v1/*` paths (strategy-server's own surface).
- The agent's `/v1/agent/*` paths — every agent endpoint is mounted
  under the `/v1/agent/` prefix.
- Components prefixed with `Agent` where they would collide with the
  host's components (e.g. `AgentSession`).

## Building

The agent is part of the host's `cmd/strategy-server` build; there is
no separate binary.

```sh
go build ./...
go build ./cmd/strategy-server
go test ./...
pre-commit run --all-files
```

The framework-version injection is unchanged from the standalone
service: the authoritative version is the const
`prompts.WasmFrameworkVersion`; at validate time TinyGo injects it into
the wasm's `dorastrategy.FrameworkVersion` var via `-ldflags`, and the
host's `CheckFrameworkVersion` rejects manifests whose
`framework_version` doesn't match (including missing values).

## Tests

The agent's unit and integration tests live under `internal/agent/...`:

```sh
go test ./internal/agent/...
```

Tests that need a real Postgres follow the host's convention and are
gated on `DATABASE_URL`. Key integration tests:

- `internal/agent/httpapi/integration_auth_test.go` — agent routes
  require auth.
- `internal/agent/httpapi/integration_authed_test.go` — successful
  auth returns 200.
- `internal/agent/httpapi/integration_prefix_test.go` — bare
  `/v1/sessions` does not leak to the agent.
- `internal/agent/httpapi/integration_ratelimit_test.go` — per-user
  rate limiter returns 429 with `Retry-After`; refills after depletion.
- `internal/agent/httpapi/integration_mountorder_test.go` — host +
  agent rate limiters run in the right order.
- `internal/agent/store/history_store_test.go` — round-trip against
  the host's public schema (candles/trades/prices), real DB.
- `internal/agent/backtest/integration_e2e_test.go` — full backtest
  E2E, gated on `AGENT_E2E=1`.

The standalone service's subprocess e2e suite was not carried over;
live verification of `cmd/strategy-server` is a separate follow-up
(see `docs/agent-binary-smoke.md`).

## Running with Docker

The agent is part of the host's image — there is no separate agent
image. The host's [`Dockerfile`](../Dockerfile) builds the
`strategy-server` binary (with the agent embedded) alongside
`mcp-server` and `price-daemon`, and
[`docker-compose.yml`](../docker-compose.yml) orchestrates the
services. See the host README's "Docker" section for image contents,
build steps (including the GitHub token secret), and compose
environment variables.
