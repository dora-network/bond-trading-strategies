# dora-agent Documentation + ChatUI Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. After every task, the working tree must be green (`go build ./...` + `go test ./...` + `pre-commit run --all-files`).

**Goal:** Migrate the dora-agent's user-facing documentation (README, agent overview, env-var reference, API surface) and manual-testing chat UI (`docs/chatui/`) into bond-trading-strategies. Two user-driven commits: docs first, chatui second.

**Architecture:** Copy `dora-agent/development/README.md` content into a new `docs/agent.md` (the host's README stays focused on the three core binaries; the agent is a 4th service with its own doc). Copy `dora-agent/development/docs/chatui/{index.html,app.js,style.css}` (1986 lines) into `docs/chatui/` in the host. Update the chatui to talk to the host's strategy-server at `:8081` and the `/v1/agent/*` route prefix. Document the agent surface in the host's `README.md` as a fourth `###` subsection under "## Services".

**Tech Stack:** Plain HTML/JS/CSS (no build step for the chatui), Go 1.26 (for any small test harness if needed).

**Spec source:** `dora-agent/development/README.md` (370 lines), `dora-agent/development/docs/chatui/` (1986 lines), `dora-agent/development/CHATUI-REVIEW.md` (review notes).

**Branch:** `tan/feat-integrate-dora-agent`

**Pre-commit gate:** `pre-commit run --all-files` must be green after every task.

**Commit gate:** Stage at the end of each commit boundary. User reviews and commits personally per the worktree's `AGENTS.md`.

---

## Pre-flight

- [ ] **Step 0.1: Confirm working directory and branch**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
git branch --show-current
git status --short
```

Expected: `tan/feat-integrate-dora-agent`, no uncommitted changes. If anything is uncommitted, stop and surface to the user.

- [ ] **Step 0.2: Confirm baseline pre-commit is green**

```bash
pre-commit run --all-files 2>&1 | tail -10
```

Expected: all hooks pass. If anything fails, fix before proceeding.

---

# Part A: Documentation migration (Commit 1)

## Task 1: Create `docs/agent.md` from dora-agent's README

The dora-agent's `README.md` is the canonical user-facing doc for the agent. Migrate it to `docs/agent.md` in the host, with edits for: (a) the env-var renames from the integration spec, (b) the route-prefix translation, (c) the host's actual ports (`:8081` instead of `:9000`), (d) drop references to `cmd/agent-cli` and the admin listener (both gone per the spec).

**Files:**
- Create: `docs/agent.md`

- [ ] **Step 1: Read the dora-agent README in full**

```bash
cat /home/tanq/code/dora/repos/dora-services/dora-agent/development/README.md
```

- [ ] **Step 2: Write `docs/agent.md` with the migrated content**

The full content is the dora-agent README, edited for the integration:

```markdown
# dora-agent

## Introduction

The agent is an AI assistant embedded in the bond-trading-strategies service
that helps users construct, back-test, and (with WASM plugins) deploy trading
strategies on the Dora Network. It is served by the same `cmd/strategy-server`
binary as the rest of the strategy stack; the agent's HTTP API lives at
`/v1/agent/*` and shares the strategy-server's auth gate.

Users describe their strategy to the agent. The agent compiles the
description into a Go module that implements the `dorastrategy.Strategy`
interface, validates the module against the framework's surface, and runs
backtests against historic Dora candle data:

- **`go-wasm`**: the strategy module is compiled to a `.wasm` blob with TinyGo
  and run in-process via wazero. The agent's `WasmStarter` drives the backtest
  loop directly, pre-fetches historic candles, simulates fills, and persists
  results to the same store.

Backtests are submitted via the HTTP API
(`POST /v1/agent/strategies/{id}/versions/{rev}/backtest`) into the
`agent.backtests` + `agent.backtest_fills` schema.

## Quick start

### 1. Configure environment

The agent shares `cmd/strategy-server`'s `.env`. Required additions (all kept
`AGENT_*` prefixed because they have no strategy-server counterpart):

- `DATABASE_URL` — Postgres DSN. The host's `tern` migrator applies both the
  host's migrations 001-014 and the agent's 015 in a single run.
- `ENCRYPTION_KEY` — base64-encoded 32-byte AES-256-GCM key. Replaces the
  agent's previous envelope-encryption master key. The host's existing
  `strategy/http/crypto.go` (moved to `internal/secrets/crypto.go`) is the
  underlying primitive.
- `DORA_BASE_URL` — upstream Dora REST API base URL (e.g. `https://dev.dora.co`).
  Renamed from `AGENT_DORA_BASE_URL`.
- `DORA_API_KEY` — your Dora API key (used by coding agents to test the service).
  Renamed from `AGENT_DORA_API_KEY`.
- `DORA_AUTH_CACHE_TTL` — TTL for the `/v1/user/self` cache. Renamed from
  `AGENT_AUTH_CACHE_TTL`. Default 5m.
- `AGENT_RATE_LIMIT_PER_MIN` — per-user token-bucket cap on the agent subtree.
  Default 20. Deliberately distinct from the host's IP/global/read/write
  rate limiters; a `/v1/agent/*` request is rate-limited twice.
- `AGENT_LLM_TIMEOUT`, `AGENT_LLM_MAX_ITERS`, `AGENT_MAX_PROMPT_BYTES` —
  LLM turn-driver knobs. See `.env.example` for defaults.
- `AGENT_MODEL_CAPS_PATH` — model capabilities JSON. Defaults to
  `configs/model_caps.json`.
- `AGENT_GENERATE_GOPROXY` — GOPROXY value forwarded to the validator's
  `go mod tidy` step.
- `AGENT_GENERATE_ALLOWLIST` — comma-separated module paths a strategy may
  require; the validator strips any other require line from the strategy's
  go.mod. Default includes the Dora client SDK and the dora-strategy-wasm
  framework.
- `AGENT_GENERATE_MAX_REPAIRS` — max repair attempts the LLM may run on a
  failed build. Default 2.
- `AGENT_GENERATE_MAX_FILES` — max files a generated strategy may contain.
  Default 50.
- `AGENT_GENERATE_MAX_BYTES` — max total bytes of generated strategy source.
  Default 1 MiB.
- `AGENT_LIVE_MEMORY_LIMIT` — per-instance wazero memory cap for live
  deployments. Default 256 MiB.
- `AGENT_LIVE_RESTART_WINDOW`, `AGENT_LIVE_MAX_RESTARTS` — restart budget
  knobs.
- `AGENT_WSBROKER_URL` — Dora multiplex websocket base URL. Empty defaults
  to `<DORA_BASE_URL>/plex`.
- `AGENT_WASM_ARTIFACT_ROOT` — directory where compiled `.wasm` artifacts
  are persisted. Default `/tmp/bond-trading-strategy-agent-wasm`.
- `AGENT_CAPTURE_PENDING_RETENTION`, `AGENT_CAPTURE_PENDING_SWEEP_INTERVAL` —
  capture-pending sweeper knobs.
- `AGENT_ALLOW_HTTP_BASE_URL` — relax the `https://` check on provider
  `base_url` values (local development only).
- `CORS_ALLOWED_ORIGINS` — comma-separated exact origins for the chat UI.
  Renamed from `AGENT_CORS_ALLOWED_ORIGINS`. Empty = CORS disabled.
- `LOG_LEVEL` — slog level. Renamed from `AGENT_LOG_LEVEL`. One of debug,
  info, warn, error.

The agent does **not** use the following env vars (deletions from the
standalone repo):
- `AGENT_POSTGRES_DSN` (use `DATABASE_URL`)
- `AGENT_HISTORY_DSN` (the agent's history package is gone; replaced by
  `internal/agent/store/history_store.go` on the shared pool)
- `AGENT_MASTER_KEY` (envelope encryption is gone; `ENCRYPTION_KEY` is the
  AES-256-GCM key)
- `AGENT_ADDR`, `AGENT_ADMIN_ADDR` (strategy-server owns the listener)
- `AGENT_DORA_API_KEY` (use `DORA_API_KEY`; the agent has no separate
  admin key)
- `AGENT_ALLOWED_DORA_ROLES` (role gate is removed)

### 2. Run database migrations

The host's `tern` migrator runs all migrations in numeric order on
`migrations/`. The agent's `015_agent_consolidated_schema.sql` creates the
agent's tables in a dedicated `agent` schema (not `public`), so they're
isolated from the host's tables.

```bash
tern migrate --config migrations/tern.conf
```

### 3. Start the server

The agent runs as part of `cmd/strategy-server`. There is no separate agent
binary; the integration collapses the standalone dora-agent into the
strategy-server process.

```bash
make start-strategy-server
```

(or whichever equivalent the host uses — verify against the host's
`Makefile` `start-strategy-server` target.)

The server listens on the host's port (default `:8081`). `GET /healthz`
returns `200 ok`. `GET /v1/openapi` returns the merged OpenAPI spec
including the agent's paths.

### 4. Use the chat UI

The chat UI is a single-page HTML/JS/CSS app in `docs/chatui/`. Open
`docs/chatui/index.html` in a browser, point it at your strategy-server
(`http://localhost:8081`), paste your Dora API key, and you're talking to
the agent.

Browser CORS: set `CORS_ALLOWED_ORIGINS` to the UI's origin
(e.g. `http://localhost:5500` if you serve the chatui with `python -m
http.server`).

### 5. Hit the API directly (curl / httpie)

```bash
# Without auth, the agent subtree returns 401.
curl -i http://localhost:8081/v1/agent/sessions

# With a Dora API key:
curl -i http://localhost:8081/v1/agent/sessions \
  -H "Authorization: ApiKey $DORA_API_KEY"

# Create a session:
curl -X POST http://localhost:8081/v1/agent/sessions \
  -H "Authorization: ApiKey $DORA_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"title":"my first strategy"}'

# Send a message (SSE):
curl -N -X POST http://localhost:8081/v1/agent/sessions/$SESSION_ID/messages \
  -H "Authorization: ApiKey $DORA_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"content":"build me a momentum strategy on NVDA-USD"}'
```

## API surface

The agent's HTTP API is mounted under `/v1/agent/*`. All endpoints require
`Authorization: ApiKey <key>` or `Authorization: Bearer <token>`; the
tenant-id header is optional and forwarded to Dora when present.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/v1/agent/sessions` | List chat sessions |
| `POST` | `/v1/agent/sessions` | Create a session |
| `GET` | `/v1/agent/sessions/{id}` | Get session details |
| `DELETE` | `/v1/agent/sessions/{id}` | Delete a session |
| `POST` | `/v1/agent/sessions/{id}/messages` | Send a message (SSE stream) |
| `POST` | `/v1/agent/sessions/{id}/save` | Save the session as a strategy |
| `GET` | `/v1/agent/strategies` | List strategies |
| `GET` | `/v1/agent/strategies/{id}` | Get strategy details |
| `GET` | `/v1/agent/strategies/{id}/versions` | List strategy versions |
| `GET` | `/v1/agent/strategies/{id}/versions/{revision}` | Get version details |
| `POST` | `/v1/agent/strategies/{id}/halt` | Halt a live deployment |
| `POST` | `/v1/agent/strategies/{id}/resume` | Resume a halted deployment |
| `POST` | `/v1/agent/strategies/{id}/restart` | Restart a deployment |
| `POST` | `/v1/agent/strategies/{id}/rollback` | Rollback to a previous version |
| `POST` | `/v1/agent/strategies/{id}/versions/{revision}/deploy` | Deploy a version |
| `GET` | `/v1/agent/strategies/{id}/deployments` | List deployments |
| `GET` | `/v1/agent/strategies/{id}/deployments/{deployment_id}` | Get deployment details |
| `POST` | `/v1/agent/strategies/{id}/deployments/{deployment_id}/stop` | Stop a deployment |
| `GET` | `/v1/agent/strategies/{id}/deployments/{deployment_id}/logs` | Get deployment logs |
| `POST` | `/v1/agent/strategies/{id}/deployments/{deployment_id}/resume` | Resume a stopped deployment |
| `POST` | `/v1/agent/strategies/{id}/deployments/{deployment_id}/restart` | Restart a deployment |
| `POST` | `/v1/agent/strategies/{id}/deployments/{deployment_id}/hotswap` | Hotswap a deployment's strategy version |
| `POST` | `/v1/agent/strategies/{id}/versions/{revision}/backtest` | Submit a backtest |
| `GET` | `/v1/agent/strategies/{id}/backtests` | List backtests |
| `GET` | `/v1/agent/strategies/{id}/backtests/{backtest_id}` | Get backtest result |
| `POST` | `/v1/agent/strategies/{id}/backtests/{backtest_id}/cancel` | Cancel a backtest |
| `GET` | `/v1/agent/provider-config` | List per-user LLM provider configs |
| `POST` | `/v1/agent/provider-config` | Create/update a provider config |
| `DELETE` | `/v1/agent/provider-config/{provider}` | Delete a provider config |

The full OpenAPI spec is served at `GET /v1/openapi` (auth-exempt).

## Architecture

The agent shares the strategy-server's process. It is mounted at
`/v1/agent/*` inside the existing `requireAuth`-wrapped mux, behind the
host's IP/global/read/write rate limiters. A `/v1/agent/*` request flows
through:

1. Strategy-server's existing CORS middleware.
2. Strategy-server's IP/global/read/write rate limiters.
3. Strategy-server's `requireAuth` (parses Authorization, resolves the
   Dora user, sets `authctx.AuthInfo`).
4. Agent's per-user token-bucket rate limiter (`AGENT_RATE_LIMIT_PER_MIN`).
5. Agent's request-ID + logging middleware.
6. The agent's `*http.ServeMux` (every pattern under the `/v1/agent/`
   prefix).

The agent's runtime is constructed by `internal/agent/wiring.Wire(...)`
which returns a `*Runtime` whose `Server.RoutesAt("/v1/agent")` is the HTTP
handler. Construction order:

1. `internal/agent/secrets.NewSealer(encryptionKey)` — AES-256-GCM wrapper
   around the host's `internal/secrets/crypto.go`. Used to seal/unseal
   per-user LLM provider API keys and the server-held admin Dora key.
2. CRUD stores: `users`, `session`, `providerconfig`, `strategies`,
   `backtest`, `deployment`.
3. Capture-pending janitor (`strategies.Janitor`) and backtest janitor.
4. `store.NewHistoryStore(pool)` — pgxpool-backed fetcher against the
   host's `public.candles_history` / `public.trades_history` /
   `public.price_history` tables.
5. `safety.NewKernel(pool)` — per-user risk caps and the kill switch.
6. `wasmruntime.New(...)` — per-call wazero Runtimes with a shared
   CompilationCache. Each plugin instance lives on its own runtime; the
   `dora-strategy-wasm` framework provides the import declarations
   (`host_*`).
7. `wsbroker.New(...)` — Dora multiplex websocket client at
   `<DORA_BASE_URL>/plex`.
8. `orchestrator.New(...)` — live deployment orchestrator; per-deployment
   candle/price subscription state.
9. `httpapi.New(...)` — agent HTTP server with `RoutesAt(prefix)` for
   mount.

The agent shares the host's PostgreSQL pool, encryption key, and auth
gate. No second pool, no separate encryption layer, no separate auth
listener.

## OpenAPI spec

The full OpenAPI 3.1 spec is served at `GET /v1/openapi` (auth-exempt).
The agent's paths appear under `/v1/agent/*`; shared types live alongside
the host's. Component names that would collide with the host's get an
`Agent` prefix.

## Building

The agent is part of the host's `cmd/strategy-server` build. No separate
binary, no separate Docker image, no separate CI step.

```bash
go build ./...
go build ./cmd/strategy-server
```

## Tests

The agent's unit tests live under `internal/agent/...`. Tests that need a
real Postgres are gated on `DATABASE_URL` (per host convention) and skip
without it. The full agent test suite:

```bash
go test ./internal/agent/...
```

A live verification run (with `DATABASE_URL` set against a testcontainers
Postgres) exercises the original-spec integration tests (HTTP-level auth
checks, route-prefix translation, per-user rate limit, mount order) and
the `history_store` real-DB round-trip.
```

- [ ] **Step 3: Verify the file renders and links resolve**

```bash
wc -l docs/agent.md
```

Expected: roughly 250-350 lines.

- [ ] **Step 4: Stage the new file**

```bash
git add docs/agent.md
```

---

## Task 2: Add an `### Agent` subsection to the host's README

The host's `README.md` has a `## Services` section with three `###`
subsections (price-daemon, strategy-server, mcp-server). Add a fourth
`### agent` subsection that links to `docs/agent.md` and summarizes the
key facts (mount path, env vars, port).

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Find the right insertion point**

```bash
grep -n "### \`mcp-server\`" README.md
```

The new `### \`agent\`` subsection goes after `mcp-server`'s "Run locally"
section ends, before the `## Docker` section.

- [ ] **Step 2: Add the subsection**

Insert after the `mcp-server` `#### Run locally` block (around line 364,
just before `## Docker`):

```markdown
### `agent` (AI strategy builder)

The agent is an AI assistant that helps users construct, back-test, and
deploy bond strategies. It is embedded in the `strategy-server` binary
and served at `/v1/agent/*` — the same process, the same Postgres pool,
the same encryption key, the same auth gate.

#### What it supports

- **Strategy generation** from a natural-language description. The agent
  compiles the description into a Go module implementing the
  `dorastrategy.Strategy` interface and validates it against the
  framework's surface.
- **Backtests** against historic Dora candle data. The strategy is
  compiled to a `.wasm` blob with TinyGo and run in-process via wazero.
  Results persist to the `agent.backtests` + `agent.backtest_fills`
  schema.
- **Live deployments** via `POST /v1/agent/strategies/{id}/versions/{rev}/deploy`.
  The agent subscribes to the Dora multiplex websocket, drives the
  wazero instance, and persists per-strategy decisions.
- **Per-user LLM provider configuration** (saved encrypted in
  `agent.provider_configs`).

#### Run locally

The agent runs as part of `strategy-server`; there is no separate
binary. Required env-var additions to the host's `.env` are listed in
[`docs/agent.md`](docs/agent.md). The agent's HTTP surface is documented
in the merged OpenAPI spec at `GET /v1/openapi`.

#### Chat UI

A single-page manual-testing chat UI lives in `docs/chatui/`. Open
`docs/chatui/index.html` in a browser, point it at your strategy-server,
paste your Dora API key.

See [`docs/agent.md`](docs/agent.md) for the full reference (env vars,
API surface, architecture, build/test commands).
```

- [ ] **Step 3: Verify the README still parses**

```bash
grep -nE "^##|^###" README.md | head -20
```

Expected: the new `### \`agent\`` subsection appears in the right place.

- [ ] **Step 4: Stage the README change**

```bash
git add README.md
```

---

## Task 3: Run pre-commit

- [ ] **Step 1: Run the full pre-commit suite**

```bash
pre-commit run --all-files 2>&1 | tail -10
```

Expected: GREEN. If anything fails (e.g. a markdown linter or a hook that
reformats), fix in place and re-run.

---

## Task 4: Commit the docs (Commit 1 — user-driven)

After Tasks 1-3 pass, the user reviews the staged set and commits
personally. Suggested message:

```
docs(agent): migrate dora-agent user-facing docs

- New docs/agent.md: full agent reference (introduction, quick start,
  env vars, API surface table, architecture, OpenAPI, build, tests).
  Content adapted from dora-agent/README.md with edits for the
  integration: env-var renames (DORA_BASE_URL etc.), route prefix
  (/v1/agent/*), host port (8081), removed cmd/agent-cli and admin
  listener references.
- README.md: added "### agent (AI strategy builder)" subsection under
  "## Services" that summarizes the agent's capabilities and links to
  docs/agent.md.
```

The user reviews and commits. Do NOT run `git commit` yourself.

---

# Part B: ChatUI migration (Commit 2)

## Task 5: Copy the chatui files

The dora-agent's `docs/chatui/` is a 3-file static site (index.html, app.js,
style.css, total 1986 lines). Copy verbatim; edits happen in Tasks 6-7.

**Files:**
- Create: `docs/chatui/index.html`
- Create: `docs/chatui/app.js`
- Create: `docs/chatui/style.css`

- [ ] **Step 1: Copy the three files**

```bash
mkdir -p docs/chatui
cp /home/tanq/code/dora/repos/dora-services/dora-agent/development/docs/chatui/index.html docs/chatui/index.html
cp /home/tanq/code/dora/repos/dora-services/dora-agent/development/docs/chatui/app.js docs/chatui/app.js
cp /home/tanq/code/dora/repos/dora-services/dora-agent/development/docs/chatui/style.css docs/chatui/style.css
wc -l docs/chatui/*
```

Expected: 102 + 1225 + 659 lines (matches the source).

- [ ] **Step 2: Don't stage yet (Tasks 6-7 edit the files)**

Skip `git add` until the URL + path edits are in.

---

## Task 6: Update the default baseUrl to the host's strategy-server

The chatui's `app.js:11` hardcodes `http://127.0.0.1:9000` as the default
`baseUrl` (the dora-agent's old port). Change it to `http://127.0.0.1:8081`
(the host's strategy-server default).

**Files:**
- Modify: `docs/chatui/app.js`

- [ ] **Step 1: Find the hardcoded URL**

```bash
grep -nE "127\.0\.0\.1:9000|baseUrl:.*9000|placeholder.*9000" docs/chatui/index.html docs/chatui/app.js
```

- [ ] **Step 2: Replace `:9000` with `:8081` in `app.js:11`**

Edit the line:

```js
baseUrl: localStorage.getItem('dora-agent.baseUrl') || 'http://127.0.0.1:9000',
```

to:

```js
baseUrl: localStorage.getItem('dora-agent.baseUrl') || 'http://127.0.0.1:8081',
```

- [ ] **Step 3: Replace the placeholder in `index.html`**

Edit the input placeholder:

```html
<input id="baseUrl" type="text" placeholder="http://127.0.0.1:9000" value="">
```

to:

```html
<input id="baseUrl" type="text" placeholder="http://127.0.0.1:8081" value="">
```

- [ ] **Step 4: Update the page title**

Edit `index.html`'s `<title>`:

```html
<title>dora-agent chat</title>
```

to:

```html
<title>bond-trading-strategies agent chat</title>
```

Same for the `<h1>` header inside the body.

- [ ] **Step 5: Verify the changes**

```bash
grep -nE "127\.0\.0\.1:(9000|8081)|dora-agent chat|bond-trading-strategies agent" docs/chatui/index.html docs/chatui/app.js | head -10
```

Expected: no `:9000` references; the title is "bond-trading-strategies
agent chat".

---

## Task 7: Translate the bare `/v1/...` paths to `/v1/agent/...`

The chatui's `app.js` calls bare `/v1/sessions`, `/v1/strategies/...`,
`/v1/provider-config`, etc. The integration spec moved every agent route
under `/v1/agent/`. Translate every path.

The `/v1/openapi` and `/healthz` paths are **host routes**, not agent
routes — they stay as-is.

**Files:**
- Modify: `docs/chatui/app.js`

- [ ] **Step 1: List every `/v1/...` reference in app.js**

```bash
grep -nE "'/v1/[^']*'|\"/v1/[^\"]*\"" docs/chatui/app.js | head -40
```

Expected hits: about 15-20 paths.

- [ ] **Step 2: Translate each path**

For every reference to a path that the spec's route-translation table
moves under `/v1/agent/*`, change the prefix. Concretely, every path
starting with `/v1/` and pointing at an agent endpoint becomes
`/v1/agent/...`. The `/v1/openapi` reference stays as-is.

A representative diff (find each occurrence, edit individually or with a
scripted sed):

```diff
- api('GET', '/v1/sessions') || []
+ api('GET', '/v1/agent/sessions') || []
- api('POST', '/v1/sessions', body)
+ api('POST', '/v1/agent/sessions', body)
- api('DELETE', '/v1/sessions/' + state.activeSessionId)
+ api('DELETE', '/v1/agent/sessions/' + state.activeSessionId)
- api('GET', '/v1/strategies/' + state.activeStrategyId + '/versions')
+ api('GET', '/v1/agent/strategies/' + state.activeStrategyId + '/versions')
- api('GET', '/v1/strategies/' + state.activeStrategyId + '/deployments')
+ api('GET', '/v1/agent/strategies/' + state.activeStrategyId + '/deployments')
- api('GET', '/v1/strategies/' + state.activeStrategyId + '/deployments/' + id)
+ api('GET', '/v1/agent/strategies/' + state.activeStrategyId + '/deployments/' + id)
- api('GET', '/v1/strategies/' + state.activeStrategyId + '/versions/' + revisionId)
+ api('GET', '/v1/agent/strategies/' + state.activeStrategyId + '/versions/' + revisionId)
- api('POST', '/v1/strategies/' + state.activeStrategyId + '/versions/' + state.selectedRevisionId + '/deploy', { ... })
+ api('POST', '/v1/agent/strategies/' + state.activeStrategyId + '/versions/' + state.selectedRevisionId + '/deploy', { ... })
- api('POST', '/v1/strategies/' + state.activeStrategyId + '/deployments/' + state.activeDeploymentId + suffix)
+ api('POST', '/v1/agent/strategies/' + state.activeStrategyId + '/deployments/' + state.activeDeploymentId + suffix)
- api('GET', '/v1/sessions/' + sessionID)
+ api('GET', '/v1/agent/sessions/' + sessionID)
- state.baseUrl + '/v1/sessions/' + state.activeSessionId + '/messages'
+ state.baseUrl + '/v1/agent/sessions/' + state.activeSessionId + '/messages'
- api('GET', '/v1/strategies/' + strategyId + '/versions/' + revision)
+ api('GET', '/v1/agent/strategies/' + strategyId + '/versions/' + revision)
- api('POST', '/v1/provider-config', body)
+ api('POST', '/v1/agent/provider-config', body)
- api('GET', '/v1/provider-config')
+ api('GET', '/v1/agent/provider-config')
- api('DELETE', '/v1/provider-config/' + sel)
+ api('DELETE', '/v1/agent/provider-config/' + sel)
```

- [ ] **Step 3: Verify no bare `/v1/...` agent paths remain (except `/v1/openapi`)**

```bash
grep -nE "'/v1/[^']*'|\"/v1/[^\"]*\"" docs/chatui/app.js
```

Expected hits: only `/v1/openapi` and any localStorage keys (`dora-agent.baseUrl`).

- [ ] **Step 4: Spot-check a path by opening the chatui in a browser (manual)**

If you have a running strategy-server:

```bash
python3 -m http.server 5500 --directory docs/chatui &
# Open http://localhost:5500 in a browser; point at http://localhost:8081
# Verify GET /v1/agent/sessions returns the expected 401 (no auth) response.
```

If you don't have a running server, skip this step. The Task 9 smoke in
Plan 1 covers the equivalent verification.

---

## Task 8: Run pre-commit

- [ ] **Step 1: Run the full pre-commit suite**

```bash
pre-commit run --all-files 2>&1 | tail -10
```

Expected: GREEN. (The chatui files are static HTML/JS/CSS; the pre-commit
hooks may include markdown or trailing-whitespace checks that pass
trivially. JS linters may or may not be configured; if so, ensure they
pass.)

- [ ] **Step 2: Stage the chatui**

```bash
git add docs/chatui/
```

---

## Task 9: Commit the chatui (Commit 2 — user-driven)

After Tasks 5-8 pass, the user reviews the staged set and commits
personally. Suggested message:

```
docs(chatui): migrate manual-testing chat UI

- New docs/chatui/{index.html,app.js,style.css}: 1986-line static
  chat UI copied verbatim from dora-agent/docs/chatui.
- Default baseUrl changed from http://127.0.0.1:9000 (old dora-agent
  port) to http://127.0.0.1:8081 (host strategy-server port).
- Page title + h1 changed to "bond-trading-strategies agent chat".
- Bare /v1/... paths in app.js translated to /v1/agent/... per the
  integration spec's route table. /v1/openapi and /healthz are host
  routes and stay as-is.
- Open docs/chatui/index.html in a browser to use; CORS must be
  configured on strategy-server (CORS_ALLOWED_ORIGINS).
```

The user reviews and commits. Do NOT run `git commit` yourself.

---

## Self-review

- **Spec coverage:** Task 1 (new `docs/agent.md`) + Task 2 (host README
  agent subsection) cover the dora-agent README migration. Tasks 5-7
  (chatui copy + URL + path edits) cover the chatui migration. The
  `dora-agent/CHATUI-REVIEW.md` is referenced as a source but is not
  migrated — it's a review of the old chatui, not a user-facing doc;
  it can stay in the dora-agent repo as historical context.
- **Placeholder scan:** No "TBD" or "TODO" markers. Each task has a
  concrete outcome.
- **Type consistency:** The `docs/agent.md` file uses the same env-var
  names the integration's `.env.example` uses (`DORA_BASE_URL`, etc.).
  The chatui edits use the same routes the spec's route-translation
  table defines.
- **dora-agent repo:** Untouched. The README and chatui are copied out;
  the dora-agent repo retains the originals.
