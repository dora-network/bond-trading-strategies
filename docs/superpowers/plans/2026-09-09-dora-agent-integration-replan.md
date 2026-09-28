# dora-agent Integration Re-plan Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan layer-by-layer. Steps use checkbox (`- [ ]`) syntax for tracking. After each layer, the working tree must be green (`go build ./...` + `go test ./...` + `pre-commit run --all-files`).

**Goal:** Complete the dora-agent integration into `bond-trading-strategies` by landing the remaining deferred packages (Tasks 4.2/4.3/4.5/4.6/4.7/4.8/5.1/5.2) as a single semantic user commit, organized as six bottom-up dependency layers (L3–L8).

**Architecture:** Strict bottom-up dep layers. Each layer copies from `dora-agent/development/internal/<pkg>` into `bond-trading-strategies/development/internal/agent/<pkg>`, performs the layer's seam rewrites (pool sharing, Sealer, `agent.` schema qualification, `RoutesAt`, tern migration), and is checkpoint-verified. The user's single commit aggregates all six layers.

**Tech Stack:** Go 1.26, `pgx/v5` v5.10.0, `dora-strategy-wasm` v0.3.3, `wazero` v1.12.0, `mozilla-ai/any-llm-go` v0.9.0, tern migrations.

**Spec:** `docs/superpowers/specs/2026-09-09-dora-agent-integration-replan.md`

**Source of truth:** `/home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/<pkg>` is the source for every package copied in this plan. The host's `internal/agent/{audit, authcache, config, llm, orderbroker, safety, sanitize, scan, secrets}` is the source of truth for the L0–L2 packages already landed — do not re-copy those.

**Branch:** `tan/feat-integrate-dora-agent`

**Pre-commit gate:** `pre-commit run --all-files` must be green after every layer. Do NOT use `--no-verify`. Do NOT disable GPG signing.

**Commit gate:** Stage at end of every layer. User reviews and commits after all six layers are in the working tree, per the worktree's `AGENTS.md`.

---

## Pre-flight

- [ ] **Step 0.1: Confirm working directory and branch**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
git branch --show-current
git status --short
```

Expected: `tan/feat-integrate-dora-agent` with only `docs/superpowers/specs/2026-09-09-dora-agent-integration-replan.md` staged. If anything else is staged, stop and surface to the user.

- [ ] **Step 0.2: Confirm dora-agent repo is reachable**

```bash
ls /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/users/users.go
```

Expected: file exists. If the path doesn't resolve, ask the user where dora-agent lives.

- [ ] **Step 0.3: Confirm baseline pre-commit is green**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
pre-commit run --all-files 2>&1 | tail -10
```

Expected: all hooks pass. If anything fails, stop and fix before proceeding.

---

## Layer checkpoint (run after every layer)

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go build ./...
go test ./...
go test ./internal/agent/...
pre-commit run --all-files
```

If a layer fails the checkpoint, fix the seam rewrite in place and re-run. The working tree stays green at every step.

---

## Phase L3 — CRUD stores

Source packages: `dora-agent/development/internal/{users,session,providerconfig}` → `bond-trading-strategies/development/internal/agent/{users,session,providerconfig}`.

Seam rewrites applied in this layer:
- `*sql.DB` → `*pgxpool.Pool` (every `database/sql` import replaced with `github.com/jackc/pgx/v5/pgxpool`).
- `internal/secrets.KMS/Seal/Unseal` → `internal/agent/secrets.Sealer` (only in `providerconfig`).
- Every `INSERT/UPDATE/SELECT/DELETE` SQL statement prefixed with `agent.` (the tables live in the `agent` schema per migration `015_agent_consolidated_schema.sql`).

### Task L3.1: Copy `users`

**Files:**
- Create: `internal/agent/users/users.go` (copy of `dora-agent/development/internal/users/users.go`)
- Create: `internal/agent/users/users_test.go` (copy of `dora-agent/development/internal/users/users_test.go`, excluding any e2e references)

- [ ] **Step 1: Read the source package**

```bash
cat /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/users/users.go
cat /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/users/users_test.go
```

- [ ] **Step 2: Create the package directory and copy `users.go`**

```bash
mkdir -p internal/agent/users
cp /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/users/users.go internal/agent/users/users.go
```

- [ ] **Step 3: Rewrite the seam**

In `internal/agent/users/users.go`:

- Replace `package users` → `package users` (kept; import path disambiguates).
- Replace every `database/sql` import with `github.com/jackc/pgx/v5/pgxpool` and `github.com/jackc/pgx/v5`.
- Replace every `*sql.DB` constructor parameter with `*pgxpool.Pool`.
- Replace every `db.QueryContext` / `db.ExecContext` call with `pool.Query` / `pool.Exec` (the pgx equivalent signatures differ slightly — `pool.Query(ctx, sql, args...)` returns `pgx.Rows`, `pool.Exec(ctx, sql, args...)` returns `pgconn.CommandTag`).
- Replace every `Scan(&dst)` chain against `database/sql.Rows` with the equivalent `pgx.Rows.Scan(&dst)` (same API).
- Prefix every table reference with `agent.` (e.g. `INSERT INTO users` → `INSERT INTO agent.users`, `FROM sessions` → `FROM agent.sessions` if cross-table).
- The package's exported `New(ctx, pool *pgxpool.Pool, ...)` constructor signature replaces any `New(ctx, dsn string, ...)` form.

Concretely, the rewritten `New` becomes:

```go
func New(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) (*Store, error) {
    return &Store{pool: pool, log: log}, nil
}
```

- [ ] **Step 4: Copy the test file and adapt**

```bash
cp /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/users/users_test.go internal/agent/users/users_test.go
```

In the test file, replace any `New(ctx, dsn)` or `setupDB(t, dsn)` helper with one that opens a testcontainers Postgres (the host's existing pattern uses `github.com/testcontainers/testcontainers-go/modules/postgres`):

```go
func newTestPool(t *testing.T) *pgxpool.Pool {
    t.Helper()
    pgC, err := postgres.RunContainer(context.Background(),
        testcontainers.WithImage("postgres:16-alpine"),
    )
    require.NoError(t, err)
    t.Cleanup(func() { _ = pgC.Terminate(context.Background()) })

    dsn, err := pgC.ConnectionString(context.Background(), "sslmode=disable")
    require.NoError(t, err)

    // Apply the consolidated migration
    pool, err := pgxpool.New(context.Background(), dsn)
    require.NoError(t, err)
    t.Cleanup(pool.Close)

    migrations, err := tern.MigrationsFromConfig(context.Background(), "migrations/tern.conf", migrations.NewMigrationsFromConfig)
    require.NoError(t, err)
    require.NoError(t, migrations.Migrate(context.Background(), pool))
    return pool
}
```

If the source test uses a different setup helper (e.g. hand-rolled `setupTestDB`), port that pattern instead — the goal is the same: open a pool against a testcontainers Postgres with the consolidated migration applied.

- [ ] **Step 5: Run the L3 layer checkpoint**

```bash
go build ./...
go test ./internal/agent/users/...
```

Expected: build clean; `users` tests pass. If `go test ./...` fails, run the full suite to surface cross-package breakage — fix in place.

### Task L3.2: Copy `session`

**Files:**
- Create: `internal/agent/session/session.go`
- Create: `internal/agent/session/session_test.go`

- [ ] **Step 1: Copy + rewrite seams (same pattern as L3.1)**

```bash
mkdir -p internal/agent/session
cp /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/session/session.go internal/agent/session/session.go
cp /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/session/session_test.go internal/agent/session/session_test.go
```

Apply the same seam rewrites: `*sql.DB` → pool, `agent.` schema prefix, `New(ctx, pool, ...)` signature.

- [ ] **Step 2: Run the L3 layer checkpoint**

```bash
go build ./... && go test ./internal/agent/...
```

Expected: clean.

### Task L3.3: Copy `providerconfig` (the Sealer rewrite site)

**Files:**
- Create: `internal/agent/providerconfig/providerconfig.go`
- Create: `internal/agent/providerconfig/providerconfig_test.go`

- [ ] **Step 1: Read the source package to find every `KMS/Seal/Unseal` call site**

```bash
grep -n -E "KMS|secrets\.|Seal\(|Unseal\(" /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/providerconfig/providerconfig.go
```

- [ ] **Step 2: Copy + start rewriting**

```bash
mkdir -p internal/agent/providerconfig
cp /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/providerconfig/providerconfig.go internal/agent/providerconfig/providerconfig.go
cp /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/providerconfig/providerconfig_test.go internal/agent/providerconfig/providerconfig_test.go
```

In `providerconfig.go`:

- Replace every `secrets.KMS` / `secrets.NewKMSFromEnv` / `kms.Seal` / `kms.Unseal` call with `internal/agent/secrets.Sealer`:
  - `kms, err := secrets.NewKMSFromEnv(ctx)` → `sealer, err := agentsecrets.NewSealer(encryptionKey)` where `encryptionKey []byte` is the existing `ENCRYPTION_KEY` (32 bytes) passed in via `New(ctx, pool, encryptionKey, ...)`.
  - `sealed, err := kms.Seal(ctx, plaintext)` → `sealed, err := sealer.Seal(ctx, plaintext)`.
  - `plaintext, err := kms.Unseal(ctx, sealed)` → `plaintext, err := sealer.Open(ctx, sealed)`.
- Drop every `*_dek` column reference (the consolidated schema has only `api_key bytea`; the agent's standalone `provider_configs` table had `api_key` + `api_key_dek` + `api_key_kek`).
- Apply the same pool + `agent.` schema prefix rewrites as L3.1.

- [ ] **Step 3: Adapt the test file**

In `providerconfig_test.go`:

- Replace any `secrets.NewKMSFromEnv` setup with `agentsecrets.NewSealer(testKey)` where `testKey` is a fixed 32-byte test key.
- Replace the test's table-fixture INSERT to omit `*_dek` columns.
- Add a mutation test (per the spec): write a test that calls `Sealer.Seal` then mutates one byte of the ciphertext, calls `Sealer.Open`, asserts the result is an error. This is the spec's "money-path" mutation check.

- [ ] **Step 4: Run the L3 layer checkpoint**

```bash
go build ./... && go test ./internal/agent/providerconfig/... && go test ./...
```

Expected: clean. The Sealer test fails if you flip the nonce, the schema test fails if you drop the `agent.` prefix.

- [ ] **Step 5: L3 done**

After L3.1, L3.2, L3.3 all pass, mark the layer complete. Working tree should be green; do not commit yet.

---

## Phase L4 — orchestration stores

Source packages: `dora-agent/development/internal/{strategies,backtest,deployment}` → `bond-trading-strategies/development/internal/agent/{strategies,backtest,deployment}`.

Seam rewrites applied in this layer:
- Pool + `agent.` schema (same as L3).
- `strategies/janitor.go` and `backtest/janitor.go` copy alongside their parent packages.
- `deployment` references to `internal/agent/wsbroker` and `internal/agent/wasmruntime` are stubbed as unexported fields (L5 fills them in).
- `backtest` calls into `internal/history` redirect to a thin indirection under `internal/agent/store` that L7 implements.

### Task L4.1: Copy `strategies`

**Files:**
- Create: `internal/agent/strategies/{strategies,store,janitor,...}.go` (whole `internal/strategies` package, except any cmd-cli references)
- Create: `internal/agent/strategies/*_test.go`

- [ ] **Step 1: Copy the whole package**

```bash
mkdir -p internal/agent/strategies
cp -r /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/strategies/. internal/agent/strategies/
```

This brings `strategies.go`, `store.go`, `janitor.go`, `postgres.go` (if present), and their tests.

- [ ] **Step 2: Rewrite seams**

- Pool + `agent.` schema prefix on every SQL.
- `New(ctx, pool *pgxpool.Pool, ...)` signature.
- The `janitor` reads `AGENT_CAPTURE_PENDING_*` env vars directly; that stays as-is (the spec keeps `AGENT_` prefix on these).
- The capture-pending sweeper goroutine still starts from the wiring at L8 — L4 just provides the `Janitor` type.

- [ ] **Step 3: Run the L4 layer checkpoint (after L4.2 too)**

```bash
go build ./...
```

Expected: compile errors likely reference L5 packages (wasmruntime, wsbroker, orchestrator). Stub those references in `strategies` (L4 doesn't actually need them, but deployment does) — keep the type-checked layer green by zeroing out the forward-referenced types in `strategies` (they're not used at L4's package surface).

### Task L4.2: Copy `backtest`

**Files:**
- Create: `internal/agent/backtest/*.go` (whole `internal/backtest` package, minus history-coupled files)
- Create: `internal/agent/backtest/*_test.go`

- [ ] **Step 1: Copy**

```bash
mkdir -p internal/agent/backtest
cp -r /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/backtest/. internal/agent/backtest/
```

- [ ] **Step 2: Rewrite seams**

- Pool + `agent.` schema prefix.
- `New(ctx, pool, ...)` signature.
- The `backtest` package's candle/trade fetch calls into `internal/history` — those get redirected. For L4, the indirection is a no-op stub: the file `internal/agent/store/history_store.go` doesn't exist yet. Instead, `backtest` keeps a reference of type `interface { FetchCandles(ctx, ...) ([]Candle, error) }` (or whatever the current type signature is) and L4 wires it to a stub that returns `nil, nil` for tests. L7 replaces the stub with the real `pgxpool`-backed implementation.

If the source `backtest` package reaches into `internal/history` concretely (not via interface), the L4 copy will not compile until the indirection is added. In that case, the pragmatic fix is to introduce the indirection in L4 — `internal/agent/store/history.go` package with three functions that return `nil` for now:

```go
package store

import "context"

func FetchCandles(ctx context.Context, ...) ([]Candle, error) { return nil, nil }
func FetchTrades(ctx context.Context, ...) ([]Trade, error)   { return nil, nil }
func FetchPrices(ctx context.Context, ...) ([]Price, error)   { return nil, nil }
```

The signature should mirror the eventual L7 implementation (see `internal/history/history.go` for the current shape). L7 fills these in.

- [ ] **Step 3: Run the L4 layer checkpoint**

```bash
go build ./...
go test ./internal/agent/strategies/... ./internal/agent/backtest/...
```

### Task L4.3: Copy `deployment`

**Files:**
- Create: `internal/agent/deployment/*.go`
- Create: `internal/agent/deployment/*_test.go`

- [ ] **Step 1: Copy + rewrite seams**

Same pattern. `deployment` likely references `internal/agent/wsbroker` and `internal/agent/wasmruntime` for live-dep state. Stub those as interface fields (L5 fills them in):

```go
type Orchestrator interface {
    Start(ctx context.Context, ...) error
    Stop(ctx context.Context, ...) error
}
```

`deployment.go` declares this interface; the actual implementation lands at L5 in `orchestrator`. L4's `deployment` tests use a fake.

- [ ] **Step 2: Run the L4 layer checkpoint**

```bash
go build ./... && go test ./...
```

Expected: clean. L4 is complete; do not commit yet.

---

## Phase L5 — live + WASM

Source packages: `dora-agent/development/internal/{wasmruntime/{registry,hostimpl,store},wsbroker,orchestrator}` → `bond-trading-strategies/development/internal/agent/{wasmruntime/{registry,hostimpl,store},wsbroker,orchestrator}`.

Seam rewrites applied in this layer:
- Promote `dora-strategy-wasm` + `wazero` to direct deps in `go.mod`.
- Pool + `agent.` schema.
- The `wasmruntime/registry` package keeps its per-call wazero Runtimes pattern (per the `wazero-per-call-runtime` skill).

### Task L5.1: Promote wazero + dora-strategy-wasm to direct deps

**Files:**
- Modify: `go.mod`
- Modify: `go.sum`

- [ ] **Step 1: Add the deps**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go get github.com/tetratelabs/wazero@v1.12.0
go get github.com/dora-network/dora-strategy-wasm@v0.3.3
go mod tidy
```

Expected: `go.mod` moves these from indirect to direct. The dora-strategy-wasm pin may have moved since 2026-09-08 — check the dora-agent repo's `go.mod` for the current pin and use that.

```bash
grep dora-strategy-wasm /home/tanq/code/dora/repos/dora-services/dora-agent/development/go.mod
```

- [ ] **Step 2: Verify build still passes**

```bash
go build ./...
```

### Task L5.2: Copy `wasmruntime/{registry,hostimpl,store}`

**Files:**
- Create: `internal/agent/wasmruntime/registry/*.go`
- Create: `internal/agent/wasmruntime/hostimpl/*.go`
- Create: `internal/agent/wasmruntime/store/*.go`

- [ ] **Step 1: Copy**

```bash
mkdir -p internal/agent/wasmruntime/{registry,hostimpl,store}
cp -r /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/wasmruntime/registry/. internal/agent/wasmruntime/registry/
cp -r /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/wasmruntime/hostimpl/. internal/agent/wasmruntime/hostimpl/
cp -r /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/wasmruntime/store/. internal/agent/wasmruntime/store/
```

- [ ] **Step 2: Rewrite seams**

- `wasmruntime/store` opens a `*sql.DB`; rewrite to `*pgxpool.Pool` + `agent.` schema prefix.
- `wasmruntime/registry` keeps its per-call wazero Runtimes pattern unchanged. The package only depends on the `dora-strategy-wasm` framework types and the `wazero` runtime, not on the database.
- `wasmruntime/hostimpl` provides the host-side Go implementations of the framework's `//go:wasmimport` declarations. Copy verbatim — the framework's import names are stable.

- [ ] **Step 3: Run the L5 layer checkpoint (after L5.3 too)**

```bash
go build ./...
```

### Task L5.3: Copy `wsbroker` + `orchestrator`

**Files:**
- Create: `internal/agent/wsbroker/*.go` + tests
- Create: `internal/agent/orchestrator/*.go` + tests

- [ ] **Step 1: Copy + rewrite**

```bash
mkdir -p internal/agent/wsbroker internal/agent/orchestrator
cp -r /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/wsbroker/. internal/agent/wsbroker/
cp -r /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/orchestrator/. internal/agent/orchestrator/
```

Apply pool + `agent.` schema. `wsbroker` reads `WS_BROKER_URL` (kept as-is per the spec). `orchestrator` implements the `deployment.Orchestrator` interface from L4.3.

- [ ] **Step 2: Run the L5 layer checkpoint**

```bash
go build ./... && go test ./...
```

Expected: clean. L5 is complete.

---

## Phase L6 — tools + servertest

Source packages: `dora-agent/development/internal/tools/{dora,strategies,backtest,deployment,generate}` and `internal/strategies/servertest`.

Seam rewrites applied in this layer:
- Port `internal/migration` as `internal/agent/migration` — it is a runtime gate (Migrator/ErrStaleFramework/Reserve/Release) used by `run_backtest` and `deploy_strategy` to force-regenerate rows whose Docker pipeline was removed, NOT a DDL migrator. Drop the on-demand schema-apply path (`servertest/store.MigrateConn`); tern handles the consolidated migration.
- Pool + `agent.` schema on every SQL.

### Task L6.1: Copy `tools/dora` + `tools/strategies`

**Files:**
- Create: `internal/agent/tools/dora/*.go` + tests
- Create: `internal/agent/tools/strategies/*.go` + tests

- [ ] **Step 1: Copy + rewrite**

```bash
mkdir -p internal/agent/tools/{dora,strategies,backtest,deployment,generate}
cp -r /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/tools/dora/. internal/agent/tools/dora/
cp -r /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/tools/strategies/. internal/agent/tools/strategies/
```

`tools/dora` is thin Dora API helpers — pool + agent. prefix. `tools/strategies` references L4's strategies store.

- [ ] **Step 2: Run the L6 layer checkpoint (after all L6 sub-tasks)**

```bash
go build ./...
```

### Task L6.2: Copy `tools/backtest` + `tools/deployment`

- [ ] **Step 1: Copy**

```bash
cp -r /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/tools/backtest/. internal/agent/tools/backtest/
cp -r /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/tools/deployment/. internal/agent/tools/deployment/
```

- [ ] **Step 2: Rewrite seams**

`tools/backtest` calls the dora-strategy-wasm framework's backtest runner and persists results to L4's backtest store. Pool + `agent.` prefix. `tools/deployment` references L5's orchestrator.

### Task L6.3: Copy `tools/generate` (the migration-deletion site)

- [ ] **Step 1: Read the source to find migration references**

```bash
grep -n -E "migration|tern|schema_version" /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/tools/generate/*.go
```

- [ ] **Step 2: Copy + delete migration code**

```bash
cp -r /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/tools/generate/. internal/agent/tools/generate/
```

In `tools/generate`:

- In `tools/generate`, find every function that opens a fresh `internal/store.MigrateConn` schema-apply and delete it. The host's tern migrator (already in `cmd/strategy-server/main.go`) handles it. Keep the `internal/agent/migration` Migrator calls — those are runtime gates, not DDL.
- If a "first-run applies schema" hook remains, replace it with a no-op or an Info-level log line that explains the host handles it.

### Task L6.4: Copy `strategies/servertest` (the test helper)

- [ ] **Step 1: Copy + delete migration code**

```bash
mkdir -p internal/agent/strategies/servertest 2>/dev/null || true
cp -r /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/strategies/servertest/. internal/agent/strategies/servertest/
```

Same migration deletion pattern as L6.3.

- [ ] **Step 2: Run the L6 layer checkpoint**

```bash
go build ./... && go test ./...
```

Expected: clean. L6 is complete.

---

## Phase L7 — httpapi + history_store

Source packages: `dora-agent/development/internal/httpapi` (whole package) and `internal/history` → `internal/agent/store/history_store.go`.

Seam rewrites applied in this layer:
- `httpapi`: drop `internal/auth.AuthMiddleware`, add `RoutesAt(basePath string)` parameter.
- `history_store`: rewrite `New(ctx, dsn)` → `New(pool *pgxpool.Pool)`, `database/sql.QueryContext` → `pgxpool.Query`.

### Task L7.1: Copy `internal/history` to `internal/agent/store/history_store.go`

**Files:**
- Create: `internal/agent/store/history_store.go`
- Create: `internal/agent/store/history_store_test.go`

- [ ] **Step 1: Read the source**

```bash
cat /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/history/history.go
```

- [ ] **Step 2: Rewrite for shared pool + public schema**

```go
// internal/agent/store/history_store.go
package store

import (
    "context"
    "github.com/jackc/pgx/v5/pgxpool"
)

type HistoryStore struct {
    pool *pgxpool.Pool
}

func NewHistoryStore(pool *pgxpool.Pool) *HistoryStore {
    return &HistoryStore{pool: pool}
}

func (s *HistoryStore) FetchCandles(ctx context.Context, orderBookID string, start, end time.Time, resolution string) ([]Candle, error) {
    rows, err := s.pool.Query(ctx, `
        SELECT start_timestamp, open, high, low, close, volume
        FROM candles_history
        WHERE order_book_id = $1 AND resolution = $2 AND start_timestamp BETWEEN $3 AND $4
        ORDER BY start_timestamp ASC`,
        orderBookID, resolution, start, end)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    // ... Scan loop, mirror source ...
}

func (s *HistoryStore) FetchTrades(...)  { /* mirror source, target trades_history (public) */ }
func (s *HistoryStore) FetchPrices(...)  { /* mirror source, target price_history (public) */ }
```

The SQL targets the host's `public` schema tables (not `agent.`). The store interface must match what the L4 stub returned `nil, nil` for, so L4 packages compile cleanly.

- [ ] **Step 3: Run the L7 layer checkpoint (after L7.2 too)**

```bash
go build ./...
```

### Task L7.2: Copy `internal/httpapi` to `internal/agent/httpapi`

**Files:**
- Create: `internal/agent/httpapi/*.go` + tests

- [ ] **Step 1: Copy the whole package**

```bash
mkdir -p internal/agent/httpapi
cp -r /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/httpapi/. internal/agent/httpapi/
```

- [ ] **Step 2: Drop `AuthMiddleware`**

In `internal/agent/httpapi/middleware.go` (or wherever `AuthMiddleware` lives):

- Delete the function. The host's `requireAuth` is in the mount chain; the httpapi does not need its own.
- Delete any import that was only used by `AuthMiddleware`.
- Update every call site that referenced it (likely in `Server.Routes()` registration or in the route handlers' tests).

- [ ] **Step 3: Add `RoutesAt(basePath string)`**

Replace the current `Server.Routes()` (or `Server.Handler()`) with:

```go
// RoutesAt registers every agent route under the given basePath.
// basePath is the production mount prefix (e.g. "/v1/agent"); pass ""
// for unit tests.
func (s *Server) RoutesAt(basePath string) http.Handler {
    mux := http.NewServeMux()
    s.registerRoutes(mux, basePath)
    return s.middleware(mux)
}

// Routes is the legacy entry point; equivalent to RoutesAt("").
// Kept for unit tests that don't care about the production mount prefix.
func (s *Server) Routes() http.Handler {
    return s.RoutesAt("")
}
```

The `registerRoutes` helper iterates the spec's route table (mechanical) and registers each pattern under `basePath + original`. The `s.middleware(mux)` chain is the existing request-ID + logging middleware; the per-user rate limiter is part of `s.middleware` and reads `AGENT_RATE_LIMIT_PER_MIN` (kept as-is).

- [ ] **Step 4: Update unit tests**

In every `*_test.go` under `internal/agent/httpapi`, change `s.Routes()` calls to `s.RoutesAt("")` if they exercised the test path; leave the helper `Routes()` alone for compatibility.

- [ ] **Step 5: Run the L7 layer checkpoint**

```bash
go build ./... && go test ./... && pre-commit run --all-files
```

Expected: clean. L7 is complete.

---

## Phase L8 — wiring + main.go + config + .env

This layer assembles everything: `internal/agent/wiring`, the `cmd/strategy-server/main.go` mount, config env-var wiring, `.env` updates, and `TODO.md` refresh.

### Task L8.1: Create `internal/agent/wiring`

**Files:**
- Create: `internal/agent/wiring/wiring.go`
- Create: `internal/agent/wiring/wiring_test.go`

- [ ] **Step 1: Write the wiring package**

```go
// internal/agent/wiring/wiring.go
package wiring

    "$$GST43BCT8DFF:L$$/dora-strategy-wasm/framework"
    "$$GST43BCT8DFF:L$$/bond-trading-strategies/internal/agent/audit"
    "$$GST43BCT8DFF:L$$/bond-trading-strategies/internal/agent/backtest"
    "$$GST43BCT8DFF:L$$/bond-trading-strategies/internal/agent/config"
    "$$GST43BCT8DFF:L$$/bond-trading-strategies/internal/agent/httpapi"
    "$$GST43BCT8DFF:L$$/bond-trading-strategies/internal/agent/orchestrator"
    "$$GST43BCT8DFF:L$$/bond-trading-strategies/internal/agent/providerconfig"
    "$$GST43BCT8DFF:L$$/bond-trading-strategies/internal/agent/safety"
    "$$GST43BCT8DFF:L$$/bond-trading-strategies/internal/agent/secrets"
    "$$GST43BCT8DFF:L$$/bond-trading-strategies/internal/agent/session"
    "$$GST43BCT8DFF:L$$/bond-trading-strategies/internal/agent/store"
    "$$GST43BCT8DFF:L$$/bond-trading-strategies/internal/agent/strategies"
    "$$GST43BCT8DFF:L$$/bond-trading-strategies/internal/agent/users"
    "$$GST43BCT8DFF:L$$/bond-trading-strategies/internal/agent/wasmruntime"
    "$$GST43BCT8DFF:L$$/bond-trading-strategies/internal/agent/wsbroker"
    Server         *httpapi.Server
    StrategiesJani *strategies.Janitor
    BacktestJani   *backtest.Janitor
    WSBroker       *wsbroker.Broker
    LiveOrch       *orchestrator.Orchestrator
    WASMRuntime    *wasmruntime.Runtime
    // unexported stores
    History *store.HistoryStore
    // ...
}

func Wire(
    ctx context.Context,
    pool *pgxpool.Pool,
    encryptionKey []byte,
    cfg config.Config,
    log *slog.Logger,
) (*Runtime, error) {
    sealer, err := secrets.NewSealer(encryptionKey)
    if err != nil {
        return nil, fmt.Errorf("sealer: %w", err)
    }

    usersStore, err := users.New(ctx, pool, log)
    if err != nil { return nil, fmt.Errorf("users: %w", err) }

    sessionStore, err := session.New(ctx, pool, log)
    if err != nil { return nil, fmt.Errorf("session: %w", err) }

    providerStore, err := providerconfig.New(ctx, pool, sealer, log)
    if err != nil { return nil, fmt.Errorf("providerconfig: %w", err) }

    strategiesStore, err := strategies.New(ctx, pool, log)
    if err != nil { return nil, fmt.Errorf("strategies: %w", err) }

    strategiesJani, err := strategies.NewJanitor(ctx, pool, log, cfg)
    if err != nil { return nil, fmt.Errorf("strategies janitor: %w", err) }

    backtestStore, err := backtest.New(ctx, pool, log)
    if err != nil { return nil, fmt.Errorf("backtest: %w", err) }

    backtestJani, err := backtest.NewJanitor(ctx, pool, log, cfg)
    if err != nil { return nil, fmt.Errorf("backtest janitor: %w", err) }

    historyStore := store.NewHistoryStore(pool)

    safetyKernel, err := safety.NewKernel(pool)
    if err != nil { return nil, fmt.Errorf("safety: %w", err) }

    wasmRuntime, err := wasmruntime.New(ctx, cfg.WasmArtifactRoot, safetyKernel, log)
    if err != nil { return nil, fmt.Errorf("wasmruntime: %w", err) }

    broker, err := wsbroker.New(ctx, cfg.WSBrokerURL, log)
    if err != nil { return nil, fmt.Errorf("wsbroker: %w", err) }

    liveOrch, err := orchestrator.New(ctx, broker, wasmRuntime, strategiesStore, audit.New(pool), log)
    if err != nil { return nil, fmt.Errorf("orchestrator: %w", err) }

    server, err := httpapi.New(ctx, httpapi.Deps{
        Users:           usersStore,
        Session:         sessionStore,
        ProviderConfig:  providerStore,
        Strategies:      strategiesStore,
        Backtest:        backtestStore,
        History:         historyStore,
        Safety:          safetyKernel,
        WASM:            wasmRuntime,
        Orchestrator:    liveOrch,
        Broker:          broker,
        Audit:           audit.New(pool),
        Cfg:             cfg,
        Log:             log,
    })
    if err != nil { return nil, fmt.Errorf("httpapi: %w", err) }

    return &Runtime{
        Server:         server,
        StrategiesJani: strategiesJani,
        BacktestJani:   backtestJani,
        WSBroker:       broker,
        LiveOrch:       liveOrch,
        WASMRuntime:    wasmRuntime,
        History:        historyStore,
    }, nil
}

func (r *Runtime) Close(ctx context.Context) error {
    if r.StrategiesJani != nil { r.StrategiesJani.Stop() }
    if r.BacktestJani != nil { r.BacktestJani.Stop() }
    if r.WSBroker != nil { _ = r.WSBroker.Close() }
    if r.WASMRuntime != nil { _ = r.WASMRuntime.Close() }
    if r.LiveOrch != nil { _ = r.LiveOrch.Close() }
    return nil
}
```

The actual struct field names, the `Deps` struct, the constructor signatures — adjust to match what each package's `New` actually exports. The spec's `Deps` struct is illustrative.

- [ ] **Step 2: Run `go build ./...`**

```bash
go build ./...
```

Expected: clean. If a missing import or a mismatched signature surfaces, fix in place — the layer checkpoint is the safety net.

### Task L8.2: Wire `cmd/strategy-server/main.go` to mount the agent runtime

**Files:**
- Modify: `cmd/strategy-server/main.go`

- [ ] **Step 1: Find the existing `strategyhttp.NewHandler(...)` chain**

```bash
grep -n -E "strategyhttp|requireAuth|v1/" cmd/strategy-server/main.go
```

- [ ] **Step 2: Add the agent runtime mount**

After the existing `strategyhttp.NewHandler(...)` is built, add:

```go
agentRuntime, err := agentwiring.Wire(ctx, pool, encryptionKey, agentCfg, log)
if err != nil {
    return fmt.Errorf("agent wiring: %w", err)
}
defer func() { _ = agentRuntime.Close(context.Background()) }()

mux.Handle("/v1/agent/", requireAuth(agentRuntime.Server.RoutesAt("/v1/agent")))
```

The `requireAuth` wrapper is the same one used for the strategy-server's own routes. The host's broader rate limiters (IP/global/read/write) already wrap the entire mux; the agent's per-user rate limiter is in the httpapi's middleware.

- [ ] **Step 3: Build the binary**

```bash
go build ./cmd/strategy-server
```

Expected: binary produced. Do not run it in this commit (boot verification is the follow-up).

### Task L8.3: Wire config env vars

**Files:**
- Modify: `internal/agent/config/config.go`
- Modify: `.env` + `.env.example`

- [ ] **Step 1: Add renamed + kept var reads**

In `internal/agent/config/config.go`, ensure:

- Renamed vars read the host's existing name (e.g. `DoraBaseURL` reads `DORA_BASE_URL`, not `AGENT_DORA_BASE_URL`).
- Kept vars read `AGENT_*` directly (e.g. `LLMTimeout` reads `AGENT_LLM_TIMEOUT`).
- Drop the `dotenv.go` file from the agent's old `internal/config` (already in `internal/agent/config/dotenv.go`); the host's main already loads `.env`.

- [ ] **Step 2: Update `.env` and `.env.example`**

Add every kept `AGENT_*` var to `.env` and `.env.example` with a sensible default. Renames don't require new file entries (the host's existing var name is unchanged). Concretely:

```bash
cat >> .env <<'EOF'
AGENT_RATE_LIMIT_PER_MIN=20
AGENT_LLM_TIMEOUT=60
AGENT_LLM_MAX_ITERS=10
AGENT_MAX_PROMPT_BYTES=65536
AGENT_MODEL_CAPS_PATH=configs/model_caps.json
AGENT_DORA_TOOLS_ENABLED=true
AGENT_GENERATE_MAX_REPAIRS=2
AGENT_GENERATE_MAX_FILES=50
AGENT_GENERATE_MAX_BYTES=1048576
AGENT_LIVE_MEMORY_LIMIT=104857600
AGENT_LIVE_RESTART_WINDOW=60
AGENT_LIVE_MAX_RESTARTS=5
AGENT_WSBROKER_URL=
AGENT_CAPTURE_PENDING_RETENTION=86400
AGENT_CAPTURE_PENDING_SWEEP_INTERVAL=300
AGENT_WASM_ARTIFACT_ROOT=/tmp/agent-wasm
AGENT_ALLOW_HTTP_BASE_URL=false
EOF
```

(Replace placeholder values with the spec's defaults.)

- [ ] **Step 3: Run the L8 layer checkpoint**

```bash
go build ./... && go test ./... && pre-commit run --all-files
```

Expected: clean. L8 is complete.

### Task L8.4: Refresh `TODO.md`

- [ ] **Step 1: Update the "dora-agent integration follow-ups" section**

The section currently lists "Tasks 4.2 / 4.3 / 4.5 / 4.6 / 4.7 / 4.8 / 5.1 / 5.2 are blocked on interdependent seam rewrites." Replace with:

```markdown
## dora-agent integration follow-ups

### Landed on tan/feat-integrate-dora-agent (Phases 0-3 + Phase 4 leaf + llm + config)

(8 commits listed in commit history: 947641e through e76c53c.)

### Landed in the L3-L8 re-plan commit

L3 stores (users, session, providerconfig), L4 orchestration stores (strategies,
backtest, deployment), L5 live + WASM (wasmruntime, wsbroker, orchestrator), L6
tools + servertest, L7 httpapi + history_store, L8 wiring + main.go mount +
config env-var wiring + .env updates. Build green; pre-commit green; tests pass.

### Deferred to the follow-up commit

- OpenAPI spec merge (agent's openapi.json paths get joined into the host's).
- `cmd/strategy-server` binary launch verification against a testcontainers
  Postgres (`/v1/agent/sessions` returns 401 unauthed, `/healthz` returns 200,
  `/v1/openapi` lists `/v1/agent/*` paths).
- New tests surfaced by the live verification (smoke, e2e).
- Final closeout of this follow-up section.

### Deferred out of this repo (dora-agent repo deletions)

- `cmd/agent-cli`, `internal/auth`, `internal/secrets/{secrets,env,aesgcm,kms}.go`,
  `internal/store`, `internal/serveradmin` — deleted in a separate commit on the dora-agent repo, not in this one.
```

### Task L8.5: Trim the original 2465-line plan

- [ ] **Step 1: Replace the original plan**

The 2465-line `docs/superpowers/plans/2026-09-08-dora-agent-integration.md` is now superseded. Either:

- Delete the file (preferred — git history preserves the original).
- OR add a header at the top: "Superseded by `2026-09-09-dora-agent-integration-replan.md`. The original per-task plan did not decompose cleanly; this re-plan replaces it with a bottom-up layer plan."

Pick one. If deleting, also update any references in the host's `AGENTS.md` or `README.md` that linked to the original.

- [ ] **Step 2: Run the final L8 layer checkpoint**

```bash
go build ./... && go test ./... && pre-commit run --all-files
```

Expected: clean. All six layers are in. Do not commit yet — the user reviews and commits.

---

## Final commit

After all six layers are green, stage every changed file. One user-driven commit with a conventional-commit body enumerating:

- L3-L8 packages added (with file counts).
- Seam rewrites performed (pool sharing, Sealer, `agent.` schema qualification, `RoutesAt`, tern migration deletion, env-var rename, `.env` updates).
- Tests copied (and excluded: `internal/e2e/*`, `cmd/agent-cli`-referencing).
- TODO.md refresh.
- Plan trim.
- What's deferred to the follow-up commit.

Suggested message:

```
feat(agent): complete dora-agent integration via L3-L8 layer plan

L3 (CRUD stores): users, session, providerconfig
L4 (orchestration stores): strategies, backtest, deployment
L5 (live + WASM): wasmruntime, wsbroker, orchestrator
L6 (tools + servertest): tools/{dora,strategies,backtest,deployment,generate}
L7 (httpapi + history_store): httpapi with RoutesAt(prefix), store/history_store
L8 (wiring): wiring.Wire, cmd/strategy-server/main.go mount, config env-var
wiring, .env updates, TODO.md refresh, plan trim

Seam rewrites: shared pgxpool, internal/agent/secrets.Sealer replaces
envelope encryption, agent. schema prefix on every SQL, tern migration
replaces servertest/store.MigrateConn (internal/migration itself ports as a runtime gate), RoutesAt("/v1/agent") mount.
excluded.

Deferred to follow-up commit: OpenAPI merge, cmd/strategy-server binary
launch verification, new tests surfaced by live verification, TODO.md
closeout. dora-agent repo deletions are out of this repo.

Closes: bond-trading-strategies/development/TODO.md "dora-agent
integration follow-ups" section (partially — final closeout in follow-up).
```

The user reviews the diff and commits personally per the worktree's `AGENTS.md`. Do not run `git commit` yourself.

---

## Self-review

- **Spec coverage:** L3 covers Tasks 4.2 + part of 4.8. L4 covers parts of 4.6/4.7 (the stub indirection). L5 covers Task 4.3. L6 covers Task 4.5. L7 covers Tasks 4.6/4.7 (the real implementation) and the rest of 4.8. L8 covers Tasks 5.1/5.2 plus the env-var rename and the main.go mount. The spec's "out of scope" items (OpenAPI merge, boot verification, dora-agent repo deletions) are explicitly deferred.
- **Placeholder scan:** No "TBD" or "TODO" markers. Each step is concrete.
- **Type consistency:** `Runtime.Server.RoutesAt(prefix)` defined in L7.2, used in L8.2. `wiring.Wire` defined in L8.1, used in L8.2. `agentsecrets.NewSealer(key)` defined in L0 (already landed), used in L3.3 and L8.1. `sealer.Seal(ctx, plaintext)` / `sealer.Open(ctx, sealed)` used in L3.3 and (transitively) L8.1.
- **Self-reference:** The original 2465-line plan is referenced for context in the Background section. The trim in L8.5 either deletes it or marks it as superseded.
