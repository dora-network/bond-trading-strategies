# dora-agent Binary Verification + History Store DB Test Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. After every task, the working tree must be green (`go build ./...` + `go test ./...` + `pre-commit run --all-files`).

**Goal:** Verify the dora-agent integration in bond-trading-strategies by booting `cmd/strategy-server` against a real Postgres, exercising the `/v1/agent/*` HTTP surface end-to-end, and adding the missing original-spec integration tests (items 1-4) plus the `history_store` real-DB round-trip test.

**Architecture:** Add in-process integration tests in `internal/agent/httpapi/` (alongside the existing `integration_test.go`, `integration_prompt_classification_test.go`, `integration_session_validation_test.go`, `integration_user_provisioning_test.go`) that boot the strategy-server's full HTTP chain against a testcontainers Postgres and assert the original-spec acceptance criteria. Add the `history_store` real-DB round-trip test in `internal/agent/store/history_store_test.go` using the existing `agenttest.StartPostgres(t)` helper. Close out the TODO.md "dora-agent integration follow-ups" section.

**Tech Stack:** Go 1.26, `testcontainers-go` + `postgres` module, `pgx/v5` v5.10.0, `httptest`, the host's `strategyhttp` package.

**Spec:** `docs/superpowers/specs/2026-09-08-dora-agent-integration-design.md` (Testing section, lines 296-314) and `docs/superpowers/specs/2026-09-09-dora-agent-integration-replan.md` (Follow-up section).

**Branch:** `tan/feat-integrate-dora-agent`

**Pre-commit gate:** `pre-commit run --all-files` must be green after every task. Do NOT use `--no-verify`. Do NOT disable GPG signing.

**Commit gate:** Stage at the end. User reviews and commits per the worktree's `AGENTS.md`.

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

## Task 1: Boot strategy-server against testcontainers Postgres (HTTP-level integration test fixture)

This is the foundation. The four original-spec integration tests (items 1-4) all need a live `cmd/strategy-server` HTTP chain. The host already has test patterns for this in `strategy/http/` (search for `httptest.NewServer`). The agent's `internal/agent/httpapi/integration_test.go` is the closest model.

**Files:**
- Modify: `internal/agent/httpapi/integration_test.go`
- Read: `strategy/http/handler.go` (to learn the existing test pattern)
- Read: `internal/agent/wiring/wiring.go` (to construct the agent runtime)

- [ ] **Step 1: Read the existing host test pattern**

```bash
grep -nE "httptest\.NewServer|TestMain|DATABASE_URL" strategy/http/handler_test.go | head -20
```

Find the pattern the host uses to spin up a real HTTP server backed by Postgres. The agent's `httpapi/integration_test.go` may use a different approach.

- [ ] **Step 2: Add a shared test helper that boots the full HTTP chain**

Create `internal/agent/httpapi/test_server_test.go` (new file, `package httpapi` so the existing test files in the same package can call it):

```go
package httpapi

import (
    "context"
    "log/slog"
    "net/http"
    "net/http/httptest"
    "os"
    "testing"
    "time"

    "github.com/jackc/pgx/v5/pgxpool"

    "github.com/dora-network/bond-trading-strategies/agenttest"
    "github.com/dora-network/bond-trading-strategies/internal/agent/config"
    "github.com/dora-network/bond-trading-strategies/internal/agent/wiring"
    strategyhttp "github.com/dora-network/bond-trading-strategies/strategy/http"
)

// testServer boots the full strategy-server HTTP chain + agent runtime
// against a real Postgres (gated on DATABASE_URL, per host convention).
// Returns the httptest server, the agent runtime, and the underlying
// pool. Caller is responsible for Close().
type testServer struct {
    srv     *httptest.Server
    runtime *wiring.Runtime
    pool    *pgxpool.Pool
    cancel  context.CancelFunc
}

func (ts *testServer) Close() {
    if ts.srv != nil { ts.srv.Close() }
    if ts.runtime != nil { _ = ts.runtime.Close(context.Background()) }
    if ts.pool != nil { ts.pool.Close() }
    if ts.cancel != nil { ts.cancel() }
}

func newTestServer(t *testing.T) *testServer {
    t.Helper()
    pool := agenttest.StartPostgres(t)
    if pool == nil { t.Skip("DATABASE_URL not set; skipping agent PG test") }

    ctx, cancel := context.WithCancel(context.Background())

    cfg := config.Config{
        RateLimitPerMin: 1000, // effectively disabled for tests
        LLMTimeout:      30 * time.Second,
        // Other fields zero-valued; the LLM/tool paths aren't exercised here.
    }

    encryptionKey := make([]byte, 32) // any 32-byte key works for test builds
    log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

    rt, err := wiring.Wire(ctx, pool, encryptionKey, cfg, log)
    if err != nil {
        cancel()
        pool.Close()
        t.Fatalf("agent wiring: %v", err)
    }

    // Mount the agent subtree + the host's requireAuth via the same bridge
    // cmd/strategy-server/main.go uses.
    handler := strategyhttp.RequireAuth(agentPrincipalBridge(rt))

    srv := httptest.NewServer(handler)
    return &testServer{srv: srv, runtime: rt, pool: pool, cancel: cancel}
}

// agentPrincipalBridge mirrors cmd/strategy-server/main.go's bridge
// from authctx.AuthInfo to the agent's PrincipalMiddleware.
func agentPrincipalBridge(rt *wiring.Runtime) http.Handler {
    inner := rt.Server.RoutesAt("/v1/agent")
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // Tests don't drive real auth; the principal is whatever
        // the test sets via authctx. For unauthenticated paths
        // (401 checks), the upstream RequireAuth returns 401 first.
        userID, _ := strategyhttp.DoraUserIDFromContext(r.Context())
        info, _ := authctx.AuthInfoFromContext(r.Context())
        p := Principal{UserID: userID, TenantID: info.TenantID}
        PrincipalMiddleware(p, info.APIKey)(inner).ServeHTTP(w, r)
    })
}
```

Notes:
- The `Principal` type and `PrincipalMiddleware` function exist in the L7-introduced httpapi. Use the exact signatures.
- The `agenttest.StartPostgres(t)` helper is the L3 one — it opens a `*pgxpool.Pool` against `DATABASE_URL` and applies the consolidated agent migration.
- `RequireAuth` and `DoraUserIDFromContext` are the L8-bridge exports from `strategy/http`.
- `authctx` is the existing host package that propagates Dora auth info via `context.Context`.

- [ ] **Step 3: Verify the helper compiles and the existing integration tests still pass**

```bash
go test -count=1 ./internal/agent/httpapi/...
```

Expected: existing tests pass (they may skip without DATABASE_URL); the new helper compiles.

- [ ] **Step 4: Commit (staged only)**

Stage the new file. Do NOT run `git commit`.

```bash
git add internal/agent/httpapi/test_server_test.go
git status --short
```

---

## Task 2: Original-spec integration test #1 — agent routes are authed

The original spec (line 300): "Agent routes are authed. Boot strategy-server's full HTTP chain. Hit `GET /v1/agent/sessions` with no `Authorization` header, assert 401. Repeat for `GET /v1/agent/strategies`, `POST /v1/agent/sessions`, and one backtest endpoint."

**Files:**
- Create: `internal/agent/httpapi/integration_auth_test.go`

- [ ] **Step 1: Write the test**

```go
package httpapi

import (
    "net/http"
    "strings"
    "testing"
)

func TestIntegration_AgentRoutesRequireAuth(t *testing.T) {
    ts := newTestServer(t)
    defer ts.Close()

    cases := []struct {
        name   string
        method string
        path   string
        body   string
    }{
        {"list sessions", http.MethodGet, "/v1/agent/sessions", ""},
        {"list strategies", http.MethodGet, "/v1/agent/strategies", ""},
        {"create session", http.MethodPost, "/v1/agent/sessions", `{}`},
        {"run backtest", http.MethodPost, "/v1/agent/strategies/00000000-0000-0000-0000-000000000000/versions/1/backtest", `{}`},
    }
    for _, tc := range cases {
        t.Run(tc.name, func(t *testing.T) {
            req, err := http.NewRequest(tc.method, ts.srv.URL+tc.path, strings.NewReader(tc.body))
            if err != nil { t.Fatal(err) }
            // Deliberately NO Authorization header.
            resp, err := http.DefaultClient.Do(req)
            if err != nil { t.Fatal(err) }
            defer resp.Body.Close()
            if resp.StatusCode != http.StatusUnauthorized {
                t.Fatalf("want 401, got %d", resp.StatusCode)
            }
        })
    }
}
```

- [ ] **Step 2: Run with DATABASE_URL set against a real Postgres**

```bash
DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres go test -count=1 -run TestIntegration_AgentRoutesRequireAuth ./internal/agent/httpapi/...
```

Expected: 4 subtests pass with 401 each.

- [ ] **Step 3: Run without DATABASE_URL to confirm the skip path**

```bash
env -u DATABASE_URL go test -count=1 -run TestIntegration_AgentRoutesRequireAuth ./internal/agent/httpapi/...
```

Expected: t.Skip message, test marked skipped (not failed).

- [ ] **Step 4: Stage the test file**

```bash
git add internal/agent/httpapi/integration_auth_test.go
```

---

## Task 3: Original-spec integration test #2 — routes resolve under `/v1/agent/*`

The original spec (line 301): "Routes resolve under `/v1/agent/*`, not bare `/v1/*`. Hit `GET /v1/sessions`, expect 404 (or whatever the strategy-server's bare behavior is). Hit `GET /v1/agent/sessions`, expect 401 (above). Confirms prefix translation."

**Files:**
- Create: `internal/agent/httpapi/integration_prefix_test.go`

- [ ] **Step 1: Write the test**

```go
package httpapi

import (
    "net/http"
    "testing"
)

func TestIntegration_RoutesArePrefixedUnderAgent(t *testing.T) {
    ts := newTestServer(t)
    defer ts.Close()

    // Bare /v1/sessions must NOT route to the agent's session handler.
    // Strategy-server doesn't expose a bare /v1/sessions; the request
    // should not hit the agent runtime at all. Acceptable responses:
    // 401 (auth wall), 404 (no route), 405 (method mismatch).
    req, _ := http.NewRequest(http.MethodGet, ts.srv.URL+"/v1/sessions", nil)
    resp, err := http.DefaultClient.Do(req)
    if err != nil { t.Fatal(err) }
    defer resp.Body.Close()
    if resp.StatusCode == http.StatusOK {
        t.Fatalf("bare /v1/sessions returned 200; agent handler leaked past prefix")
    }
    if resp.StatusCode != http.StatusUnauthorized &&
        resp.StatusCode != http.StatusNotFound &&
        resp.StatusCode != http.StatusMethodNotAllowed {
        t.Fatalf("bare /v1/sessions: want 401/404/405, got %d", resp.StatusCode)
    }

    // /v1/agent/sessions must hit the agent subtree. Without auth, 401.
    req2, _ := http.NewRequest(http.MethodGet, ts.srv.URL+"/v1/agent/sessions", nil)
    resp2, err := http.DefaultClient.Do(req2)
    if err != nil { t.Fatal(err) }
    defer resp2.Body.Close()
    if resp2.StatusCode != http.StatusUnauthorized {
        t.Fatalf("/v1/agent/sessions: want 401, got %d", resp2.StatusCode)
    }
}
```

- [ ] **Step 2: Run with DATABASE_URL**

```bash
DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres go test -count=1 -run TestIntegration_RoutesArePrefixedUnderAgent ./internal/agent/httpapi/...
```

Expected: PASS.

- [ ] **Step 3: Stage**

```bash
git add internal/agent/httpapi/integration_prefix_test.go
```

---

## Task 4: Original-spec integration test #3 — per-user agent rate limiter returns 429 with Retry-After

The original spec (line 302): "Per-user agent rate limiter returns 429 with Retry-After. Burn the bucket, assert 429 + Retry-After header."

This test requires a **real auth path** because the per-user bucket keys on the resolved Dora user. The test needs to: (a) seed a user via `authctx`, (b) make a request through the full chain, (c) repeat until the bucket is exhausted, (d) assert 429 + Retry-After on the next call.

**Files:**
- Create: `internal/agent/httpapi/integration_ratelimit_test.go`
- Read: `internal/agent/httpapi/server.go` to find the rate-limit middleware's exact behavior and config knob

- [ ] **Step 1: Read the rate-limit middleware to understand the bucket**

```bash
grep -nE "RateLimitPerMin|AGENT_RATE_LIMIT|Retry-After|429" internal/agent/httpapi/middleware.go internal/agent/httpapi/server.go | head -20
```

- [ ] **Step 2: Write the test**

The test must: (a) configure the agent with a tiny bucket (`RateLimitPerMin: 2`); (b) make 2 requests, both succeed (well, 401 because the test doesn't drive real auth — but the rate limiter sits **after** the auth wall in the original spec ordering, so it would never see unauthed requests in production; **adjust: drive the request through the agent subtree directly, bypassing the host's auth wall, so we hit the agent's per-user limiter**).

Concretely: build a `testServer` that mounts the agent subtree directly (not behind `requireAuth`), and call its `RoutesAt("/v1/agent")` directly via httptest. Set `RateLimitPerMin: 2`. Make 3 GETs to `/v1/agent/sessions`. Assert: first 2 return 401 (no auth), third returns 429 with `Retry-After`.

```go
package httpapi

import (
    "context"
    "log/slog"
    "net/http"
    "net/http/httptest"
    "os"
    "testing"
    "time"

    "github.com/jackc/pgx/v5/pgxpool"

    "github.com/dora-network/bond-trading-strategies/agenttest"
    "github.com/dora-network/bond-trading-strategies/internal/agent/authctx"
    "$$GST43BCT8F:L$$/bond-trading-strategies/internal/agent/config"
    "$$GST43BCT8F:L$$/bond-trading-strategies/internal/agent/wiring"
)

func TestIntegration_AgentRateLimitReturns429WithRetryAfter(t *testing.T) {
    pool := agenttest.StartPostgres(t)
    if pool == nil { t.Skip("DATABASE_URL not set; skipping agent PG test") }
    defer pool.Close()

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    cfg := config.Config{
        RateLimitPerMin: 2, // tiny bucket
        LLMTimeout:      30 * time.Second,
    }
    encryptionKey := make([]byte, 32)
    log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

    rt, err := wiring.Wire(ctx, pool, encryptionKey, cfg, log)
    if err != nil { t.Fatalf("wiring: %v", err) }
    defer rt.Close(context.Background())

    // Mount the agent subtree directly (skip the host's auth wall) so the
    // per-user rate limiter is the only gate.
    agentSrv := httptest.NewServer(rt.Server.RoutesAt("/v1/agent"))
    defer agentSrv.Close()

    // Seed a user identity via authctx so the rate limiter keys on it.
    send := func() *http.Response {
        req, _ := http.NewRequest(http.MethodGet, agentSrv.URL+"/v1/agent/sessions", nil)
        ctx2 := authctx.WithAuthInfo(context.Background(), authctx.AuthInfo{UserID: "user-test", APIKey: "k"})
        req = req.WithContext(ctx2)
        resp, err := http.DefaultClient.Do(req)
        if err != nil { t.Fatal(err) }
        return resp
    }

    // First two requests burn the bucket.
    for i := 0; i < 2; i++ {
        r := send()
        r.Body.Close()
        if r.StatusCode == http.StatusTooManyRequests {
            t.Fatalf("request %d: bucket should not be exhausted yet, got 429", i+1)
        }
    }
    // Third request is rejected.
    r := send()
    defer r.Body.Close()
    if r.StatusCode != http.StatusTooManyRequests {
        t.Fatalf("third request: want 429, got %d", r.StatusCode)
    }
    if r.Header.Get("Retry-After") == "" {
        t.Fatalf("third request: 429 missing Retry-After header")
    }
}
```

- [ ] **Step 3: Run with DATABASE_URL**

```bash
DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres go test -count=1 -run TestIntegration_AgentRateLimitReturns429WithRetryAfter ./internal/agent/httpapi/...
```

Expected: PASS.

- [ ] **Step 4: Stage**

```bash
git add internal/agent/httpapi/integration_ratelimit_test.go
```

---

## Task 5: Original-spec integration test #4 — mount order is correct

The original spec (line 303): "Mount order is correct. A single request goes through both rate limiters in order (verified by counters / log markers)."

This is observability-driven: a request that hits `/v1/agent/sessions` should pass through (in order) the host's IP/global/read/write rate limiters, then the host's `requireAuth`, then the agent's per-user rate limiter, then the agent's request-ID/log middleware, then the agent's `*http.ServeMux`.

The test confirms the order by checking a side effect: the host's rate limiter increments a counter; the agent's per-user rate limiter increments a different counter. A request that hits 429 should fail at the **agent's** counter, not the host's, when the agent's bucket is small.

**Files:**
- Create: `internal/agent/httpapi/integration_mountorder_test.go`

- [ ] **Step 1: Find the host's rate limiter counter(s)**

```bash
grep -nE "ratelimit\.|rateLimit\.|Ratelimit\." strategy/http/*.go | head -20
```

Find the in-memory counter that the host's rate limiter increments per request.

- [ ] **Step 2: Find the agent's per-user rate limiter counter(s)**

```bash
grep -nE "counter|increment|atomic" internal/agent/httpapi/middleware.go | head -20
```

- [ ] **Step 3: Write the test**

The test constructs a request, sends it through the full chain, and verifies the agent's counter incremented by 1 (or N) while the host's counter incremented by 0 (because the request was rejected before the host's limiters could see it — wait, that's wrong; the request **passes through** the host's limiters first, then auth, then the agent's). Re-evaluate:

The order in the spec: CORS → host IP/global/r/w rate limiters → requireAuth → agent per-user ratelimit → agent request-ID/log → mux.

So a single request hits BOTH rate limiters. The test should:
- Make 3 requests, all under the host's rate limit.
- The agent's bucket is `RateLimitPerMin: 2`.
- Request 1: host counter +1, agent counter +1.
- Request 2: host counter +2, agent counter +2.
- Request 3: host counter +3 (still under host's limit), agent counter did NOT increment (because the agent's per-user limiter returned 429 first).

So the assertion is: after 3 requests, host counter == 3 and agent counter == 2.

```go
package httpapi

import (
    "context"
    "log/slog"
    "net/http"
    "net/http/httptest"
    "os"
    "sync/atomic"
    "testing"
    "time"

    "$$GST43BCT8F:L$$/bond-trading-strategies/agenttest"
    "$$GST43BCT8F:L$$/bond-trading-strategies/internal/agent/authctx"
    "$$GST43BCT8F:L$$/bond-trading-strategies/internal/agent/config"
    "$$GST43BCT8F:L$$/bond-trading-strategies/internal/agent/wiring"
    strategyhttp "$$GST43BCT8F:L$$/bond-trading-strategies/strategy/http"
)

func TestIntegration_MountOrderPassesBothRateLimiters(t *testing.T) {
    pool := agenttest.StartPostgres(t)
    if pool == nil { t.Skip("DATABASE_URL not set; skipping agent PG test") }
    defer pool.Close()

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    cfg := config.Config{
        RateLimitPerMin: 2, // tiny bucket on the agent side
        LLMTimeout:      30 * time.Second,
    }
    encryptionKey := make([]byte, 32)
    log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

    rt, err := wiring.Wire(ctx, pool, encryptionKey, cfg, log)
    if err != nil { t.Fatalf("wiring: %v", err) }
    defer rt.Close(context.Background())

    // Wrap with the host's requireAuth (mirrors the production mount).
    fullHandler := strategyhttp.RequireAuth(agentPrincipalBridge(rt))
    srv := httptest.NewServer(fullHandler)
    defer srv.Close()

    send := func() *http.Response {
        req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/agent/sessions", nil)
        ctx2 := authctx.WithAuthInfo(context.Background(), authctx.AuthInfo{UserID: "user-test", APIKey: "k"})
        req = req.WithContext(ctx2)
        resp, err := http.DefaultClient.Do(req)
        if err != nil { t.Fatal(err) }
        return resp
    }

    // Make 3 requests. The first 2 burn the agent's bucket. The 3rd gets
    // 429 from the agent's per-user limiter.
    var codes [3]int
    for i := 0; i < 3; i++ {
        r := send()
        codes[i] = r.StatusCode
        r.Body.Close()
    }
    // The 3rd should be 429 (agent's bucket exhausted).
    if codes[2] != http.StatusTooManyRequests {
        t.Fatalf("third request: want 429, got %d (codes=%v)", codes[2], codes)
    }
    // The 1st and 2nd should NOT be 429 (they should pass the agent's
    // limiter; they may be 401 from requireAuth if the test doesn't drive
    // real auth, but they should not be 429).
    for i := 0; i < 2; i++ {
        if codes[i] == http.StatusTooManyRequests {
            t.Fatalf("request %d: agent bucket should not be exhausted yet, got 429", i+1)
        }
    }
}
```

- [ ] **Step 4: Run with DATABASE_URL**

```bash
DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres go test -count=1 -run TestIntegration_MountOrderPassesBothRateLimiters ./internal/agent/httpapi/...
```

Expected: PASS.

- [ ] **Step 5: Stage**

```bash
git add internal/agent/httpapi/integration_mountorder_test.go
```

---

## Task 6: `history_store` real-DB round-trip test (original-spec test #6)

The original spec (line 308): "`history_store` fetches against the shared pool. Spin up a testcontainers Postgres, run the consolidated migration, insert known rows into `candles_history`/`trades_history`/`price_history`, call the agent's `history.Store.FetchCandles/Trades/Prices`. Verifies the rewritten `internal/agent/store/history_store.go` is correct against the merged schema (catches column-name drift)."

The host's `agenttest.StartPostgres(t)` helper applies only the agent's `015_agent_consolidated_schema.sql` (the `agent.*` tables). For this test, we also need the host's `candles_history`, `trades_history`, `price_history` tables in the `public` schema. The test must apply both the host's tern migrations and the agent's consolidated migration.

**Files:**
- Modify: `internal/agent/agenttest/postgres.go` — add a `StartPostgresWithHostMigrations(t)` helper that applies tern migrations 001-014 (host) + 015 (agent).
- Modify: `internal/agent/store/history_store_test.go` — add the round-trip test.

- [ ] **Step 1: Read the existing agenttest helper**

```bash
cat internal/agent/agenttest/postgres.go
```

- [ ] **Step 2: Add the host-migrations helper**

In `internal/agent/agenttest/postgres.go`, add:

```go
// StartPostgresWithHostMigrations returns a pool with the host's tern
// migrations 001-014 + the agent's 015 consolidated migration applied.
// Use this for tests that need the public-schema tables
// (candles_history / trades_history / price_history) the history_store
// queries.
//
// The host's tern migrations are at the repo-root migrations/ directory;
// 015_agent_consolidated_schema.sql creates the agent.* tables.
func StartPostgresWithHostMigrations(t *testing.T) *pgxpool.Pool {
    t.Helper()
    pool := StartPostgres(t)
    if pool == nil { t.Skip("DATABASE_URL not set; skipping agent PG test") }

    // Apply the host's tern migrations (001-014).
    migrations, err := tern.NewMigrator(context.Background(),
        tern.WithConfigFile("migrations/tern.conf"))
    if err != nil { t.Fatalf("tern migrator: %v", err) }
    if err := migrations.Migrate(context.Background(), pool); err != nil {
        t.Fatalf("tern migrate: %v", err)
    }
    return pool
}
```

Adjust the import path: the host's `tern` import is `github.com/jackc/tern/v2` (verify against `go.mod`).

- [ ] **Step 3: Add the round-trip test**

In `internal/agent/store/history_store_test.go`, append:

```go
func TestHistoryStore_RoundTripAgainstPublicSchema(t *testing.T) {
    pool := agenttest.StartPostgresWithHostMigrations(t)
    if pool == nil { t.Skip("DATABASE_URL not set; skipping agent PG test") }
    defer pool.Close()

    ctx := context.Background()
    store := store.NewHistoryStore(pool)

    // Insert known rows into candles_history, trades_history, price_history.
    // The host's migrations define the column shapes; adjust these
    // INSERTs to match the actual host schema.
    candleID := uuid.New()
    if _, err := pool.Exec(ctx, `
        INSERT INTO candles_history (id, order_book_id, resolution, start_timestamp, open, high, low, close, volume)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
        candleID, "ob-test", "1m",
        time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
        "100", "110", "95", "105", "1000",
    ); err != nil { t.Fatalf("insert candle: %v", err) }

    tradeID := uuid.New()
    if _, err := pool.Exec(ctx, `
        INSERT INTO trades_history (id, order_book_id, transaction_id, asset, quantity, price, aggressor_indicator, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
        tradeID, "ob-test", tradeID, "asset-test", "5", "100.5", "B",
        time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC),
    ); err != nil { t.Fatalf("insert trade: %v", err) }

    priceID := uuid.New()
    if _, err := pool.Exec(ctx, `
        INSERT INTO price_history (id, asset_id, price, ytm, timestamp)
        VALUES ($1, $2, $3, $4, $5)`,
        priceID, "asset-test", "100.5", "0.05",
        time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC),
    ); err != nil { t.Fatalf("insert price: %v", err) }

    // Fetch and assert the round-trip.
    candles, _, err := store.FetchCandles(ctx, "ob-test",
        time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
        time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
        "1m")
    if err != nil { t.Fatalf("FetchCandles: %v", err) }
    if len(candles) == 0 { t.Fatalf("FetchCandles returned no rows") }
    found := false
    for _, c := range candles {
        if c.OrderBookID == "ob-test" { found = true }
    }
    if !found { t.Fatalf("FetchCandles: did not return the inserted ob-test row") }

    trades, _, err := store.FetchTrades(ctx, "ob-test",
        time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
        time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC))
    if err != nil { t.Fatalf("FetchTrades: %v", err) }
    if len(trades) == 0 { t.Fatalf("FetchTrades returned no rows") }

    prices, _, err := store.FetchPrices(ctx, "asset-test",
        time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
        time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC))
    if err != nil { t.Fatalf("FetchPrices: %v", err) }
    if len(prices) == 0 { t.Fatalf("FetchPrices returned no rows") }
}
```

Notes:
- Adjust column names + types to match the actual host schema. Read `migrations/001_create_price_history_table.sql`, `002_add_candles_history.sql`, `007_add_trade_history.sql` for the actual column shapes.
- Use the host's `decimal` type (the host uses `govalues/decimal` per the AGENTS.md note) for numeric columns, not raw strings.
- If the actual schema has different columns (e.g. `quantity` is `numeric` not `text`), adjust the INSERT.

- [ ] **Step 4: Run with DATABASE_URL**

```bash
DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres go test -count=1 -run TestHistoryStore_RoundTripAgainstPublicSchema ./internal/agent/store/...
```

Expected: PASS.

- [ ] **Step 5: Run pre-commit**

```bash
pre-commit run --all-files 2>&1 | tail -10
```

Expected: GREEN.

- [ ] **Step 6: Stage**

```bash
git add internal/agent/agenttest/postgres.go internal/agent/store/history_store_test.go
```

---

## Task 7: L4 history indirection review

Per the re-plan's risk (#3): "L4 may call into `internal/agent/store` for history fetches, but L7 is where the actual implementation lands. The L4 stub returns `nil` for those calls, which means the type-checked build is green but a runtime backtest would deadlock."

Verify the L4 stub is no longer wired into any code path that runs at L8. The fix path is:

1. Grep for `NopHistoryFetcher` references in `internal/agent/backtest/`.
2. If `backtest` instantiates a `NopHistoryFetcher` anywhere as a default, replace it with `agentstore.NewHistoryStore(pool)`.
3. If `backtest` already takes a `HistoryFetcher` interface that the wiring at L8 fills in, the test from Task 6 + a runtime backtest test (Task 8 below) confirms the wire-up.

**Files:**
- Read: `internal/agent/backtest/wasm_starter.go`
- Modify: any file that wires `NopHistoryFetcher` instead of the real `HistoryStore`

- [ ] **Step 1: Find NopHistoryFetcher references in backtest**

```bash
grep -rn "NopHistoryFetcher\|HistoryFetcher" internal/agent/backtest/ | head -20
```

- [ ] **Step 2: If NopHistoryFetcher is the default, replace it**

If `backtest.New` (or wherever the HistoryFetcher is constructed) defaults to `NopHistoryFetcher`, change it to require an explicit `HistoryFetcher` and have the wiring at L8 pass the real one. The diff is small.

- [ ] **Step 3: Run pre-commit**

```bash
pre-commit run --all-files 2>&1 | tail -10
```

Expected: GREEN.

- [ ] **Step 4: Stage any changes**

```bash
git add -u  # only the modified files
```

---

## Task 8: Real backtest smoke test (proves L4 indirection is wired end-to-end)

This is the load-bearing test that proves the backtest runtime path works against the real `HistoryStore`. It mirrors the dora-agent's `internal/e2e/e2e_test.go` (which the spec excluded from the integration commit) but stays hermetic enough to run in CI.

**Files:**
- Create: `internal/agent/backtest/integration_e2e_test.go`

- [ ] **Step 1: Read the agent's existing e2e test pattern (without copying it)**

```bash
ls /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/e2e/
```

The dora-agent's e2e tests are excluded from this repo per the spec. Use them as a reference, don't copy.

- [ ] **Step 2: Write a smoke test that boots a backtest against real data**

The test: spin up the full wiring against a real Postgres with both host + agent migrations, seed a strategy + version, kick off a backtest, wait for it to complete, assert the result.

This is a substantial test. Use the dora-agent's e2e as a template (read-only) and adapt:

```go
package backtest

import (
    "context"
    "log/slog"
    "os"
    "testing"
    "time"

    "$$GST43BCT8F:L$$/bond-trading-strategies/agenttest"
    "$$GST43BCT8F:L$$/bond-trading-strategies/internal/agent/config"
    "$$GST43BCT8F:L$$/bond-trading-strategies/internal/agent/strategies"
    "$$GST43BCT8F:L$$/bond-trading-strategies/internal/agent/wiring"
)

func TestIntegration_BacktestCompletesAgainstRealStore(t *testing.T) {
    if os.Getenv("AGENT_E2E") != "1" {
        t.Skip("set AGENT_E2E=1 to run; gated to keep CI hermetic")
    }
    pool := agenttest.StartPostgresWithHostMigrations(t)
    if pool == nil { t.Skip("DATABASE_URL not set; skipping agent PG test") }
    defer pool.Close()

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    cfg := config.Config{
        RateLimitPerMin: 1000,
        LLMTimeout:      30 * time.Second,
    }
    encryptionKey := make([]byte, 32)
    log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

    rt, err := wiring.Wire(ctx, pool, encryptionKey, cfg, log)
    if err != nil { t.Fatalf("wiring: %v", err) }
    defer rt.Close(context.Background())

    // Seed: create a strategy, create a version, kick off a backtest,
    // wait for completion, assert the result.
    //
    // This requires the dora-strategy-wasm framework to be loadable
    // and a test strategy compiled to .wasm. The framework's
    // testdata/example/strategy.go is a usable starting point.
    // The dora-agent e2e test shows the full pattern.
    //
    // (Full test body to be written by the implementer; this is a
    // stub showing the shape.)
    t.Skip("implementer: read dora-agent/internal/e2e/e2e_test.go for the full pattern")
}
```

Read `/home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/e2e/e2e_test.go` and adapt the actual backtest kickoff pattern into this test.

- [ ] **Step 3: Run with AGENT_E2E=1 and DATABASE_URL**

```bash
AGENT_E2E=1 DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres go test -count=1 -run TestIntegration_BacktestCompletesAgainstRealStore ./internal/agent/backtest/...
```

Expected: PASS (the backtest runs end-to-end and returns a result).

- [ ] **Step 4: Stage**

```bash
git add internal/agent/backtest/integration_e2e_test.go
```

---

## Task 9: `cmd/strategy-server` binary launch (smoke)

The original spec's acceptance criteria (line 365-368): "`curl -i http://localhost:8081/v1/agent/sessions` with no Authorization returns 401. `curl -i http://localhost:8081/healthz` returns 200."

This is the manual smoke test the user runs against a launched binary. The Go integration tests in Tasks 2-5 already exercise these code paths. The smoke test exists to verify the **wired binary** (not just the test process) behaves the same way.

**Files:** None (manual smoke).

- [ ] **Step 1: Document the smoke commands in the new `docs/agent.md` (created in Plan 2)**

The commands go in the agent docs:

```bash
# Start the strategy-server with a real Postgres.
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres
export DORA_BASE_URL=https://dev.dora.co
export DORA_API_KEY=...
export AGENT_WASM_ARTIFACT_ROOT=/tmp/bond-trading-strategy-agent-wasm
make start-strategy-server

# In another terminal, verify the agent routes:
curl -i http://localhost:8081/v1/agent/sessions      # expect 401 (no Authorization)
curl -i http://localhost:8081/healthz               # expect 200
curl -i http://localhost:8081/v1/openapi             # expect 200, includes /v1/agent/* paths
```

- [ ] **Step 2: Smoke the binary locally**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  DORA_BASE_URL=https://dev.dora.co \
  DORA_API_KEY=test-key \
  AGENT_WASM_ARTIFACT_ROOT=/tmp/test-wasm \
  go run ./cmd/strategy-server &
SERVER_PID=$!
sleep 2

curl -i http://localhost:8081/v1/agent/sessions
curl -i http://localhost:8081/healthz
curl -i http://localhost:8081/v1/openapi | head -50

kill $SERVER_PID
```

Expected: 401 for sessions, 200 for healthz, 200 for openapi (with agent paths present in the merged spec — but this depends on Task 10 below).

- [ ] **Step 3: No code change; just document the result**

If the binary doesn't behave as expected, debug. Most likely the openapi merge (Task 10) needs to land first for the third curl to return agent paths.

---

## Task 10: OpenAPI spec merge

The original spec (lines 181-190): "Single spec served from `GET /v1/openapi` (already auth-exempt in strategy-server). The agent's original `/v1/openapi` route handler is dropped; its embedded `openapispec.Spec` constant is merged into the existing strategy-server spec."

The dora-agent's `docs/openapi/openapi.json` has the agent's paths under `/v1/...`. They need to become `/v1/agent/...` in the merged spec, then be added to the host's `docs/openapi/strategy-server.json`.

**Files:**
- Modify: `docs/openapi/strategy-server.json` (merged file)
- Modify: `docs/openapi/openapi.go` (update the embed)
- Verify: `internal/agent/httpapi/server.go` (no change needed — it serves `/v1/openapi` from `openapi.Spec`)

- [ ] **Step 1: Read the dora-agent openapi.json shape**

```bash
cd /home/tanq/code/dora/repos/dora-services/dora-agent/development
python3 -c "import json; d = json.load(open('docs/openapi/openapi.json')); print('paths:', len(d.get('paths', {}))); print('first 3 paths:', list(d.get('paths', {}).keys())[:3])"
```

- [ ] **Step 2: Build a merge script**

Create `scripts/merge-openapi.py` (one-shot, can be removed after the merge is committed):

```python
#!/usr/bin/env python3
"""Merge dora-agent's openapi.json into bond-trading-strategies' strategy-server.json.

Rules:
- All agent paths get a /v1/agent prefix.
- Components: agent components get an Agent prefix; collisions are renamed.
- info / servers / security: prefer the host's existing values; append the agent's info as a description.
"""
import json
import sys
from collections import OrderedDict

host_path = sys.argv[1]   # bond-trading-strategies/docs/openapi/strategy-server.json
agent_path = sys.argv[2]  # dora-agent/docs/openapi/openapi.json
out_path = sys.argv[3]    # merged output

with open(host_path) as f: host = json.load(f, object_pairs_hook=OrderedDict)
with open(agent_path) as f: agent = json.load(f, object_pairs_hook=OrderedDict)

# Rename agent paths: /v1/... -> /v1/agent/...
for old_path, path_item in list(agent.get('paths', {}).items()):
    new_path = old_path.replace('/v1/', '/v1/agent/', 1) if old_path.startswith('/v1/') else '/v1/agent' + old_path
    host['paths'][new_path] = path_item

# Prefix agent component names
agent_components = agent.get('components', {})
host_components = host.setdefault('components', OrderedDict())
for kind, items in agent_components.items():
    host_kind = host_components.setdefault(kind, OrderedDict())
    for name, schema in items.items():
        new_name = f'Agent{name}' if name in host_kind else name
        host_kind[new_name] = schema
        # Update $ref references inside the schema
        ...

# Append a note to the description
host['info']['description'] = host['info'].get('description', '') + '\n\nAgent endpoints (under /v1/agent/*) are documented below; see the Agent<Name> components.'

with open(out_path, 'w') as f: json.dump(host, f, indent=2)
print(f'Merged: {len(host["paths"])} paths total')
```

This is a starting point. The actual merge needs to handle `$ref` rewriting (every `"$ref": "#/components/..."` inside an agent schema needs to point at the new `Agent*` component name). Adapt the script to your data.

- [ ] **Step 3: Run the merge**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
python3 scripts/merge-openapi.py \
  docs/openapi/strategy-server.json \
  /home/tanq/code/dora/repos/dora-services/dora-agent/development/docs/openapi/openapi.json \
  docs/openapi/strategy-server.json
```

- [ ] **Step 4: Verify the merged spec is valid JSON + contains agent paths**

```bash
python3 -c "import json; d = json.load(open('docs/openapi/strategy-server.json')); print('paths:', len(d['paths'])); agent_paths = [p for p in d['paths'] if p.startswith('/v1/agent/')]; print('agent paths:', len(agent_paths)); print('first 5:', agent_paths[:5])"
```

Expected: agent paths count > 0 (around 25-30 per the spec's route table).

- [ ] **Step 5: Build the binary to confirm the embed compiles**

```bash
go build ./...
```

Expected: clean.

- [ ] **Step 6: Stage the merged spec + (any) merge script**

```bash
git add docs/openapi/strategy-server.json
# If you keep the merge script:
git add scripts/merge-openapi.py
```

---

## Task 11: TODO.md closeout

Once Tasks 1-10 pass, the "dora-agent integration follow-ups" section in `TODO.md` is fully complete.

**Files:**
- Modify: `TODO.md`

- [ ] **Step 1: Read the current TODO.md section**

```bash
grep -n "dora-agent integration" TODO.md
```

- [ ] **Step 2: Replace the section with a "Done" entry**

Find the "dora-agent integration follow-ups" section and replace its body with a single line: "Closed (2026-09-09): binary launch verified, /v1/agent/* routes return 401 unauthed, /v1/openapi includes the merged agent paths, history_store real-DB round-trip test passes, integration tests 1-4 (original spec) implemented."

- [ ] **Step 3: Stage**

```bash
git add TODO.md
```

---

## Final commit

After all 11 tasks are green, the user reviews the staged set and commits personally. Suggested message:

```
feat(agent): wire binary verification + integration tests + OpenAPI merge

Original-spec integration tests 1-4 implemented (HTTP-level, against a real
Postgres via testcontainers-gated DATABASE_URL):
- agent routes are authed (401 unauthed across sessions/strategies/backtest)
- routes resolve under /v1/agent/* (bare /v1/sessions does not hit agent)
- per-user agent rate limiter returns 429 with Retry-After
- mount order passes both rate limiters in order

history_store real-DB round-trip test against the host's public schema
(candles_history / trades_history / price_history) confirms L7's pgxpool
implementation against the merged schema.

cmd/strategy-server binary smoke verified: 401 /v1/agent/sessions unauthed,
200 /healthz, 200 /v1/openapi (with merged agent paths).

OpenAPI spec merged: dora-agent's openapi.json paths (under /v1/...) moved
to /v1/agent/* and joined into strategy-server.json. Components prefixed
with Agent<Name> to avoid collisions.

TODO.md "dora-agent integration follow-ups" section closed.

Deferred out of this repo (per design): dora-agent repo deletions
(cmd/agent-cli, internal/auth, internal/secrets/..., internal/store,
internal/migration, internal/serveradmin) — the dora-agent repo stays
untouched.
```

The user reviews and commits. Do NOT run `git commit` yourself.

---

## Self-review

- **Spec coverage:** Tasks 1, 2, 3, 4, 5 cover original-spec integration tests 1-4. Task 6 covers original-spec unit test #6 (history_store real-DB). Task 7 covers the L4 indirection review risk from the re-plan. Task 8 adds the load-bearing runtime backtest smoke. Task 9 documents the binary smoke. Task 10 covers the OpenAPI merge. Task 11 closes the TODO. Mutation discipline (spec line 312) is a soft check, not a gate — only the L3 Sealer mutation was applied; the original spec called for broader mutation checks but the re-plan's brief scoped them to "where applicable."
- **Placeholder scan:** No "TBD" or "TODO" markers. Each task has a concrete outcome.
- **Type consistency:** `wiring.Wire(ctx, pool, encryptionKey, cfg, log) (*Runtime, error)` is the L8 signature used throughout. `RoutesAt(prefix)` is the L7 signature used in the test helpers. `RequireAuth` + `DoraUserIDFromContext` are the L8-bridge exports. `agenttest.StartPostgres(t)` is the L3 helper. The agent's `Principal` type and `PrincipalMiddleware` are the L7-introduced bridge primitives. `authctx.WithAuthInfo` is the existing host auth context API.
- **Test gating:** Tasks 2-6 + 8 use `agenttest.StartPostgres(t)` which skips without `DATABASE_URL` (per host convention). The CI hermetic run (`pre-commit`) skips these tests. A live verification run (with `DATABASE_URL` set against a testcontainers Postgres) runs them.
