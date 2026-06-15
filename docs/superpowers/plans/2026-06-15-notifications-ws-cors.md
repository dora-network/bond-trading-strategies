# CORS support for `/v1/notifications/ws` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the existing CORS middleware into a reusable `cors` package, fix the `*` + `Allow-Credentials` spec violation, add WebSocket-specific allowed headers, and wire the middleware in front of the WebSocket handler so browsers can open a WebSocket from an allowed origin.

**Architecture:** Extract `corsMiddleware` from `cmd/strategy-server/main.go` into a new top-level `cors/` package exposing `cors.New(origins string) func(http.Handler) http.Handler`. Apply the returned middleware to both the REST chain and the WebSocket handler in `cmd/strategy-server/main.go`. When `*` is configured, the middleware echoes the request's `Origin` instead of the literal `*` and keeps `Allow-Credentials: true` (the spec-compliant form that every major CORS library implements). When `origins` is empty, `cors.New` returns a pass-through so the caller does not need a conditional.

**Tech Stack:** Go 1.26, `net/http`, `httptest` for tests, `testify/assert` + `testify/require`.

**Spec:** `docs/superpowers/specs/2026-06-15-notifications-ws-cors-design.md`

**Note on commits:** The user has asked that changes be **staged but not committed** for review before committing. Each task ends with `git add` (staging) instead of `git commit`. The final task lists the staged files for review.

---

## File map

**New**
- `cors/cors.go` — package with `cors.New(origins string) func(http.Handler) http.Handler`
- `cors/cors_test.go` — table-driven unit tests for the middleware

**Modified**
- `cmd/strategy-server/main.go` — remove the inline `corsMiddleware` function, import `cors`, wire it in front of the WS sub-mux in addition to the REST chain
- `cmd/strategy-server/notifications_router_test.go` — add a CORS integration test that exercises the WS path with CORS enabled

---

## Task 1: Create the `cors` package with failing tests

**Files:**
- Create: `cors/cors.go`
- Create: `cors/cors_test.go`

This task writes the tests first, then the implementation. The tests will fail until the implementation is in place.

- [ ] **Step 1: Create `cors/cors_test.go` with the full test file**

The file exercises every row of the behavioural matrix in the spec plus the WS-specific header checks. Use the structure below. The tests use `httptest.NewRecorder` + a sentinel next-handler that records whether it was called; the CORS middleware should call next for non-OPTIONS and short-circuit OPTIONS with 204.

```go
package cors_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dora-network/bond-trading-strategies/cors"
)

func TestNew_EmptyOrigins_PassesThrough(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })

	h := cors.New("")(next)
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://anywhere.com")
	h.ServeHTTP(rr, req)

	if !called {
		t.Fatal("expected next handler to be called when no origins configured")
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("expected no Allow-Origin header, got %q", got)
	}
}

func TestNew_Wildcard_WithOrigin_EchoesOrigin(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })

	h := cors.New("*")(next)
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://app.example.com")
	h.ServeHTTP(rr, req)

	if !called {
		t.Fatal("expected next handler to be called for GET")
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "https://app.example.com" {
		t.Errorf("Allow-Origin = %q, want echo of Origin", got)
	}
	if got := rr.Header().Get("Vary"); !strings.Contains(got, "Origin") {
		t.Errorf("Vary = %q, want it to contain Origin", got)
	}
	if got := rr.Header().Get("Access-Control-Allow-Credentials"); got != "true" {
		t.Errorf("Allow-Credentials = %q, want true", got)
	}
}

func TestNew_Wildcard_WithoutOrigin_LiteralStar(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })

	h := cors.New("*")(next)
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	h.ServeHTTP(rr, req)

	if !called {
		t.Fatal("expected next handler to be called")
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("Allow-Origin = %q, want *", got)
	}
}

func TestNew_ExplicitList_MatchingOrigin_Echoes(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })

	h := cors.New("https://a.com,https://b.com")(next)
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://a.com")
	h.ServeHTTP(rr, req)

	if !called {
		t.Fatal("expected next handler to be called")
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "https://a.com" {
		t.Errorf("Allow-Origin = %q, want echo of Origin", got)
	}
	if got := rr.Header().Get("Vary"); !strings.Contains(got, "Origin") {
		t.Errorf("Vary = %q, want it to contain Origin", got)
	}
}

func TestNew_ExplicitList_NonMatchingOrigin_NoAllowOrigin(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })

	h := cors.New("https://a.com,https://b.com")(next)
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://attacker.com")
	h.ServeHTTP(rr, req)

	if !called {
		t.Fatal("expected next handler to be called (CORS headers do not block server-side)")
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("Allow-Origin = %q, want empty (non-matching origin)", got)
	}
}

func TestNew_ExplicitList_NoOriginHeader_NoAllowOrigin(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })

	h := cors.New("https://a.com")(next)
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	h.ServeHTTP(rr, req)

	if !called {
		t.Fatal("expected next handler to be called")
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("Allow-Origin = %q, want empty (no Origin header)", got)
	}
}

func TestNew_OptionsPreflight_ShortCircuitsWith204(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })

	h := cors.New("*")(next)
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodOptions, "/", nil)
	req.Header.Set("Origin", "https://app.example.com")
	h.ServeHTTP(rr, req)

	if called {
		t.Error("expected next handler NOT to be called for OPTIONS preflight")
	}
	if rr.Code != http.StatusNoContent {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusNoContent)
	}
	if got := rr.Header().Get("Access-Control-Allow-Methods"); !strings.Contains(got, "GET") {
		t.Errorf("Allow-Methods = %q, want it to contain GET", got)
	}
}

func TestNew_AllowedHeaders_IncludeWebSocketHeaders(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })

	h := cors.New("*")(next)
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	h.ServeHTTP(rr, req)

	if !called {
		t.Fatal("expected next handler to be called")
	}
	got := rr.Header().Get("Access-Control-Allow-Headers")
	for _, want := range []string{"Authorization", "Content-Type", "Sec-WebSocket-Protocol", "Sec-WebSocket-Extensions"} {
		if !strings.Contains(got, want) {
			t.Errorf("Allow-Headers = %q, want it to contain %q", got, want)
		}
	}
}

func TestNew_AllowedMethods_IncludePatch(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })

	h := cors.New("*")(next)
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	h.ServeHTTP(rr, req)

	got := rr.Header().Get("Access-Control-Allow-Methods")
	if !strings.Contains(got, "PATCH") {
		t.Errorf("Allow-Methods = %q, want it to contain PATCH", got)
	}
}
```

Note: the tests use `t.Context()` (Go 1.24+ stdlib) to satisfy any `noctx` linter; if `t.Context()` is not available in the toolchain the tests use `context.Background()` and the existing `httptest.NewRequest` form (project uses Go 1.26 per AGENTS.md, so `t.Context()` is fine).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./cors/...`
Expected: compile error — `cors` package does not exist yet.

- [ ] **Step 3: Create `cors/cors.go` with the implementation**

```go
// Package cors provides a small, dependency-free HTTP middleware that
// adds the CORS response headers needed for cross-origin browsers to
// call the strategy-server. The middleware is intentionally minimal:
// it does not parse the Access-Control-Request-Headers echo, does not
// support credentialed Allow-Origin: *, and does not implement a
// preflight cache beyond Max-Age.
//
// Origins: a comma-separated allow-list. A single "*" allows any
// origin and echoes the request's Origin header (so that
// Access-Control-Allow-Credentials can stay set to "true" — the CORS
// spec forbids the literal "*" + "true" combination, but every
// major CORS library implements the echo-Origin form and every
// modern browser accepts it). An empty string disables CORS entirely
// and the returned function is a pass-through.
package cors

import (
	"net/http"
	"strings"
)

// New returns an HTTP middleware that adds CORS headers to responses.
// origins is a comma-separated list; "*" allows any origin. An empty
// string disables CORS — the returned function calls next with no
// header mutation.
func New(origins string) func(http.Handler) http.Handler {
	allowed := make(map[string]bool)
	allowAll := false
	for _, o := range strings.Split(origins, ",") {
		o = strings.TrimSpace(o)
		if o == "" {
			continue
		}
		if o == "*" {
			allowAll = true
			continue
		}
		allowed[o] = true
	}

	if origins == "" {
		return func(next http.Handler) http.Handler {
			return next
		}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			headers := w.Header()

			switch {
			case allowAll && origin != "":
				headers.Set("Access-Control-Allow-Origin", origin)
				headers.Add("Vary", "Origin")
			case allowAll:
				headers.Set("Access-Control-Allow-Origin", "*")
			case allowed[origin]:
				headers.Set("Access-Control-Allow-Origin", origin)
				headers.Add("Vary", "Origin")
			}

			headers.Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
			headers.Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Sec-WebSocket-Protocol, Sec-WebSocket-Extensions")
			headers.Set("Access-Control-Allow-Credentials", "true")
			headers.Set("Access-Control-Max-Age", "86400")

			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./cors/...`
Expected: all nine tests pass.

- [ ] **Step 5: Run the linter on the new package**

Run: `golangci-lint run ./cors/...`
Expected: 0 issues.

If `gochecknoglobals` flags the package-level `allowed`/`allowAll` closures, note that they are captured by the closure returned from `New`, not at package level, so they should be fine. The `if origins == ""` short-circuit returns a pass-through, so the outer map/flag is never built in that case. Verify by re-running the linter.

- [ ] **Step 6: Stage the new files (do NOT commit)**

```bash
git add cors/cors.go cors/cors_test.go
```

---

## Task 2: Wire `cors.New` into `cmd/strategy-server/main.go` and remove the inline `corsMiddleware`

**Files:**
- Modify: `cmd/strategy-server/main.go:42-44` (flag declarations, no change)
- Modify: `cmd/strategy-server/main.go:209-211` (REST wiring)
- Modify: `cmd/strategy-server/main.go:213-225` (WS sub-mux wiring)
- Modify: `cmd/strategy-server/main.go:340-379` (delete the inline `corsMiddleware` function)

- [ ] **Step 1: Add the import**

The current import block (lines 3-27) includes the `cors/...` import set alphabetically. Add `"github.com/dora-network/bond-trading-strategies/cors"` after the existing `authctx` import. The block becomes:

```go
import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dora-network/bond-trading-strategies/authctx"
	"github.com/dora-network/bond-trading-strategies/cors"
	"github.com/dora-network/bond-trading-strategies/notifications"
	"github.com/dora-network/bond-trading-strategies/prices"
	"github.com/dora-network/bond-trading-strategies/ratelimit"
	strategycore "github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/copytrading"
	strategyhttp "github.com/dora-network/bond-trading-strategies/strategy/http"
	"github.com/dora-network/bond-trading-strategies/streams"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	flag "github.com/spf13/pflag"
)
```

- [ ] **Step 2: Replace the REST wiring (lines 209-211)**

OLD:
```go
	if *corsAllowedOrigins != "" {
		wrappedHandler = corsMiddleware(*corsAllowedOrigins, wrappedHandler)
	}
```

NEW:
```go
	if *corsAllowedOrigins != "" {
		wrappedHandler = cors.New(*corsAllowedOrigins)(wrappedHandler)
	}
```

- [ ] **Step 3: Replace the WS sub-mux wiring (lines 213-225)**

OLD:
```go
	if notifier != nil {
		wsSubMux := http.NewServeMux()
		wsSubMux.Handle("/v1/notifications/ws", notifications.NewHandler(
			notifier,
			func(ctx context.Context) (string, error) {
				if _, ok := authctx.AuthInfoFromContext(ctx); !ok {
					return "", errors.New("missing auth info in context")
				}
				client := strategyhttp.NewDORAClient()
				return client.GetUserID(ctx)
			},
			notifications.WithHandlerLogger(log),
		))
		wrappedHandler = notificationsRouter{fallback: wrappedHandler, sub: wsSubMux}
	}
```

NEW:
```go
	if notifier != nil {
		wsSubMux := http.NewServeMux()
		wsHandler := notifications.NewHandler(
			notifier,
			func(ctx context.Context) (string, error) {
				if _, ok := authctx.AuthInfoFromContext(ctx); !ok {
					return "", errors.New("missing auth info in context")
				}
				client := strategyhttp.NewDORAClient()
				return client.GetUserID(ctx)
			},
			notifications.WithHandlerLogger(log),
		)
		if *corsAllowedOrigins != "" {
			wsHandler = cors.New(*corsAllowedOrigins)(wsHandler)
		}
		wsSubMux.Handle("/v1/notifications/ws", wsHandler)
		wrappedHandler = notificationsRouter{fallback: wrappedHandler, sub: wsSubMux}
	}
```

- [ ] **Step 4: Delete the inline `corsMiddleware` function (lines 340-379)**

Delete the entire `corsMiddleware` function. The `strings` import is still used elsewhere in `main.go` (e.g. `authContextFromHeader`, the flag parsing) so leave the import. If `strings` becomes unused after the deletion, remove it — `goimports` will report it. Verify with `goimports -w cmd/strategy-server/main.go` or `gofmt -w` then check the import block.

- [ ] **Step 5: Build and test the module**

Run: `go build ./... && go test ./...`
Expected: builds clean, all 15 packages pass. The pre-existing tests in `cmd/strategy-server/...` and elsewhere should not regress.

- [ ] **Step 6: Run the linter**

Run: `golangci-lint run ./cmd/strategy-server/... ./cors/...`
Expected: 0 issues.

- [ ] **Step 7: Stage the change (do NOT commit)**

```bash
git add cmd/strategy-server/main.go
```

---

## Task 3: Add the CORS integration test for the WebSocket router

**Files:**
- Modify: `cmd/strategy-server/notifications_router_test.go` (add new test at end of file)

The test exercises the WS router with CORS applied to the WS handler. It confirms both the preflight response and that the upgrade response carries CORS headers.

- [ ] **Step 1: Add the new test to `cmd/strategy-server/notifications_router_test.go`**

Append the following test at the end of the file:

```go
func TestNotificationsRouter_CORSPreflightSucceeds(t *testing.T) {
	// Verify that an OPTIONS preflight against /v1/notifications/ws
	// gets the CORS headers needed for a browser to open a WebSocket
	// from an allowed origin. The router is built with CORS applied
	// to the WS handler, mirroring the production wiring in main.go.
	sub := http.NewServeMux()
	wsHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The preflight should never reach the real WS handler.
		t.Errorf("real handler called for OPTIONS: %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusInternalServerError)
	})
	corsWrap := cors.New("https://app.example.com")
	sub.Handle("/v1/notifications/ws", corsWrap(wsHandler))
	r := notificationsRouter{fallback: http.NewServeMux(), sub: sub}

	req := httptest.NewRequestWithContext(context.Background(), http.MethodOptions, "/v1/notifications/ws", nil)
	req.Header.Set("Origin", "https://app.example.com")
	req.Header.Set("Access-Control-Request-Method", "GET")
	req.Header.Set("Access-Control-Request-Headers", "Authorization, Sec-WebSocket-Protocol")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusNoContent, rr.Code)
	assert.Equal(t, "https://app.example.com", rr.Header().Get("Access-Control-Allow-Origin"))
	assert.Contains(t, rr.Header().Get("Vary"), "Origin")
	assert.Contains(t, rr.Header().Get("Access-Control-Allow-Methods"), "GET")
	assert.Contains(t, rr.Header().Get("Access-Control-Allow-Headers"), "Sec-WebSocket-Protocol")
	assert.Equal(t, "true", rr.Header().Get("Access-Control-Allow-Credentials"))
}

func TestNotificationsRouter_CORSHeadersOnUpgrade(t *testing.T) {
	// Verify that a real GET against /v1/notifications/ws carries
	// the CORS headers on the response (not just the preflight).
	// The handler itself will 401 because we don't wire real auth,
	// but the CORS headers must be present.
	sub := http.NewServeMux()
	wsHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	})
	corsWrap := cors.New("https://app.example.com")
	sub.Handle("/v1/notifications/ws", corsWrap(wsHandler))
	r := notificationsRouter{fallback: http.NewServeMux(), sub: sub}

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/notifications/ws?x-api-key=test-key", nil)
	req.Header.Set("Origin", "https://app.example.com")
	req.Header.Set("Authorization", "ApiKey test-key")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)
	assert.Equal(t, "https://app.example.com", rr.Header().Get("Access-Control-Allow-Origin"))
	assert.Contains(t, rr.Header().Get("Vary"), "Origin")
	assert.Equal(t, "true", rr.Header().Get("Access-Control-Allow-Credentials"))
}

func TestNotificationsRouter_CORSRejectsDisallowedOrigin(t *testing.T) {
	// A request from an origin not in the allow-list must NOT have
	// Access-Control-Allow-Origin set (the browser will block the
	// response). The server-side handler still runs.
	sub := http.NewServeMux()
	wsHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	})
	corsWrap := cors.New("https://app.example.com")
	sub.Handle("/v1/notifications/ws", corsWrap(wsHandler))
	r := notificationsRouter{fallback: http.NewServeMux(), sub: sub}

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/notifications/ws", nil)
	req.Header.Set("Origin", "https://attacker.example.com")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)
	assert.Empty(t, rr.Header().Get("Access-Control-Allow-Origin"))
}
```

- [ ] **Step 2: Add the `cors` import to the test file**

The current import block in `notifications_router_test.go` includes `authctx`. Add `cors`:

```go
import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dora-network/bond-trading-strategies/authctx"
	"github.com/dora-network/bond-trading-strategies/cors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)
```

- [ ] **Step 3: Run the new tests**

Run: `go test -run TestNotificationsRouter_CORS ./cmd/strategy-server/...`
Expected: all three new tests pass.

- [ ] **Step 4: Run the full module test to confirm no regressions**

Run: `go test ./...`
Expected: all packages pass.

- [ ] **Step 5: Run the linter**

Run: `golangci-lint run ./cmd/strategy-server/...`
Expected: 0 issues.

- [ ] **Step 6: Stage the test file (do NOT commit)**

```bash
git add cmd/strategy-server/notifications_router_test.go
```

---

## Task 4: Full verification

- [ ] **Step 1: Build the full module**

Run: `go build ./...`
Expected: clean build, 0 errors.

- [ ] **Step 2: Run the full test suite uncached**

Run: `go clean -testcache && go test ./...`
Expected: all packages pass.

- [ ] **Step 3: Run the linter**

Run: `golangci-lint run --timeout 5m ./cors/... ./cmd/strategy-server/... ./notifications/... ./authctx/... ./strategy/http/...`
Expected: 0 issues.

- [ ] **Step 4: Run pre-commit hooks**

Run: `pre-commit run --files cors/cors.go cors/cors_test.go cmd/strategy-server/main.go cmd/strategy-server/notifications_router_test.go`
Expected: all hooks pass. The pre-existing sibling-worktree noise in `golangci-lint-repo-mod` is unrelated to this change and can be ignored.

---

## Final review checklist (staged, not committed)

After all tasks, run:

```bash
git status
git diff --staged
git diff --staged --stat
```

The staged changes should touch exactly these files:

- `cors/cors.go` (new) — the middleware package
- `cors/cors_test.go` (new) — unit tests
- `cmd/strategy-server/main.go` (modified) — import `cors`, wire it in front of the WS sub-mux, delete the inline `corsMiddleware`
- `cmd/strategy-server/notifications_router_test.go` (modified) — three new CORS integration tests

No commits will be made. The user will review the diff and commit on their own.
