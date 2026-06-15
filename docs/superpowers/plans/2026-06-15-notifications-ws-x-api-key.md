# `x-api-key` Query Param for `/v1/notifications/ws` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow `GET /v1/notifications/ws?x-api-key=<key>` as an alternative to the `Authorization` header; header wins when both are present. Update handler, router, tests, and OpenAPI spec.

**Architecture:** Add a header-then-query fallback in `notificationsRouter.ServeHTTP` (the WS-specific router in `cmd/strategy-server/main.go`) that injects credentials into the request context using the existing `strategyhttp.WithAPIKey` helper. Relax the handler's `Authorization` header prefix check so it trusts context-resident `AuthInfo` first. Document the new query parameter in the OpenAPI spec.

**Tech Stack:** Go 1.26, `net/http`, `strategyhttp` (existing), `coder/websocket` (existing), OpenAPI 3 JSON (existing).

**Spec:** `docs/superpowers/specs/2026-06-15-notifications-ws-x-api-key-design.md`

**Note on commits:** The user has asked that changes be **staged but not committed** so they can review the diff before committing. Each task therefore ends with `git add` (staging) instead of `git commit`. The final task lists the staged files for review.

---

## File map

**New**
- `authctx/authctx.go` — leaf package containing `AuthInfo`, `WithAPIKey`, `WithBearerToken`, `AuthInfoFromContext` (moved from `strategy/http/auth.go`)
- `authctx/authctx_test.go` — minimal smoke tests for the relocated helpers

**Modified**
- `strategy/http/auth.go` — replace moved definitions with type aliases / thin re-exports of `authctx`
- `strategy/http/dora_client_test.go` — switch one internal usage at `dora_client_test.go:198` to the new package path (verify during implementation whether the type still resolves through the alias or needs an explicit import)
- `cmd/strategy-server/main.go` — extend `notificationsRouter.ServeHTTP` to fall back to the `x-api-key` query parameter
- `notifications/handler.go` — trust context-resident `AuthInfo` (now via the new `authctx` package) before checking the `Authorization` header; add the `authctx` import
- `notifications/handler_test.go` — add `TestHandler_AcceptsXAPIKeyQueryParam`
- `docs/openapi/strategy-server.json` — add `x-api-key` parameter to `/v1/notifications/ws`; update description

---

## Task 1: Add the failing test for query-param auth

**Files:**
- Modify: `notifications/handler_test.go:21-35` (add new test after `TestHandler_RejectsMissingAuth`)

- [ ] **Step 1: Add the test**

Append the following test to `notifications/handler_test.go`, immediately after `TestHandler_RejectsMissingAuth` (closing brace on line 35):

```go
func TestHandler_AcceptsXAPIKeyQueryParam(t *testing.T) {
	bus := notifications.NewBus(&captureLog{}, notifications.NewHub())
	h := notifications.NewHandler(bus, func(_ context.Context) (string, error) {
		return "user-1", nil
	})
	srv := httptest.NewServer(h)
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	wsURL := "ws" + strings.TrimPrefix(u.String(), "http") + "/v1/notifications/ws?x-api-key=test-key"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	//nolint:bodyclose // coder/websocket docs: caller never closes resp.Body
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer conn.Close(websocket.StatusNormalClosure, "")

	evt := notifications.Event{ID: uuid.NewString(), Type: notifications.EventRunStarted, UserID: "user-1", Timestamp: time.Now().UTC()}
	require.NoError(t, bus.Publish(ctx, evt))

	_, data, err := conn.Read(ctx)
	require.NoError(t, err)
	var got notifications.Event
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, evt.ID, got.ID)
}
```

This test only exercises the handler in isolation. It will fail because the handler currently requires an `Authorization` header prefix — `TestHandler_RejectsMissingAuth` already proves that path. We need a way for the handler to learn the key from the query string.

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test -run TestHandler_AcceptsXAPIKeyQueryParam ./notifications/...`
Expected: FAIL — websocket.Dial returns a non-nil error (handshake fails with 401).

- [ ] **Step 3: Stage the test (do not commit)**

```bash
git add notifications/handler_test.go
```

---

## Task 2: Move auth-context types to a new leaf package and relax the handler's header check

**Files:**
- Create: `authctx/authctx.go`
- Create: `authctx/authctx_test.go` (small, optional but recommended)
- Modify: `strategy/http/auth.go`
- Modify: `strategy/http/dora_client_test.go` (one line, only if the type stops resolving through the alias — verify by building)
- Modify: `notifications/handler.go:44-48` and imports

The `notifications` package currently cannot import `strategy/http` because `strategy/http` imports `notifications` (used by the lifecycle event publishing in `strategy/http/handler.go:16` and `strategy/http/notify.go:7`). The handler therefore needs to read `AuthInfo` from a context key defined in a third package that both `strategy/http` and `notifications` can import. We move the four context helpers out of `strategy/http/auth.go` into a new leaf package `authctx`, and leave thin re-exports behind so existing callers (`strategyhttp.WithAPIKey`, `strategyhttp.AuthInfoFromContext`) keep compiling.

- [ ] **Step 1: Create `authctx/authctx.go`**

Create a new top-level directory `authctx/` and file `authctx/authctx.go`. Copy the relevant definitions out of `strategy/http/auth.go` (lines 9-19, 79-82, 89-95, 97-102, 104-109) verbatim, with the `package` declaration changed to `package authctx`. Drop the `authFromContext` private helper — its callers (lines 89-95) are folded into `AuthInfoFromContext`. Keep doc comments intact. The result is the file below (use this exact text — adjust doc-comment wording only if you find a typo while copying):

```go
// Package authctx is a leaf package that owns the request-context
// types and helpers used to carry DORA credentials (API key or bearer
// token) across package boundaries. It exists as a separate package so
// that both strategy/http and notifications can read and write
// credentials on the same request context without forming an import
// cycle.
package authctx

import "context"

type contextKey struct{}

// AuthInfo holds the parsed Authorization header credentials extracted by
// requireAuth. Exactly one of APIKey or BearerToken will be non-empty.
type AuthInfo struct {
	// APIKey is populated when the Authorization header carries the "ApiKey" prefix.
	APIKey string
	// BearerToken is populated when the Authorization header carries the "Bearer" prefix.
	BearerToken string
}

// AuthInfoFromContext returns the AuthInfo stored in ctx and true when
// present, or (nil, false) when no credentials are present.
func AuthInfoFromContext(ctx context.Context) (*AuthInfo, bool) {
	info, ok := ctx.Value(contextKey{}).(AuthInfo)
	if !ok {
		return nil, false
	}
	return &info, true
}

// WithAPIKey returns a context that carries the given API key for DORA
// authentication. It is intended for server startup code that needs to
// make DORA API calls outside of an HTTP request context.
func WithAPIKey(ctx context.Context, apiKey string) context.Context {
	return context.WithValue(ctx, contextKey{}, AuthInfo{APIKey: apiKey})
}

// WithBearerToken returns a context that carries the given bearer token
// for DORA authentication. The token is forwarded as the DORA access
// token on subsequent API calls.
func WithBearerToken(ctx context.Context, token string) context.Context {
	return context.WithValue(ctx, contextKey{}, AuthInfo{BearerToken: token})
}
```

Note the key type changed from `authContextKey` (private) to a lowercase `contextKey` that lives in the same package as the helpers — this is fine because the previous key was unexported; no caller outside `strategy/http` could have been depending on it.

- [ ] **Step 2: Replace the moved definitions in `strategy/http/auth.go`**

In `strategy/http/auth.go`, delete the now-duplicated definitions and replace them with type aliases and re-exports so that `strategyhttp.WithAPIKey`, `strategyhttp.WithBearerToken`, `strategyhttp.AuthInfoFromContext` keep working unchanged. The new content of the file is:

- Delete lines 9-10 (the `authContextKey` and `doraUserIDContextKey` struct declarations — but keep `doraUserIDContextKey` because it is a different key, used only by `requireAuth` and the DORA user ID retrieval helper).
- Delete lines 12-19 (the `AuthInfo` struct).
- Delete lines 77-82 (`authFromContext`).
- Delete lines 84-95 (`AuthInfoFromContext`).
- Delete lines 97-102 (`WithAPIKey`).
- Delete lines 104-109 (`WithBearerToken`).
- Keep lines 21-75 (`requireAuth`) unchanged.
- Keep lines 111-116 (`doraUserIDFromContext`) unchanged.
- Add at the bottom of the file:

```go
// AuthInfo, WithAPIKey, WithBearerToken and AuthInfoFromContext have
// moved to the authctx package. The aliases below preserve the
// strategyhttp import path for existing callers; new code should import
// authctx directly to make the dependency direction explicit.
type AuthInfo = authctx.AuthInfo

var (
	WithAPIKey         = authctx.WithAPIKey
	WithBearerToken    = authctx.WithBearerToken
	AuthInfoFromContext = authctx.AuthInfoFromContext
)
```

Add the import `authctx "github.com/dora-network/bond-trading-strategies/authctx"` to the import block (alphabetical order, between any existing strategy/* imports if present, else after standard libs).

- [ ] **Step 3: Build and test `strategy/http`**

Run: `go build ./...` then `go test ./strategy/http/...`
Expected: builds clean, all existing tests pass. If `dora_client_test.go:198` fails because it directly references the old `authContextKey` and `AuthInfo` types (which were package-private to `strategy/http`), update that single line to use `authctx` — see Step 4.

- [ ] **Step 4: Update `dora_client_test.go:198` if needed**

Read `strategy/http/dora_client_test.go` around line 198. The relevant code constructs a context with the unexported `authContextKey{}` value. After Step 2, `authContextKey` no longer exists in the `strategy/http` package — it lives in `authctx` as `contextKey{}` (also unexported). The test cannot use the unexported `contextKey` from outside the package, so it needs to use `authctx.WithAPIKey(ctx, "test-key")` instead. Change line 198 to:

```go
ctx := authctx.WithAPIKey(context.Background(), "test-key")
```

and add the import `authctx "github.com/dora-network/bond-trading-strategies/authctx"` to the test file's import block. Run `go test ./strategy/http/...` to confirm.

- [ ] **Step 5: Add a tiny smoke test for `authctx`**

Create `authctx/authctx_test.go`:

```go
package authctx_test

import (
	"context"
	"testing"

	"github.com/dora-network/bond-trading-strategies/authctx"
)

func TestWithAndReadAPIKey(t *testing.T) {
	ctx := authctx.WithAPIKey(context.Background(), "k1")
	info, ok := authctx.AuthInfoFromContext(ctx)
	if !ok {
		t.Fatal("expected auth info to be present")
	}
	if info.APIKey != "k1" {
		t.Errorf("got APIKey %q, want %q", info.APIKey, "k1")
	}
}

func TestWithAndReadBearerToken(t *testing.T) {
	ctx := authctx.WithBearerToken(context.Background(), "t1")
	info, ok := authctx.AuthInfoFromContext(ctx)
	if !ok {
		t.Fatal("expected auth info to be present")
	}
	if info.BearerToken != "t1" {
		t.Errorf("got BearerToken %q, want %q", info.BearerToken, "t1")
	}
}

func TestAbsentContextReturnsFalse(t *testing.T) {
	if _, ok := authctx.AuthInfoFromContext(context.Background()); ok {
		t.Error("expected no auth info on plain context")
	}
}
```

- [ ] **Step 6: Update the notifications handler**

In `notifications/handler.go`:

- Add the import:

```go
import (
	"context"
	"log/slog"
	"net/http"
	"strings"

	"github.com/coder/websocket"
	"github.com/dora-network/bond-trading-strategies/authctx"
)
```

- Replace lines 44-48:

OLD:
```go
	authHeader := r.Header.Get("Authorization")
	if !strings.HasPrefix(authHeader, "ApiKey ") && !strings.HasPrefix(authHeader, "Bearer ") {
		http.Error(w, "missing or unsupported Authorization header", http.StatusUnauthorized)
		return
	}
```

NEW:
```go
	if _, ok := authctx.AuthInfoFromContext(r.Context()); !ok {
		authHeader := r.Header.Get("Authorization")
		if !strings.HasPrefix(authHeader, "ApiKey ") && !strings.HasPrefix(authHeader, "Bearer ") {
			http.Error(w, "missing or unsupported Authorization header", http.StatusUnauthorized)
			return
		}
	}
```

Preserve the surrounding tabs (this file uses tabs for indentation — see the existing `authHeader :=` line which begins with a single tab).

- [ ] **Step 7: Run notifications tests**

Run: `go test ./notifications/...`
Expected: existing tests pass; `TestHandler_AcceptsXAPIKeyQueryParam` (from Task 1) still fails (router not updated yet — that's Task 3).

- [ ] **Step 8: Run the full module build and test**

Run: `go build ./... && go test ./...`
Expected: builds clean, all tests except the still-failing Task 1 test pass. If the Task 1 test now unexpectedly passes, the router is in scope here too — but it should not pass yet.

- [ ] **Step 9: Stage the changes (do not commit)**

```bash
git add authctx/authctx.go authctx/authctx_test.go strategy/http/auth.go strategy/http/dora_client_test.go notifications/handler.go
```

(The Task 1 test file `notifications/handler_test.go` is already staged from the previous task; do not re-add it.)

---

## Task 3: Update the router to populate context from the query parameter

**Files:**
- Modify: `cmd/strategy-server/main.go:298-311` (`notificationsRouter.ServeHTTP`)

- [ ] **Step 1: Update `ServeHTTP`**

Replace `cmd/strategy-server/main.go:298-311`:

```go
func (r notificationsRouter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
    if req.URL.Path != "/v1/notifications/ws" {
        r.fallback.ServeHTTP(w, req)
        return
    }
    // Parse the Authorization header and put it into the request context
    // so the WebSocket handler's ResolveUserID callback can read it via
    // strategyhttp.AuthInfoFromContext.
    authHeader := req.Header.Get("Authorization")
    if ctx, ok := authContextFromHeader(req.Context(), authHeader); ok {
        req = req.WithContext(ctx)
    }
    r.sub.ServeHTTP(w, req)
}
```

with:

```go
func (r notificationsRouter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
    if req.URL.Path != "/v1/notifications/ws" {
        r.fallback.ServeHTTP(w, req)
        return
    }
    // Populate the request context with credentials so the WebSocket
    // handler's ResolveUserID callback can read them via
    // strategyhttp.AuthInfoFromContext. The Authorization header takes
    // precedence; the x-api-key query parameter is a fallback for
    // clients that cannot set request headers on the WS handshake.
    ctx := req.Context()
    if newCtx, ok := authContextFromHeader(ctx, req.Header.Get("Authorization")); ok {
        ctx = newCtx
    } else if key := req.URL.Query().Get("x-api-key"); key != "" {
        ctx = strategyhttp.WithAPIKey(ctx, key)
    }
    r.sub.ServeHTTP(w, req.WithContext(ctx))
}
```

No new imports are needed — `strategyhttp` is already imported (see `cmd/strategy-server/main.go:22`) and `net/url` is not required because we use `r.URL.Query()` (a method on `*url.URL`, already in the request value type).

- [ ] **Step 2: Build the module to confirm types compile**

Run: `go build ./...`
Expected: clean build, no errors.

- [ ] **Step 3: Stage the change (do not commit)**

```bash
git add cmd/strategy-server/main.go
```

---

## Task 4: Add a router-level unit test for query-param fallback

**Files:**
- Modify: `cmd/strategy-server/main.go` (or a new test file colocated with the router) — add a white-box test

The router is package-private and the `fallback`/`sub` fields are unexported. Add a test file `cmd/strategy-server/notifications_router_test.go` (package `main`) to keep it hermetic from the rest of `main`.

- [ ] **Step 1: Create the test file**

Create `cmd/strategy-server/notifications_router_test.go`:

```go
package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	strategyhttp "github.com/dora-network/bond-trading-strategies/strategy/http"
)

type captured struct {
	hadAuthInfo bool
	gotAPIKey   string
}

func TestNotificationsRouter_HeaderTakesPrecedence(t *testing.T) {
	cap := &captured{}
	sub := http.NewServeMux()
	sub.HandleFunc("/v1/notifications/ws", func(w http.ResponseWriter, r *http.Request) {
		if info, ok := strategyhttp.AuthInfoFromContext(r.Context()); ok {
			cap.hadAuthInfo = true
			cap.gotAPIKey = info.APIKey
		}
		w.WriteHeader(http.StatusOK)
	})
	r := notificationsRouter{fallback: http.NewServeMux(), sub: sub}

	req := httptest.NewRequest(http.MethodGet, "/v1/notifications/ws?x-api-key=query-key", nil)
	req.Header.Set("Authorization", "ApiKey header-key")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.True(t, cap.hadAuthInfo)
	assert.Equal(t, "header-key", cap.gotAPIKey)
}

func TestNotificationsRouter_FallsBackToQueryParam(t *testing.T) {
	cap := &captured{}
	sub := http.NewServeMux()
	sub.HandleFunc("/v1/notifications/ws", func(w http.ResponseWriter, r *http.Request) {
		if info, ok := strategyhttp.AuthInfoFromContext(r.Context()); ok {
			cap.hadAuthInfo = true
			cap.gotAPIKey = info.APIKey
		}
		w.WriteHeader(http.StatusOK)
	})
	r := notificationsRouter{fallback: http.NewServeMux(), sub: sub}

	req := httptest.NewRequest(http.MethodGet, "/v1/notifications/ws?x-api-key=query-key", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.True(t, cap.hadAuthInfo)
	assert.Equal(t, "query-key", cap.gotAPIKey)
}

func TestNotificationsRouter_NoCredentialsPassesThrough(t *testing.T) {
	called := false
	sub := http.NewServeMux()
	sub.HandleFunc("/v1/notifications/ws", func(w http.ResponseWriter, r *http.Request) {
		if _, ok := strategyhttp.AuthInfoFromContext(r.Context()); ok {
			t.Errorf("expected no auth info in context")
		}
		called = true
		w.WriteHeader(http.StatusOK)
	})
	r := notificationsRouter{fallback: http.NewServeMux(), sub: sub}

	req := httptest.NewRequest(http.MethodGet, "/v1/notifications/ws", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.True(t, called)
	_ = context.TODO() // keep import if linter complains
}
```

Drop the trailing `_ = context.TODO()` line if `goimports` flags `context` as unused — in the third test the import may not be needed. If `goimports` removes it, no action required.

- [ ] **Step 2: Run the tests**

Run: `go test -run TestNotificationsRouter ./cmd/strategy-server/...`
Expected: all three tests PASS.

- [ ] **Step 3: Stage the test (do not commit)**

```bash
git add cmd/strategy-server/notifications_router_test.go
```

---

## Task 5: Update the OpenAPI spec

**Files:**
- Modify: `docs/openapi/strategy-server.json:489-521`

- [ ] **Step 1: Update the description and add the parameter**

In `docs/openapi/strategy-server.json`, replace the `/v1/notifications/ws` block (lines 489-521) — locate the operation, then change two things:

a) Update `description` (line 494) to mention the query-param alternative:

old:
```
"description": "Upgrades the HTTP connection to a WebSocket. The server authenticates using the standard Authorization header, resolves the DORA user, and streams JSON-encoded Event objects as text frames. Optional query params: Last-Event-ID (UUIDv7) replays events from the log; types (comma-separated) filters by EventType. Heartbeat is WS-level ping/pong every 30s."
```

new:
```
"description": "Upgrades the HTTP connection to a WebSocket. The server authenticates using the standard Authorization header (ApiKey <key> or Bearer <token>), resolves the DORA user, and streams JSON-encoded Event objects as text frames. Clients that cannot set request headers may alternatively pass `x-api-key` as a query parameter; the header takes precedence when both are present. Optional query params: Last-Event-ID (UUIDv7) replays events from the log; types (comma-separated) filters by EventType. Heartbeat is WS-level ping/pong every 30s."
```

b) Append a new parameter object to the `parameters` array, after the `types` entry (after the closing `}` of the existing second parameter, before the `]`). The new entry:

```json
,
{
  "name": "x-api-key",
  "in": "query",
  "required": false,
  "schema": { "type": "string" },
  "description": "Alternative to the Authorization header for clients that cannot set request headers on the WebSocket handshake. Ignored when the Authorization header is present."
}
```

The leading comma is correct — it separates this new object from the previous one in the array. Do **not** add a comma after the closing `}` of this new object (the next character will be `]`).

- [ ] **Step 2: Validate the JSON is well-formed**

Run: `python3 -c "import json,sys; json.load(open('docs/openapi/strategy-server.json')); print('ok')"`
Expected: `ok`

If `python3` is unavailable: `jq -e . docs/openapi/strategy-server.json > /dev/null && echo ok`

- [ ] **Step 3: Stage the spec (do not commit)**

```bash
git add docs/openapi/strategy-server.json
```

---

## Task 6: Full verification pass

- [ ] **Step 1: Build**

Run: `go build ./...`
Expected: clean.

- [ ] **Step 2: Test the whole module**

Run: `go test ./...`
Expected: all tests pass, including the new `TestHandler_AcceptsXAPIKeyQueryParam` and the three `TestNotificationsRouter_*` tests.

- [ ] **Step 3: Run the linter**

Run: `golangci-lint run ./notifications/... ./cmd/strategy-server/...`
Expected: no new findings.

- [ ] **Step 4: Pre-commit hooks**

Run: `pre-commit run --all-files`
Expected: all hooks pass.

If a pre-commit hook fails, fix the underlying issue — do not skip hooks (per project policy).

---

## Final review checklist (staged, not committed)

After all tasks, run:

```bash
git status
git diff --staged
```

The staged changes should touch exactly these files:

- `cmd/strategy-server/main.go` — router fallback added
- `cmd/strategy-server/notifications_router_test.go` — new router tests
- `notifications/handler.go` — context-first auth check
- `notifications/handler_test.go` — new query-param test
- `docs/openapi/strategy-server.json` — `x-api-key` parameter + description

No commits will be made. The user will review the diff and commit on their own.
