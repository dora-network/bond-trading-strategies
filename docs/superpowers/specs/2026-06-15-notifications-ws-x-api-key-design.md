# Allow `x-api-key` query param on `/v1/notifications/ws`

## Problem

`GET /v1/notifications/ws` currently requires an `Authorization` header
(`ApiKey <key>` or `Bearer <token>`). Browser and some scripted clients
cannot set request headers on a WebSocket handshake and have no way to
authenticate.

We need to accept the API key as a `x-api-key` query parameter on this
endpoint as an alternative to the header. Header takes precedence; the
query parameter is a fallback. Both absent → 401 (unchanged).

## Scope

In scope:

- `GET /v1/notifications/ws` — accept `?x-api-key=<key>` as an alternative
  to the `Authorization` header.
- OpenAPI spec for `strategy-server` — document the new parameter.
- Tests covering the query-param happy path and the precedence rule.

Out of scope:

- All other endpoints under `/v1/...` keep the existing
  `requireAuth` middleware behaviour (header only).
- The outbound WebSocket client in `notifications/client.go` and the
  `wsclient` e2e helper — they already send `Authorization` and don't
  need to change.
- Bearer tokens via query parameter — header only for now.
- Any change to the **behaviour** of `strategy/http/auth.go`'s
  `requireAuth` middleware. (Structural relocation of the
  `AuthInfo` context-key types to a new leaf package is permitted and
  required — see "Structural change" below.)

## Structural change: shared auth context types

Adding a `strategyhttp.AuthInfoFromContext` call to `notifications.Handler`
introduces an import cycle, because `strategy/http` already imports
`notifications` (used by the lifecycle event publishing in
`strategy/http/handler.go` and `strategy/http/notify.go`).

The auth-context types (`AuthInfo`, `WithAPIKey`, `WithBearerToken`,
`AuthInfoFromContext`) must therefore live in a **new leaf package** that
both `strategy/http` and `notifications` can import without forming a
cycle. The chosen path is `internal/authctx` (or a top-level `authctx`
package — pick whichever fits the project layout; the spec uses
`authctx` for clarity).

- Move `AuthInfo`, `WithAPIKey`, `WithBearerToken`, `AuthInfoFromContext`
  from `strategy/http/auth.go` into `authctx/authctx.go` verbatim.
- In `strategy/http/auth.go`, replace the moved definitions with type
  aliases / thin re-exports so existing callers
  (`strategyhttp.WithAPIKey`, `strategyhttp.AuthInfoFromContext`) keep
  compiling without source changes.
- Update the one internal call site in
  `strategy/http/dora_client_test.go:198` to use `authctx` types
  directly (or leave it as-is if it only uses the now-aliased types
  transitively — verify during implementation).
- The `notifications` handler imports `authctx` and uses
  `authctx.AuthInfoFromContext`.
- The router in `cmd/strategy-server/main.go` continues to call
  `strategyhttp.WithAPIKey` / `strategyhttp.WithBearerToken`; no
  changes there.

This is structural only. `requireAuth`'s runtime behaviour, the
`Authorization` header parsing, the `DORAUserID` resolution, and every
caller of the re-exported functions are unchanged.

## Design

### Resolution order (per request)

1. If `Authorization` header is set and recognised by the existing
   `authContextFromHeader` helper (`ApiKey <key>` or `Bearer <token>`),
   use it. Header wins when both are present.
2. Else, if `x-api-key` query parameter is non-empty, treat it as an
   `ApiKey` credential and put it in the request context via
   `strategyhttp.WithAPIKey`.
3. Else, leave the context unchanged. The handler will 401.

### Code changes

#### 1. `cmd/strategy-server/main.go` — `notificationsRouter.ServeHTTP`

Today (line 298):

```go
func (r notificationsRouter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
    if req.URL.Path != "/v1/notifications/ws" {
        r.fallback.ServeHTTP(w, req)
        return
    }
    authHeader := req.Header.Get("Authorization")
    if ctx, ok := authContextFromHeader(req.Context(), authHeader); ok {
        req = req.WithContext(ctx)
    }
    r.sub.ServeHTTP(w, req)
}
```

Change to: try the header first, then fall back to `x-api-key`:

```go
func (r notificationsRouter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
    if req.URL.Path != "/v1/notifications/ws" {
        r.fallback.ServeHTTP(w, req)
        return
    }
    ctx := req.Context()
    if newCtx, ok := authContextFromHeader(ctx, req.Header.Get("Authorization")); ok {
        ctx = newCtx
    } else if key := req.URL.Query().Get("x-api-key"); key != "" {
        ctx = strategyhttp.WithAPIKey(ctx, key)
    }
    r.sub.ServeHTTP(w, req.WithContext(ctx))
}
```

#### 2. `notifications/handler.go` — `Handler.ServeHTTP`

Today (line 44):

```go
authHeader := r.Header.Get("Authorization")
if !strings.HasPrefix(authHeader, "ApiKey ") && !strings.HasPrefix(authHeader, "Bearer ") {
    http.Error(w, "missing or unsupported Authorization header", http.StatusUnauthorized)
    return
}
```

Change to: trust context if it already carries credentials (populated by
the router from either the header or the query param), else fall back
to the header prefix check.

```go
if _, ok := strategyhttp.AuthInfoFromContext(r.Context()); !ok {
    authHeader := r.Header.Get("Authorization")
    if !strings.HasPrefix(authHeader, "ApiKey ") && !strings.HasPrefix(authHeader, "Bearer ") {
        http.Error(w, "missing or unsupported Authorization header", http.StatusUnauthorized)
        return
    }
}
```

This adds one new import: `github.com/dora-network/bond-trading-strategies/strategy/http`
(aliased `strategyhttp` in the calling file's import set; check the
existing import block before adding).

#### 3. `notifications/handler_test.go` — new test

Add `TestHandler_AcceptsXAPIKeyQueryParam`:

- Build a `notifications.Handler` with the same `resolveUser` closure
  used by `TestHandler_DeliversLiveEvents`.
- Dial `ws://…/v1/notifications/ws?x-api-key=test-key` with **no**
  `Authorization` header.
- Expect a successful upgrade and that publishing an event delivers it
  to the client.

Keep `TestHandler_RejectsMissingAuth` as-is (no header + no query → 401).

The existing `TestHandler_DeliversLiveEvents` already exercises the
header path; together the three tests cover all three outcomes
(header, query, neither).

#### 4. `docs/openapi/strategy-server.json` — `/v1/notifications/ws`

- Add a third parameter entry for `x-api-key` in `query`, type `string`,
  required `false`, description noting it is an alternative to the
  `Authorization` header for clients that cannot set headers.
- Update the operation `description` to mention both auth methods and
  the precedence (header first, query as fallback).
- Leave `security: [{ "ApiKeyAuth": [] }]` in place — the OpenAPI
  security scheme documents the header; the query param is a
  documented extension for this one endpoint.

## Behavioural matrix

| `Authorization` header | `?x-api-key=` | Outcome |
|------------------------|---------------|---------|
| present + valid        | (any)         | header used |
| present + invalid      | (any)         | 401 from existing header check |
| absent                 | non-empty     | query used, upgrade succeeds |
| absent                 | absent/empty  | 401 "missing or unsupported Authorization header" |
| absent                 | present but empty | 401 (treated as absent) |

## Risks

- **Query-string credentials in logs.** Proxies and access logs may
  record the full URL including the key. The existing
  `cmd/strategy-server/main.go` access log uses Go's default
  `http.Server` logger which only logs the request line (path) — query
  strings are not logged by default. No new log lines are added. We
  will not URL-redact the query in our own logs because we don't log
  the URL ourselves. **Mitigation note for the user**: if a reverse
  proxy in front of strategy-server logs full URLs, that proxy will
  record the key. The same risk already exists for `?Last-Event-ID=` and
  `?types=`, so this is not a new exposure class.
- **CSRF.** WebSocket clients connecting with `x-api-key` from a
  browser would expose the key to any page making the same request.
  This is the user's intent in choosing query-param auth, and matches
  the same risk profile as the existing `Last-Event-ID` and `types`
  query parameters. No new CSRF risk is introduced for non-browser
  clients (curl, `wscat`, server-to-server).
- **Precedence surprise.** A client that sets both will silently have
  the header win. This matches the spec and is the safe default
  (the header is the canonical, less-leaky method).

## Verification

- `go test ./notifications/...` — all existing tests pass; new
  `TestHandler_AcceptsXAPIKeyQueryParam` passes.
- `go test ./cmd/strategy-server/...` — if any router tests exist,
  they pass.
- `go test ./...` — full module build clean.
- `golangci-lint run ./notifications/... ./cmd/strategy-server/...` —
  no new lint findings.
- `pre-commit run --all-files` — pre-commit hooks pass.
- Manual: `wscat -c "ws://localhost:8081/v1/notifications/ws?x-api-key=$DORA_API_KEY"`
  against a running strategy-server upgrades and receives a published
  event.

## Out of scope (deferred)

- Bearer tokens via query parameter.
- Extending the same fallback to other `/v1/...` endpoints.
- Adding a request-rate-limit carve-out for query-param auth.
