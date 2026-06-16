# CORS support for `/v1/notifications/ws`

## Problem

The strategy-server's CORS middleware is wired into the REST chain but
the WebSocket route bypasses it. Browsers cannot open a WebSocket from a
different origin against `/v1/notifications/ws`: the preflight `OPTIONS`
gets no CORS headers, and the upgrade response gets no CORS headers.

The existing `corsMiddleware` in `cmd/strategy-server/main.go:342` also
has a CORS spec violation: when `*` is configured it sets
`Access-Control-Allow-Origin: *` together with
`Access-Control-Allow-Credentials: true`, which the CORS spec forbids
and which browsers reject for credentialed requests. We will fix this as
part of the work because the WS path needs credentials (`Authorization`
header).

## Goal

A browser at any origin in the allow-list (or any origin when `*` is
configured) can:

1. Send `OPTIONS /v1/notifications/ws` preflight → get a 204 with the
   right CORS headers.
2. Send `GET /v1/notifications/ws?x-api-key=…` (or with `Authorization`)
   → get CORS headers on the upgrade response, then a successful
   WebSocket.

The existing REST behaviour is preserved: same flag, same env var, same
user-visible configuration surface.

## Scope

In scope:

- New top-level package `cors/` with the middleware extracted from
  `cmd/strategy-server/main.go`.
- Apply the middleware to BOTH the REST chain and the WebSocket
  sub-mux in `cmd/strategy-server/main.go`.
- Fix the `*` + `Allow-Credentials` spec violation by echoing the
  request's `Origin` instead of the literal `*` when `*` is configured.
- Add `Sec-WebSocket-Protocol` and `Sec-WebSocket-Extensions` to
  `Access-Control-Allow-Headers`.
- Add `PATCH` to `Access-Control-Allow-Methods`.
- Unit tests for the new `cors` package.
- An integration test on the WS router showing CORS headers on the
  preflight and on the upgrade.

Out of scope:

- Per-route CORS configuration (one config for REST and WS).
- Cookie-based authentication.
- CORS for the price-daemon and mcp-server binaries (they have their
  own server setups and no current browser clients).
- Changes to the existing `--cors-allowed-origins` flag or
  `CORS_ALLOWED_ORIGINS` env var (semantics are the same; the bug
  fix changes what browsers actually accept, not what the operator
  types).

## Design

### Package extraction

Move the existing `corsMiddleware` function (lines 340-379 of
`cmd/strategy-server/main.go`) into a new top-level package
`cors/cors.go`. Expose it as:

```go
package cors

// New returns an HTTP middleware that adds CORS headers to every
// response. origins is a comma-separated list. A single "*" allows
// any origin. An empty string disables CORS — the returned function
// is a pass-through.
func New(origins string) func(http.Handler) http.Handler
```

The middleware behaviour:

- Parse `origins` into a set + `allowAll bool`.
- On every request:
  - Read `Origin` header. If empty, skip CORS headers (the request is
    not a CORS request).
  - If `allowAll`: echo the request's `Origin` in
    `Access-Control-Allow-Origin`. Set `Vary: Origin`. Set
    `Access-Control-Allow-Credentials: true`.
  - Else if `Origin` is in the allow-list: echo it. Set `Vary: Origin`.
    Set `Access-Control-Allow-Credentials: true`.
  - Else: do not set `Access-Control-Allow-Origin` at all. The browser
    will block the response.
- Always set:
  - `Access-Control-Allow-Methods: GET, POST, PUT, PATCH, DELETE, OPTIONS`
  - `Access-Control-Allow-Headers: Authorization, Content-Type, Sec-WebSocket-Protocol, Sec-WebSocket-Extensions`
  - `Access-Control-Allow-Credentials: true`
  - `Access-Control-Max-Age: 86400`
- If `r.Method == http.MethodOptions`: write 204 No Content and return
  (do not call the next handler).

When `origins` is empty, `cors.New("")` returns a function that calls
`next.ServeHTTP` directly with no header mutation. This is the
behaviour callers need to make CORS "off" without conditional wiring
in the caller.

### Wiring in `cmd/strategy-server/main.go`

Today (lines 207-225):

```go
wrappedHandler := rl.Middleware(handlerImpl)

if *corsAllowedOrigins != "" {
    wrappedHandler = corsMiddleware(*corsAllowedOrigins, wrappedHandler)
}

if notifier != nil {
    wsSubMux := http.NewServeMux()
    wsSubMux.Handle("/v1/notifications/ws", notifications.NewHandler(...))
    wrappedHandler = notificationsRouter{fallback: wrappedHandler, sub: wsSubMux}
}
```

The WS request enters `notificationsRouter` first, which short-circuits
the fallback for `/v1/notifications/ws` and dispatches to the sub-mux
directly. CORS never runs for the WS path.

After:

```go
corsWrap := cors.New(*corsAllowedOrigins) // pass-through when empty

wrappedHandler := rl.Middleware(handlerImpl)
if *corsAllowedOrigins != "" {
    wrappedHandler = corsWrap(wrappedHandler)
}

if notifier != nil {
    wsSubMux := http.NewServeMux()
    wsHandler := notifications.NewHandler(...)
    if *corsAllowedOrigins != "" {
        wsHandler = corsWrap(wsHandler)
    }
    wsSubMux.Handle("/v1/notifications/ws", wsHandler)
    wrappedHandler = notificationsRouter{fallback: wrappedHandler, sub: wsSubMux}
}
```

CORS runs in front of the WS handler (when configured), so both
preflight `OPTIONS` and the upgrade `GET` see the CORS headers.

Delete the inline `corsMiddleware` function from `main.go` once the
package is in place.

### Behavioural matrix

The matrix below covers every combination of configured origins and
incoming `Origin` header. The "Action" column describes the response
headers set.

| `origins` config        | Request `Origin`     | Action                                                                                       |
|-------------------------|----------------------|----------------------------------------------------------------------------------------------|
| `*`                     | `https://any.com`    | `Allow-Origin: https://any.com`, `Vary: Origin`, `Allow-Credentials: true`                   |
| `*`                     | (empty)              | `Allow-Origin: *`, `Allow-Credentials: true`                                                 |
| `https://a.com,b.com`   | `https://a.com`      | `Allow-Origin: https://a.com`, `Vary: Origin`, `Allow-Credentials: true`                     |
| `https://a.com,b.com`   | `https://c.com`      | (no `Allow-Origin` header)                                                                   |
| `https://a.com,b.com`   | (empty)              | (no `Allow-Origin` header)                                                                   |
| (empty / unset)         | (any)                | middleware not applied; handler runs as if no CORS middleware was present                   |

### Files

**New**
- `cors/cors.go` — package with the middleware
- `cors/cors_test.go` — table-driven unit tests

**Modified**
- `cmd/strategy-server/main.go` — remove inline `corsMiddleware`,
  import `cors`, wire it in front of the WS sub-mux
- `cmd/strategy-server/notifications_router_test.go` — add a test
  that confirms CORS headers on `OPTIONS /v1/notifications/ws` and on
  the upgrade path

### Tests

**`cors/cors_test.go`** (table-driven, exercises the middleware in
isolation):
- `*` + `Origin: https://x.com` → echoes X
- `*` + no `Origin` → `*`
- explicit list + matching `Origin` → echoes
- explicit list + non-matching `Origin` → no `Allow-Origin`
- explicit list + no `Origin` → no `Allow-Origin`
- `OPTIONS` → 204, all headers, next handler not called
- `GET` (non-OPTIONS) → all headers, next handler called
- empty config → next handler called, no CORS headers set
- `Allow-Headers` contains `Sec-WebSocket-Protocol` and
  `Sec-WebSocket-Extensions`
- `Allow-Methods` contains `PATCH`

**`cmd/strategy-server/notifications_router_test.go`** (integration
test for the WS router with CORS wired in):
- Build a router with CORS configured for `https://app.example.com`
- `OPTIONS /v1/notifications/ws` with `Origin: https://app.example.com`
  and `Access-Control-Request-Method: GET` → 204, correct headers
- `GET /v1/notifications/ws` with `Origin: https://app.example.com` and
  an `Authorization` header → response carries
  `Access-Control-Allow-Origin: https://app.example.com` (the response
  will be 401 because we do not wire real auth in the test, but the
  CORS headers are what we assert on)

### Risks

- **Spec compliance of `*` + credentials.** The CORS RFC forbids
  `Access-Control-Allow-Origin: *` together with
  `Access-Control-Allow-Credentials: true`. We deliberately deviate
  from the strict reading by echoing `Origin` when `*` is configured
  and keeping `Allow-Credentials: true`. This matches the behaviour
  of every major CORS library (Express `cors`, Go `rs/cors`, etc.) and
  is what browsers actually accept in practice. Documented in the
  package doc comment.
- **Middleware ordering.** CORS runs INSIDE the rate limiter. An
  attacker can therefore exhaust the rate budget with cheap OPTIONS
  preflights. This is acceptable: OPTIONS are short, the rate limit
  is per-IP, and preflights are how browsers work.
- **Empty `origins` means "no CORS at all".** Today the behaviour is
  the same (the existing code wraps only when the flag is non-empty).
  The new `cors.New("")` returns a pass-through so the caller doesn't
  need a conditional. Documented.

### Verification

- `go test ./cors/...` — unit tests pass
- `go test ./cmd/strategy-server/...` — new CORS test passes, existing
  router tests still pass
- `go test ./...` — full module build clean
- `golangci-lint run ./cors/... ./cmd/strategy-server/...` — 0 issues
- `pre-commit run --all-files` — pre-commit hooks pass
- Manual: `curl -i -X OPTIONS -H 'Origin: https://app.example.com' -H
  'Access-Control-Request-Method: GET' http://localhost:8081/v1/notifications/ws`
  returns 204 with CORS headers
