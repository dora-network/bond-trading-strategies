# Request logging for strategy-server HTTP API

## Problem

The strategy-server exposes a REST API under `/v1/...`, a health
endpoint, an OpenAPI spec, and a WebSocket endpoint at
`/v1/notifications/ws`. None of these are logged at the request layer
today. When an operator wants to know who hit which endpoint, whether
an authentication failure is a misconfigured client or an attacker, or
which request produced a 5xx, the only signal is the per-handler
`h.log`/`slog` calls scattered through the codebase. There is no
single audit trail and no consistent field set.

The user-visible requirement: every request to a strategy-server
endpoint must be logged, with the time, the route, the caller
(identified where possible), and any error the request produced. The
API key (or bearer token) used to authenticate the request must never
appear in the log. A request that cannot be attributed to a user
(missing or invalid credentials) must still be logged.

## Goal

For every non-exempt request to the strategy-server, emit exactly one
structured log record after the response is written. The record
identifies the endpoint, the HTTP status, the duration, the resolved
caller (when authentication succeeded), and any error string the
handler attached. Requests that fail authentication, or hit an
endpoint that does not perform authentication (the WebSocket upgrade),
are logged with `user_id="unauthenticated"`. The `/healthz` and
`/v1/openapi` endpoints are exempt and produce no log record.

## Scope

In scope:

- A new `RequestLog` HTTP middleware in `strategy/http` that wraps an
  `http.Handler`, records duration, status, bytes, and a
  caller-attached error string, and emits one slog record per request.
- A `LoggingResponseWriter` that captures status, bytes, and the
  attached error string. Implements `http.Hijacker` by delegating to
  the underlying writer so the WebSocket upgrade path is not broken.
- A small hook in `strategy/http.writeError` so that handlers that
  produce a non-2xx response can attach the error string the caller
  will see in the body.
- Wiring the middleware in `cmd/strategy-server/main.go` around the
  full handler chain (REST + WebSocket) so every request flows through
  it exactly once.
- Exempting `/healthz` and `/v1/openapi` from logging (hardcoded list
  in the middleware, since the set is small and stable).
- Unit tests for the middleware in `strategy/http/logging_test.go`.

Out of scope:

- A request-id / correlation-id header. The user explicitly chose not
  to add one.
- Per-request rate limiting or sampling. Every non-exempt request is
  logged.
- Logging inside `price-daemon` or `mcp-server`. Each binary has its
  own setup and is not part of this change.
- Changes to the existing `h.log`/`slog` call sites in handler.go.
  Those continue to log domain events (resuming a run, saving a
  backtest result). The new middleware adds the request layer; the two
  are complementary.
- Any change to `authctx`. The middleware reads the existing
  `doraUserIDFromContext` helper; it does not introduce a new
  credential type.
- Any change to `cors`, `notifications`, or the price daemon. The
  middleware only inspects the request and the response status; it
  does not depend on the CORS or notification packages.

## Design

### Log line shape

A single slog record is emitted per non-exempt request, after the
handler returns. The record is built by the middleware and emitted
on the `*slog.Logger` passed to it (in production, the
`slog.Default()` already configured by `cmd/strategy-server/main.go`):

| Level       | When                                   |
|-------------|----------------------------------------|
| `slog.Info` | `status < 400`, no attached error      |
| `slog.Warn` | `400 ≤ status < 500`                   |
| `slog.Error`| `status ≥ 500`                         |

Note: a 401 from `requireAuth` falls in the `slog.Warn` band, with
`user_id="unauthenticated"` and `err` set to the body the handler
wrote (e.g. `"missing Authorization header"`).
Fields on the record (all `slog.Attr`):
Fields on the record (all `slog.Attr`, set in this order):

| Field         | Type   | Source                                            |
|---------------|--------|---------------------------------------------------|
| `method`      | string | `r.Method`                                        |
| `route`       | string | `r.Pattern` if non-nil, else `r.URL.Path`         |
| `path`        | string | `r.URL.Path`                                      |
| `status`      | int    | captured by the wrapper                           |
| `duration_ms` | int64  | `(time.Since(start)) / time.Millisecond`          |
| `bytes`       | int    | captured by the wrapper                           |
| `user_id`     | string | from context via `doraUserIDFromContext`; empty → `"unauthenticated"` |
| `err`         | string | attached by `writeError` (or any handler via `WithError`); omitted if empty |

The `msg` attribute is the literal string `"request"`. Operators
filter on it.

The route is taken from `r.Pattern` (Go 1.26, available in this
module — `go.mod` declares `go 1.26.2`). For requests matched by the
strategy-server's `http.ServeMux`, `r.Pattern` is the mux pattern
(e.g. `/v1/backtests/{id}`). For requests that did not match a
pattern (e.g. 404s from the global mux), `r.Pattern` is nil and we
fall back to `r.URL.Path`. This gives operators a useful "which
endpoint" grouping for matched routes and a literal path for
unmatched ones, with no allocation.

The credentials used to authenticate the request — the raw `ApiKey`
or `Bearer` token — are never read by the middleware, the wrapper, or
`writeError`. The middleware reads only `doraUserIDFromContext`, which
returns the already-resolved DORA user ID. `authctx.AuthInfoFromContext`
is not consulted. A unit test asserts that the request's
`Authorization` header value (with a known secret) does not appear in
the emitted log output.

### Why one middleware at the chain root, not at `NewHandler`

The strategy-server's `cmd/strategy-server/main.go` builds a chain
that starts with a rate limiter and CORS, then a
`notificationsRouter` that dispatches `/v1/notifications/ws` to a
sub-mux and falls through to the strategy `Handler.ServeHTTP` for
everything else. The WebSocket path therefore bypasses
`strategy/http.Handler.ServeHTTP` entirely. The
`notificationsRouter` comment at `main.go:300-305` documents that a
`ResponseWriter` wrapper which shadows `http.Hijacker` would break
`websocket.Accept` — this is why no wrapper exists today.

Two options were considered:

- **Wrap inside `strategy/http.NewHandler`** (around the `authedMux`).
  Misses the WebSocket path; needs an additional wrapper in
  `main.go` to cover WS, which means every REST request gets logged
  twice.
- **Wrap once in `main.go`** around the whole chain (`wrappedHandler`
  as the input). Covers REST and WS with a single log line per
  request. The wrapper still implements `http.Hijacker` by
  delegating, so the WS upgrade works.

The second option is chosen. The chain becomes:

```
server.Handler
  └─> requestLog                       (new, in main.go)
       └─> rateLimit / CORS            (existing)
            └─> notificationsRouter    (existing)
                 ├─> wsSubMux -> /v1/notifications/ws upgrade
                 └─> strategy/http.Handler.ServeHTTP
                      ├─> /healthz, /v1/openapi   -> h.mux  (not logged)
                      └─> requireAuth -> h.mux -> handler  (logged at outer layer)
```

`strategy/http.NewHandler` is unchanged. The log middleware sits
outside the chain, so it sees the response status set by both
`requireAuth` (for 401) and the inner handler (for everything else),
and reads the resolved `user_id` from context (set by `requireAuth`).

### Components

**`strategy/http/logging.go`** (new file) — owns:

- `type LoggingResponseWriter struct { ... }` — wraps
  `http.ResponseWriter`. Records `status` (defaults to 200, set on
  the first `WriteHeader` call) and `bytes` (sum of `Write` calls).
  Holds an `errMsg string` (empty unless `WithError` is called).
  Implements `http.Hijacker` by delegating to the underlying writer
  when it supports the interface (type-asserts on construction;
  falls back to a no-op `Hijack` returning an error if the underlying
  writer does not implement hijacking — this should never happen in
  production but is a safe default). Exposes:
  - `WithError(msg string)` — stores the message; the middleware
    reads it after the handler returns.
  - `Status() int`, `Bytes() int`, `Err() string` — read by the
    middleware.
- `func RequestLog(log *slog.Logger, exempt map[string]struct{}) func(http.Handler) http.Handler`
  — the middleware. `exempt` is the set of paths that produce no log
  record. The middleware:
  1. Checks `exempt[r.URL.Path]`; if hit, calls `next.ServeHTTP(w, r)`
     and returns.
  2. Records `start := time.Now()`.
  3. Wraps `w` in `LoggingResponseWriter`.
  4. Calls `next.ServeHTTP(lw, r)`.
  5. After the handler returns, builds the slog record with the
     fields above and the level rules above. Calls
     `log.LogAttrs(ctx, level, "request", attrs...)`.

**`strategy/http/handler.go`** (existing) — one small change:

- In `writeError`, after `writeJSON(w, status, ErrorResponse{Error: message})`,
  call `if lw, ok := w.(*LoggingResponseWriter); ok { lw.WithError(message) }`.
  The wrapper is constructed as a pointer (`&LoggingResponseWriter{...}`)
  in the middleware, and `http.ResponseWriter` is an interface, so the
  type assertion must match a pointer. The assertion is the contract:
  every request that reaches `writeError` has flowed through
  `RequestLog` in `main.go`, so `w` is a `*LoggingResponseWriter`.
  The `ok` check is defensive and never false in production.

**`cmd/strategy-server/main.go`** (existing) — one small change:

- After the rate limiter and CORS wrappers, and before
  `notificationsRouter` is constructed, wrap
  `wrappedHandler` with `strategy/http.RequestLog(slog.Default(),
  exempt)`. The exempt set is built once at startup:
  `map[string]struct{}{"/healthz": {}, "/v1/openapi": {}}`.

No other files are modified.

### Error handling and security

- **Credentials are never logged.** The middleware reads only
  `r.Method`, `r.URL.Path`, `r.Pattern`, and the request context (for
  `doraUserIDFromContext`). The wrapper's `WithError` is set only by
  `writeError`, which builds the message from in-code string
  constants. A unit test asserts the request's `Authorization`
  header value does not appear in the emitted record.
- **Level rules are deterministic and total.** Every status has
  exactly one level. 401 is `slog.Warn`; 500 is `slog.Error`;
  everything 2xx and 3xx is `slog.Info`.
- **The `user_id` field always has a value.** When authentication
  failed or has not run yet, the literal string `"unauthenticated"`
  is used. This makes it greppable: `user_id="unauthenticated"`
  finds every 401 and every WS upgrade.
- **Hijack delegation.** `LoggingResponseWriter.Hijack()` returns
  `(net.Conn, *bufio.ReadWriter, error)` by calling the underlying
  writer's `Hijack()` when supported. The `notifications` package
  and `websocket.Accept` require this; without it, every WS
  connection would fail. The existing comment at
  `main.go:300-305` is the reason this is necessary and is
  preserved.
- **No panics on missing wrapper.** If a handler is ever called with
  a `ResponseWriter` that is not the wrapper (e.g. from a test that
  bypasses the middleware), `writeError`'s type-assertion `ok` check
  means the `err` field is simply not attached for that request. The
  request itself is still logged by the middleware (since the
  middleware is what wraps the writer in the first place — the only
  way to bypass it is to bypass the chain root, which means the
  request is not logged at all).

### Files

**New**
- `strategy/http/logging.go` — `LoggingResponseWriter` and
  `RequestLog`.
- `strategy/http/logging_test.go` — table-driven unit tests.

**Modified**
- `strategy/http/handler.go` — `writeError` calls
  `lw.WithError(message)`.
- `cmd/strategy-server/main.go` — wrap `wrappedHandler` with
  `RequestLog`. No other change.

### Tests

**`strategy/http/logging_test.go`** (table-driven where it makes
sense, individual tests where each one has a distinct setup):

- `TestRequestLog_InfoOnSuccess` — request returns 200, log handler
  records `slog.Info`, fields include `method`, `route`, `path`,
  `status=200`, `duration_ms≥0`, `bytes>0`, `user_id="<set>"`, no
  `err` attribute.
- `TestRequestLog_WarnOn4xx` — handler returns 400, log handler
  records `slog.Warn`, `err` is the body message.
- `TestRequestLog_ErrorOn5xx` — handler returns 500, log handler
  records `slog.Error`, `err` is the body message.
- `TestRequestLog_UnauthenticatedOn401` — request to a
  `requireAuth`-protected path with no `Authorization` header. Log
  line is `slog.Warn`, `status=401`, `user_id="unauthenticated"`,
  `err` set.
- `TestRequestLog_UnauthenticatedOnNonAuthPath` — request to a path
  that does not run `requireAuth` (e.g. the WS path). Log line has
  `user_id="unauthenticated"`.
- `TestRequestLog_UsesPattern` — request matched by a mux with a
  pattern → `route` is the pattern, not the literal path.
- `TestRequestLog_FallsBackToPath` — request that does not match any
  pattern → `route` is the literal `r.URL.Path`.
- `TestRequestLog_SkipsExemptPaths` — request to `/healthz` and
  `/v1/openapi` → no log record emitted.
- `TestRequestLog_PreservesHijacker` — handler that calls
  `w.Hijack()` succeeds; the wrapper delegates. Use a fake
  `http.ResponseWriter` that implements `http.Hijacker` and assert
  its `Hijack` method was called.
- `TestRequestLog_NeverLogsCredentials` — request with
  `Authorization: ApiKey secret-abc-123`. Capture the slog output
  and assert the substring `secret-abc-123` is not present. Also
  assert the key is not hashed, fingerprinted, or otherwise
  derivable from the output.
  The assertion is intentionally strict: the test must fail if
  ANY substring of the `Authorization` header value appears in
  the emitted record in any encoding (raw, lowercase, hex,
  base64, length, prefix, or suffix). The operational guarantee
  is "the key never appears in the log in any form", not
  "the key is not printed verbatim".
  derivable from the output.
- `TestRequestLog_NoErrFieldWhenAbsent` — 200 with no `WithError`
  call → emitted record has no `err` attribute.

The slog capture uses `slog.NewJSONHandler` writing to a
`*bytes.Buffer`, which the test reads and decodes as JSON to assert
on fields. This matches how the project uses slog elsewhere
(`batching_writer.go` uses it directly).

No integration tests in `cmd/strategy-server` are added. The wiring
in `main.go` is a single line; the unit tests above cover the
middleware in isolation. The existing handler tests
(`strategy/http/handler_test.go`) are unchanged and still pass.

### Verification

- `go test ./strategy/http/...` — all existing tests pass; the new
  tests pass.
- `go test ./...` — full module build clean.
- `golangci-lint run ./strategy/http/... ./cmd/strategy-server/...`
  — 0 issues.
- `pre-commit run --all-files` — pre-commit hooks pass.
- Manual: start the server, `curl http://localhost:8081/healthz`
  produces no log line; `curl http://localhost:8081/v1/strategies`
  with no `Authorization` produces one `slog.Warn` line with
  `user_id="unauthenticated"` and `err="missing Authorization
  header"`; `curl http://localhost:8081/v1/strategies -H
  'Authorization: ApiKey <key>'` produces one `slog.Info` line with
  `user_id=<resolved>`; `curl http://localhost:8081/v1/notifications/ws`
  with a valid `Authorization` header produces one `slog.Info` line
  for the upgrade. None of these lines contain the API key.

### Risks

- **Log volume.** Every non-exempt request is logged at `slog.Info`
  on success. A busy server with 1k req/s produces 1k info lines per
  second. This is intentional — the user asked for a complete audit
  trail. Operators who find this too noisy can lower the default
  level on the `slog.Handler` they pass to `RequestLog`; the
  middleware respects whatever level the handler is configured to
  emit at (it calls `log.LogAttrs(ctx, level, "request", ...)`, so
  if the configured level is `Warn`, the `Info` records are
  dropped). The default is unchanged: `slog.Default()`.
- **Order in `main.go`.** The middleware wraps the chain root, so
  the log record is emitted after the response is fully written. If
  a future change adds a slow wrapper after the request log (e.g.
  a metrics middleware), the log timestamps will still reflect the
  end-of-handler time, not the end-of-middleware time. This is the
  correct semantic.
- **`authctx.AuthInfoFromContext` is not used.** A future change that
  adds a new auth scheme to `authctx` does not need to update this
  middleware, because the middleware reads the resolved DORA user
  ID, not the raw credentials. This is a feature, not a bug, but is
  worth noting for future maintainers.
