# WebSocket `OriginPatterns` for cross-origin upgrades

## Problem

The `coder/websocket.Accept` function applies a server-side CSRF check
that rejects cross-origin WebSocket upgrades unless either
`AcceptOptions.OriginPatterns` or `AcceptOptions.InsecureSkipVerify` is
set. The CORS headers added by the `cors` middleware do **not** affect
this check — CORS is a browser-side mechanism; the library's check is
purely server-side.

Today, the notifications handler at `notifications/handler.go:58` calls:

```go
websocket.Accept(w, r, &websocket.AcceptOptions{})
```

with an empty `AcceptOptions`, so any cross-origin browser upgrade is
rejected by the library even when the CORS preflight succeeded. The
CORS work landed in the previous PR but did not fix the upgrade
itself.

## Goal

A browser at an origin in `--cors-allowed-origins` can complete a
cross-origin WebSocket upgrade against `/v1/notifications/ws`.

The same flag (`--cors-allowed-origins` / `CORS_ALLOWED_ORIGINS`)
governs three things, each used by the appropriate part of the system:

1. CORS response headers (already done by the `cors` middleware)
2. `AcceptOptions.OriginPatterns` or `InsecureSkipVerify` on the WS
   accept (this change)
3. The DORA client's `Origin` if it ever makes a server-to-server
   request that needs CORS (not currently a case)

## Scope

In scope:

- New function `cors.OriginPatterns(origins string) (patterns []string, allowAll bool)`
  in the existing `cors/` package. Parses the same input format that
  `cors.New` already parses; produces the values needed by
  `coder/websocket.AcceptOptions`.
- `notifications.Handler` accepts the new options via a new
  `HandlerOption`. The `notifications.NewHandler` constructor takes
  `Notifier`, `ResolveUserID`, and `...HandlerOption`. The new
  option is `WithAcceptOptions(websocket.AcceptOptions)` — but
  importing `coder/websocket` into the `notifications` package is
  the current behaviour (the handler already imports it for
  `websocket.Accept`), so this is a no-cost extension.
- `cmd/strategy-server/main.go` calls `cors.OriginPatterns` once with
  the configured value and threads the result into the WS handler via
  the new option.

Out of scope:

- Per-route origin configuration (one config for REST and WS).
- Subprotocol negotiation (`Sec-WebSocket-Protocol`).
- The outbound WebSocket client in `notifications/client.go` — it
  dials outbound; the `Origin` header it sends is the strategy-server
  itself, not a browser origin. The accept check does not apply.
- Per-IP allow-listing, rate-limiting origins, etc.

## Design

### Mapping rules

`cors.OriginPatterns` accepts the same input format as `cors.New`:
comma-separated entries, trimmed, empty entries skipped. Each entry
maps to either a pattern, the `allowAll` flag, or both:

| Input entry                  | `patterns`            | `allowAll` |
|------------------------------|-----------------------|------------|
| `*`                          | (none)                | `true`     |
| `https://app.example.com`    | `["app.example.com"]` | `false`    |
| `http://app.example.com:8080`| `["app.example.com:8080"]` | `false` |
| `*.example.com`              | `["*.example.com"]`   | `false`    |
| `https://*.example.com`      | `["*.example.com"]`   | `false`    |
| empty / whitespace           | (skipped)             | (unchanged)|

The library's doc on `OriginPatterns` says: "Do not use * as a
pattern to allow any origin, prefer to use InsecureSkipVerify instead
to bring attention to the danger of such a setting." So `*` always
maps to `InsecureSkipVerify: true`, never to a pattern entry.

The library uses `path.Match` (not URL matching) to compare patterns
against `scheme://host` of the request `Origin`. Stripping the scheme
when present (entries like `https://app.example.com`) is correct;
the library re-adds it before matching.

If the input contains both `*` and explicit entries, `*` wins (the
patterns slice is ignored when `InsecureSkipVerify` is set on the
accept options — the library returns the request origin unchanged
when the verifier is off).

### `cors` package additions

```go
// OriginPatterns parses the same input format as New and returns the
// values needed to configure coder/websocket.AcceptOptions. OriginPatterns
// lists the host patterns that the WS library should match against
// the request Origin. allowAll is true when the input contained a
// bare "*"; in that case the caller should set
// AcceptOptions.InsecureSkipVerify = true (and leave OriginPatterns
// empty) — the library's documentation recommends this over a "*"
// pattern entry because it is more visible at the call site.
func OriginPatterns(origins string) (patterns []string, allowAll bool)
```

The function reuses the same parsing logic as `New` — split on `,`,
trim, skip empty, special-case `*`. The strip-scheme step is new.

The shared parsing logic is small enough (a 10-line loop) that
extracting it into a private helper is overkill. Both `New` and
`OriginPatterns` will have their own copy. If a third entry-point
ever needs the same parsing, refactor then.

### `notifications.Handler` change

Add a new option:

```go
// WithAcceptOptions configures the WebSocket accept options used
// when accepting the connection. Callers that need to allow
// cross-origin WebSocket upgrades (e.g. browser clients at a
// different origin) should set OriginPatterns or InsecureSkipVerify
// here. An empty AcceptOptions (the default) rejects all
// cross-origin upgrades via the coder/websocket library's CSRF check.
func WithAcceptOptions(opts websocket.AcceptOptions) HandlerOption
```

`Handler` gains a private field:

```go
type Handler struct {
    notifier      Notifier
    resolveUser   ResolveUserID
    log           *slog.Logger
    acceptOptions websocket.AcceptOptions
}
```

`ServeHTTP` uses the stored options:

```go
conn, err := websocket.Accept(w, r, &h.acceptOptions)
```

This is a one-line change in `ServeHTTP` and a one-line addition in
`NewHandler` (default zero value). Backward compatible: existing
callers that don't pass `WithAcceptOptions` get the same
empty-options behaviour as today.

### `cmd/strategy-server/main.go` wiring

After the existing `var wsHandler http.Handler = notifications.NewHandler(...)`,
add the new option when CORS is configured:

```go
var wsHandler http.Handler = notifications.NewHandler(
    notifier,
    resolveUserID,
    notifications.WithHandlerLogger(log),
    notifications.WithAcceptOptions(wsAcceptOptions),
)
```

Where `wsAcceptOptions` is computed once at startup:

```go
wsPatterns, wsAllowAll := cors.OriginPatterns(*corsAllowedOrigins)
wsAcceptOptions := websocket.AcceptOptions{
    OriginPatterns:     wsPatterns,
    InsecureSkipVerify: wsAllowAll,
}
```

Note: when CORS is not configured (`--cors-allowed-origins` empty),
`wsAllowAll` is false and `wsPatterns` is nil, so the
`AcceptOptions` is the zero value — same as today, same-host upgrades
still work because the library always authorizes the request's own
host.

### Files

**Modified**
- `cors/cors.go` — add `OriginPatterns` function
- `cors/cors_test.go` — add unit tests for `OriginPatterns`
- `notifications/handler.go` — add `WithAcceptOptions` option and
  `acceptOptions` field; use it in `ServeHTTP`
- `cmd/strategy-server/main.go` — call `cors.OriginPatterns` and pass
  the result into the WS handler
- `notifications/handler_test.go` — add a test that confirms the
  option is wired (a positive case is sufficient; the library's own
  negative-case behaviour is tested upstream)

### Tests

**`cors/cors_test.go` (new tests for `OriginPatterns`):**
- `*` → `patterns=nil, allowAll=true`
- `https://app.example.com` → `patterns=["app.example.com"], allowAll=false`
- `http://app.example.com:8080` → `patterns=["app.example.com:8080"]`
- `*.example.com` → `patterns=["*.example.com"]`
- `https://*.example.com` → `patterns=["*.example.com"]`
- Multiple entries comma-separated → all in order
- Whitespace, empty entries → handled
- Empty input → `patterns=nil, allowAll=false`
- Mixed `*` and entries → `allowAll=true, patterns` still populated
  (the caller is responsible for ignoring patterns when allowAll is
  true; the function does not collapse them)

**`notifications/handler_test.go` (new test):**
- Build a `notifications.Handler` with `WithAcceptOptions` set to
  `OriginPatterns: ["app.example.com"]`
- Dial `ws://…/v1/notifications/ws?x-api-key=test-key` with
  `Origin: https://app.example.com` set on the request
- Assert the upgrade succeeds (status 101, conn non-nil)
- Build a handler with `OriginPatterns: []` and `InsecureSkipVerify: true`
- Dial with `Origin: https://attacker.com`
- Assert the upgrade succeeds

The negative case (cross-origin without patterns or skip-verify) is
already covered by the library's own tests; we don't need to
re-verify it.

### Risks

- **Operator surprise.** `--cors-allowed-origins=*` is now even
  more permissive: it allows any cross-origin REST request (via CORS
  headers) AND any cross-origin WS upgrade (via
  `InsecureSkipVerify`). This is consistent — both paths mean "open
  to all" — and documented in the spec. If the operator wanted "CORS
  open but WS restricted" they would need a separate flag, which is
  out of scope for this change.
- **Pattern semantics.** The library uses `path.Match`, which is
  shell-style globbing, not regex. `*` matches any sequence of
  non-`/` characters. `https://app.example.com/path` would NOT
  match `*.example.com` because the origin has no path. Documented
  in the `OriginPatterns` doc comment.
- **The `Host` header.** The library always authorizes the request's
  own host. So `--cors-allowed-origins=` (empty) does not break
  same-origin upgrades; the host header is sufficient.
- **Subprotocols.** The library still negotiates subprotocols from
  the client's request. We don't change that.

### Verification

- `go test ./cors/...` — all `OriginPatterns` tests pass
- `go test ./notifications/...` — all handler tests pass, including
  the new option test
- `go test ./cmd/strategy-server/...` — no regression
- `go test ./...` — full module clean
- `golangci-lint run ./cors/... ./notifications/... ./cmd/strategy-server/...` — 0 issues
- `pre-commit run --files cors/cors.go cors/cors_test.go notifications/handler.go notifications/handler_test.go cmd/strategy-server/main.go` — passes
- Manual: with `--cors-allowed-origins=https://app.example.com`, a
  `wscat` from a script on `https://app.example.com` connects
  successfully to `wss://…/v1/notifications/ws?x-api-key=…` (the
  cross-origin upgrade is no longer rejected)
