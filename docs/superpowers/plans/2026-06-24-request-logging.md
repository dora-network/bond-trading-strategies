# Request Logging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Emit one structured slog record per non-exempt HTTP request to the strategy-server, identifying the endpoint, status, duration, and resolved caller, with credentials never appearing in the log.

**Architecture:** Single `RequestLog` middleware in `strategy/http` wrapping the full handler chain in `cmd/strategy-server/main.go`. A `LoggingResponseWriter` captures status, bytes, and an attached error string; it implements `http.Hijacker` by delegation so the WebSocket upgrade still works. `writeError` calls the wrapper's `WithError` so the log line carries the response body's error message. Exempt paths (`/healthz`, `/v1/openapi`) bypass the middleware. Levels: `Info` for status < 400, `Warn` for 4xx, `Error` for 5xx. 401 is `Warn` with `user_id="unauthenticated"`.

**Tech Stack:** Go 1.26 (`go.mod` declares `go 1.26.2`; `r.Pattern` is on `*http.Request` since Go 1.22). `log/slog` (already in use project-wide). `net/http` middleware pattern. `testify/assert` + `testify/require` (already used in `strategy/http`).

---

## File structure

| File | Status | Responsibility |
|---|---|---|
| `strategy/http/logging.go` | New | `LoggingResponseWriter` and `RequestLog` middleware |
| `strategy/http/logging_test.go` | New | Unit tests for the middleware |
| `strategy/http/handler.go` | Modify | `writeError` calls `lw.WithError(message)` |
| `cmd/strategy-server/main.go` | Modify | Wrap `wrappedHandler` with `RequestLog`; build exempt set |

No other files change. `authctx`, `cors`, `notifications`, the strategy `Service`, the price daemon, the MCP server, and the database schema are untouched.

---

## Task 1: `LoggingResponseWriter` — capture status, bytes, and an error string

**Files:**
- Create: `strategy/http/logging.go`
- Test: `strategy/http/logging_test.go`

- [ ] **Step 1: Write the failing test for the wrapper's basic contract**

In `strategy/http/logging_test.go`:

```go
package http_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	strategyhttp "bond-trading-strategies/strategy/http"
)

func TestLoggingResponseWriter_CapturesStatusAndBytes(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	lw := strategyhttp.NewLoggingResponseWriter(rec)

	lw.WriteHeader(http.StatusTeapot)
	n, err := lw.Write([]byte("hello"))

	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, http.StatusTeapot, lw.Status())
	assert.Equal(t, 5, lw.Bytes())
	assert.Equal(t, "", lw.Err())
}
```

- [ ] **Step 2: Run the test to verify it fails to compile**

Run: `go test ./strategy/http/ -run TestLoggingResponseWriter_CapturesStatusAndBytes -v`
Expected: compile error — `strategyhttp.NewLoggingResponseWriter`, `lw.Status()`, `lw.Bytes()`, `lw.Err()` are not defined.

- [ ] **Step 3: Write the wrapper with the public API**

In `strategy/http/logging.go`:

```go
package http

import (
	"bufio"
	"errors"
	"net"
	"net/http"
)

// LoggingResponseWriter wraps an http.ResponseWriter to capture the
// status code, the number of bytes written, and an optional error
// string attached via WithError. The middleware reads these fields
// after the handler returns to emit the request log record.
//
// If the wrapped writer implements http.Hijacker (required for
// WebSocket upgrades), Hijack delegates to it; otherwise Hijack
// returns an error. See cmd/strategy-server/main.go for context.
type LoggingResponseWriter struct {
	w      http.ResponseWriter
	status int
	bytes  int
	errMsg string
}

// NewLoggingResponseWriter wraps w. The default captured status is
// 200; the first call to WriteHeader sets it to the actual code.
func NewLoggingResponseWriter(w http.ResponseWriter) *LoggingResponseWriter {
	return &LoggingResponseWriter{w: w, status: http.StatusOK}
}

// Header exposes the wrapped writer's Header.
func (l *LoggingResponseWriter) Header() http.Header { return l.w.Header() }

// WriteHeader records the status code and forwards to the wrapped
// writer. Subsequent calls are passed through unchanged (matching
// net/http's behaviour).
func (l *LoggingResponseWriter) WriteHeader(status int) {
	l.status = status
	l.w.WriteHeader(status)
}

// Write forwards to the wrapped writer and counts the bytes written.
func (l *LoggingResponseWriter) Write(b []byte) (int, error) {
	n, err := l.w.Write(b)
	l.bytes += n
	return n, err
}

// WithError stores an error string that the log middleware will
// include as the "err" field on the request log record. It is
// typically called by writeError right after writing the response.
// Safe to call multiple times; the last value wins.
func (l *LoggingResponseWriter) WithError(msg string) { l.errMsg = msg }

// Status returns the status code that was written. If WriteHeader
// was never called, this is 200 (the Go default for a successful
// response).
func (l *LoggingResponseWriter) Status() int { return l.status }

// Bytes returns the total number of bytes written to the body.
func (l *LoggingResponseWriter) Bytes() int { return l.bytes }

// Err returns the error string attached via WithError, or "" if
// none was attached.
func (l *LoggingResponseWriter) Err() string { return l.errMsg }

// Hijack implements http.Hijacker. The notifications package and
// websocket.Accept require it; without delegation, the WebSocket
// upgrade path would fail. If the wrapped writer does not implement
// http.Hijacker, this returns an error.
func (l *LoggingResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := l.w.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("underlying ResponseWriter does not implement http.Hijacker")
	}
	return h.Hijack()
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test ./strategy/http/ -run TestLoggingResponseWriter_CapturesStatusAndBytes -v`
Expected: PASS.

- [ ] **Step 5: Add the WithError and default-status tests**

Append to `strategy/http/logging_test.go`:

```go
func TestLoggingResponseWriter_DefaultStatusIs200(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	lw := strategyhttp.NewLoggingResponseWriter(rec)
	_, _ = lw.Write([]byte("body"))

	assert.Equal(t, http.StatusOK, lw.Status())
	assert.Equal(t, 4, lw.Bytes())
	assert.Equal(t, "", lw.Err())
}

func TestLoggingResponseWriter_WithError(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	lw := strategyhttp.NewLoggingResponseWriter(rec)
	lw.WithError("missing Authorization header")
	lw.WithError("overwritten")

	assert.Equal(t, "overwritten", lw.Err())
}
```

- [ ] **Step 6: Run the new tests**

Run: `go test ./strategy/http/ -run TestLoggingResponseWriter -v`
Expected: all three PASS.

- [ ] **Step 7: Add the hijack-delegation test**

Append to `strategy/http/logging_test.go`:

```go
// fakeHijacker records whether Hijack was called and returns a
// canned (nil, nil, nil) response.
type fakeHijacker struct {
	httptest.ResponseRecorder
	hijacked bool
}

func (f *fakeHijacker) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	f.hijacked = true
	return nil, nil, nil
}

func TestLoggingResponseWriter_DelegatesHijack(t *testing.T) {
	t.Parallel()

	fake := &fakeHijacker{}
	lw := strategyhttp.NewLoggingResponseWriter(fake)
	_, _, err := lw.Hijack()

	require.NoError(t, err)
	assert.True(t, fake.hijacked, "Hijack must delegate to the underlying writer")
}

func TestLoggingResponseWriter_HijackFailsWithoutHijacker(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	lw := strategyhttp.NewLoggingResponseWriter(rec)
	_, _, err := lw.Hijack()

	require.Error(t, err)
}
```

Add `"bufio"` and `"net"` to the import list in `logging_test.go` (the
existing `"net/http"` import is enough for the rest).

- [ ] **Step 8: Run the hijack tests**

Run: `go test ./strategy/http/ -run TestLoggingResponseWriter -v`
Expected: all five tests PASS.

- [ ] **Step 9: Commit**

```bash
git add strategy/http/logging.go strategy/http/logging_test.go
git commit -m "feat(http): add LoggingResponseWriter with status, bytes, error capture"
```

---

## Task 2: `RequestLog` middleware — emit the slog record

**Files:**
- Modify: `strategy/http/logging.go` (add `RequestLog`)
- Modify: `strategy/http/logging_test.go` (add middleware tests)

- [ ] **Step 1: Write the failing test for the success case**

Append to `strategy/http/logging_test.go`:

```go
import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http/httptest"
	"testing"

	"strconv"
	"time"
	// existing imports above
)

func parseLog(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
	return rec
}

func TestRequestLog_InfoOnSuccess(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	handler := strategyhttp.RequestLog(logger, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/strategies", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	log := parseLog(t, &buf)
	assert.Equal(t, "request", log["msg"])
	assert.Equal(t, "INFO", log["level"])
	assert.Equal(t, "GET", log["method"])
	assert.Equal(t, "/v1/strategies", log["path"])
	assert.Equal(t, float64(200), log["status"])
	assert.Equal(t, "/v1/strategies", log["route"], "no mux pattern → route is the path")
	assert.GreaterOrEqual(t, log["duration_ms"], float64(0))
	assert.Equal(t, float64(11), log["bytes"])
	_, hasErr := log["err"]
	assert.False(t, hasErr, "err must be omitted when not set")
}
```

Note: add `"strconv"` and `"time"` to the import list at the top of
the file if not already present. `time` is unused here but is used by
later tests in this task; add it now.

- [ ] **Step 2: Run the test to verify it fails to compile**

Run: `go test ./strategy/http/ -run TestRequestLog_InfoOnSuccess -v`
Expected: compile error — `strategyhttp.RequestLog` is not defined.

- [ ] **Step 3: Implement `RequestLog`**

Add to `strategy/http/logging.go` (below the existing wrapper code):

```go
// RequestLog returns a middleware that records one slog record per
// request. The record carries method, route (from r.Pattern, falling
// back to r.URL.Path when the request was not matched by a mux
// pattern), path, status, duration_ms, bytes, user_id, and an
// optional err string attached via LoggingResponseWriter.WithError.
//
// Paths in exempt produce no log record. The default behavior
// (exempt == nil) logs every request.
//
// Log levels:
//   - status < 400: slog.Info
//   - 400 ≤ status < 500: slog.Warn
//   - status ≥ 500: slog.Error
//
// 401 responses from requireAuth are logged at slog.Warn with
// user_id="unauthenticated" and err set to the body the handler
// wrote.
//
// The middleware reads only r.Method, r.URL.Path, r.Pattern, and the
// resolved DORA user id from the request context. It never reads
// the Authorization header or any other request body. The user_id
// helper (doraUserIDFromContext) is defined in auth.go and returns
// the empty string when authentication has not run.
func RequestLog(log *slog.Logger, exempt map[string]struct{}) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if _, ok := exempt[r.URL.Path]; ok {
				next.ServeHTTP(w, r)
				return
			}

			start := time.Now()
			lw := NewLoggingResponseWriter(w)
			next.ServeHTTP(lw, r)

			status := lw.Status()
			level := slog.LevelInfo
			switch {
			case status >= 500:
				level = slog.LevelError
			case status >= 400:
				level = slog.LevelWarn
			}

			userID, _ := doraUserIDFromContext(r.Context())
			if userID == "" {
				userID = "unauthenticated"
			}

			route := r.Pattern
			if route == "" {
				route = r.URL.Path
			}

			attrs := []slog.Attr{
				slog.String("method", r.Method),
				slog.String("route", route),
				slog.String("path", r.URL.Path),
				slog.Int("status", status),
				slog.Int64("duration_ms", time.Since(start).Milliseconds()),
				slog.Int("bytes", lw.Bytes()),
				slog.String("user_id", userID),
			}
			if errMsg := lw.Err(); errMsg != "" {
				attrs = append(attrs, slog.String("err", errMsg))
			}

			log.LogAttrs(r.Context(), level, "request", attrs...)
		})
	}
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test ./strategy/http/ -run TestRequestLog_InfoOnSuccess -v`
Expected: PASS.

- [ ] **Step 5: Add the level-rule tests (4xx and 5xx)**

Append to `strategy/http/logging_test.go`:

```go
func TestRequestLog_WarnOn4xx(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	handler := strategyhttp.RequestLog(logger, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		strategyhttp.WriteError(w, http.StatusBadRequest, "bad input")
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/backtests", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	log := parseLog(t, &buf)
	assert.Equal(t, "WARN", log["level"])
	assert.Equal(t, float64(400), log["status"])
	assert.Equal(t, "bad input", log["err"])
}

func TestRequestLog_ErrorOn5xx(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	handler := strategyhttp.RequestLog(logger, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		strategyhttp.WriteError(w, http.StatusInternalServerError, "boom")
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/backtests", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	log := parseLog(t, &buf)
	assert.Equal(t, "ERROR", log["level"])
	assert.Equal(t, float64(500), log["status"])
	assert.Equal(t, "boom", log["err"])
}
```

- [ ] **Step 6: Run the new tests**

Run: `go test ./strategy/http/ -run 'TestRequestLog_(WarnOn4xx|ErrorOn5xx)' -v`
Expected: PASS.

- [ ] **Step 7: Add the unauthenticated and pattern tests**

Append to `strategy/http/logging_test.go`:

```go
func TestRequestLog_UnauthenticatedWhenUserIDMissing(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	handler := strategyhttp.RequestLog(logger, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"unauthorised"}`))
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	log := parseLog(t, &buf)
	assert.Equal(t, "WARN", log["level"])
	assert.Equal(t, float64(401), log["status"])
	assert.Equal(t, "unauthenticated", log["user_id"])
}

func TestRequestLog_UsesPatternWhenSet(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/backtests/{id}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	wrapped := strategyhttp.RequestLog(logger, nil)(mux)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests/abc", nil)
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	log := parseLog(t, &buf)
	assert.Equal(t, "/v1/backtests/{id}", log["route"])
	assert.Equal(t, "/v1/backtests/abc", log["path"])
}

func TestRequestLog_FallsBackToPathWithoutPattern(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// 404 from a bare http.ServeMux with no matching route.
	mux := http.NewServeMux()
	wrapped := strategyhttp.RequestLog(logger, nil)(mux)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/unknown", nil)
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	log := parseLog(t, &buf)
	assert.Equal(t, "/unknown", log["route"])
	assert.Equal(t, "/unknown", log["path"])
	assert.Equal(t, float64(404), log["status"])
}
```

- [ ] **Step 8: Add the exempt-paths test**

Append:

```go
func TestRequestLog_SkipsExemptPaths(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	exempt := map[string]struct{}{
		"/healthz":  {},
		"/v1/openapi": {},
	}
	handler := strategyhttp.RequestLog(logger, exempt)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	for _, path := range []string{"/healthz", "/v1/openapi"} {
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}

	assert.Empty(t, buf.String(), "exempt paths must not produce a log record")
}
```

- [ ] **Step 9: Add the credentials test**

Append:

```go
func TestRequestLog_NeverLogsCredentials(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	const secret = "secret-abc-123"
	handler := strategyhttp.RequestLog(logger, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"unauthorised"}`))
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests", nil)
	req.Header.Set("Authorization", "ApiKey "+secret)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	out := buf.String()
	// The secret must not appear in any form. The check is
	// intentionally strict: any substring of the secret would
	// indicate the key was logged, even partially.
	assert.NotContains(t, out, secret)
	assert.NotContains(t, out, "abc-123")
	assert.NotContains(t, out, "abc")
	assert.NotContains(t, out, "123")
	assert.NotContains(t, out, "ApiKey")
	assert.NotContains(t, out, "Bearer")
}

func TestRequestLog_NoErrFieldWhenAbsent(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	handler := strategyhttp.RequestLog(logger, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/x", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	log := parseLog(t, &buf)
	_, hasErr := log["err"]
	assert.False(t, hasErr)
}
```

- [ ] **Step 10: Run the full middleware test suite**

Run: `go test ./strategy/http/ -run 'TestRequestLog_|TestLoggingResponseWriter_' -v`
Expected: all tests PASS.

- [ ] **Step 11: Commit**

```bash
git add strategy/http/logging.go strategy/http/logging_test.go
git commit -m "feat(http): add RequestLog middleware with level rules and exempt paths"
```

---

## Task 3: `writeError` attaches the error string to the wrapper

**Files:**
- Modify: `strategy/http/handler.go` (add the `WithError` call in `writeError`)

- [ ] **Step 1: Read `writeError` to confirm its current shape**

Open `strategy/http/handler.go:2313-2315`:

```go
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, ErrorResponse{Error: message})
}
```

- [ ] **Step 2: Update `writeError` to call `WithError` on the wrapper**

Replace the function body with:

```go
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, ErrorResponse{Error: message})
	if lw, ok := w.(*LoggingResponseWriter); ok {
		lw.WithError(message)
	}
}
```

The pointer-type assertion is the contract: every request that
reaches `writeError` has flowed through `RequestLog` in
`cmd/strategy-server/main.go`, so `w` is a `*LoggingResponseWriter`.
The `ok` check is defensive — if a future caller bypasses the
middleware (e.g. a test handler), the `err` field is simply not
attached for that request; the request itself is not logged at all
in that case.

- [ ] **Step 3: Run the existing strategy/http tests to make sure nothing broke**

Run: `go test ./strategy/http/ -v`
Expected: all tests PASS. The existing handler tests do not assert
on the log output, so adding `WithError` should be transparent.

- [ ] **Step 4: Add a focused test that asserts the wired-up behaviour end-to-end**

The simplest way to exercise this is to add a test that runs the
middleware around a handler that calls `writeError`, and asserts the
log record carries the message. Append to
`strategy/http/logging_test.go`:

```go
// TestRequestLog_AttachesErrorFromWriteError ensures writeError
// stashes the message on the wrapper so the middleware can include
// it in the log record. This covers the wiring between
// writeError's WithError call and the middleware's Err() read.
func TestRequestLog_AttachesErrorFromWriteError(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	handler := strategyhttp.RequestLog(logger, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		strategyhttp.WriteError(w, http.StatusBadRequest, "config: invalid field")
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/backtests", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	log := parseLog(t, &buf)
	assert.Equal(t, "WARN", log["level"])
	assert.Equal(t, "config: invalid field", log["err"])
}
```

- [ ] **Step 5: Run the new test**

Run: `go test ./strategy/http/ -run TestRequestLog_AttachesErrorFromWriteError -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add strategy/http/handler.go strategy/http/logging_test.go
git commit -m "feat(http): attach error message to logging wrapper from writeError"
```

---

## Task 4: Wire the middleware in `cmd/strategy-server/main.go`

**Files:**
- Modify: `cmd/strategy-server/main.go` (import `strategy/http` if not already; wrap `wrappedHandler`)

- [ ] **Step 1: Check the existing import block**

In `cmd/strategy-server/main.go`, the `strategyhttp` alias should
already exist (used to construct the handler). Confirm by reading
the import block at the top of the file. If only
`strategyhttp "bond-trading-strategies/strategy/http"` is present,
nothing to add; if a different alias is used, use that.

- [ ] **Step 2: Wrap `wrappedHandler` after the CORS wrapping**

The current code at `cmd/strategy-server/main.go:210-214`:

```go
wrappedHandler := rl.Middleware(handlerImpl)

if *corsAllowedOrigins != "" {
    wrappedHandler = cors.New(*corsAllowedOrigins)(wrappedHandler)
}
```

After (the `RequestLog` wrap goes between CORS and the
`notificationsRouter`):

```go
wrappedHandler := rl.Middleware(handlerImpl)

if *corsAllowedOrigins != "" {
    wrappedHandler = cors.New(*corsAllowedOrigins)(wrappedHandler)
}

exemptLogPaths := map[string]struct{}{
    "/healthz":   {},
    "/v1/openapi": {},
}
wrappedHandler = strategyhttp.RequestLog(slog.Default(), exemptLogPaths)(wrappedHandler)
```

`strategyhttp` is the existing alias (the file already imports
`strategyhttp "bond-trading-strategies/strategy/http"` for the
handler construction). `slog` is already imported.

- [ ] **Step 3: Build to confirm it compiles**

Run: `go build ./cmd/strategy-server/...`
Expected: clean build, no errors.

- [ ] **Step 4: Run the strategy/http tests one more time**

Run: `go test ./strategy/http/...`
Expected: all tests PASS. The wiring is in `main.go`, not in
`strategy/http`, so this is a sanity check that the change in
`handler.go` (Task 3) is still compatible with the rest of the
package.

- [ ] **Step 5: Commit**

```bash
git add cmd/strategy-server/main.go
git commit -m "feat(strategy-server): wire RequestLog middleware around handler chain"
```

---

## Task 5: Verification and final cleanup

**Files:** none modified

- [ ] **Step 1: Run the full test suite**

Run: `go test ./...`
Expected: all tests PASS, no panics, no race detector failures (the
project's test command is `go test ./...`; the race detector is
opt-in via `go test -race ./...`).

- [ ] **Step 2: Run the race detector on the strategy/http package**

Run: `go test -race ./strategy/http/...`
Expected: PASS. The middleware uses no shared state, so a race
failure here would indicate a bug in the wrapper.

- [ ] **Step 3: Run golangci-lint on the changed files**

Run: `golangci-lint run ./strategy/http/... ./cmd/strategy-server/...`
Expected: 0 issues. The project's lint config enforces `gochecknoglobals`,
`mnd`, `errorlint`, and others — make sure no new violations
appear from the new files.

- [ ] **Step 4: Run pre-commit hooks on the changed files**

Run: `pre-commit run --files strategy/http/logging.go strategy/http/logging_test.go strategy/http/handler.go cmd/strategy-server/main.go`
Expected: all hooks pass (lint, go-imports, vet, mod-tidy, tests
including race).

- [ ] **Step 5: Manual smoke test (optional but recommended)**

If a local PostgreSQL is available, run the server and exercise the
endpoints:

```bash
make start-strategy-server &
sleep 2
curl -i http://localhost:8081/healthz        # no log line for /healthz
curl -i http://localhost:8081/v1/strategies  # one slog.Warn line: user_id="unauthenticated", err="missing Authorization header"
curl -i -H 'Authorization: ApiKey <key>' http://localhost:8081/v1/strategies  # one slog.Info line: user_id=<resolved>
curl -i -H 'Authorization: ApiKey secret-abc-123' http://localhost:8081/v1/strategies  # log line does not contain "secret-abc-123"
```

Verify the server logs (stdout / journal) show the expected log
records and that the literal `secret-abc-123` does not appear in
any of them.

- [ ] **Step 6: Final commit (if any cleanup was needed)**

If step 5 surfaced a bug, fix it and commit. Otherwise, no commit.

```bash
git add -A
git commit -m "fix: address smoke-test findings"  # only if needed
```

---

## Self-Review

**Spec coverage:**
- Request log middleware (`strategy/http`) → Task 1 + Task 2
- `LoggingResponseWriter` with hijack delegation → Task 1
- `writeError` calls `WithError` → Task 3
- Wire in `cmd/strategy-server/main.go` → Task 4
- Exempt paths → Task 2 (`TestRequestLog_SkipsExemptPaths`) + Task 4 (exempt map in `main.go`)
- Unit tests in `strategy/http/logging_test.go` → Tasks 1, 2, 3
- 11 test cases from the spec:
  1. `TestRequestLog_InfoOnSuccess` → Task 2 step 1
  2. `TestRequestLog_WarnOn4xx` → Task 2 step 5
  3. `TestRequestLog_ErrorOn5xx` → Task 2 step 5
  4. `TestRequestLog_UnauthenticatedOn401` → Task 2 step 7
  5. `TestRequestLog_UnauthenticatedOnNonAuthPath` → covered by `TestRequestLog_UnauthenticatedWhenUserIDMissing` in Task 2 step 7 (uses an empty context, which is what a non-auth path produces)
  6. `TestRequestLog_UsesPattern` → Task 2 step 7
  7. `TestRequestLog_FallsBackToPath` → Task 2 step 7
  8. `TestRequestLog_SkipsExemptPaths` → Task 2 step 8
  9. `TestRequestLog_PreservesHijacker` → covered by `TestLoggingResponseWriter_DelegatesHijack` in Task 1 step 7
  10. `TestRequestLog_NeverLogsCredentials` → Task 2 step 9
  11. `TestRequestLog_NoErrFieldWhenAbsent` → Task 2 step 9

**Placeholder scan:** none. Every step has concrete code or
commands with expected output.

**Type consistency:** `LoggingResponseWriter` is constructed via
`NewLoggingResponseWriter` and used as `*LoggingResponseWriter`
throughout. `RequestLog` takes `*slog.Logger` and a
`map[string]struct{}` exempt set — used consistently in tests and in
`main.go`. `doraUserIDFromContext` is the existing helper from
`auth.go` (not redeclared). `WriteError` is the existing exported
function from `handler.go` (not redeclared).

**Risks identified during review:**
- The `doraUserIDFromContext` helper is package-internal. The
  middleware in `logging.go` is in the same package, so the call
  is valid.
- `WriteError` is the existing public function (capital W). It is
  defined in `handler.go` and used by handlers throughout the
  package. Tests use the same name.
- `strategyhttp` is the existing import alias in
  `cmd/strategy-server/main.go`. Confirmed by reading the import
  block.
- The exempt set in `main.go` uses `"/v1/openapi"` (matches the
  existing route registration in `NewHandler`).
