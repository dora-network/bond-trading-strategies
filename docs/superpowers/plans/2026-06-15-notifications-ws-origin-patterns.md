# WebSocket `OriginPatterns` for cross-origin upgrades Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Configure `coder/websocket.AcceptOptions.OriginPatterns` (or `InsecureSkipVerify` for `*`) from the existing `--cors-allowed-origins` flag, so cross-origin WebSocket upgrades are no longer rejected by the library's CSRF check.

**Architecture:** Add a `cors.OriginPatterns(origins string) (patterns []string, allowAll bool)` function to the `cors` package. Add a `WithAcceptOptions(websocket.AcceptOptions)` option to `notifications.Handler`. In `cmd/strategy-server/main.go`, call `cors.OriginPatterns` once with the configured value and thread the result into the WS handler. Single source of truth — the same `--cors-allowed-origins` flag governs REST CORS, WS CORS, and the WS CSRF check.

**Tech Stack:** Go 1.26, `github.com/coder/websocket` (existing), `net/http` (existing).

**Spec:** `docs/superpowers/specs/2026-06-15-notifications-ws-origin-patterns-design.md`

**Note on commits:** The user has asked that changes be **staged but not committed** for review before committing. Each task ends with `git add` (staging) instead of `git commit`. The final task lists the staged files for review.

---

## File map

**Modified**
- `cors/cors.go` — add `OriginPatterns(origins string) (patterns []string, allowAll bool)`
- `cors/cors_test.go` — add unit tests for the new function
- `notifications/handler.go` — add `WithAcceptOptions` option and `acceptOptions` field; use it in `ServeHTTP`
- `notifications/handler_test.go` — add a test that confirms the option is wired
- `cmd/strategy-server/main.go` — call `cors.OriginPatterns` and pass the result into the WS handler

**No new files.**

---

## Task 1: Add `cors.OriginPatterns` with failing tests

**Files:**
- Modify: `cors/cors_test.go` (append new tests)
- Modify: `cors/cors.go` (add new function)

TDD: write the unit tests first, confirm they fail (because the function doesn't exist), then write the implementation.

- [ ] **Step 1: Append tests to `cors/cors_test.go`**

Open `cors/cors_test.go` and append the following tests to the end of the file. They use the same package, same `testing` import, and the same test style as the existing nine `TestNew_*` tests.

```go
func TestOriginPatterns_Star_AllowAllNoPatterns(t *testing.T) {
	patterns, allowAll := cors.OriginPatterns("*")
	if !allowAll {
		t.Error("expected allowAll=true for *")
	}
	if len(patterns) != 0 {
		t.Errorf("expected no patterns for *, got %v", patterns)
	}
}

func TestOriginPatterns_EmptyInput_NoPatternsNotAllowed(t *testing.T) {
	patterns, allowAll := cors.OriginPatterns("")
	if allowAll {
		t.Error("expected allowAll=false for empty input")
	}
	if len(patterns) != 0 {
		t.Errorf("expected no patterns for empty input, got %v", patterns)
	}
}

func TestOriginPatterns_SingleURL_StripsScheme(t *testing.T) {
	patterns, allowAll := cors.OriginPatterns("https://app.example.com")
	if allowAll {
		t.Error("expected allowAll=false for explicit URL")
	}
	want := []string{"app.example.com"}
	if !equalStrings(patterns, want) {
		t.Errorf("patterns = %v, want %v", patterns, want)
	}
}

func TestOriginPatterns_URLWithPort_PreservesPort(t *testing.T) {
	patterns, _ := cors.OriginPatterns("http://app.example.com:8080")
	want := []string{"app.example.com:8080"}
	if !equalStrings(patterns, want) {
		t.Errorf("patterns = %v, want %v", patterns, want)
	}
}

func TestOriginPatterns_GlobPattern_KeepsWildcard(t *testing.T) {
	patterns, allowAll := cors.OriginPatterns("*.example.com")
	if allowAll {
		t.Error("expected allowAll=false for glob")
	}
	want := []string{"*.example.com"}
	if !equalStrings(patterns, want) {
		t.Errorf("patterns = %v, want %v", patterns, want)
	}
}

func TestOriginPatterns_URLWithGlob_StripsSchemeKeepsWildcard(t *testing.T) {
	patterns, _ := cors.OriginPatterns("https://*.example.com")
	want := []string{"*.example.com"}
	if !equalStrings(patterns, want) {
		t.Errorf("patterns = %v, want %v", patterns, want)
	}
}

func TestOriginPatterns_MultipleEntries_AllInOrder(t *testing.T) {
	patterns, _ := cors.OriginPatterns("https://a.com,https://b.com,*.c.com")
	want := []string{"a.com", "b.com", "*.c.com"}
	if !equalStrings(patterns, want) {
		t.Errorf("patterns = %v, want %v", patterns, want)
	}
}

func TestOriginPatterns_WhitespaceAndEmptyEntries_Skipped(t *testing.T) {
	patterns, _ := cors.OriginPatterns(" https://a.com , , https://b.com ")
	want := []string{"a.com", "b.com"}
	if !equalStrings(patterns, want) {
		t.Errorf("patterns = %v, want %v", patterns, want)
	}
}

func TestOriginPatterns_StarMixedWithEntries_AllowAllTrue(t *testing.T) {
	// The library ignores patterns when InsecureSkipVerify is set, but
	// this function does not collapse them — the caller is responsible
	// for choosing which to use.
	patterns, allowAll := cors.OriginPatterns("https://a.com,*")
	if !allowAll {
		t.Error("expected allowAll=true when * is present")
	}
	if len(patterns) == 0 {
		t.Error("expected patterns to be populated (caller decides which to use)")
	}
}
```

Add the helper function at the bottom of the test file (after the last test):

```go
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `go test -run TestOriginPatterns ./cors/...`
Expected: compile error — `cors.OriginPatterns` is not defined.

- [ ] **Step 3: Add the `OriginPatterns` function to `cors/cors.go`**

Open `cors/cors.go`. The current file is 76 lines. Add the new function at the end (after the closing `}` of `New`). The new code:

```go
// OriginPatterns parses the same input format as New and returns the
// values needed to configure coder/websocket.AcceptOptions. OriginPatterns
// lists the host patterns that the WS library should match against
// the request Origin. allowAll is true when the input contained a
// bare "*"; in that case the caller should set
// AcceptOptions.InsecureSkipVerify = true (and leave OriginPatterns
// empty) — the library's documentation recommends this over a "*"
// pattern entry because it is more visible at the call site.
//
// Entries are stripped of their URL scheme when present (e.g.
// "https://app.example.com" becomes "app.example.com"); the library
// re-adds the scheme before matching. Glob patterns (entries
// containing "*") are passed through unchanged after scheme stripping.
// The library uses path.Match for pattern matching, so "*" matches
// any sequence of non-"/" characters — origin hosts never contain
// "/", so this is the expected behaviour.
func OriginPatterns(origins string) (patterns []string, allowAll bool) {
	for _, o := range strings.Split(origins, ",") {
		o = strings.TrimSpace(o)
		if o == "" {
			continue
		}
		if o == "*" {
			allowAll = true
			continue
		}
		// Strip scheme if present: "https://app.example.com" -> "app.example.com".
		// The library re-adds the scheme before pattern matching.
		if i := strings.Index(o, "://"); i >= 0 {
			o = o[i+3:]
		}
		patterns = append(patterns, o)
	}
	return patterns, allowAll
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./cors/...`
Expected: all tests pass (the existing 9 `TestNew_*` plus the 9 new `TestOriginPatterns_*`).

- [ ] **Step 5: Run the linter**

Run: `golangci-lint run ./cors/...`
Expected: 0 issues.

- [ ] **Step 6: Stage the change (do NOT commit)**

```bash
git add cors/cors.go cors/cors_test.go
```

---

## Task 2: Add `WithAcceptOptions` to `notifications.Handler`

**Files:**
- Modify: `notifications/handler.go` (add field, option, and use in `ServeHTTP`)

- [ ] **Step 1: Read `notifications/handler.go` to confirm the current state**

The current `Handler` struct (lines 19-23) and the option pattern (lines 27-37) are short. The `Accept` call is at line 58: `websocket.Accept(w, r, &websocket.AcceptOptions{})`.

- [ ] **Step 2: Add the `acceptOptions` field to the `Handler` struct**

Replace the struct (lines 19-23):

OLD:
```go
type Handler struct {
	notifier    Notifier
	resolveUser ResolveUserID
	log         *slog.Logger
}
```

NEW:
```go
type Handler struct {
	notifier      Notifier
	resolveUser   ResolveUserID
	log           *slog.Logger
	acceptOptions websocket.AcceptOptions
}
```

- [ ] **Step 3: Add the `WithAcceptOptions` option after the existing `WithHandlerLogger`**

The current options (lines 35-37) are:

```go
type HandlerOption func(*Handler)

func WithHandlerLogger(l *slog.Logger) HandlerOption { return func(h *Handler) { h.log = l } }
```

Append the new option immediately after:

```go
// WithAcceptOptions configures the WebSocket accept options used
// when accepting the connection. Callers that need to allow
// cross-origin WebSocket upgrades (e.g. browser clients at a
// different origin) should set OriginPatterns or InsecureSkipVerify
// here. An empty AcceptOptions (the default) rejects all
// cross-origin upgrades via the coder/websocket library's CSRF check.
func WithAcceptOptions(opts websocket.AcceptOptions) HandlerOption {
	return func(h *Handler) { h.acceptOptions = opts }
}
```

- [ ] **Step 4: Update `ServeHTTP` to use the stored options**

Replace line 58:

OLD:
```go
conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{})
```

NEW:
```go
conn, err := websocket.Accept(w, r, &h.acceptOptions)
```

- [ ] **Step 5: Run `gofmt -w notifications/handler.go` to canonicalize**

- [ ] **Step 6: Build and test the notifications package**

Run: `go build ./... && go test ./notifications/...`
Expected: builds clean, all existing tests pass (the default zero-value `acceptOptions` matches the previous behaviour byte-for-byte).

- [ ] **Step 7: Stage the change (do NOT commit)**

```bash
git add notifications/handler.go
```

---

## Task 3: Add a handler test that confirms `WithAcceptOptions` is wired

**Files:**
- Modify: `notifications/handler_test.go` (append a new test)

- [ ] **Step 1: Append the new test to `notifications/handler_test.go`**

The test dials a WebSocket from an allowed origin and confirms the upgrade succeeds. It uses the same `httptest.NewServer` + `websocket.Dial` pattern as the existing tests in the file.

```go
func TestHandler_AcceptsConfiguredOriginPattern(t *testing.T) {
	bus := notifications.NewBus(&captureLog{}, notifications.NewHub())
	h := notifications.NewHandler(
		bus,
		func(_ context.Context) (string, error) { return "user-1", nil },
		notifications.WithAcceptOptions(websocket.AcceptOptions{
			OriginPatterns: []string{"app.example.com"},
		}),
	)
	srv := httptest.NewServer(h)
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	wsURL := "ws" + strings.TrimPrefix(u.String(), "http") + "/v1/notifications/ws?x-api-key=test-key"

	header := http.Header{}
	header.Set("Origin", "https://app.example.com")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	//nolint:bodyclose // coder/websocket docs: caller never closes resp.Body
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{HTTPHeader: header})
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

func TestHandler_AcceptsInsecureSkipVerify(t *testing.T) {
	bus := notifications.NewBus(&captureLog{}, notifications.NewHub())
	h := notifications.NewHandler(
		bus,
		func(_ context.Context) (string, error) { return "user-1", nil },
		notifications.WithAcceptOptions(websocket.AcceptOptions{
			InsecureSkipVerify: true,
		}),
	)
	srv := httptest.NewServer(h)
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	wsURL := "ws" + strings.TrimPrefix(u.String(), "http") + "/v1/notifications/ws?x-api-key=test-key"

	header := http.Header{}
	header.Set("Origin", "https://anywhere.example.com")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	//nolint:bodyclose // coder/websocket docs: caller never closes resp.Body
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{HTTPHeader: header})
	require.NoError(t, err)
	defer conn.Close(websocket.StatusNormalClosure, "")
}
```

- [ ] **Step 2: Run the new tests**

Run: `go test -run 'TestHandler_AcceptsConfiguredOriginPattern|TestHandler_AcceptsInsecureSkipVerify' ./notifications/...`
Expected: both pass.

- [ ] **Step 3: Run the full notifications test suite**

Run: `go test ./notifications/...`
Expected: all tests pass.

- [ ] **Step 4: Stage the test (do NOT commit)**

```bash
git add notifications/handler_test.go
```

---

## Task 4: Wire `cors.OriginPatterns` into `cmd/strategy-server/main.go`

**Files:**
- Modify: `cmd/strategy-server/main.go` (compute `AcceptOptions` from the flag, pass it to the handler)

- [ ] **Step 1: Read the current WS handler wiring**

The relevant code is around lines 214-230 of `main.go` (the `var wsHandler http.Handler = notifications.NewHandler(...)` block). The imports already include `github.com/coder/websocket` (the `websocket.Accept` is referenced via the `coder/websocket` package — verify the import is in place).

Run: `grep -n "coder/websocket" cmd/strategy-server/main.go`
Expected: one match. If absent, add the import.

- [ ] **Step 2: Compute the accept options once and pass them in**

Replace the existing WS handler block (lines 214-230):

OLD:
```go
	if notifier != nil {
		wsSubMux := http.NewServeMux()
		var wsHandler http.Handler = notifications.NewHandler(
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

NEW:
```go
	if notifier != nil {
		wsSubMux := http.NewServeMux()
		wsPatterns, wsAllowAll := cors.OriginPatterns(*corsAllowedOrigins)
		var wsHandler http.Handler = notifications.NewHandler(
			notifier,
			func(ctx context.Context) (string, error) {
				if _, ok := authctx.AuthInfoFromContext(ctx); !ok {
					return "", errors.New("missing auth info in context")
				}
				client := strategyhttp.NewDORAClient()
				return client.GetUserID(ctx)
			},
			notifications.WithHandlerLogger(log),
			notifications.WithAcceptOptions(websocket.AcceptOptions{
				OriginPatterns:     wsPatterns,
				InsecureSkipVerify: wsAllowAll,
			}),
		)
		if *corsAllowedOrigins != "" {
			wsHandler = cors.New(*corsAllowedOrigins)(wsHandler)
		}
		wsSubMux.Handle("/v1/notifications/ws", wsHandler)
		wrappedHandler = notificationsRouter{fallback: wrappedHandler, sub: wsSubMux}
	}
```

When `*corsAllowedOrigins` is empty (the default), `cors.OriginPatterns("")` returns `(nil, false)`, so the `AcceptOptions` is the zero value — same as before this change. Same-host upgrades still work because the library always authorizes the request's own host.

- [ ] **Step 3: Run `gofmt -w cmd/strategy-server/main.go` to canonicalize**

- [ ] **Step 4: Build and test**

Run: `go build ./... && go test ./...`
Expected: clean build, all 16 packages pass.

- [ ] **Step 5: Stage the change (do NOT commit)**

```bash
git add cmd/strategy-server/main.go
```

---

## Task 5: Full verification

- [ ] **Step 1: Build the full module**

Run: `go build ./...`
Expected: clean.

- [ ] **Step 2: Run the full test suite uncached**

Run: `go clean -testcache && go test ./...`
Expected: all packages pass.

- [ ] **Step 3: Run the linter**

Run: `golangci-lint run --timeout 5m ./cors/... ./notifications/... ./cmd/strategy-server/...`
Expected: 0 issues.

- [ ] **Step 4: Run pre-commit on the staged files**

Run: `pre-commit run --files cors/cors.go cors/cors_test.go notifications/handler.go notifications/handler_test.go cmd/strategy-server/main.go`
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

- `cors/cors.go` — added `OriginPatterns` function
- `cors/cors_test.go` — added 9 unit tests + 1 helper
- `notifications/handler.go` — added `WithAcceptOptions` option and `acceptOptions` field; use stored options in `ServeHTTP`
- `notifications/handler_test.go` — added 2 new tests
- `cmd/strategy-server/main.go` — call `cors.OriginPatterns` and pass the result into the WS handler

No commits will be made. The user will review the diff and commit on their own.
