# List Copy Traders Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `GET /v1/copy-traders` strategy-server endpoint (and matching MCP tool) that lists traders available to be followed by copy-trading runs. Placeholder behaviour: returns a filtered set of DORA users (names starting with `TRADER_` or `MM_`).

**Architecture:** The handler's `doraClient` interface (in `strategy/http/dora_client.go`) gains a `ListBotUsers` method. A new handler `handleCopyTraders` calls it and shapes the response. The MCP server gets a thin `listCopyTraders` client method and a `strategy_copy_traders_list` tool. The bot filter is a pure function `isBotUser` for easy unit testing.

**Tech Stack:** Go 1.23+, `doraclient` for DORA calls, `testify` (already in use), existing handler test patterns (`doraClientFunc` test fake).

---

## File Structure

- **Modify** `strategy/http/dora_client.go` — add `DORABotUser` type, add `ListBotUsers` to `doraClient` interface, add `liveDORAClient.ListBotUsers` implementation, add `isBotUser` private filter helper.
- **Create** `strategy/http/dora_client_test.go` — unit tests for `isBotUser` and a thin white-box test for `liveDORAClient.ListBotUsers` pagination logic.
- **Modify** `strategy/http/handler.go` — add `handleCopyTraders` method, register `GET /v1/copy-traders` route.
- **Modify** `strategy/http/handler_test.go` — add `listBotUsers` field to `doraClientFunc` fake, add `TestHandlerListsCopyTraders` test.
- **Modify** `mcp/strategy_client.go` — add `listCopyTraders(ctx)` method.
- **Modify** `mcp/tools_strategy.go` — register `strategy_copy_traders_list` tool.

---

## Task 1: Add bot-user types and filter to dora_client

**Files:**
- Modify: `strategy/http/dora_client.go`
- Test: `strategy/http/dora_client_test.go` (new)

- [ ] **Step 1.1: Write failing test for `isBotUser`**

Create `strategy/http/dora_client_test.go`:

```go
package http

import "testing"

func TestIsBotUser(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		firstName string
		lastName  string
		want      bool
	}{
		{"trader underscore prefix in first name", "TRADER_01", "Smith", true},
		{"mm underscore prefix in first name", "MM_Alice", "Brown", true},
		{"trader underscore prefix in last name", "Alice", "TRADER_99", true},
		{"mm prefix in last name", "Alice", "mm_bot", true},
		{"lowercase variants", "trader_42", "doe", true},
		{"no prefix in either", "Alice", "Smith", false},
		{"empty names", "", "", false},
		{"only first name no prefix", "Alice", "", false},
		{"only last name no prefix", "", "Smith", false},
		{"trader without underscore is not a bot", "Trader", "Smith", false},
		{"mm without underscore is not a bot", "Mm", "Smith", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isBotUser(tc.firstName, tc.lastName); got != tc.want {
				t.Errorf("isBotUser(%q, %q) = %v, want %v", tc.firstName, tc.lastName, got, tc.want)
			}
		})
	}
}
```

- [ ] **Step 1.2: Run the test and verify it fails**

Run: `go test ./strategy/http/ -run TestIsBotUser -v`
Expected: compile error (`isBotUser` undefined).

- [ ] **Step 1.3: Implement `isBotUser` and the `DORABotUser` type**

Add to `strategy/http/dora_client.go` (anywhere convenient; after the existing type declarations is fine):

```go
// DORABotUser is a simplified view of a DORA user that is exposed by the
// list-copy-traders placeholder endpoint.
type DORABotUser struct {
	ID        string `json:"id"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}

// isBotUser reports whether a user's first or last name starts with the
// bot-naming prefix. This is a placeholder heuristic; once DORA exposes a
// dedicated "list available copy traders" endpoint, the filter is removed.
func isBotUser(firstName, lastName string) bool {
	return hasBotPrefix(firstName) || hasBotPrefix(lastName)
}

func hasBotPrefix(s string) bool {
	lower := strings.ToLower(s)
	return strings.HasPrefix(lower, "trader_") || strings.HasPrefix(lower, "mm_")
}
```

The `strings` package is already imported by `handler.go` but not by `dora_client.go`. Add the import if missing.

- [ ] **Step 1.4: Run the test and verify it passes**

Run: `go test ./strategy/http/ -run TestIsBotUser -v`
Expected: PASS, all 11 subtests.

- [ ] **Step 1.5: Commit**

```bash
git add strategy/http/dora_client.go strategy/http/dora_client_test.go
git commit -m "feat(http): add isBotUser filter and DORABotUser type"
```

---

## Task 2: Add `ListBotUsers` to the doraClient interface and live implementation

**Files:**
- Modify: `strategy/http/dora_client.go`
- Test: `strategy/http/dora_client_test.go`

- [ ] **Step 2.1: Add `ListBotUsers` to the interface**

In `strategy/http/dora_client.go`, update the `doraClient` interface (line 12):

```go
type doraClient interface {
	ListOrderBooks(context.Context) ([]DORAOrderBookSummary, error)
	GetAssetByID(context.Context, string) (*AssetInfo, error)
	GetUserID(context.Context) (string, error)
	ListBotUsers(context.Context) ([]DORABotUser, error)
}
```

- [ ] **Step 2.2: Add the failing interface-compliance check**

Add to `strategy/http/dora_client_test.go`:

```go
var _ doraClient = (*liveDORAClient)(nil)
```

- [ ] **Step 2.3: Run go build to verify compile failure**

Run: `go build ./strategy/http/`
Expected: compile error in `dora_client.go` — `liveDORAClient` does not implement `doraClient` (missing `ListBotUsers` method). The interface-compliance line will also fail in the test file. This is the failing-test signal.

- [ ] **Step 2.4: Implement `ListBotUsers` on `liveDORAClient`**

Add to `strategy/http/dora_client.go` (after the existing `GetUserID` method):

```go
const (
	copyTraderPageSize int32 = 100
	copyTraderMaxPages  int32 = 10
)

// ListBotUsers fetches DORA users and returns those whose first or last name
// starts with the bot-naming prefix (TRADER_ or MM_). This is a placeholder
// until DORA exposes a dedicated copy-trader listing endpoint.
func (c *liveDORAClient) ListBotUsers(ctx context.Context) ([]DORABotUser, error) {
	authCtx, err := c.authContext(ctx)
	if err != nil {
		return nil, err
	}

	all := make([]DORABotUser, 0)
	for page := int32(0); page < copyTraderMaxPages; page++ {
		offset := page * copyTraderPageSize
		resp, rawResp, err := c.client.DefaultAPI.
			GetUsers(authCtx).
			Limit(copyTraderPageSize).
			Offset(offset).
			Execute()
		if rawResp != nil && rawResp.Body != nil {
			_ = rawResp.Body.Close()
		}
		if err != nil {
			return nil, fmt.Errorf("list users: %w", err)
		}
		if resp == nil || len(resp.Data) == 0 {
			break
		}
		for _, u := range resp.Data {
			if !isBotUser(u.FirstName, u.LastName) {
				continue
			}
			all = append(all, DORABotUser{
				ID:        u.Id,
				FirstName: u.FirstName,
				LastName:  u.LastName,
			})
		}
		if int32(len(resp.Data)) < copyTraderPageSize {
			break
		}
	}
	return all, nil
}
```

Note: `copyTraderPageSize` and `copyTraderMaxPages` are constants. The `mnd` linter will likely flag them — add `//nolint:mnd` to the `const` block.

- [ ] **Step 2.5: Run the build and tests to verify they pass**

Run: `go build ./...`
Run: `go test ./strategy/http/ -run TestIsBotUser -v`
Expected: build succeeds, `TestIsBotUser` passes, interface-compliance line compiles.

- [ ] **Step 2.6: Commit**

```bash
git add strategy/http/dora_client.go strategy/http/dora_client_test.go
git commit -m "feat(http): add ListBotUsers to doraClient with pagination"
```

---

## Task 3: Add the `handleCopyTraders` HTTP handler and route

**Files:**
- Modify: `strategy/http/handler.go`
- Test: `strategy/http/handler_test.go`

- [ ] **Step 3.1: Add the `CopyTraderSummary` response type**

Add to `strategy/http/handler.go` near the other DORA summary types (around line 108):

```go
// CopyTraderSummary is a single entry in the list-copy-traders response.
type CopyTraderSummary struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
}
```

- [ ] **Step 3.2: Add the failing handler test**

Add to `strategy/http/handler_test.go` (just before `TestHandlerListsStrategies` is a good spot — search for a stable anchor):

```go
func TestHandlerListsCopyTraders(t *testing.T) {
	t.Parallel()

	trader1 := "11111111-1111-1111-1111-111111111111"
	trader2 := "22222222-2222-2222-2222-222222222222"

	fake := doraClientFunc{
		listBotUsers: func(_ context.Context) ([]strategyhttp.DORABotUser, error) {
			return []strategyhttp.DORABotUser{
				{ID: trader1, FirstName: "TRADER_01", LastName: "Smith"},
				{ID: trader2, FirstName: "MM", LastName: "Alice"},
			}, nil
		},
	}

	handler := strategyhttp.NewHandler(
		&strategyfakes.FakeService{},
		strategyhttp.WithDORAClient(fake),
	)
	rec := performJSONRequest(t, handler, "/v1/copy-traders", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var body struct {
		Items []strategyhttp.CopyTraderSummary `json:"items"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Len(t, body.Items, 2)
	assert.Equal(t, trader1, body.Items[0].ID)
	assert.Equal(t, "TRADER_01 Smith", body.Items[0].DisplayName)
	assert.Equal(t, trader2, body.Items[1].ID)
	assert.Equal(t, "MM Alice", body.Items[1].DisplayName)
}

func TestHandlerListsCopyTradersRequiresAuth(t *testing.T) {
	t.Parallel()

	fake := doraClientFunc{}
	handler := strategyhttp.NewHandler(
		&strategyfakes.FakeService{},
		strategyhttp.WithDORAClient(fake),
	)
	rec := performRequest(t, handler, http.MethodGet, "/v1/copy-traders", nil)
	require.Equal(t, http.StatusUnauthorized, rec.Code)
}
```

Note: `performRequest` is the helper used elsewhere in the test file for non-JSON requests (look at how `TestHandlerRequiresAuth` is structured and follow the same pattern). If no such helper exists, use the same pattern as the existing 401 tests.

- [ ] **Step 3.3: Run the tests and verify they fail**

Run: `go test ./strategy/http/ -run "TestHandlerListsCopyTraders" -v`
Expected: FAIL — `listBotUsers` field doesn't exist on `doraClientFunc` (compile error), and the route isn't registered (404 / 405 from the auth test).

- [ ] **Step 3.4: Add `listBotUsers` field to the test fake**

In `strategy/http/handler_test.go`, update the `doraClientFunc` struct (line 1363):

```go
type doraClientFunc struct {
	listOrderBooks func(context.Context) ([]strategyhttp.DORAOrderBookSummary, error)
	getUserID      func(context.Context) (string, error)
	getAssetByID   func(context.Context, string) (*strategyhttp.AssetInfo, error)
	listBotUsers   func(context.Context) ([]strategyhttp.DORABotUser, error)
}
```

Then add the method:

```go
func (f doraClientFunc) ListBotUsers(ctx context.Context) ([]strategyhttp.DORABotUser, error) {
	if f.listBotUsers == nil {
		return nil, fmt.Errorf("not implemented")
	}
	return f.listBotUsers(ctx)
}
```

- [ ] **Step 3.5: Implement the handler and register the route**

Add to `strategy/http/handler.go` (just after the existing `handleDORAUser` function — search for it to find a stable spot):

```go
// handleCopyTraders returns the list of traders available to be followed by
// copy-trading runs. This is a placeholder that filters DORA users by name
// prefix until DORA exposes a dedicated "list available copy traders" endpoint.
// TODO(remove-placeholder): when DORA ships the new endpoint, swap the body of
// this handler to call it directly. The response shape must stay the same.
func (h *Handler) handleCopyTraders(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, http.MethodGet)
		return
	}

	users, err := h.doraClient.ListBotUsers(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Errorf("list copy traders: %w", err).Error())
		return
	}

	items := make([]CopyTraderSummary, 0, len(users))
	for _, u := range users {
		items = append(items, CopyTraderSummary{
			ID:          u.ID,
			DisplayName: strings.TrimSpace(u.FirstName + " " + u.LastName),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}
```

Find the existing `mux.HandleFunc` calls (search for `/v1/dora/user`, line ~416) and add a sibling:

```go
h.mux.HandleFunc("/v1/copy-traders", h.handleCopyTraders)
```

Verify the `writeJSONError` and `writeJSON` helpers exist; they're already used by the existing handlers.

- [ ] **Step 3.6: Run the tests and verify they pass**

Run: `go test ./strategy/http/ -run "TestHandlerListsCopyTraders" -v`
Expected: PASS for both tests.

- [ ] **Step 3.7: Commit**

```bash
git add strategy/http/handler.go strategy/http/handler_test.go
git commit -m "feat(http): add GET /v1/copy-traders endpoint"
```

---

## Task 4: Run full test suite and lint

**Files:** none modified

- [ ] **Step 4.1: Run the full Go test suite**

Run: `go test ./...`
Expected: all packages OK.

- [ ] **Step 4.2: Run go vet**

Run: `go vet ./...`
Expected: no output.

- [ ] **Step 4.3: Run golangci-lint on the changed packages**

Run: `golangci-lint run --timeout 5m ./strategy/http/...`
Expected: zero issues. If the linter flags the response types or constants, add targeted `//nolint:` comments with a justification comment, the same way the existing code does.

- [ ] **Step 4.4: Commit any lint fixes**

```bash
git add strategy/http/
git commit -m "fix(http): address lint issues in copy-traders" --allow-empty
```

(Empty commit is fine if no changes.)

---

## Task 5: Add MCP client method and tool

**Files:**
- Modify: `mcp/strategy_client.go`
- Modify: `mcp/tools_strategy.go`

- [ ] **Step 5.1: Add `listCopyTraders` to the strategy client**

In `mcp/strategy_client.go`, add (just after `getDORAUser` is a natural spot):

```go
func (c *strategyClient) listCopyTraders(ctx context.Context) (map[string]any, error) {
	return doStrategyJSON[map[string]any](ctx, c, http.MethodGet, "/v1/copy-traders", nil)
}
```

- [ ] **Step 5.2: Add the failing build check**

Run: `go build ./mcp/...`
Expected: success. (The tool registration comes next; this step is just to verify the client method compiles.)

- [ ] **Step 5.3: Register the new MCP tool**

In `mcp/tools_strategy.go`, find the existing `strategy_dora_user` tool registration and add the new tool right after it:

```go
s.AddTool(
	mcp.NewTool("strategy_copy_traders_list",
		mcp.WithDescription("List available copy traders. Placeholder that filters DORA users whose names start with TRADER_ or MM_ until DORA exposes a dedicated endpoint."),
	),
	func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		result, err := client.listCopyTraders(ctx)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return jsonText(result)
	},
)
```

- [ ] **Step 4.4: Build and test the mcp package**

Run: `go build ./...`
Run: `go test ./mcp/...`
Expected: build succeeds, mcp tests pass (no new test required for this thin proxy — it follows the exact same pattern as `strategy_dora_user`).

- [ ] **Step 5.5: Lint the mcp package**

Run: `golangci-lint run --timeout 5m ./mcp/...`
Expected: zero new issues. Apply targeted `//nolint:` comments if the linter flags anything, matching the existing style in the file.

- [ ] **Step 5.6: Commit**

```bash
git add mcp/strategy_client.go mcp/tools_strategy.go
git commit -m "feat(mcp): add strategy_copy_traders_list tool"
```

---

## Task 6: Final verification

- [ ] **Step 6.1: Run the full test suite one more time**

Run: `go test ./...`
Expected: all packages OK.

- [ ] **Step 6.2: Run go vet and lint across the changed packages**

Run: `go vet ./...`
Run: `golangci-lint run --timeout 5m ./strategy/http/... ./mcp/...`
Expected: zero issues.

- [ ] **Step 6.3: Verify the new endpoint shape end-to-end**

Confirm by reading the response shape against the spec at `docs/superpowers/specs/2026-06-01-list-copy-traders-design.md`:
- `GET /v1/copy-traders` returns `{"items": [{"id": "uuid", "display_name": "..."}]}`
- 401 without auth, 500 on DORA failure, 200 with `items: []` when no bots
- The handler has a `TODO(remove-placeholder)` comment

If any of those is missing, fix and amend the relevant commit.

---

## Self-Review Notes

- **Spec coverage:** DORA client method (Task 2), handler + route (Task 3), MCP tool (Task 5), bot filter unit test (Task 1), handler test (Task 3), placeholder comment (Task 3), migration note in spec — all addressed.
- **Placeholder scan:** No "TBD" or "implement later" in any step. Constants are defined; the TODO comment in the handler is intentional and explicit.
- **Type consistency:** `DORABotUser` (Task 1) → `ListBotUsers` returns `[]DORABotUser` (Task 2) → `CopyTraderSummary` (Task 3) carries `id` and `display_name` matching the spec → MCP tool returns the same shape (Task 5). All names match across tasks.
- **Commit cadence:** One commit per task; lint fixes get their own commit if needed.
