# List Copy Traders via DORA Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the `ListBotUsers` placeholder on the strategy-server's DORA client with a direct call to DORA's new `GET /v1/user/copy-traders` endpoint, drop `display_name` from the public response for anonymity, and update tests, MCP tool description, and OpenAPI spec to match.

**Architecture:** Swap the `doraClient` interface method `ListBotUsers(ctx) ([]DORABotUser, error)` for `ListCopyTraders(ctx) ([]string, error)`. The live implementation paginates DORA's new endpoint with `page` (1-based) and `limit`, terminating on empty/short pages — no internal cap. The handler maps the returned UUIDs into `CopyTraderSummary{ID}` items; the public response is `{"items": [{"id": "..."}]}`. The MCP layer is unchanged (still proxies `/v1/copy-traders`).

**Tech Stack:** Go 1.23+, `dora-client-go` (bumped in a separate PR — assumed already pinned at a version exposing `GetCopyTraders`), `testify`, `httptest`, `net/http` for the live-client fixture.

**Spec:** `docs/superpowers/specs/2026-07-22-list-copy-traders-via-dora-design.md`

**Prerequisite:** The pinned `dora-client-go` version in `go.mod` must already expose `DefaultAPI.GetCopyTraders(...).Page(p).Limit(l).Execute()`. If a `go build` step below fails because the method doesn't exist, stop and surface to the user — the SDK bump is a separate repo change.

---

## Task 1: Update the `doraClient` interface and replace the `ListBotUsers` live implementation

**Files:**
- Modify: `strategy/http/dora_client.go`
- Test: `strategy/http/dora_client_test.go`

This task cuts over the interface and the live implementation. Tests come in Task 2.

- [ ] **Step 1.1: Update the `doraClient` interface**

In `strategy/http/dora_client.go` (currently lines 17–22), replace the `ListBotUsers` method with `ListCopyTraders` and drop the `DORABotUser` return type from the interface:

```go
type doraClient interface {
    ListOrderBooks(context.Context) ([]DORAOrderBookSummary, error)
    GetAssetByID(context.Context, string) (*AssetInfo, error)
    GetUserID(context.Context) (string, error)
    ListCopyTraders(context.Context) ([]string, error)
}
```

- [ ] **Step 1.2: Delete the `DORABotUser` struct, `isBotUser`, and `hasBotPrefix`**

In `strategy/http/dora_client.go`, remove the entire `DORABotUser` block (currently lines 24–30), the `isBotUser` function (lines 32–37), and the `hasBotPrefix` function (lines 39–42). **Keep the `strings` import** — it is still used by `NewDORAClient` (`strings.TrimRight(baseURL, "/")`) and `GetUserID` (`strings.TrimSpace(string(body))`) in the same file.

- [ ] **Step 1.3: Drop the `copyTraderMaxPages` constant**

In `strategy/http/dora_client.go` (currently lines 44–49), the const block becomes:

```go
const (
    apiKeyPrefix               = "ApiKey"
    copyTraderPageSize   int32 = 100
    responsePreviewBytes       = 4096
)
```

The `copyTraderPageSize` constant stays because the new pagination loop uses it.

- [ ] **Step 1.4: Replace the `ListBotUsers` live method with `ListCopyTraders`**

In `strategy/http/dora_client.go`, replace the existing `ListBotUsers` method (currently lines 188–229, including its comment block) with:

```go
// ListCopyTraders returns the user IDs of DORA users who have allow_copy_trading
// enabled. Pagination is hidden from the caller: pages of `copyTraderPageSize`
// are requested until DORA returns an empty data array or a short page.
func (c *liveDORAClient) ListCopyTraders(ctx context.Context) ([]string, error) {
    authCtx, err := c.authContext(ctx)
    if err != nil {
        return nil, err
    }

    // ponytail: no max-pages cap. Termination relies on DORA returning either
    // an empty data array or a short one (len(resp.Data) < limit). If DORA ever
    // returns a full page indefinitely, this loops forever. Upgrade when DORA
    // exposes a total/has_more field on GetCopyTradersResponse.
    var all []string
    for page := int32(1); ; page++ {
        resp, rawResp, err := c.client.DefaultAPI.
            GetCopyTraders(authCtx).
            Page(page).
            Limit(copyTraderPageSize).
            Execute()
        if rawResp != nil && rawResp.Body != nil {
            _ = rawResp.Body.Close()
        }
        if err != nil {
            return nil, fmt.Errorf("list copy traders: %w", err)
        }
        if resp == nil || len(resp.Data) == 0 {
            break
        }
        all = append(all, resp.Data...)
        if len(resp.Data) < int(copyTraderPageSize) {
            break
    }
    return all, nil
}
```

**Note for the implementer of Task 3:** `liveDORAClient.ListCopyTraders` returns `nil` (not an empty slice) when DORA returns no copy traders — the `var all []string` declaration defaults to nil and the loop never appends. The handler step in Task 3 must initialise `items` with `make([]CopyTraderSummary, 0, len(ids))` (not `var items []CopyTraderSummary`) so that an empty result serializes as `"items": []` and not `"items": null`. This is captured in the Task 3 step below.
Run: `go build ./...`
Expected: compile errors in `strategy/http/dora_client_test.go` (references `TestIsBotUser`, `TestLiveDORAClient_ListBotUsers`, `copyTraderMaxPages`) and `strategy/http/handler.go` (references `DORABotUser`, `ListBotUsers`) and `strategy/http/handler_test.go` (references `listBotUsers`, `strategyhttp.DORABotUser`). These are expected; the next tasks fix them.

- [ ] **Step 1.6: Commit**

```bash
git add strategy/http/dora_client.go
git commit -m "feat(http): swap ListBotUsers for ListCopyTraders on doraClient"
```

---

## Task 2: Rewrite the dora_client_test.go tests for the new method

**Files:**
- Modify: `strategy/http/dora_client_test.go`

Replace `TestIsBotUser` and `TestLiveDORAClient_ListBotUsers` with a single `TestLiveDORAClient_ListCopyTraders` that exercises the new pagination logic against a fake DORA server.

- [ ] **Step 2.1: Delete `TestIsBotUser`**

In `strategy/http/dora_client_test.go`, remove the entire `TestIsBotUser` function (currently lines 21–50) and any test-fixture types it references inline.

- [ ] **Step 2.2: Delete the `ListBotUsers` pagination test and its helpers**

In `strategy/http/dora_client_test.go`, remove the entire `TestLiveDORAClient_ListBotUsers` function (currently lines 102–275), including the inner `makeUser`, `fullBotPage`, and `fullBotIDs` helpers. The `sync` and `strconv` imports remain useful (the new test uses them); the `doraclient` import is no longer needed for the new test — check after the rewrite.

- [ ] **Step 2.3: Add `TestLiveDORAClient_ListCopyTraders`**

Replace the deleted test with the following in `strategy/http/dora_client_test.go`. The new test mocks `GET /v1/user/copy-traders` and verifies the live client iterates pages correctly:

```go
func TestLiveDORAClient_ListCopyTraders(t *testing.T) {
    t.Parallel()

    fullPageIDs := func(prefix string) []string {
        // Note: `int32` only appears in the production code at the SDK call
        // boundary (`Page(page)` and `Limit(copyTraderPageSize)`). In the test
        // we track page numbers as plain `int` — `strconv.Atoi` returns `int`,
        // and there's no SDK boundary to cross from the test handler.
        ids := make([]string, 0, int(copyTraderPageSize))
        for i := range int(copyTraderPageSize) {
            ids = append(ids, fmt.Sprintf("019c0000-0000-7000-8000-%s%010d", prefix, i))
        }
        return ids
    }


    cases := []struct {
        name        string
        pages       [][]string
        wantIDs     []string
        wantCalls   int
        wantPages   []int
    }{
        {
            name: "happy path paginated",
            pages: [][]string{
                fullPageIDs("a"),
                fullPageIDs("b")[:50],
            },
            wantIDs:   append(fullPageIDs("a"), fullPageIDs("b")[:50]...),
            wantCalls: 2,
            wantPages: []int{1, 2},
        },
        {
            name:        "empty first page terminates",
            pages:       [][]string{nil},
            wantIDs:     nil,
            wantCalls:   1,
            wantPages:   []int{1},
        },
        {
            name:        "short first page terminates",
            pages:       [][]string{{"019c0000-0000-7000-8000-000000000001", "019c0000-0000-7000-8000-000000000002"}},
            wantIDs:     []string{"019c0000-0000-7000-8000-000000000001", "019c0000-0000-7000-8000-000000000002"},
            wantCalls:   1,
            wantPages:   []int{1},
        },
        {
            name: "5xx propagated",
            pages: [][]string{
                {}, // unused; handler returns 500 immediately
            },
            wantIDs:   nil,
            wantCalls: 1,
            wantPages: []int{1},
        },
        {
            name: "full pages then a short page terminates",
            pages: [][]string{
                fullPageIDs("c"),
                fullPageIDs("d"),
                {"019c0000-0000-7000-8000-000000000099"},
            },
            wantIDs: append(append(fullPageIDs("c"), fullPageIDs("d")...), "019c0000-0000-7000-8000-000000000099"),
            wantCalls: 3,
            wantPages: []int{1, 2, 3},
        },
    }

    for _, tc := range cases {
        t.Run(tc.name, func(t *testing.T) {
            t.Parallel()

            var (
                mu    sync.Mutex
                pages []int
                calls int
            )

            srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
                if r.URL.Path != "/v1/user/copy_traders" {
                    http.NotFound(w, r)
                    return
                }
                page, err := strconv.Atoi(r.URL.Query().Get("page"))
                if err != nil || page < 1 {
                    http.Error(w, "bad page", http.StatusBadRequest)
                    return
                }

                mu.Lock()
                if calls >= len(tc.pages) {
                    mu.Unlock()
                    http.Error(w, "unexpected extra request", http.StatusInternalServerError)
                    return
                }
                if tc.name == "5xx propagated" {
                    mu.Unlock()
                    http.Error(w, "boom", http.StatusInternalServerError)
                    return
                }
                pageData := tc.pages[calls]
                pages = append(pages, page)
                calls++
                mu.Unlock()

                resp := doraclient.GetCopyTradersResponse{
                    Metadata: doraclient.Metadata{
                        StatusCode: 200,
                        TraceId:    "trace",
                        RequestId:  "req",
                    },
                    Data: pageData,
                }
                w.Header().Set("Content-Type", "application/json")
                _ = json.NewEncoder(w).Encode(resp)
            }))
            defer srv.Close()

            cfg := doraclient.NewConfiguration()
            cfg.Servers = doraclient.ServerConfigurations{
                {URL: srv.URL, Description: "test"},
            }
            client := &liveDORAClient{client: doraclient.NewAPIClient(cfg)}

            ctx := authctx.WithAPIKey(context.Background(), "test-key")
            got, err := client.ListCopyTraders(ctx)

            if tc.name == "5xx propagated" {
                require.Error(t, err)
                assert.Contains(t, err.Error(), "list copy traders")
                return
            }
            require.NoError(t, err)
            assert.Equal(t, tc.wantIDs, got)

            mu.Lock()
            gotCalls := calls
            gotPages := append([]int(nil), pages...)
            mu.Unlock()
            assert.Equal(t, tc.wantCalls, gotCalls)
            assert.Equal(t, tc.wantPages, gotPages)
        })
    }
}
```

Notes:
- The `5xx propagated` case uses a sub-test-name string check to control handler behaviour. If a reader finds that ugly, a refactor to a `wantErr bool` field on the case struct is fine, but it must stay inside this file.
- The `doraclient.GetCopyTradersResponse` type and `doraclient.Metadata` type are assumed to exist in the bumped dora-client-go. If a build error names an unknown type, surface to the user — the SDK regen is the source of truth.

- [ ] **Step 2.4: Run the new test**

Run: `go test ./strategy/http/ -run TestLiveDORAClient_ListCopyTraders -v`
Expected: PASS for all 5 sub-tests. (The test is the source of truth; if any sub-test fails, fix the live implementation in `dora_client.go` to match the test's expected behaviour.)

- [ ] **Step 2.5: Commit**

```bash
git add strategy/http/dora_client_test.go
git commit -m "test(http): rewrite dora_client pagination tests for ListCopyTraders"
```

---

## Task 3: Reduce `CopyTraderSummary` and rewrite `handleCopyTraders`

**Files:**
- Modify: `strategy/http/handler.go`

Drop `DisplayName` from the response type and rewire the handler to call the new client method. The `strings` import stays (used elsewhere in the file).

- [ ] **Step 3.1: Reduce `CopyTraderSummary` to a single `id` field**

In `strategy/http/handler.go` (currently lines 172–176), replace the type with:

```go
// CopyTraderSummary is a single entry in the list-copy-traders response.
// The id is the DORA user UUID and matches the `followed_trader` field
// accepted by CopyTradingConfig. Names and other identifying information are
// intentionally omitted for user anonymity.
type CopyTraderSummary struct {
    ID string `json:"id"`
}
```

- [ ] **Step 3.2: Rewrite `handleCopyTraders`**

In `strategy/http/handler.go` (currently lines 725–754), replace the entire `handleCopyTraders` method — including the placeholder TODO comment above it — with:

```go
// handleCopyTraders returns the list of traders available to be followed by
// copy-trading runs. The list is sourced from DORA's dedicated
// `GET /v1/user/copy-traders` endpoint, which is server-side filtered to users
// with copy trading enabled. Only the user IDs are exposed; names and other
// identifying information are intentionally omitted for user anonymity.
func (h *Handler) handleCopyTraders(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodGet {
        writeMethodNotAllowed(w, http.MethodGet)
        return
    }

    client := h.doraClient
    if client == nil {
        client = NewDORAClient()
    }
    ids, err := client.ListCopyTraders(r.Context())
    if err != nil {
        writeError(w, http.StatusInternalServerError, fmt.Sprintf("list copy traders: %v", err))
        return
    }

    // `make(..., 0, len(ids))` is required, not `var items []CopyTraderSummary`:
    // when DORA returns no copy traders, the live client returns `nil`, and
    // writeJSON serialises a nil slice as `"items": null`. The spec guarantees
    // `"items": []` for empty results, so the handler must produce a non-nil
    // empty slice explicitly.
    items := make([]CopyTraderSummary, 0, len(ids))
    for _, id := range ids {
        items = append(items, CopyTraderSummary{ID: id})
    }
    writeJSON(w, http.StatusOK, map[string]any{"items": items})
}
```

- [ ] **Step 3.3: Run the build**

Run: `go build ./...`
Expected: the only remaining compile errors are in `strategy/http/handler_test.go` (references `listBotUsers`, `strategyhttp.DORABotUser`, and `DisplayName` assertions). Handler code itself compiles.

- [ ] **Step 3.4: Commit**

```bash
git add strategy/http/handler.go
git commit -m "refactor(http): drop display_name from /v1/copy-traders response"
```

---

## Task 4: Update `doraClientFunc` and the four copy-traders handler tests

**Files:**
- Modify: `strategy/http/handler_test.go`

Rename the test-fake field and rewrite the four tests to assert the new shape.

- [ ] **Step 4.1: Rename the test-fake field and method**

In `strategy/http/handler_test.go`:

1. In the `doraClientFunc` struct (currently lines 1646–1651), rename the `listBotUsers` field to `listCopyTraders` and change its signature:

```go
type doraClientFunc struct {
    listOrderBooks func(context.Context) ([]strategyhttp.DORAOrderBookSummary, error)
    getUserID      func(context.Context) (string, error)
    getAssetByID   func(context.Context, string) (*strategyhttp.AssetInfo, error)
    listCopyTraders func(context.Context) ([]string, error)
}
```

2. Replace the `doraClientFunc.ListBotUsers` method (currently lines 1674–1679) with:

```go
func (f doraClientFunc) ListCopyTraders(ctx context.Context) ([]string, error) {
    if f.listCopyTraders == nil {
        return nil, fmt.Errorf("not implemented")
    }
    return f.listCopyTraders(ctx)
}
```

- [ ] **Step 4.2: Rewrite `TestHandlerListsCopyTraders`**

In `strategy/http/handler_test.go` (currently lines 31–67), replace the test body with:

```go
func TestHandlerListsCopyTraders(t *testing.T) {
    t.Parallel()

    trader1 := "11111111-1111-1111-1111-111111111111"
    trader2 := "22222222-2222-2222-2222-222222222222"

    fake := doraClientFunc{
        listCopyTraders: func(_ context.Context) ([]string, error) {
            return []string{trader1, trader2}, nil
        },
    }

    handler := strategyhttp.NewHandler(
        &strategyfakes.FakeService{},
        strategyhttp.WithDORAClient(fake),
        strategyhttp.WithTradesHistoryStore(nil),
    )
    rec := httptest.NewRecorder()
    req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/copy-traders", nil)
    req.Header.Set("Authorization", "ApiKey test-key")
    handler.ServeHTTP(rec, req)

    require.Equal(t, http.StatusOK, rec.Code)

    // Assert items contain only the id field — no display_name leakage.
    var rawItems []map[string]any
    require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &struct {
        Items *[]map[string]any `json:"items"`
    }{Items: &rawItems}.Items))
    require.Len(t, rawItems, 2)
    for _, item := range rawItems {
        _, hasDisplayName := item["display_name"]
        assert.False(t, hasDisplayName, "response must not contain display_name")
        assert.Len(t, item, 1, "each item must contain only the id field")
    }

    var body struct {
        Items []strategyhttp.CopyTraderSummary `json:"items"`
    }
    require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
    require.Len(t, body.Items, 2)
    assert.Equal(t, trader1, body.Items[0].ID)
    assert.Equal(t, trader2, body.Items[1].ID)
}
```

Note: the second `json.Unmarshal` is intentionally redundant with the first — it provides a clean, type-safe assertion of the IDs after the structural check that the keys are right. If you find this ugly, you may collapse to a single decode with `struct { Items []struct{ ID string `json:"id"` } `json:"items"` }`; the structural `display_name` check still needs to be a separate step.

- [ ] **Step 4.3: Rewrite `TestHandlerListsCopyTradersEmpty`**

In `strategy/http/handler_test.go` (currently lines 83–110), replace the test body with:

```go
func TestHandlerListsCopyTradersEmpty(t *testing.T) {
    t.Parallel()

    fake := doraClientFunc{
        listCopyTraders: func(_ context.Context) ([]string, error) {
            return []string{}, nil
        },
    }

    handler := strategyhttp.NewHandler(
        &strategyfakes.FakeService{},
        strategyhttp.WithDORAClient(fake),
        strategyhttp.WithTradesHistoryStore(nil),
    )
    rec := httptest.NewRecorder()
    req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/copy-traders", nil)
    req.Header.Set("Authorization", "ApiKey test-key")
    handler.ServeHTTP(rec, req)

    require.Equal(t, http.StatusOK, rec.Code)

    var body struct {
        Items []strategyhttp.CopyTraderSummary `json:"items"`
    }
    require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
    require.NotNil(t, body.Items)
    require.Empty(t, body.Items)
}
```

The functional change: the fake now returns `[]string{}` instead of `nil`. The handler's `make([]CopyTraderSummary, 0, len(ids))` always returns a non-nil empty slice, so the `require.NotNil` and `require.Empty` assertions still hold.

- [ ] **Step 4.4: Rewrite `TestHandlerListsCopyTradersDORAError`**

In `strategy/http/handler_test.go` (currently lines 112–132), replace the test body with:

```go
func TestHandlerListsCopyTradersDORAError(t *testing.T) {
    t.Parallel()

    fake := doraClientFunc{
        listCopyTraders: func(_ context.Context) ([]string, error) {
            return nil, assert.AnError
        },
    }

    handler := strategyhttp.NewHandler(
        &strategyfakes.FakeService{},
        strategyhttp.WithDORAClient(fake),
        strategyhttp.WithTradesHistoryStore(nil),
    )
    rec := httptest.NewRecorder()
    req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/copy-traders", nil)
    req.Header.Set("Authorization", "ApiKey test-key")
    handler.ServeHTTP(rec, req)

    require.Equal(t, http.StatusInternalServerError, rec.Code)
}
```

The only change is `listBotUsers` → `listCopyTraders` and the return-type swap.

- [ ] **Step 4.5: Verify `TestHandlerListsCopyTradersRequiresAuth`**

In `strategy/http/handler_test.go` (currently lines 69–81), this test does not reference `listBotUsers` directly — it uses `doraClientFunc{}` with no fields set, so it still compiles and tests the auth requirement correctly. No change needed.

- [ ] **Step 4.6: Run the four tests**

Run: `go test ./strategy/http/ -run "TestHandlerListsCopyTraders" -v`
Expected: all four tests PASS.

- [ ] **Step 4.7: Run the full strategy/http test suite**

Run: `go test ./strategy/http/...`
Expected: all tests PASS. The `TestLiveDORAClient_ListCopyTraders` from Task 2 plus the four handler tests all pass. No other test in the package references the deleted types.

- [ ] **Step 4.8: Commit**

```bash
git add strategy/http/handler_test.go
git commit -m "test(http): update copy-traders handler tests for ListCopyTraders"
```

---

## Task 5: Update the MCP tool description

**Files:**
- Modify: `mcp/tools_strategy.go`

The MCP client (`mcp/strategy_client.go`) already calls `/v1/copy-traders` — that file is unchanged. Only the tool description string is updated.

- [ ] **Step 5.1: Update the tool description**

In `mcp/tools_strategy.go` (currently lines 171–174), replace the tool's `WithDescription(...)` argument with:

```go
mcp.NewTool("strategy_copy_traders_list",
    mcp.WithDescription("List copy traders available to be followed by a copy-trading run."),
),
```

The `//nolint:lll` directive can be removed because the new line is short.

- [ ] **Step 5.2: Build the mcp package**

Run: `go build ./mcp/...`
Expected: success. No behaviour change, only the description string moved.

- [ ] **Step 5.3: Run the mcp test suite**

Run: `go test ./mcp/...`
Expected: PASS. `mcp/server_test.go` exercises the tool registry; no test asserts the exact description text.

- [ ] **Step 5.4: Commit**

```bash
git add mcp/tools_strategy.go
git commit -m "docs(mcp): drop placeholder wording from copy-traders tool description"
```

---

## Task 6: Update the OpenAPI spec

**Files:**
- Modify: `docs/openapi/strategy-server.json`

Drop `display_name` from the `CopyTraderSummary` schema, drop it from the `required` list, and update the schema's `description` so it no longer references the removed field. The operation `summary` is already accurate and stays as-is.

- [ ] **Step 6.1: Update the `CopyTraderSummary` schema**

In `docs/openapi/strategy-server.json` (currently lines 703–711), replace the schema with:

```json
      "CopyTraderSummary": {
        "type": "object",
        "description": "A single entry in the list-copy-traders response. The `id` is the DORA user id and matches the `followed_trader` field accepted by CopyTradingConfig. Names and other identifying information are intentionally omitted for user anonymity.",
        "properties": {
          "id": { "type": "string", "format": "uuid" }
        },
        "required": ["id"]
      },
```

Preserve the surrounding 2-space JSON indentation. Verify the file still parses:

Run: `python3 -c "import json; json.load(open('docs/openapi/strategy-server.json')); print('ok')"`
Expected: `ok`

If `python3` is unavailable: `jq -e . docs/openapi/strategy-server.json > /dev/null && echo ok`

- [ ] **Step 6.2: Run go build to verify the embed still works**

Run: `go build ./...`
Expected: success. The embedded spec compiles into the strategy-server binary.

- [ ] **Step 6.3: Commit**

```bash
git add docs/openapi/strategy-server.json
git commit -m "docs(openapi): drop display_name from CopyTraderSummary schema"
```

---

## Task 7: Final verification

**Files:** none (read-only checks)

- [ ] **Step 7.1: Run the full test suite**

Run: `go test ./...`
Expected: PASS for all packages.

- [ ] **Step 7.2: Run the linter**

Run: `golangci-lint run ./...`
Expected: clean. The `//nolint:errcheck`/`//nolint:gosec`/`//nolint:tagliatelle` patterns are preserved from the existing code; no new linter violations.

- [ ] **Step 7.3: Run `go mod tidy`**

Run: `go mod tidy`
Expected: no diff. No new dependencies were added.

- [ ] **Step 7.4: Run the pre-commit suite**

Run: `pre-commit run --all-files`
Expected: all hooks pass (fix-end-of-files, check-yaml, check-added-large-files, check-executables-have-shebangs, check-json, detect-private-key, mixed-line-ending, plus the go-* hooks for the modified Go files). If a hook fails, fix the underlying issue — do not bypass.

- [ ] **Step 7.5: Verify the placeholder symbols are fully gone**

Run: `grep -rn "DORABotUser\|isBotUser\|hasBotPrefix\|ListBotUsers\|listBotUsers\|copyTraderMaxPages" strategy/ mcp/`
Expected: no output. Every reference has been migrated.

- [ ] **Step 7.6: Smoke test (manual, against a dev strategy-server)**

Set up a dev strategy-server per `AGENTS.md` and run:

```bash
curl -sS -H "Authorization: ApiKey $DORA_API_KEY" \
     http://localhost:8081/v1/copy-traders | jq .
```

Expected: `{"items": [{"id": "<uuid>"}, ...]}` with no `display_name` field. The IDs should match the DORA users who have `allow_copy_trading` enabled.

- [ ] **Step 7.7: No commit**

This task is read-only. The commits from Tasks 1–6 stand. If `pre-commit` produced a fixup commit during 7.4, that commit is part of this PR and is fine.
