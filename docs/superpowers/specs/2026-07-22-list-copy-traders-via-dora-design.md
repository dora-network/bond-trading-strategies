# List Copy Traders via DORA — Design

## Goal

Replace the placeholder copy-traders endpoint with a direct call to DORA's new dedicated `GET /v1/user/copy-traders` endpoint. The placeholder filtered DORA users by name prefix; the new endpoint is server-side filtered by the user's `allow_copy_trading` flag. The strategy-server response shape drops `display_name` because the new DORA endpoint returns only user IDs and the service refuses to surface identifying information for anonymity.

## Background

The current `GET /v1/copy-traders` strategy-server endpoint is a placeholder backed by a filtered DORA `GET /v1/user` query (filter for names starting with `TRADER_` or `MM_`). The placeholder was specced to be removed when DORA shipped a dedicated copy-trader listing endpoint, which it now has. The relevant DORA endpoint is `GET /v1/user/copy-traders` (admin-scoped), with the following contract (verified against `dora-api-admin/references/openapi.json`):

- Method: `GET`
- Path: `/v1/user/copy-traders`
- Query: `page` (default 1, min 1), `limit` (default 100, min 1, max 100)
- Auth: standard `Authorization: ApiKey <key>` or `Authorization: Bearer <token>`
- 200: `{ "data": ["<uuid>", ...], ...ResponseEnvelope }` — flat array of user UUIDs
- 400/500: standard `ResponseEnvelope`

The `dora-client-go` module is being bumped in lockstep to expose a `GetCopyTraders` method on `DefaultAPI` matching this spec. The strategy-server assumes that bump is already in place and uses the typed client.

## Scope

- Replace `liveDORAClient.ListBotUsers` and its `DORABotUser` return type with a new `ListCopyTraders` method that calls DORA's new endpoint.
- Drop the `display_name` field from `CopyTraderSummary`. The strategy-server response becomes `{"items": [{"id": "<uuid>"}]}`.
- Delete the now-unused `DORABotUser` struct, `isBotUser`, and `hasBotPrefix` helpers.
- Update handler tests, MCP tool description, and the OpenAPI doc to match the new shape.
- Leave the previous design doc (`docs/superpowers/specs/2026-06-01-list-copy-traders-design.md`) as historical record.

Out of scope:
- Caching the trader list (the placeholder didn't cache either).
- Re-introducing any name/identifier field on the response. Anonymity is a security requirement.
- MCP tool changes beyond the description string.

## API

### `GET /v1/copy-traders` (strategy-server)

Auth: same `Authorization: ApiKey <key>` or `Authorization: Bearer <token>` as other strategy-server routes.

Response (200):
```json
{
  "items": [
    { "id": "019c4d05-32f0-7c4f-8ef2-9de056e04557" }
  ]
}
```

- `id` — DORA user UUID, intended for use as the `followed_trader` config field of a copy-trading run.

Errors:
- 401 — auth missing or invalid (standard `{"error": "..."}` envelope).
- 500 — DORA call failed (network, 5xx, decode). Body is the standard `{"error": "..."}` envelope.

Behaviour:
- Pagination is hidden from the caller. The handler iterates DORA pages internally and returns a single flat `items` array.
- The internal pagination terminates as soon as a page returns an empty `data` array or fewer than `copyTraderPageSize` (100) items. There is no hard upper bound on the number of pages iterated.
- Empty result is `{"items": []}`, not `null`.

### MCP tool `strategy_copy_traders_list`

- Description: "List copy traders available to be followed by a copy-trading run."
- Calls `GET /v1/copy-traders` on the strategy-server.
- Returns the JSON response as-is. No new wrapping or shaping.

## Components

### 1. `strategy/http/dora_client.go`

- Replace the `doraClient` interface method:
  ```go
  ListBotUsers(context.Context) ([]DORABotUser, error)
  ```
  with
  ```go
  ListCopyTraders(context.Context) ([]string, error)
  ```
- Delete the `DORABotUser` struct, the `isBotUser` function, and the `hasBotPrefix` function. None of them are referenced elsewhere.
- Add the live implementation:
  ```go
  // ponytail: no max-pages cap. Termination relies on DORA returning either
  // an empty data array or a short one (len(resp.Data) < limit). If DORA ever
  // returns a full page indefinitely, this loops forever. Upgrade when DORA
  // exposes a total/has_more field on GetCopyTradersResponse.
  func (c *liveDORAClient) ListCopyTraders(ctx context.Context) ([]string, error) {
      authCtx, err := c.authContext(ctx)
      if err != nil {
          return nil, err
      }

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
      }
      return all, nil
  }
  ```
- Keep the `copyTraderPageSize` constant (100). Drop the `copyTraderMaxPages` constant — the loop now terminates on DORA's signal, not a hard cap. If a future caller wants a safety net, the DORA response schema will need a `total` or `has_more` field; add the cap back then.

### 2. `strategy/http/handler.go`

- Reduce `CopyTraderSummary` to a single `id` field:
  ```go
  type CopyTraderSummary struct {
      ID string `json:"id"`
  }
  ```
- Rewrite `handleCopyTraders` to call `h.dora.ListCopyTraders(r.Context())`, map the UUIDs into `CopyTraderSummary` items, and return the `{"items": [...]}` envelope. The route registration (`GET /v1/copy-traders`) is unchanged.
- Remove the placeholder TODO comment above `handleCopyTraders`.
- Drop the `strings` import if it becomes unused (it was only used for `strings.TrimSpace` on the old display name).

### 3. `mcp/strategy_client.go` and `mcp/tools_strategy.go`

- `mcp/strategy_client.go` — unchanged. The `listCopyTraders(ctx)` method already hits `GET /v1/copy-traders` and returns the response as a generic map.
- `mcp/tools_strategy.go` — update the `strategy_copy_traders_list` tool description to "List copy traders available to be followed by a copy-trading run." Drop the "placeholder" wording.

### 4. `docs/openapi/strategy-server.json`

Edit the `CopyTraderSummary` schema in `docs/openapi/strategy-server.json`:
- Drop the `display_name` property.
- Remove `display_name` from the `required` list.
- Update the schema's `description` to drop the reference to the dropped field. The current description mentions both `id` and `display_name`; the new description only needs to mention `id` (DORA user UUID, used as `followed_trader`).

The `GET /v1/copy-traders` operation's `summary` is already accurate ("List traders available to be followed by a copy-trading run"); no change needed. Path, operationId, security, and response wrapper stay.

## Data Flow

```
MCP client  ──►  MCP server  ──►  strategy-server  ──►  DORA /v1/user/copy-traders
                                (handler)            (paginated, server-side filtered
                                                     by allow_copy_trading)
```

## Error Handling

- 401 — auth missing/invalid. Standard `{"error": "..."}` envelope. Same as today.
- 500 — DORA call failure (network, 5xx, decode error, missing `data`). The error is wrapped with `list copy traders: %w` and surfaced through `writeError` as the standard `{"error": "..."}` envelope. The MCP tool surfaces the same error to the caller.
- Empty result — `{"items": []}`. Not `null`. The handler allocates a non-nil empty slice.
- Pagination — no internal cap. The loop continues until DORA returns an empty `data` page or a page smaller than `copyTraderPageSize`. If DORA ever returns exactly `limit` items indefinitely, the loop runs forever; this is a known ceiling pending a `total`/`has_more` field on the DORA response. No log is emitted in the meantime.

## Testing

### `strategy/http/dora_client_test.go` (white-box)

- Delete `TestIsBotUser` and `TestLiveDORAClient_ListBotUsers` (and any related fixtures). They cover deleted code.
- Add `TestLiveDORAClient_ListCopyTraders`. Uses an `httptest.NewServer` mocking the DORA server. Cases:
  - **happy path paginated** — page 1 returns 100 IDs, page 2 returns 50; client collects 150, no page 3 request.
  - **empty page terminates** — page 1 returns `{"data": []}`; client returns an empty/nil slice, no page 2 request.
  - **5xx propagated** — server returns HTTP 500; client returns a wrapped error.
  - **short result terminates** — page 1 returns 25 IDs (less than `copyTraderPageSize`); client returns 25, no page 2 request.
  - **full-page-no-end (defensive)** — server returns a full `copyTraderPageSize` for several pages and then a short page (e.g. 1 ID) on page N; client stops at page N. Documents that the short-page break is the practical bound on the loop.

### `strategy/http/handler_test.go` (white-box)

- Rename the `doraClientFunc` field `listBotUsers` → `listCopyTraders`. Change the field's type to `func(context.Context) ([]string, error)`.
- `TestHandlerListsCopyTraders` — fake returns 2 UUIDs. Assert the response JSON contains two items, each with only an `id` field. Assert no `display_name` key is present in the response body.
- `TestHandlerListsCopyTradersEmpty` — fake returns `[]string{}`. Assert the response JSON has `items: []` (not `null`).
- `TestHandlerListsCopyTradersDORAError` — fake returns an error. Assert 500 with the standard error envelope.
- `TestHandlerListsCopyTradersRequiresAuth` — unchanged structurally, just uses the renamed fake field.

### MCP

No new tests. The MCP tool is a thin proxy; the strategy-server tests cover the contract. `mcp/server_test.go` continues to pass unchanged.

### Verification

- `go test ./strategy/http/...` — passes.
- `go test ./mcp/...` — passes.
- `golangci-lint run ./...` — clean.
- `go mod tidy` — no diff.
- Smoke test (manual, against a dev strategy-server): `curl -H "Authorization: ApiKey $DORA_API_KEY" http://localhost:8081/v1/copy-traders` returns `{"items": [{"id": "<uuid>"}, ...]}` with no `display_name` key.

## Cleanup

- Delete `DORABotUser` struct, `isBotUser` function, `hasBotPrefix` function in `strategy/http/dora_client.go`.
- Drop the `strings` import in `strategy/http/handler.go` if unused after the rewrite.
- Remove the placeholder TODO comment above `handleCopyTraders`.
- Leave `docs/superpowers/specs/2026-06-01-list-copy-traders-design.md` as historical record. Do not edit it.

## Migration / Rollout

The `dora-client-go` bump that introduces `GetCopyTraders` must land in a separate PR before this one. This PR depends on that bump being in the pinned module version. The strategy-server change is small and self-contained once the SDK is updated.

No database migration. No config change. The route, auth, and response envelope are unchanged. The only client-visible change is the removal of `display_name` from each item, which is the intended security improvement.

## Open Questions

None at design time. The two follow-ups flagged during brainstorming were resolved:
- Old spec: leave as historical record.
- MCP tool description: "List copy traders available to be followed by a copy-trading run."
