# List Copy Traders — Design

## Goal

Add a strategy-server endpoint that lists traders available to be followed by copy-trading runs. The endpoint is a placeholder backed by a filtered DORA user query; it will be replaced by a direct call to DORA's new trader-listing endpoint once it ships.

## Background

The copy-trading strategy requires a `followed_trader` UUID in its config. Today there is no way for a client to discover which traders can be followed — the UUID has to be known out-of-band. DORA is adding an endpoint to expose available traders, but it is not yet ready. Until then we return a placeholder response from the strategy-server.

## Scope

- Add `GET /v1/copy-traders` to the strategy-server.
- Add a corresponding MCP tool `strategy_copy_traders_list`.
- Placeholder behaviour: query DORA `GET /v1/user` (admin), filter for users whose `first_name` or `last_name` starts with `TRADER_` or `MM_` (case-insensitive). Return matching users.
- Migration path: when DORA exposes a dedicated trader-listing endpoint, swap the placeholder body for a direct call. Response shape stays the same so no client changes are required.

Out of scope: caching, response pagination, real-time refresh, discovery of traders outside the bot filter.

## API

### `GET /v1/copy-traders`

Auth: same `Authorization: ApiKey <key>` or `Bearer <token>` as other strategy-server routes.

Response (200):
```json
{
  "items": [
    {"id": "019c4d05-32f0-7c4f-8ef2-9de056e04557", "display_name": "TRADER_01"},
    {"id": "019c4d05-32f0-7c4f-8ef2-9de056e04558", "display_name": "MM_Alice"}
  ]
}
```

- `id` — DORA user UUID, intended for use as the `followed_trader` config field.
- `display_name` — best-effort human label: `first_name + " " + last_name` if both are non-empty, otherwise whichever is non-empty. Trimmed.

Errors: 401 if auth missing/invalid, 500 if the DORA call fails. Body shape is the standard `{"error": "..."}`.

### MCP tool `strategy_copy_traders_list`

- Description: "List available copy traders (placeholder until DORA exposes a dedicated endpoint)."
- Calls `GET /v1/copy-traders` on the strategy-server.
- Returns the JSON response as-is.

## Components

### 1. `dora/client.go`

Add a new summary type and method:

```go
type UserSummary struct {
    ID        string `json:"id"`
    FirstName string `json:"first_name"`
    LastName  string `json:"last_name"`
}

func (c *Client) ListBotUsers(ctx context.Context) ([]UserSummary, error)
```

`ListBotUsers`:
- Calls `c.apiClient.DefaultAPI.GetUsers(authCtx)` paginated with `limit=100`.
- Iterates pages until a page returns fewer than 100 items.
- For each user, checks `first_name` and `last_name` for the prefixes `TRADER_` or `MM_` (case-insensitive). Matches are collected; non-matches are skipped.
- Returns the collected matches as `[]UserSummary`.

### 2. `strategy/http/handler.go`

- Register `GET /v1/copy-traders` on the existing mux.
- Handler body: call `h.dora.ListBotUsers(ctx)`, map to `{"id", "display_name"}` items, return JSON.
- Inline TODO comment documenting the migration: when DORA ships the new endpoint, replace the body with a call to the new client method. Response shape is unchanged.

### 3. `mcp/strategy_client.go` + `mcp/tools_strategy.go`

- Add `listCopyTraders(ctx) (map[string]any, error)` to the strategy client that performs `GET /v1/copy-traders`.
- Register a new MCP tool `strategy_copy_traders_list` that invokes the method and returns the response.

## Data Flow

```
MCP client  ──►  MCP server  ──►  strategy-server  ──►  DORA /v1/user
                                (handler)            (filter for TRADER_/MM_)
```

## Error Handling

- DORA call failure (network, 5xx, auth): handler returns 500 with the wrapped error. MCP tool surfaces the error.
- Empty result (no matching bots): handler returns 200 with `{"items": []}`.
- DORA pagination: stop iterating when a page returns fewer than `limit` items, or when `offset` exceeds a sanity cap (10 pages = 1000 users) to avoid runaway loops.

## Testing

- `dora/client_test.go` (new): unit test the filter logic. The filter is internal — expose it via a small `isBotUser(UserSummary) bool` helper and test that directly. Coverage: TRADER_ prefix, MM_ prefix, lowercase variants, names that don't match, empty names, mixed-case edge cases.
- `strategy/http/handler_test.go`: add `TestHandlerListsCopyTraders`. The handler already has a `doraClientFunc` test seam from the existing `DORAUser` test; reuse it. Test that the response items contain the expected `{id, display_name}` fields and that the auth requirement is enforced (401 without auth).
- `mcp/`: no new test required — the new tool is a thin proxy and existing `strategy_list` test pattern applies if needed.

## Migration Plan

1. Land this placeholder.
2. When DORA ships the trader-listing endpoint, add a new method to `dora/client.go` (e.g. `ListCopyTraders`).
3. Swap the body of the `handleCopyTraders` handler to call the new method.
4. Remove `ListBotUsers` and its test once the swap is verified.
5. No MCP or response-shape changes — clients keep working.

## Open Questions

None at design time. The filter heuristic (`TRADER_` / `MM_` prefix) is the placeholder contract; DORA's eventual endpoint may return richer data but the strategy-server response will continue to expose just `{id, display_name}` until clients ask for more.
