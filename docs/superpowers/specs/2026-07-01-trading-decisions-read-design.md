# DORA-5773 · Trading decisions read endpoint — Design

**Status:** Draft (post-brainstorm, pre-implementation)
**Linear:** [DORA-5773](https://linear.app/dora-chain/issue/DORA-5773)
**Date:** 2026-07-01
**Branch:** `feat/5773-strategy-decisions`

## Background and motivation

The live strategy loop already writes one row per market order to
`strategy_decisions` (see migration `010_add_strategy_decisions.sql`,
`strategy/decision.go`, and `strategy/http/decision_store.go`). The
read side does not exist: there is no query method on
`PGDecisionStore`, no REST endpoint, and no MCP tool. DORA-5773 asks
for a paginated, per-run endpoint that returns those rows.

The write path is complete. This spec is the read path.

## Decisions taken during brainstorm

Recorded so reviewers and the next iteration can find them without
re-reading chat history.

1. **URL shape — diverges from the DoD.** The DoD asks for
   `/v1/trading-decisions/{user_id}/{run_id}`. The table has no
   `user_id` column, and every other read endpoint in this service
   derives the user from the auth token rather than the URL. The new
   endpoint is `GET /v1/trading-decisions/{run_id}`. The handler
   verifies ownership by looking up the run in `strategy_runs` and
   matching `dora_user_id` against the authenticated user. **Reason:**
   consistency with the rest of the API; removes a path parameter
   that has no backing column. Posted as a comment on DORA-5773.
2. **Pagination — diverges from the DoD.** The DoD asks for
   `page`/`limit` offset pagination. The new endpoint uses cursor
   pagination: drop `page`, add opaque `cursor`, return
   `next_cursor` in the response body. Sort order is newest first
   (`created_at DESC, seq DESC`). **Reason:** stable under live
   writes; efficient on the existing
   `idx_strategy_decisions_run_id_created_at` index; offset paging is
   O(N) past the first page. Posted as a comment on DORA-5773.
3. **Date filter — diverges from the rest of the API.** Other read
   endpoints silently drop malformed `from`/`to` values via
   `parseDateFilter`. The new endpoint rejects malformed dates with
   400. **Reason:** a typo'd `from` on a paginated endpoint widens
   the result set silently across many pages, which is more
   dangerous than the same typo on a single-page list. The deviation
   is documented here so future readers do not "fix" it back.
4. **`limit` semantics — matches the rest of the API.** Default 50,
   silently clamped to `[1, 200]`. `defaultPaginationLimit` is 10
   elsewhere; we set the decisions default to 50 because the typical
   UI expectation on a decisions page is to render many rows at once,
   and 50 matches the existing `maxPaginationLimit`. The 200 ceiling
   is a deliberate upward deviation from the rest of the API's 50,
   justified by the cursor-paginated contract (no `page * limit` to
   protect) and the narrow per-row payload. `parsePagination`'s
   silent-clamp style is preserved exactly.
5. **Cursor format — versioned and opaque.** `base64(json({"v": 1,
   "t": <unix_nano>, "s": <seq>}))`. Clients MUST NOT parse the
   payload; the version byte lets us change the encoding without
   breaking in-flight clients. `next_cursor` is omitted (not `""`)
   on the last page.
6. **Ownership check is an existence check, not a row read.** The new `CheckRunExists(ctx, id, doraUserID) (bool, error)` method runs `SELECT EXISTS(SELECT 1 FROM strategy_runs WHERE id = $1 AND dora_user_id = $2)` and returns a single boolean. The handler maps `false` to 404. The `RunDetail` is never loaded — the decisions endpoint only needs to know whether the run exists for the user, not its columns. **Reason:** a `SELECT EXISTS` is cheaper than a full row read (the planner can short-circuit on the first match and use the primary key), and avoiding the load-and-discard of `EncryptedAPIKey` / `Config` keeps the read path narrow. No other caller of `RunStore` needs row-level access here.
7. **JSON tags on `strategy.Decision`.** The existing struct has no
   JSON tags; today the type is only `pgx.Scan`'d, never marshaled.
   This spec adds snake_case `json:"..."` tags to every field of
   `strategy.Decision` as the single source of truth for the
   response field names. No parallel DTO is introduced.

## Architecture

```
┌──────────┐  GET /v1/trading-decisions/{run_id}?from=&to=&limit=&cursor=
│ Client   │ ─────────────────────────────────────────────────────────►
└──────────┘                                                            │
                                                                       ▼
                                                              ┌─────────────────┐
                                                              │ authedMux       │
                                                              │ (existing)      │
                                                              └─────────────────┘
                                                                       │
                                                                       ▼
                                              ┌──────────────────────────────────┐
                                              │ handleTradingDecisions           │
                                              │   • parse run_id from path       │
                                              │   • resolveDORAUserID            │
                                              │   • runStore.CheckRunExists(id, user) │
                                              │   • parseDecisionsDateFilter     │
                                              │   • parseDecisionCursor          │
                                              │   • decisionStore.ListDecisions  │
                                              │   • writeJSON                    │
                                              └──────────────────────────────────┘
                                                                       │
                                       ┌───────────────────────────────┼───────────────────────┐
                                       ▼                               ▼                       ▼
                              ┌──────────────┐              ┌──────────────────────┐    ┌────────────────┐
                              │ strategy_runs│              │ strategy_decisions   │    │ JSON response  │
                              │  (EXISTS)    │              │  (SELECT …)          │    │ {items,        │
                              │              │              │  ORDER BY DESC,      │    │  next_cursor?} │
                              │              │              │  LIMIT $limit+1)     │    │                │
                              └──────────────┘              └──────────────────────┘    └────────────────┘
```

Components:

- **`strategy/http/run_store.go`** — add `CheckRunExists(ctx, id, doraUserID) (bool, error)` to the `RunStore` interface and implement it on `*PGRunStore`. New SQL: `SELECT EXISTS(SELECT 1 FROM strategy_runs WHERE id = $1 AND dora_user_id = $2)`.
- **`strategy/http/decision_store.go`** — add `ListDecisionsParams`, `Cursor` (with `Encode` / `DecodeCursor`), and `ListDecisions(ctx, params) ([]strategy.Decision, *Cursor, error)`. New SQL: paginated SELECT on `strategy_decisions` with optional `[from, to]` filter, ordered `created_at DESC, seq DESC`, `LIMIT $limit+1`. The `+1` row is the "is there a next page" probe.
- **`strategy/decision.go`** — add `json:"..."` tags to every field of the `Decision` struct (snake_case, matching the column names in the migration).
- **`strategy/http/handler.go`** — add the following functions and register the new route in `newHandler`. The exact dispatcher pattern (mux prefix trim, sub-resource style) will mirror the existing `/v1/backtests/{id}/...` routes.
  - `handleTradingDecisions(w, r)` — top-level dispatcher that pulls `{run_id}` from the path and rejects non-GET with 405.
  - `getRunDecisions(w, r, runID)` — the 6-step handler flow described below.
  - `parseDecisionsDateFilter(r) (from, to *time.Time, err error)` — strict 400 on malformed input.
  - `parseDecisionCursor(r) (cursor *Cursor, err error)` — strict 400 on malformed input.
  - `parseDecisionLimit(r) int` — default 50, silent clamp to `[1, 200]`. Mirrors `parsePagination`'s silent-clamp behaviour: non-integer or non-positive input keeps the default; out-of-range input is clamped silently. There is no way to trigger a 400 from `limit`. Cursor-based pagination does not use the `page` query parameter.
- **`docs/openapi/strategy-server.json`** — add a new path entry. Add a `Decision` schema and a `DecisionList` response schema to `components.schemas`. Bump `info.version` (1.3.0 → 1.4.0).
- **`mcp/tools_strategy.go`** — register a single new tool `list_trading_decisions` that proxies to the strategy-server endpoint.

## Data flow

### Request

```
GET /v1/trading-decisions/{run_id}
    ?from=2026-01-01T00:00:00Z        # optional, RFC3339 or YYYY-MM-DD
    &to=2026-02-01T00:00:00Z          # optional, RFC3339 or YYYY-MM-DD
    &limit=50                         # optional, default 50, clamped to [1, 200]
    &cursor=<opaque>                  # optional, returned by a previous call
```

### Response (200)

```json
{
  "items": [
    {
      "run_id": "…",
      "seq": 1,
      "strategy_type": "mean_reversion",
      "order_book_id": "…",
      "asset": "…",
      "side": "BUY",
      "signal": "buy",
      "kind": "open",
      "quantity": "100",
      "price": "98.5",
      "leverage": "1.0",
      "inverse_leverage": "1.0",
      "from_global_position": false,
      "reason": "z_score_entry",
      "reason_detail": "z=-2.4 crossed entry",
      "client_order_id": "mean_reversion.<run_id_uuid>.<uuidv7>",
      "created_at": "2026-01-15T10:30:00Z"
    }
  ],
  "next_cursor": "eyJ2IjoxLCJ0IjoxNzM1Njg5NjAwMDAwMDAwMDAsInMiOjEwfQ"
}
```

`next_cursor` is omitted on the last page. Items array is `[]` (not `null`) on an empty page — handler always returns a non-nil slice.

### Handler flow

1. Parse `run_id` from the path → 400 on missing/unparseable.
2. `resolveDORAUserID(r.Context())` → 500 on error.
3. `runStore.CheckRunExists(ctx, runID, doraUserID)`:
   - DB error (`err != nil`) → 500.
   - Result is `false` → 404. "Not found" and "wrong user" both map to `EXISTS` returning `false`; the handler does not (and must not) distinguish them.
   - Result is `true` → continue.
4. `parseDecisionsDateFilter(r)` → 400 on malformed `from`/`to`.
5. `parseDecisionCursor(r)` → 400 on malformed `cursor`.
6. `parseLimit(r)` (small wrapper around `parsePagination`'s limit-only logic) — default 50, silent clamp to `[1, 200]`.
7. `decisionStore.ListDecisions(ctx, ListDecisionsParams{…})` → 500 on DB error.
8. Compute `next_cursor` from the probe row, write 200 JSON.

## SQL

### `CheckRunExists`

```sql
SELECT EXISTS(
    SELECT 1
    FROM strategy_runs
    WHERE id = $1 AND dora_user_id = $2
)
```

Uses the `strategy_runs` primary key on `id`; the `dora_user_id` filter
is a residual predicate. No new index required.

### `ListDecisions`

```sql
SELECT run_id, seq, strategy_type, order_book_id, asset,
       side, signal, kind, quantity, price, leverage, inverse_leverage,
       from_global_position, reason, reason_detail, client_order_id, created_at
FROM strategy_decisions
WHERE run_id = $1
  AND ($2::TIMESTAMP IS NULL OR created_at >= $2)
  AND ($3::TIMESTAMP IS NULL OR created_at <= $3)
  AND ($4::TIMESTAMP IS NULL
       OR created_at < $4
       OR (created_at = $4 AND seq < $5))
ORDER BY created_at DESC, seq DESC
LIMIT $6
```

`$2`/`$3` are the `from`/`to` bounds. `$4`/`$5` are the cursor's
`(created_at, seq)`. `$6` is `limit + 1`. The result has at most
`limit` items in the response; the extra row determines whether
`next_cursor` is set. If the query returns `<= limit` rows, the
response omits `next_cursor`.

Both queries use the existing indexes:
`strategy_runs_pkey` (id) and `idx_strategy_decisions_run_id_created_at`. No new migration.

## Error handling

| HTTP | When | Body |
|------|------|------|
| 200  | Success (zero or more items) | `{items, next_cursor?}` |
| 400  | `run_id` missing/unparseable, `from`/`to` unparseable, `cursor` unparseable | `{error}` (existing `writeError` shape) |
| 401  | Missing/invalid auth (handled by `authedMux`) | `{error}` |
| 404  | Run not found OR run exists but belongs to a different user (collapsed) | `{error: "run not found"}` |
| 405  | Non-GET on the path | `{error}` |
| 500  | DB error from `CheckRunExists` or `ListDecisions` | `{error}` (also `slog.Error`) |

The 404 collapse is deliberate: distinguishing "no such run" from
"run belongs to another user" would leak which run IDs exist. Both
map to the same body and the same predicate outcome (`EXISTS` returns
`false` in both cases).

## Testing

- **`PGDecisionStore.ListDecisions`** (table-driven, test DB):
  - Empty result (run exists, zero decisions).
  - Single page, fewer than `limit` rows → no `next_cursor`.
  - Single page, exactly `limit` rows → no `next_cursor` (the `+1` row is absent).
  - Multi-page → first page returns `limit` items and a `next_cursor`; following the cursor returns the remaining items; the final page has no `next_cursor`.
  - `from` inclusive boundary.
  - `to` inclusive boundary.
  - `from > to` → empty result.
  - Cursor ordering invariant: `(created_at, seq)` strictly decreases across pages.
- **`PGRunStore.CheckRunExists`**: present → `true`; missing run → `false`; wrong user → `false`. Only the DB error path is non-nil; the absence of a row is a normal `false` return.
- **`Cursor.Encode` / `DecodeCursor`**: round-trip; bad base64 → error; bad JSON → error; unknown version → error.
- **`parseDecisionsDateFilter`**: RFC3339 accepted, `YYYY-MM-DD` accepted, garbage rejected, `from > to` accepted (the store handles ordering; the parser is purely lexical).
- **`parseDecisionCursor`**: valid cursor accepted, bad base64 rejected, bad JSON rejected, wrong version rejected.
- **Handler `getRunDecisions`** (`httptest`): ownership 404, bad run_id 400, bad date 400, bad cursor 400, bad limit 400, success path with fixture data, last-page response omits `next_cursor`.
- **MCP tool**: mocks the strategy-server HTTP client, asserts the request URL/shape and the response plumbing. The MCP tool does no validation of its own beyond passing through to the strategy-server.
- **JSON tags**: a marshal round-trip test on `strategy.Decision` to lock the field names; prevents a future tag-rename from silently breaking the API contract.

No live integration tests beyond the table-driven DB tests; the
WebSocket / live-run path is unrelated to this work.

## File-by-file change list

| File | Change |
|------|--------|
| `strategy/decision.go` | Add snake_case `json:"..."` tags to all fields of `Decision`. |
| `strategy/http/run_store.go` | Add `CheckRunExists` to `RunStore` interface and to `PGRunStore`. |
| `strategy/http/decision_store.go` | Add `ListDecisionsParams`, `Cursor` (`Encode` / `DecodeCursor`), `ListDecisions`. |
| `strategy/http/handler.go` | Add `handleTradingDecisions`, `getRunDecisions`, `parseDecisionsDateFilter`, `parseDecisionCursor`, `parseLimit`. Register route. |
| `strategy/http/decision_store_test.go` *(new)* | Tests for `ListDecisions`, `Cursor`, `parseDecisionsDateFilter`, `parseDecisionCursor`. |
| `strategy/http/handler_test.go` *(existing, extend)* | Handler tests for the new endpoint. |
| `strategy/http/run_store_test.go` *(new or extend existing)* | `CheckRunExists` tests. |
| `docs/openapi/strategy-server.json` | Add path, add `Decision` and `DecisionList` schemas, bump version. |
| `mcp/tools_strategy.go` | Register `list_trading_decisions` tool. |
| `mcp/server_test.go` *(existing, extend)* | Test for the new MCP tool. |

## Out of scope

- **Backtest decisions.** The `strategy_decisions` table is
  populated exclusively by the live run path; the migration
  explicitly excludes backtests. The DoD says "for each run", and
  this spec honours that.
- **Updating the write path.** Already complete.
- **Filtering by `kind`, `side`, `reason`, etc.** Possible future
  extension; not in the DoD. The full `Decision` payload is returned
  in every response so a client can filter client-side.
- **Streaming / SSE.** REST only.
- **WebSocket / notification fanout.** Not asked for.

## Acceptance criteria

- `GET /v1/trading-decisions/{run_id}` returns 200 with
  `{items, next_cursor?}` for any run owned by the authenticated user.
- 404 for missing or wrong-user run; both cases return the same body.
- Pagination round-trips correctly: walking the cursor exhausts the
  result set with no duplicates and no gaps.
- `from`/`to` filtering is inclusive on both ends; a malformed
  `from` or `to` produces 400 (not a silent widening of the result
  set).
- OpenAPI doc at `docs/openapi/strategy-server.json` describes the
  new path with the new `Decision` and `DecisionList` schemas.
- MCP server exposes `list_trading_decisions` with the same
  parameters and proxies the call to the strategy-server.
- The `Decision` struct's JSON tags are locked by a marshal test
  (no field is ever emitted under both `Foo` and `foo`).
- All new and modified code passes `go test ./...`,
  `golangci-lint run`, and the pre-commit hook suite.
