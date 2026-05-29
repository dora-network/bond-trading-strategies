---
name: dora-strategy-service
description: Interact with the DORA Strategy Service REST API — backtest strategies, manage live runs, discover strategies and order books, and query benchmark tenors. Use when creating or inspecting backtests, starting/pausing/stopping live strategy runs, listing available strategies, or managing strategy lifecycle.
allowed-tools: Bash, curl
---

# DORA Strategy Service API

REST API at the URL specified in the `DORA_STRATEGIES_URL` environment variable (e.g. `http://localhost:8081`) for asynchronous strategy backtests, live strategy runs, and strategy discovery. Falls back to `.env` in the current working directory if the environment variable is not set.

## Authentication

All endpoints except `/healthz` require an API key from the `DORA_API_KEY` environment variable (with fallback to `.env` in the current working directory):

```
Authorization: ApiKey <value of DORA_API_KEY>
```

The key is validated against the DORA API. The server responds `401 Unauthorized` when the header is missing or invalid.

## Health

```
GET /healthz
```

Exempt from authentication. Returns `{"ok": true}` on success.

## Discovery

### List Strategies

```
GET /v1/strategies
```

Returns supported strategies and their capabilities.

**Example response:**
```json
{
  "items": [
    {
      "type": "mean_reversion",
      "status": "available",
      "description": "Rolling z-score mean reversion strategy.",
      "config_fields": [
        {"name": "lookback_window", "type": "integer", "description": "Rolling observation window. Must be at least 2.", "required": false, "default": 20},
        {"name": "entry_z_score", "type": "number", "description": "Entry threshold for opening positions. Must be greater than 0.", "required": false, "default": 2.0},
        {"name": "exit_z_score", "type": "number", "description": "Exit threshold for closing positions as spreads revert. Must be non-negative.", "required": false, "default": 0.5},
        {"name": "stop_loss_z_score", "type": "number", "description": "Stop-loss threshold for closing losing positions. Must be non-negative.", "required": false, "default": 3.5},
        {"name": "min_std_dev", "type": "number", "description": "Minimum spread volatility required before trading. Must be non-negative.", "required": false, "default": 0.0005},
        {"name": "max_position_size", "type": "number", "description": "Maximum fraction of capital allocated per trade. Must be in (0,1].", "required": false, "default": 1.0},
        {"name": "order_book_id", "type": "string(uuid)", "description": "Order book UUID used to locate the traded asset and place orders.", "required": false},
        {"name": "tenor", "type": "string", "description": "Benchmark Treasury tenor, for example 1M, 6M, 2Y, 5Y, 10Y, or 30Y.", "required": false},
        {"name": "initial balance", "type": "number", "description": "Maximum total position amount. Must be greater than 0.", "required": false, "default": 1.0},
        {"name": "leverage", "type": "number", "description": "Leverage multiplier for live orders. Must be greater than 0.", "required": false, "default": 1.0}
      ],
      "supports_run": true,
      "supports_backtest": true
    },
    {
      "type": "copytrading",
      "status": "not_implemented",
      "description": "Copy trades from a followed trader subject to limits.",
      "config_fields": [
        {"name": "followed_trader", "type": "string(uuid)", "description": "Trader UUID to mirror.", "required": true},
        {"name": "min_order_size", "type": "integer", "description": "Minimum copied order size.", "required": false},
        {"name": "max_order_size", "type": "integer", "description": "Maximum copied order size.", "required": false},
        {"name": "allowed_bonds", "type": "array[string(uuid)]", "description": "Optional allowlist of bond UUIDs. Empty means all bonds are eligible.", "required": false}
      ],
      "supports_run": false,
      "supports_backtest": false
    }
  ]
}
```

### List Benchmark Tenors

```
GET /v1/tenors
```

Returns supported Treasury benchmark tenors.

**Example response:**
```json
{
  "items": [
    {"code": "1M", "description": "1-month"},
    {"code": "6M", "description": "6-month"},
    {"code": "1Y", "description": "1-year"},
    {"code": "2Y", "description": "2-year"},
    {"code": "5Y", "description": "5-year"},
    {"code": "10Y", "description": "10-year"},
    {"code": "30Y", "description": "30-year"}
  ]
}
```

### List DORA Order Books

```
GET /v1/dora/orderbooks
```

Lists order books available to the configured API key.

**Example response:**
```json
{
  "items": [
    {
      "id": "uuid",
      "display_name": "Bond/UST",
      "base_asset_id": "uuid",
      "quote_asset_id": "uuid",
      "status": "active"
    }
  ]
}
```

### Get Current DORA User

```
GET /v1/dora/user
```

Returns the DORA user ID for the configured API key.

**Example response:**
```json
{"id": "user-uuid"}
```

### OpenAPI Spec

```
GET /v1/openapi
```

Returns the full OpenAPI 3.1 specification as JSON. Exempt from authentication.

## Backtests

Backtests run asynchronously. Create one, poll its status, then retrieve results.

### Create Backtest

```
POST /v1/backtests
```

**Request body:**
```json
{
  "strategy_type": "mean_reversion",
  "config": {
    "lookback_window": 20,
    "entry_z_score": 2.0,
    "exit_z_score": 0.5,
    "stop_loss_z_score": 3.5,
    "min_std_dev": 0.0005,
    "max_position_size": 1.0,
    "order_book_id": "uuid",
    "tenor": "10Y",
    "initial_balance": 10000.0,
    "leverage": 1.0
  },
  "start": "2025-01-01T00:00:00Z",
  "end": "2025-06-01T00:00:00Z"
}
```

Responds `202 Accepted` with the backtest detail containing its `id`.

**Statuses:** `running` → `completed` / `failed` / `cancelled`

### List Backtests

```
GET /v1/backtests?page=1&limit=10&status=running,completed&from=2025-01-01&to=2025-06-01
```

Query parameters:
- `page` — Page number (default 1)
- `limit` — Items per page (default 10, max 50)
- `status` — Comma-separated filter: `running`, `completed`, `failed`, `cancelled`
- `from` / `to` — Date range filter (RFC3339 or YYYY-MM-DD)

### Get Backtest Result

```
GET /v1/backtests/{id}
```

Returns a flattened result summary:
```json
{
  "total_pnl": "1234.56",
  "win_count": 12,
  "loss_count": 3,
  "max_drawdown": "-0.05",
  "sharpe_ratio": "1.8",
  "strategy_type": "mean_reversion",
  "status": "completed",
  "config": { "...": "..." },
  "asset_name": "Bond",
  "asset_symbol": "BOND",
  "error": ""
}
```

### Get Backtest Metadata (lightweight)

```
GET /v1/backtests/{id}/metadata
```

Returns `BacktestSummary` (id, status, timestamps, config) without the heavy result data. Use this to check status or get a backtest ID.

### Get Backtest Trades

```
GET /v1/backtests/{id}/trades?page=1&limit=10
```

Paginated trade records with spread, z-score, price, and signal at each observation.

### Get Backtest Closed Trades

```
GET /v1/backtests/{id}/closed-trades?page=1&limit=10
```

Paginated closed trades with P&L, entry/exit spread, z-scores, and exit reason.

### Cancel Backtest

```
DELETE /v1/backtests/{id}
```

Marks the backtest as `cancelled` and returns updated metadata.

## Live Strategy Runs

Run strategies in real-time against live market data.

### Create Run

```
POST /v1/runs
```

**Request body:**
```json
{
  "strategy_type": "mean_reversion",
  "config": {
    "lookback_window": 20,
    "entry_z_score": 2.0,
    "order_book_id": "uuid",
    "tenor": "10Y",
    "initial_balance": 0,
    "leverage": 1.0
  }
}
```

- `initial_balance` can be `0` for live runs — balance is obtained from DORA positions.
- A user may only have **one** running or paused strategy per order book. Creating a second returns `409 Conflict`.

Responds `201 Created` with the run detail.

**Statuses:** `running` → `paused` | `stopped`

### List Runs

```
GET /v1/runs
```

Returns all runs for the authenticated user as an array of `RunSummary` objects.

### Get Run

```
GET /v1/runs/{id}
```

Returns full `RunDetail` including config and any error message.

### Stop Run

```
DELETE /v1/runs/{id}
```

Stops a running or paused strategy. Returns the updated `RunDetail` with status `stopped`.

### Pause Run

```
POST /v1/runs/{id}/pause
```

Pauses a running strategy. Returns `409 Conflict` if already stopped.

### Resume Run

```
POST /v1/runs/{id}/resume
```

Resumes a paused strategy. Also works for runs that were persisted to the database on a previous server lifecycle (e.g., after restart). Returns `409 Conflict` if already stopped.

## Strategy Config Reference

### Mean Reversion

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `lookback_window` | integer | 20 | Rolling observation window (≥2) |
| `entry_z_score` | number | 2.0 | Entry threshold (>0) |
| `exit_z_score` | number | 0.5 | Exit threshold (≥0) |
| `stop_loss_z_score` | number | 3.5 | Stop-loss threshold (≥0) |
| `min_std_dev` | number | 0.0005 | Min spread volatility to trade (≥0) |
| `max_position_size` | number | 1.0 | Max fraction of capital per trade (0,1] |
| `order_book_id` | string(uuid) | — | DORA order book UUID |
| `tenor` | string | — | Benchmark Treasury tenor (1M, 6M, 2Y, 5Y, 10Y, 30Y) |
| `initial_balance` | number | 1.0 | Starting capital (backtests); 0 allowed for live runs |
| `leverage` | number | 1.0 | Leverage multiplier (>0) |

### Copy Trading *(not yet implemented)*

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `followed_trader` | string(uuid) | yes | Trader UUID to mirror |
| `min_order_size` | integer | no | Minimum copied order size |
| `max_order_size` | integer | no | Maximum copied order size |
| `allowed_bonds` | array(uuid) | no | Allowlist of bond UUIDs |

## Error Responses

All errors return HTTP status codes with a JSON body:

```json
{"error": "description of the problem"}
```

| Status | Meaning |
|--------|---------|
| `400` | Invalid request (bad config, missing field, validation error) |
| `401` | Missing or invalid Authorization header |
| `404` | Resource not found |
| `409` | Conflict (e.g. another run already active for this order book) |
| `501` | Strategy not implemented for the requested operation |
| `500` | Internal server error |

## Rate Limiting

The strategy server applies per-user and per-IP rate limiting:

- **Read:** 20 RPS per user, burst 40
- **Write** (create/delete/pause/resume): 2 RPS per user, burst 5
- **Global:** 100 RPS
- **Per-IP:** 30 RPS, burst 60

Rate limiting is configurable server-side via environment variables.

## cURL Examples

```bash
# Load .env from current directory if variables aren't already set
if [ -z "${DORA_STRATEGIES_URL:-}" ] || [ -z "${DORA_API_KEY:-}" ]; then
  [ -f .env ] && set -a && . .env && set +a
fi

# Variables
BASE="${DORA_STRATEGIES_URL:?DORA_STRATEGIES_URL is not set. Set it or add it to .env}"
AUTH="Authorization: ApiKey ${DORA_API_KEY:?DORA_API_KEY is not set. Set it or add it to .env}"

# Health check
curl -s "$BASE/healthz"

# List strategies
curl -s -H "$AUTH" "$BASE/v1/strategies"

# List tenors
curl -s -H "$AUTH" "$BASE/v1/tenors"

# List order books
curl -s -H "$AUTH" "$BASE/v1/dora/orderbooks" | jq '.items[].display_name'

# Get current user
curl -s -H "$AUTH" "$BASE/v1/dora/user"

# Create a backtest
curl -s -X POST -H "$AUTH" -H "Content-Type: application/json" \
  -d '{
    "strategy_type": "mean_reversion",
    "config": {
      "lookback_window": 20,
      "entry_z_score": 2.0,
      "order_book_id": "<uuid>",
      "tenor": "10Y",
      "initial_balance": 10000
    },
    "start": "2025-01-01T00:00:00Z",
    "end": "2025-06-01T00:00:00Z"
  }' \
  "$BASE/v1/backtests"

# Check backtest status
curl -s -H "$AUTH" "$BASE/v1/backtests/<id>/metadata" | jq '{id, status}'

# Get backtest result
curl -s -H "$AUTH" "$BASE/v1/backtests/<id>" | jq '{total_pnl, win_count, loss_count, sharpe_ratio}'

# Get backtest trades
curl -s -H "$AUTH" "$BASE/v1/backtests/<id>/trades?limit=5" | jq '.items[:3]'

# Get backtest closed trades
curl -s -H "$AUTH" "$BASE/v1/backtests/<id>/closed-trades?page=1&limit=5"

# Cancel backtest
curl -s -X DELETE -H "$AUTH" "$BASE/v1/backtests/<id>"

# List runs
curl -s -H "$AUTH" "$BASE/v1/runs" | jq '.items[].status'

# Create a live run
curl -s -X POST -H "$AUTH" -H "Content-Type: application/json" \
  -d '{
    "strategy_type": "mean_reversion",
    "config": {
      "lookback_window": 20,
      "entry_z_score": 2.0,
      "order_book_id": "<uuid>",
      "tenor": "10Y",
      "leverage": 1.0
    }
  }' \
  "$BASE/v1/runs"

# Get run detail
curl -s -H "$AUTH" "$BASE/v1/runs/<id>"

# Pause a run
curl -s -X POST -H "$AUTH" "$BASE/v1/runs/<id>/pause"

# Resume a run
curl -s -X POST -H "$AUTH" "$BASE/v1/runs/<id>/resume"

# Stop a run
curl -s -X DELETE -H "$AUTH" "$BASE/v1/runs/<id>"
```

## Tips

- Backtests are **asynchronous** — create first, then poll `/metadata` or `/` until `status` is `completed`, `failed`, or `cancelled`.
- Use `/v1/backtests/{id}/metadata` instead of `/v1/backtests/{id}` when you only need the status — it avoids loading the full result.
- A user can only have **one** active (running or paused) strategy per order book.
- `initial_balance` is **required** for backtests (must be > 0) but can be `0` or omitted for live runs (balance comes from DORA positions).
- The `/v1/openapi` endpoint returns the full OpenAPI 3.1 spec — it's exempt from auth.
- All monetary values are returned as string-encoded decimals, never floats.
- Set `DORA_STRATEGIES_URL` and `DORA_API_KEY` as environment variables, or add them to a `.env` file in the current directory (the curl examples auto-source `.env` as a fallback).
