# bond-trading-strategies

Bond trading strategy services for [DORA](https://dora.co). This repository provides three
runnable applications that connect DORA's market data pipeline with algorithmic trading
strategies and a Model Context Protocol (MCP) interface for AI agents.

- **`price-daemon`** — streams live market data from DORA WebSocket APIs and persists
  prices and candles into Postgres.
- **`strategy-server`** — exposes a REST/HTTP API for strategy discovery, backtesting,
  and live strategy run management.
- **`mcp-server`** — exposes both strategy and FRED capabilities over the Model Context
  Protocol (HTTP/SSE) for AI clients (e.g. Claude, Cline, Pi).

## Services

---

### `price-daemon`

Streams live price and candle data from DORA WebSocket APIs and persists them to Postgres.
Serves a health endpoint for monitoring stream health, write latency, and database
connectivity.

#### Flags

| Flag                      | Env var                  | Default                 | Description                                                |
| ------------------------- | ------------------------ | ----------------------- | ---------------------------------------------------------- |
| `-ws-url`                 | `WS_URL`                 | `wss://staging.dora.co` | WebSocket base URL                                         |
| `-db-url`                 | `DATABASE_URL`           | —                       | Postgres connection string **(required)**                  |
| `-api-key`                | `API_KEY`                | —                       | DORA API key                                               |
| `-asset-id`               | `ASSET_ID`               | —                       | Filter to a single asset UUID                              |
| `-order-books`            | `ORDER_BOOK_IDS`         | —                       | Comma-separated order book IDs for candle streaming        |
| `-since`                  | —                        | —                       | RFC3339 lower bound for candle history backfill            |
| `-reconnect-delay`        | —                        | `5s`                    | Delay between WebSocket reconnect attempts                 |
| `-http-addr`              | `HTTP_ADDR`              | `:8080`                 | HTTP listen address for the health server                  |
| `-health-stale-after`     | `HEALTH_STALE_AFTER`     | `1m`                    | Age after which stream/write activity is considered stale  |
| `-health-startup-grace`   | `HEALTH_STARTUP_GRACE`   | `10s`                   | Grace period after startup before health requires activity |
| `-health-db-ping`         | `HEALTH_DB_PING`         | `true`                  | Enable database ping in health endpoint                    |
| `-health-db-ping-timeout` | `HEALTH_DB_PING_TIMEOUT` | `2s`                    | Database ping timeout                                      |

#### HTTP Endpoints

| Method | Path       | Description                                                               |
| ------ | ---------- | ------------------------------------------------------------------------- |
| `GET`  | `/healthz` | Health check — reports stream health, write activity, and DB connectivity |

#### Run locally

```bash
export DATABASE_URL=postgres://user:pass@localhost:5432/dora
export DORA_API_KEY=your_dora_api_key

go run ./cmd/price-daemon \
  -ws-url wss://dev.dora.co \
  -db-url "$DATABASE_URL" \
  -api-key "$DORA_API_KEY" \
  -http-addr :8080
```

#### With candle streaming

```bash
go run ./cmd/price-daemon \
  -ws-url wss://dev.dora.co \
  -db-url "$DATABASE_URL" \
  -api-key "$DORA_API_KEY" \
  -order-books "order-book-id-1,order-book-id-2" \
  -since "2025-01-01T00:00:00Z" \
  -http-addr :8080
```

---

### `strategy-server`

Strategy REST/HTTP server that exposes available trading strategies, runs asynchronous
backtests, and manages live strategy runs. It subscribes to the live DORA price stream
so that restored or running strategies can continue trading. After a restart it
automatically restores persisted runs and backtests from Postgres.

#### Flags

| Flag               | Env var                  | Default             | Description                                |
| ------------------ | ------------------------ | ------------------- | ------------------------------------------ |
| `-addr`            | `ADDR`                   | `:8081`             | HTTP listen address                        |
| `-db-url`          | `DATABASE_URL`           | —                   | Postgres connection string **(required)**  |
| `-ws-url`          | `WS_URL`                 | `wss://dev.dora.co` | WebSocket base URL for live price feed     |
| `-api-key`         | `WS_API_KEY` / `API_KEY` | —                   | DORA API key for WebSocket price feed      |
| `-dora-base-url`   | `DORA_BASE_URL`          | —                   | DORA HTTP base URL                         |
| `-fred-api-key`    | `FRED_API_KEY`           | —                   | FRED API key (used internally)             |
| `-reconnect-delay` | —                        | `5s`                | Delay between WebSocket reconnect attempts |

#### HTTP Endpoints

| Method   | Path                   | Description                                      |
| -------- | ---------------------- | ------------------------------------------------ |
| `GET`    | `/healthz`             | Health check                                     |
| `GET`    | `/v1/strategies`       | List available strategies and their capabilities |
| `GET`    | `/v1/backtests`        | List all backtests                               |
| `POST`   | `/v1/backtests`        | Create a new backtest                            |
| `GET`    | `/v1/backtests/{id}`   | Get a specific backtest by ID                    |
| `DELETE` | `/v1/backtests/{id}`   | Cancel / delete a backtest                       |
| `GET`    | `/v1/runs`             | List all strategy runs                           |
| `POST`   | `/v1/runs`             | Create a new live strategy run                   |
| `GET`    | `/v1/runs/{id}`        | Get a specific run by ID                         |
| `DELETE` | `/v1/runs/{id}`        | Stop and delete a run                            |
| `POST`   | `/v1/runs/{id}/pause`  | Pause a running strategy                         |
| `POST`   | `/v1/runs/{id}/resume` | Resume a paused strategy                         |

#### Run locally

```bash
export DATABASE_URL=postgres://user:pass@localhost:5432/dora
export DORA_API_KEY=your_dora_api_key

go run ./cmd/strategy-server \
  -addr :8081 \
  -db-url "$DATABASE_URL" \
  -ws-url wss://dev.dora.co \
  -api-key "$DORA_API_KEY"
```

#### Example — create a strategy run

```bash
curl -X POST http://localhost:8081/v1/runs \
  -H 'Content-Type: application/json' \
  -d '{
    "strategy_type": "mean_reversion",
    "config": {
      "lookback_window": 20,
      "entry_z_score": 2.0,
      "exit_z_score": 0.5
    }
  }'
```

#### Example — create a backtest

```bash
curl -X POST http://localhost:8081/v1/backtests \
  -H 'Content-Type: application/json' \
  -d '{
    "strategy_type": "mean_reversion",
    "config": {
      "lookback_window": 20,
      "entry_z_score": 2.0,
      "exit_z_score": 0.5
    },
    "start": "2025-01-01T00:00:00Z",
    "end": "2025-03-01T00:00:00Z"
  }'
```

#### Run persistence

- Live run metadata is stored in the Postgres `strategy_runs` table.
- Runs with status `running` are auto-restored after server restart.
- Runs with status `paused` are restored as paused and can be resumed later.
- Runs with status `stopped` remain in history and are not restarted.
- Strategy state is rebuilt from saved config, not from an exact in-memory snapshot.

#### OpenAPI specification

`docs/openapi/strategy-server.json`

---

### `mcp-server`

Model Context Protocol (MCP) server that exposes the strategy system and FRED
economic data over standard MCP HTTP/SSE transport. Designed for AI agents
(e.g. Claude, Cline, Pi) to discover and interact with bond trading strategies
and yield-curve data.

#### Flags

| Flag                 | Env var             | Default                 | Description                                    |
| -------------------- | ------------------- | ----------------------- | ---------------------------------------------- |
| `-addr`              | —                   | `:8080`                 | TCP address to listen on                       |
| `-base-url`          | —                   | `http://localhost:8080` | Externally-reachable base URL                  |
| `-strategy-base-url` | `STRATEGY_BASE_URL` | —                       | Base URL of the strategy-server **(required)** |
| `-fred-api-key`      | `FRED_API_KEY`      | —                       | FRED API key                                   |

The `DORA_API_KEY` environment variable is **always required** — the server
will not start without it.

#### HTTP Endpoints (MCP transport)

| Method | Path       | Description                                 |
| ------ | ---------- | ------------------------------------------- |
| `GET`  | `/sse`     | SSE event stream — MCP client connects here |
| `POST` | `/message` | JSON-RPC message endpoint                   |

#### MCP Tools

**Strategy tools** (proxied to `strategy-server`):

| Tool                       | Description                                    |
| -------------------------- | ---------------------------------------------- |
| `strategy_list`            | List available strategies and capabilities     |
| `strategy_run_create`      | Create a new live strategy run                 |
| `strategy_run_get`         | Get a strategy run by ID (raw JSON)            |
| `strategy_run_list`        | List strategy runs (raw JSON)                  |
| `strategy_run_status`      | Natural-language summary of current runs       |
| `strategy_run_describe`    | Natural-language description of a specific run |
| `strategy_run_pause`       | Pause a running strategy                       |
| `strategy_run_resume`      | Resume a paused strategy                       |
| `strategy_run_stop`        | Stop a strategy run                            |
| `strategy_backtest_create` | Create an asynchronous backtest                |
| `strategy_backtest_get`    | Get a backtest by ID                           |
| `strategy_backtest_list`   | List all backtests                             |
| `strategy_backtest_cancel` | Cancel a backtest                              |

**FRED tools** (require `FRED_API_KEY`):

| Tool                           | Description                                                             |
| ------------------------------ | ----------------------------------------------------------------------- |
| `fred_fetch_series`            | Fetch daily US Treasury CMT yield observations from FRED                |
| `fred_fetch_latest`            | Fetch the single most-recent valid yield observation                    |
| `fred_fetch_yield_curve`       | Fetch the full US Treasury yield curve (all 11 tenors) for a given date |
| `fred_fetch_historical_yields` | Fetch a historical time-series of yields for a single tenor             |
| `fred_interpolate_yield`       | Linearly interpolate a yield at an arbitrary tenor                      |
| `fred_benchmark_yield`         | Return the interpolated benchmark yield given a bond maturity           |

#### Run locally

```bash
export DORA_API_KEY=your_dora_api_key
export FRED_API_KEY=your_fred_api_key

go run ./cmd/mcp-server \
  -addr :8080 \
  -base-url http://localhost:8080 \
  -strategy-base-url http://localhost:8081 \
  -fred-api-key "$FRED_API_KEY"
```

> **Note:** `strategy-server` must be running and reachable at the URL passed
> to `-strategy-base-url`. If `FRED_API_KEY` is omitted, FRED tools will
> return errors; strategy tools still work.

---

## Docker

A multi-stage Docker image is provided in the [`Dockerfile`](./Dockerfile).
The image builds all three binaries (`mcp-server`, `strategy-server`, `price-daemon`)
and bundles them in a minimal `alpine:3.21` runtime image.

### Image contents

```
/app
├── mcp-server        # MCP HTTP/SSE server (default entrypoint)
├── strategy-server   # REST strategy server
└── price-daemon      # Market data daemon
```

The default entrypoint is `mcp-server` listening on `:8080`. To run a different
service, override the entrypoint at runtime.

### Build the image

```bash
# Create a GitHub token file for private module access
mkdir -p .secrets
printf '%s' 'your_github_token' > .secrets/github_token

docker build \
  --secret id=github_token,src=.secrets/github_token \
  -t bond-trading-strategies:latest \
  .
```

> **Why a GitHub token?** The project depends on `github.com/dora-network/dora-client-go`,
> a private Go module. Docker builds use `--secret` to pass the token securely
> without embedding it in the image layers.

### Run individual services

**mcp-server** (the default):

```bash
docker run -p 8080:8080 \
  -e DORA_API_KEY=your_dora_key \
  -e FRED_API_KEY=your_fred_key \
  -e STRATEGY_BASE_URL=http://host.docker.internal:8081 \
  bond-trading-strategies:latest
```

**strategy-server:**

```bash
docker run -p 8081:8081 \
  -e DATABASE_URL=postgres://user:pass@host.docker.internal:5432/dora \
  -e DORA_API_KEY=your_dora_key \
  --entrypoint /app/strategy-server \
  bond-trading-strategies:latest \
  -addr :8081 \
  -db-url "$DATABASE_URL" \
  -ws-url wss://dev.dora.co \
  -api-key "$DORA_API_KEY"
```

**price-daemon:**

```bash
docker run -p 8080:8080 \
  -e DATABASE_URL=postgres://user:pass@host.docker.internal:5432/dora \
  -e DORA_API_KEY=your_dora_key \
  --entrypoint /app/price-daemon \
  bond-trading-strategies:latest \
  -ws-url wss://dev.dora.co \
  -db-url "$DATABASE_URL" \
  -api-key "$DORA_API_KEY" \
  -http-addr :8080
```

### Docker Compose

The [`docker-compose.yml`](./docker-compose.yml) orchestrates `strategy-server`
and `mcp-server` together. It handles networking, environment, and the GitHub
token secret automatically.

#### Compose services

| Service           | Port   | Image                                |
| ----------------- | ------ | ------------------------------------ |
| `strategy-server` | `8081` | `bond-trading-strategies-mcp:latest` |
| `mcp-server`      | `8080` | `bond-trading-strategies-mcp:latest` |

#### Required environment variables

| Variable       | Description                                                                             |
| -------------- | --------------------------------------------------------------------------------------- |
| `DATABASE_URL` | Postgres connection string (e.g. `postgres://user:pass@host.docker.internal:5432/dora`) |
| `DORA_API_KEY` | DORA API key for WebSocket price streaming                                              |
| `FRED_API_KEY` | FRED API key (optional for strategy tools, required for FRED tools)                     |

#### Start

```bash
export DATABASE_URL=postgres://user:pass@host.docker.internal:5432/dora
export DORA_API_KEY=your_dora_api_key
export FRED_API_KEY=your_fred_api_key
mkdir -p .secrets
printf '%s' 'your_github_token' > .secrets/github_token

docker compose up --build
```

Or via the Makefile:

```bash
make compose-up
```

#### Stop

```bash
docker compose down
# or
make compose-down
```

#### Notes

- `mcp-server` talks to `strategy-server` over the internal compose network at
  `http://strategy-server:8081` by default. Override with `STRATEGY_BASE_URL`.
- `price-daemon` is **not** included in the compose file by default — it can
  be added manually or run as a standalone container.
- The `mcp-server` entrypoint is the Docker image's default; the compose file
  overrides it for `strategy-server`.
- The `.secrets/github_token` file is required at build time for private Go
  module access but is **not** included in the final image layers.

---

## Development

### Prerequisites

| Requirement  | Notes                                            |
| ------------ | ------------------------------------------------ |
| Go 1.26+     | Required for local development and `go run`      |
| Postgres     | Required by `price-daemon` and `strategy-server` |
| DORA API key | Required for live WebSocket price streaming      |
| FRED API key | Required only for FRED-backed MCP tools          |

### Database and migrations

This repository uses PostgreSQL with [`tern`](https://github.com/jackc/tern)
migrations under `migrations/`.

Current schema includes:

- `price_history` — tick-level price data
- `candles_history` — OHLCV candle aggregates
- `strategy_runs` — persisted strategy run metadata and configuration

### Repository layout

```text
bond-trading-strategies/
├── cmd/
│   ├── mcp-server/          # MCP HTTP/SSE server entrypoint
│   ├── price-daemon/        # Market data daemon entrypoint
│   └── strategy-server/     # REST strategy server entrypoint
├── candles/                 # Candle streaming, storage, and handlers
├── docs/                    # Documentation and OpenAPI specs
├── fred/                    # FRED API client for US Treasury yields
├── mcp/                     # MCP server implementation and tool definitions
├── migrations/              # Postgres schema migrations (tern)
├── prices/                  # Price streaming, storage, and handlers
├── streams/                 # WebSocket stream framework (reconnect, lifecycle)
├── strategy/                # Core strategy engine
│   ├── config/              # Strategy configuration types
│   ├── copytrading/         # Copy-trading strategy implementation
│   ├── http/                # HTTP handler, auth, backtest/run stores
│   ├── meanreversion/       # Mean-reversion strategy implementation
│   ├── types/               # Shared strategy types
│   └── window/              # Rolling window data structures
├── testutils/               # Shared test utilities
├── Dockerfile               # Multi-stage Docker image
├── docker-compose.yml       # Compose orchestration
├── Makefile                 # Build, run, and compose targets
└── go.mod / go.sum          # Go module dependencies
```

### Makefile targets

| Target                  | Description                                              |
| ----------------------- | -------------------------------------------------------- |
| `help`                  | Print available targets                                  |
| `compose-up`            | Build and start Docker Compose services in detached mode |
| `compose-down`          | Stop Docker Compose services                             |
| `start-price-daemon`    | Run price-daemon locally via `go run`                    |
| `start-strategy-server` | Run strategy-server locally via `go run`                 |
| `start-mcp-server`      | Run mcp-server locally via `go run`                      |

Environment variables are sourced from a `.env` file if one exists (see the
Makefile include at the top).

### Typical local workflow

Start the applications in order:

1. **price-daemon** — populates and maintains market data in Postgres.
2. **strategy-server** — makes backtests and live runs available over HTTP.
3. **mcp-server** — exposes the system to AI/MCP clients.

```bash
make start-price-daemon
make start-strategy-server
make start-mcp-server
```

### Running tests

```bash
go test ./...
```

Run a focused package:

```bash
go test ./mcp/...
go test ./strategy/...
```

### Building binaries

```bash
go build -trimpath -o mcp-server ./cmd/mcp-server
go build -trimpath -o strategy-server ./cmd/strategy-server
go build -trimpath -o price-daemon ./cmd/price-daemon
```

---

## Architecture overview

```
┌──────────────┐     DORA WebSocket      ┌───────────────┐
│  price-daemon │ ◄────────────────────── │  DORA Platform │
│  (health:8080)│       price feed        │               │
└──────┬───────┘                          └───────────────┘
       │ writes
       ▼
┌──────────────┐
│   Postgres   │
│  (price/candle│
│   history,    │
│   runs,       │
│   backtests)  │
└───┬───┬──────┘
    │   │
    │   ▼
    │  ┌──────────────────┐     HTTP      ┌───────────────┐
    │  │ strategy-server   │ ◄──────────── │    AI Agent    │
    │  │    (:8081)        │               │  (via MCP)     │
    │  └──────┬───────────┘               └───────┬───────┘
    │         │                                   │
    │         │  internal HTTP                    │ MCP/SSE
    │         ▼                                   ▼
    │  ┌──────────────────┐              ┌───────────────┐
    │  │    mcp-server    │              │               │
    └──│    (:8080)       │◄─────────────│  AI Client     │
       │  (MCP/SSE +     │   SSE stream  │               │
       │   FRED tools)    │              └───────────────┘
       └──────────────────┘
```

- **price-daemon** ingests real-time market data from DORA's WebSocket APIs
  and persists it to Postgres.
- **strategy-server** reads price data (directly or via the daemon) and exposes
  strategy management over REST. It subscribes to the same WebSocket feed for
  live trading signals.
- **mcp-server** wraps both systems in the Model Context Protocol so that AI
  agents can discover strategies, run backtests, manage live runs, and query
  FRED yield-curve data using natural-language tools.
