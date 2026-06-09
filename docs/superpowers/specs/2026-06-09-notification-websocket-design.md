# Notification WebSocket

## Overview

Expose a real-time notification stream from `strategy-server` over WebSocket. Clients (direct browser/CLI, plus the MCP server acting as a relay) subscribe once per DORA user and receive lifecycle events for backtests, runs, and (later) DORA-relayed events like orders and trades.

Events are persisted to a small log table so reconnecting clients can replay missed events via `Last-Event-ID`.

## Goals

- One WS connection per client; no per-run subscribe/unsubscribe dance.
- Reuse the existing API key (or Bearer token) auth path so there is no new credential surface.
- Replay missed events after disconnect via `Last-Event-ID`.
- Relay events to MCP clients without adding new MCP routes or methods.
- Keep the transport and event types forward-compatible with v2 DORA-relayed events (orders, trades).

## Non-goals

- Cross-process pub/sub. Single-process fan-out is enough for v1.
- Webhooks / push to external services. The TODO entry "Run alerts" is deferred.
- Per-event-type access control. All events for a DORA user are visible to that user.

## Architecture

```
+---------------------- strategy-server :8081 ------------------------+
|                                                                     |
|  +--------+   Publish(ctx, Event)   +----------+   write   +------+ |
|  | Service| ----------------------> | Notifier | --------> | Log  | |
|  | runs / |                         |  (Bus)   |           | (PG) | |
|  | backts |                         +----+-----+           +------+ |
|  +--------+                              | broadcast                |
|                                          v                          |
|                                  +-------+-------+                  |
|                                  |   Hub         |  per-userID       |
|                                  |   user->subs  |  subscriber set   |
|                                  +-------+-------+                  |
|                                          |                          |
|                          /v1/notifications/ws (coder/websocket)     |
|                                          |                          |
|                          coder/websocket.Accept  -- auth via DORA   |
|                          ?Last-Event-ID=...      -- replay from log |
+---------------------------------------------------------------------+
                                    |
                                    | (outbound, coder/websocket.Dial)
                                    v
+---------------------- mcp-server :8080 -----------------------------+
|                                                                     |
|   startup: dial strategy-server's /v1/notifications/ws              |
|            (one connection, API-key auth)                           |
|                                                                     |
|   on event: srv.SendNotificationToAllClients(                       |
|              "notifications/event", payload)                        |
|                                                                     |
|   MCP clients receive the event as an MCP notification.             |
+---------------------------------------------------------------------+
```

## Package layout

```
notifications/
  notifier.go     — Notifier interface, in-process Bus implementation, Event type
  hub.go          — Hub: per-userID subscriber set, Subscribe/Unsubscribe, Broadcast
  log.go          — PG-backed notification_log writer/reader
  handler.go      — HTTP handler: GET /v1/notifications/ws (upgrade + replay + live)
  notifierfakes/  — counterfeiter-generated fakes for tests
  export_test.go  — white-box test helpers
migrations/009_create_notification_log.sql
cmd/strategy-server/main.go — wire Notifier + Log + Hub; pass into Handler
strategy/http/handler.go    — Service takes a Notifier; emit events at lifecycle points
strategy/copytrading/strategy.go — emit run.stop_loss
strategy/meanreversion/strategy.go — emit run.stop_loss
mcp/server.go               — dial WS, fan out via SendNotificationToAllClients
mcp/notify_client.go        — outbound WS client with reconnect (small loop, not streams.Daemon)
```

## Event types (v1)

```go
type EventType string

const (
    EventBacktestStarted   EventType = "backtest.started"
    EventBacktestCompleted EventType = "backtest.completed"
    EventBacktestFailed    EventType = "backtest.failed"
    EventRunStarted        EventType = "run.started"
    EventRunPaused         EventType = "run.paused"
    EventRunResumed        EventType = "run.resumed"
    EventRunStopped        EventType = "run.stopped"
    EventRunStopLoss       EventType = "run.stop_loss"
)
```

The `dora.*` namespace is reserved for v2 (e.g. `dora.order.created`, `dora.order.updated`, `dora.trade.filled`). No v1 producer emits those.

```go
type Event struct {
    ID         string    `json:"id"`         // UUIDv7, monotonic; used as Last-Event-ID
    Type       EventType `json:"type"`
    UserID     string    `json:"user_id"`
    RunID      string    `json:"run_id,omitempty"`
    BacktestID string    `json:"backtest_id,omitempty"`
    Timestamp  time.Time `json:"timestamp"`  // always UTC
    Payload    any       `json:"payload"`    // type-specific JSON object
}
```

UUIDv7 is the same choice as the rest of the codebase (see `go.mod`: `github.com/google/uuid`); v7's monotonic property makes `id > lastID` a correct replay cursor.

## Notifier interface

```go
type Notifier interface {
    Publish(ctx context.Context, evt Event) error
    Subscribe(ctx context.Context, userID string) (Subscription, error)
}

type Subscription interface {
    Events() <-chan Event
    Close() error
}
```

`Bus` (in-process, `sync.RWMutex`-protected map keyed by `userID`) implements `Notifier`. On `Publish` it:
1. Writes the event to the PG `notification_log` (best-effort; on failure it logs and falls back to the in-memory `replayCache` so live subscribers still see it).
2. Looks up the subscriber set for `evt.UserID` and sends the event to each subscriber's channel. If a subscriber's channel buffer is full, that subscriber's event is dropped and a counter increments; the subscriber is not closed.

`replayCache` is a bounded in-memory ring (e.g. 1024 most-recent events per user) so live subscribers are not held hostage to a transient DB outage.

## Wire protocol

### Upgrade

`GET /v1/notifications/ws`

Auth uses the same `Authorization: ApiKey <key>` / `Bearer <token>` header as the REST API. The handler delegates to the existing DORA-key validator; on success it resolves the DORA `user_id` (one DORA call) and uses it as the subscription key. The user_id is cached per-process for the connection's lifetime.

Supported query params:
- `Last-Event-ID` (optional) — replay events with `id > Last-Event-ID` from the log, capped at 1000 events or 24h, whichever is smaller, then live-forward. An unparseable or unknown value is logged and ignored (start at the live tail).
- `types` (optional, comma-separated) — restrict to a subset of `EventType`. Default: all.

### Frames

Server sends one JSON object per WS text frame matching `Event`. No application-level framing, no batching. Clients should treat unknown event types as opaque (forward-compat for v2 `dora.*`).

### Heartbeat

WS-level ping/pong every 30s, handled by `coder/websocket`. Clients that miss two consecutive pongs are closed with `StatusPolicyViolation`. No application-level heartbeats.

### Backpressure

Each subscriber's channel is buffered to 256 events. A full channel causes per-event drops with a `drops_total` counter; the connection is left open.

## PG schema

Migration `migrations/009_create_notification_log.sql`:

```sql
create table if not exists notification_log (
    id          uuid primary key,
    user_id     text not null,
    type        text not null,
    run_id      uuid,
    backtest_id uuid,
    payload     jsonb not null,
    created_at  timestamp not null default now()
);

create index if not exists notification_log_user_id_created_at_idx
    on notification_log (user_id, created_at desc);

create index if not exists notification_log_user_id_id_idx
    on notification_log (user_id, id);
```

The `notification_log_user_id_id_idx` index supports the replay query: `where user_id = $1 and id > $2 order by id limit 1000`.

All timestamps are stored as bare `timestamp` (no time zone) and are written in UTC. The DB server's default time zone is UTC, so `now()` and inserted values line up.

### Retention

A goroutine started from `cmd/strategy-server/main.go` runs every hour and deletes rows older than 24h:

```go
delete from notification_log where created_at < now() - interval '24 hours';
```

The `replayCache` ring covers any short gap between deletion and a slow client's reconnect.

## MCP server relay

- On startup, the MCP server dials `ws://strategy-server/v1/notifications/ws` with the configured DORA API key. This is one outbound connection per MCP-server process. The MCP server's API key identifies a single DORA user; all events for that user are forwarded.
- A small client in `mcp/notify_client.go` owns this connection. On disconnect it reconnects with exponential backoff (100ms → 5s cap), same shape as `streams.Daemon` but a single-connection implementation, not the generic daemon.
- For each received event, call `srv.SendNotificationToAllClients("notifications/event", evt)`. The MCP server's existing SSE / streamable-HTTP transport pushes it to every connected MCP session.
- No new MCP methods. No new HTTP routes. MCP clients that want to opt out can simply ignore the `notifications/event` method.
- Failure mode: if the WS to strategy-server is down for >60s, log a warning and continue serving MCP. Clients lose live updates but MCP tool calls keep working.

## Producer integration (call sites)

All producers get the `userID` from the API-key context already attached to the request (same path `ratelimit` uses).

`strategy/http/handler.go`:
1. Backtest create → `EventBacktestStarted`
2. Backtest completion path → `EventBacktestCompleted`
3. Backtest failure path → `EventBacktestFailed`
4. Run create → `EventRunStarted`
5. Run pause → `EventRunPaused`
6. Run resume → `EventRunResumed`
7. Run stop → `EventRunStopped`

`strategy/copytrading/strategy.go` and `strategy/meanreversion/strategy.go`:
8. Stop-loss trigger → `EventRunStopLoss` (both strategies)

## Error handling

- **Auth fail on upgrade** — close with `StatusPolicyViolation`, do not accept.
- **Invalid `Last-Event-ID`** — log, ignore, start at the live tail.
- **Reconnect storm (outbound MCP client)** — exponential backoff 100ms → 5s.
- **Subscriber slow / channel full** — drop the event for that subscriber, increment `drops_total`, keep the connection open.
- **PG write fails on `Publish`** — log a warning, fall back to `replayCache` for live subscribers; the event is still emitted but is not durable for `Last-Event-ID` replay across a restart.
- **PG replay read fails** — log a warning, start at the live tail.

## Testing

- `counterfeiter` fakes for `Notifier` in `notifications/notifierfakes/` (matches the existing pattern at `strategy/service.go:15`).
- White-box tests via `notifications/export_test.go`:
  - Hub: subscribe, broadcast, unsubscribe, slow-subscriber drop, multi-user isolation.
  - Bus: publish writes to log, publishes to live subscribers, falls back to `replayCache` when log is unavailable.
  - Log: insert + replay by `Last-Event-ID`, retention delete.
- Handler integration: `httptest.NewServer` with a real `Bus` + a stub log. Verify a published event reaches a connected client end-to-end (auth → upgrade → receive frame).
- Replay: connect client A, publish events, connect client B with `Last-Event-ID` set to a known event, verify B receives only the tail.
- MCP relay: spin up a fake strategy-server WS, connect it to a real `MCPServer` via the new client, push an event, assert `SendNotificationToAllClients` is called with the right method and payload. Use `mcptest` for the MCP side.
- No DB-dependent test in CI without `DATABASE_URL`; the existing repo pattern is to skip those locally. The PG log test runs in CI where `DATABASE_URL` is set.

## Migration

`tern migrate --config migrations/tern.conf`

`migrations/009_create_notification_log.sql` follows the existing `NNN_short_name.sql` convention (see `001_create_price_history_table.sql` … `008_add_backtest_trade_tables.sql`).

## Rollout

- `strategy-server` gets a new flag `--notifications-enabled` (default true) so the WS endpoint can be turned off without rebuilding if a problem surfaces.
- `mcp-server` does not start the relay if `STRATEGY_BASE_URL` or `DORA_API_KEY` is unset (already a required pair).
- No external protocol change: WS clients see a new endpoint and JSON envelopes; REST clients see no change.
