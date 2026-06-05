# TODO

## High Priority

- **Notification websocket endpoint**
  - The strategy service should expose a WebSocket endpoint that pushes real-time notifications to connected clients. Events include: backtest completed, comparison completed, order submission errors, stop-loss notifications, and run state changes.

- ~~**Rate limiter**~~ ✅
  - Add rate limiting to the strategy server's REST API so it cannot be spammed. Should be configurable (e.g., per-IP, per-user, per-endpoint) and return `429 Too Many Requests` when exceeded.

- **Ledger position stream**
  - Subscribe to DORA's `GET /v1/user/{user_id}/ledger/stream` WebSocket for real-time position updates instead of polling `GetPortfolioV2` on every trade. The ledger stream pushes account balance and position changes, which would let the strategy maintain an accurate in-memory position map without repeated REST calls. This requires:
    - A new `LedgerStream` type in `streams/` (similar to `TradeStream`) that manages the websocket connection, parses ledger update messages, and routes them to subscribers.
    - Position state tracking in the copytrading strategy — maintaining a map of `asset_id → (available, borrowed)` that gets updated by ledger events.
    - Integration in `cmd/strategy-server/main.go` to start the ledger stream alongside the trade stream.

## Planned Features

- **Backtest comparison**
  - Run multiple backtests with different configs and compare PnL, drawdown, and Sharpe ratio side-by-side.

- **Parameter sweep**
  - Let the UI submit a grid of backtests across parameter ranges (e.g., vary `entry_z_score` from 1.5 to 3.0) and surface the optimal performer.

- **Run alerts**
  - Push/webhook notifications when a live run stops, hits stop-loss, or errors out.

- **Config presets**
  - Save and load named parameter sets so users don't have to retype strategy configs every time.

- **Agent skill documentation**
  - Construct a `SKILL.md` file for the strategy server so users who prefer agent skills over MCP servers can interact with the service via a skill interface instead.
