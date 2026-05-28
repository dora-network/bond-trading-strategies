# TODO

## High Priority

- **Notification websocket endpoint**
  - The strategy service should expose a WebSocket endpoint that pushes real-time notifications to connected clients. Events include: backtest completed, comparison completed, order submission errors, stop-loss notifications, and run state changes.

- **Rate limiter**
  - Add rate limiting to the strategy server's REST API so it cannot be spammed. Should be configurable (e.g., per-IP, per-user, per-endpoint) and return `429 Too Many Requests` when exceeded.

## Planned Features

- **Backtest comparison**
  - Run multiple backtests with different configs and compare PnL, drawdown, and Sharpe ratio side-by-side.

- **Parameter sweep**
  - Let the UI submit a grid of backtests across parameter ranges (e.g., vary `entry_z_score` from 1.5 to 3.0) and surface the optimal performer.

- **Run alerts**
  - Push/webhook notifications when a live run stops, hits stop-loss, or errors out.

- **Config presets**
  - Save and load named parameter sets so users don't have to retype strategy configs every time.
