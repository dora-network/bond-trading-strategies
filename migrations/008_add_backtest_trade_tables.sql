-- Write your migrate up statements here

-- Per-trade and per-closed-trade rows for backtests. Strategy-server
-- streams each trade as the simulation produces it, instead of accumulating
-- them in one giant JSONB column on strategy_backtests. Old approach
-- crashed the DB for backtests whose combined result exceeded ~50MB.

CREATE TABLE IF NOT EXISTS strategy_backtest_trades (
  id BIGSERIAL PRIMARY KEY,
  backtest_id UUID NOT NULL,
  time TIMESTAMPTZ NOT NULL,
  bond_id UUID,
  signal TEXT NOT NULL,
  price NUMERIC(42,18),
  quantity NUMERIC(42,18),
  entry_balance NUMERIC(42,18),
  -- copytrading-specific
  order_size NUMERIC(42,18),
  cash NUMERIC(42,18),
  open_position NUMERIC(42,18),
  trade_id UUID,
  -- meanreversion-specific
  spread NUMERIC(42,18),
  position_size NUMERIC(42,18),
  zscore NUMERIC(42,18)
);

CREATE INDEX idx_strategy_backtest_trades_lookup
  ON strategy_backtest_trades(backtest_id, time);

CREATE TABLE IF NOT EXISTS strategy_backtest_closed_trades (
  id BIGSERIAL PRIMARY KEY,
  backtest_id UUID NOT NULL,
  open_time TIMESTAMPTZ NOT NULL,
  close_time TIMESTAMPTZ NOT NULL,
  bond_id UUID,
  open_signal TEXT NOT NULL,
  close_signal TEXT NOT NULL,
  quantity NUMERIC(42,18) NOT NULL,
  entry_price NUMERIC(42,18),
  exit_price NUMERIC(42,18),
  pnl NUMERIC(42,18) NOT NULL,
  entry_balance NUMERIC(42,18),
  -- copytrading-specific
  open_trade_id UUID,
  close_trade_id UUID,
  -- meanreversion-specific
  entry_spread NUMERIC(42,18),
  exit_spread NUMERIC(42,18),
  entry_zscore NUMERIC(42,18),
  exit_zscore NUMERIC(42,18),
  position_size NUMERIC(42,18),
  exit_reason TEXT
);

CREATE INDEX idx_strategy_backtest_closed_trades_lookup
  ON strategy_backtest_closed_trades(backtest_id, close_time);

---- create above / drop below ----

DROP INDEX IF EXISTS idx_strategy_backtest_closed_trades_lookup;
DROP TABLE IF EXISTS strategy_backtest_closed_trades;
DROP INDEX IF EXISTS idx_strategy_backtest_trades_lookup;
DROP TABLE IF EXISTS strategy_backtest_trades;
