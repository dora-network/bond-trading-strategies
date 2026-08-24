-- Write your migrate up statements here

-- Breakout-specific columns for the trade tables. The existing
-- meanreversion columns (spread, zscore) and copytrading columns
-- (order_size, cash, trade_id) are reused positionally; these new
-- columns capture the breakout signal-verification fields.
--
-- position_size and exit_reason already exist on both tables from the
-- meanreversion migration, so breakout reuses them directly.

ALTER TABLE strategy_backtest_trades
  ADD COLUMN IF NOT EXISTS compression_ratio NUMERIC(42,18),
  ADD COLUMN IF NOT EXISTS entry_atr NUMERIC(42,18);

ALTER TABLE strategy_backtest_closed_trades
  ADD COLUMN IF NOT EXISTS entry_compression_ratio NUMERIC(42,18),
  ADD COLUMN IF NOT EXISTS exit_compression_ratio NUMERIC(42,18);

---- create above / drop below ----

ALTER TABLE strategy_backtest_closed_trades
  DROP COLUMN IF EXISTS exit_compression_ratio,
  DROP COLUMN IF EXISTS entry_compression_ratio;

ALTER TABLE strategy_backtest_trades
  DROP COLUMN IF EXISTS entry_atr,
  DROP COLUMN IF EXISTS compression_ratio;
