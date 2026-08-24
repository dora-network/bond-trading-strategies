-- Add composite index on trades_history for the breakout backtest's
-- trade-volume integration. The breakout backtest reads trades by
-- orderbook_id (not user_id like copytrading), so it needs its own
-- index. Without this, each page query does a full table scan of
-- ~1M rows, making the trade-loading phase take minutes instead of
-- seconds.
--
-- Columns: orderbook_id (filter), created_at (keyset cursor),
-- transaction_id (tiebreaker for same-timestamp trades).
CREATE INDEX IF NOT EXISTS idx_trades_history_orderbook_created_at
  ON trades_history(orderbook_id, created_at, transaction_id);

---- create above / drop below ----

DROP INDEX IF EXISTS idx_trades_history_orderbook_created_at;
