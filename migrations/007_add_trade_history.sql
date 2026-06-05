-- Write your migrate up statements here

-- Convention: `created_at` is stored as TIMESTAMP (no time zone) but
-- every value MUST be written in UTC. The database server's default
-- timezone is UTC, so pgx reads the wall-clock value back as UTC.
-- Use TIMESTAMPTZ only if a writer can't guarantee UTC.

CREATE TABLE IF NOT EXISTS trades_history (
  transaction_id UUID NOT NULL PRIMARY KEY,
  order_id UUID NOT NULL,
  order_seq BIGINT NOT NULL,
  orderbook_id UUID NOT NULL,
  user_id UUID NOT NULL,
  asset UUID NOT NULL,
  quantity DECIMAL(42,18) NOT NULL,
  price DECIMAL(42,18) NOT NULL,
  side VARCHAR(10) NOT NULL,
  aggressor_indicator BOOLEAN NOT NULL,
  created_at TIMESTAMP NOT NULL
);

CREATE INDEX idx_trades_history_user_id_created_at on trades_history(user_id, created_at);

---- create above / drop below ----

DROP INDEX IF EXISTS idx_trades_history_user_id_created_at;
DROP TABLE IF EXISTS trades_history;
