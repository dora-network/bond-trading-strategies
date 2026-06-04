-- Write your migrate up statements here

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
