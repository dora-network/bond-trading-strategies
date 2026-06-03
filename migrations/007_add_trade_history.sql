-- Write your migrate up statements here

CREATE TABLE IF NOT EXISTS trades_history (
  transaction_id UUID,
  order_id UUID,
  order_seq BIGINT,
  orderbook_id UUID,
  user_id UUID,
  asset0 UUID,
  quantity0 DECIMAL(42,18),
  price DECIMAL(42,18),
  side VARCHAR(10),
  aggressor_indicator BOOLEAN,
  created_at TIMESTAMP
);

---- create above / drop below ----

DROP TABLE IF EXISTS trades_history
