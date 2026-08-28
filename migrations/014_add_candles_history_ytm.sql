-- Write your migrate up statements here

-- The DORA candle stream now ships per-side yields to maturity alongside
-- the OHLCV aggregates: open_ytm / high_ytm / low_ytm / close_ytm. The
-- legacy singular `ytm` field is deprecated upstream in favour of
-- close_ytm and is intentionally NOT persisted here.
--
-- Nullable so existing candles_history rows survive the upgrade with
-- NULL YTMs; new rows from the stream will populate all four columns.

ALTER TABLE candles_history
  ADD COLUMN IF NOT EXISTS open_ytm  NUMERIC(42, 18),
  ADD COLUMN IF NOT EXISTS high_ytm  NUMERIC(42, 18),
  ADD COLUMN IF NOT EXISTS low_ytm   NUMERIC(42, 18),
  ADD COLUMN IF NOT EXISTS close_ytm NUMERIC(42, 18);

---- create above / drop below ----

ALTER TABLE candles_history
  DROP COLUMN IF EXISTS close_ytm,
  DROP COLUMN IF EXISTS low_ytm,
  DROP COLUMN IF EXISTS high_ytm,
  DROP COLUMN IF EXISTS open_ytm;
