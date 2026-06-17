-- 010_add_strategy_decisions.sql
-- Records every trading decision that triggered a market order during a
-- live strategy run.  Backtest runs do NOT write to this table — it is
-- populated exclusively by the live run path.
--
-- One row per (run_id, seq) where seq is a monotonically increasing
-- counter assigned by the strategy for that run.  The order_book_id and
-- asset are captured at decision time so the row remains meaningful even
-- if the configuration is later mutated (which is not currently possible
-- for a live run, but defends against future changes).
--
-- Convention: `created_at` is stored as TIMESTAMP (no time zone) but
-- every value MUST be written in UTC.  The database server's default
-- timezone is UTC, so pgx reads the wall-clock value back as UTC.
CREATE TABLE IF NOT EXISTS strategy_decisions (
    run_id UUID NOT NULL,
    seq BIGINT NOT NULL,
    strategy_type VARCHAR(64) NOT NULL,
    order_book_id UUID NOT NULL,
    asset UUID NOT NULL,
    side VARCHAR(8) NOT NULL,
    signal VARCHAR(8) NOT NULL,
    kind VARCHAR(16) NOT NULL,
    quantity DECIMAL(42, 18) NOT NULL,
    price DECIMAL(42, 18) NOT NULL,
    leverage DECIMAL(42, 18) NOT NULL,
    inverse_leverage DECIMAL(42, 18) NOT NULL,
    from_global_position BOOLEAN NOT NULL,
    reason VARCHAR(64) NOT NULL,
    reason_detail TEXT,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (run_id, seq),
    -- side is always the DORA side string ("BUY" or "SELL").
    constraint STRATEGY_DECISIONS_SIDE_CHK
    CHECK (side IN ('BUY', 'SELL')),
    -- signal is the strategy's direction at decision time; recorded as
    -- lowercase by copy-trading and as the DORA uppercase form by
    -- mean-reversion.  Accept both cases.
    constraint STRATEGY_DECISIONS_SIGNAL_CHK
    CHECK (LOWER(signal) IN ('buy', 'sell')),
    -- kind is the closed set defined in strategy/decision.go.
    constraint STRATEGY_DECISIONS_KIND_CHK
    CHECK (kind IN ('open', 'close', 'extend')),
    -- reason is a short machine-readable code; lowercase identifier
    -- with underscores, up to 64 chars.  Adding a new reason code
    -- requires a migration to extend the application-side enum.
    constraint STRATEGY_DECISIONS_REASON_CHK
    CHECK (reason ~ '^[a-z][a-z0-9_]{0,63}$')
);

CREATE INDEX IF NOT EXISTS idx_strategy_decisions_run_id_created_at
ON strategy_decisions (run_id, created_at);

CREATE INDEX IF NOT EXISTS idx_strategy_decisions_asset_created_at
ON strategy_decisions (asset, created_at);

---- create above / drop below ----

DROP INDEX IF EXISTS idx_strategy_decisions_asset_created_at;
DROP INDEX IF EXISTS idx_strategy_decisions_run_id_created_at;
DROP TABLE IF EXISTS strategy_decisions;
