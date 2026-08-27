-- 013_add_state_to_strategy_runs.sql
-- Adds a state column for per-run strategy checkpoint data. Execution
-- strategies (e.g. TWAP) use it to persist progress between server
-- restarts so they can recover and rebalance without over-executing.
--
-- The column is nullable JSONB: existing rows have NULL (no state),
-- and strategies that don't use it never write to it. The handler's
-- SaveRun does not touch this column; it is written exclusively by the
-- dedicated SaveState method on PGRunStore.
--
-- Convention: stored as TIMESTAMP (no time zone) in UTC, consistent
-- with all other timestamp columns in this schema.
ALTER TABLE strategy_runs
    ADD COLUMN IF NOT EXISTS state jsonb;

---- create above / drop below ----

ALTER TABLE strategy_runs DROP COLUMN IF EXISTS state;
