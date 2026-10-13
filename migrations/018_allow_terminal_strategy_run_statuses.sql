-- 018_allow_terminal_strategy_run_statuses.sql
-- strategy_runs.status only allowed running/paused/stopped (003), but the
-- handler writes two more terminal statuses: "completed" for runs whose
-- loop returns on its own (maybePublishNaturalCompletion, expireRun) and
-- "failed" for error exits and undecryptable stored API keys
-- (failUnusableAPIKeyRun). Those saves violated the check constraint and
-- were only logged, so the persisted row stayed "running" — dead runs
-- were relaunched on restart. Widen the check to accept both.

ALTER TABLE strategy_runs
    DROP CONSTRAINT IF EXISTS strategy_runs_status_check;

ALTER TABLE strategy_runs
    ADD CONSTRAINT strategy_runs_status_check
    CHECK (status IN ('running', 'paused', 'stopped', 'completed', 'failed'));

---- create above / drop below ----

-- Terminal-status rows must be normalized to an old-schema value first,
-- or re-adding the restrictive constraint fails on them. "stopped" is
-- the closest old-schema terminal state.
UPDATE strategy_runs SET status = 'stopped' WHERE status IN ('completed', 'failed');

ALTER TABLE strategy_runs
    DROP CONSTRAINT IF EXISTS strategy_runs_status_check;

ALTER TABLE strategy_runs
    ADD CONSTRAINT strategy_runs_status_check
    CHECK (status IN ('running', 'paused', 'stopped'));
