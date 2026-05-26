-- create above / drop below

ALTER TABLE strategy_runs
    ADD COLUMN IF NOT EXISTS encrypted_api_key bytea;

---- create above / drop below ----

-- DROP INDEX IF EXISTS idx_strategy_runs_enc_api_key;
-- ALTER TABLE strategy_runs DROP COLUMN IF EXISTS encrypted_api_key;
