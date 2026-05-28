
ALTER TABLE strategy_runs
    ADD COLUMN IF NOT EXISTS encrypted_api_key bytea;
