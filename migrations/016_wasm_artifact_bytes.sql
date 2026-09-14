-- 016_wasm_artifact_bytes.sql
--
-- Promote agent.wasm_artifacts from metadata-only to durable storage
-- for compiled WASM blobs. The agent's pgstore delegate
-- (internal/agent/wasmruntime/store/pgstore) writes both this column
-- and the local on-disk CAS in a single Put call, and reads from it
-- on cold Fargate restart when the local CAS is empty.
--
-- Backfill: rows already in the table pre-this-migration have NULL
-- bytes and cannot be recovered. They will fail to load on the next
-- Recover cycle and the corresponding deployment rows will be marked
-- crashed; users re-deploy via the chatui. This is the deliberate
-- cold-start cliff (TODO.md).
--
-- Size cap: the column is BYTEA without an explicit limit; PG TOAST
-- handles blobs up to 1GB. A compiled strategy at the wasmFramework
-- v0.3.3 level is typically 5-50MB. Combined with one row per hash
-- and the content-addressed design (no duplicates), the total table
-- size scales linearly with the number of distinct strategies shipped.

alter table agent.wasm_artifacts
    add column bytes bytea;

---- create above / drop below ----

alter table agent.wasm_artifacts
    drop column bytes;
