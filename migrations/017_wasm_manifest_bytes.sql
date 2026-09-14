-- 017_wasm_manifest_bytes.sql
--
-- Promote agent.wasm_manifests.manifest from jsonb to bytea. The
-- pgstore delegate writes raw manifest.json bytes (the file the
-- agent produced at validate time); jsonb canonicalizes the JSON on
-- insert (re-ordering keys, adding whitespace) which means the
-- scanned-back bytes never equal the original input. Round-tripping
-- raw bytes requires bytea storage.
--
-- Existing rows (pre-migration) hold canonicalized jsonb; their
-- bytes don't match the source file either, so they are
-- effectively unreachable for cold-restart rehydration. The pgstore
-- will surface a "not found" for those hashes and the corresponding
-- deployment rows will be marked crashed on the next Recover cycle
-- (same cold-start cliff as migration 016). New puts land in the
-- bytea column and round-trip correctly.

alter table agent.wasm_manifests
    alter column manifest type bytea using manifest::text::bytea;

---- create above / drop below ----

alter table agent.wasm_manifests
    alter column manifest type jsonb using manifest::text::jsonb;
