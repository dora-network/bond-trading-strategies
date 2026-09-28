-- Consolidated schema for the agent runtime.
--
-- Single migration that creates every table the agent needs, in a
-- dedicated `agent` schema so the agent's tables are isolated from
-- strategy-server's `public` schema tables. This avoids confusion
-- between similarly-named tables (e.g. host's `strategy_backtests`
-- vs agent's `backtests`) and makes it trivial to enumerate, drop,
-- or grant on the agent's tables as a unit.
--
-- The migration is derived from the agent's 15 incremental migrations
-- (dora-agent/internal/store/migrations/001..015). The merge rules:
--
--   1. All CREATE statements use the `agent.` schema prefix.
--   2. DEK columns (`wrapped_dora_dek`, `wrapped_dek`) are absent —
--      envelope encryption is replaced by strategy-server's
--      AES-256-GCM under ENCRYPTION_KEY (see spec §1).
--   3. The encrypted-blob columns are renamed: `encrypted_dora_api_key`
--      -> `api_key`, `encrypted_api_key` -> `api_key`,
--      `encrypted_admin_dora_api_key` -> `api_key`.
--   4. The strategy_versions.target CHECK is omitted (the docker
--      pipeline is gone); the default is 'go-wasm'.
--
-- No data migration: dora-agent has never been deployed. Any
-- environment with existing agent rows must drop those tables (and
-- the schema) before applying this migration.

create schema if not exists agent;
set search_path to agent, public;

-- 001_initial_schema.sql

create table agent.users (
    dora_user_id        uuid primary key,
    tenant_id           text not null,
    roles               text[] not null,
    api_key             bytea,
    created_at          timestamptz not null default now(),
    last_seen_at        timestamptz not null default now()
);

create table agent.provider_configs (
    dora_user_id    uuid references agent.users(dora_user_id),
    provider        text not null,
    api_key         bytea not null,
    default_model   text not null,
    base_url        text,
    updated_at      timestamptz not null default now(),
    primary key (dora_user_id, provider)
);

create table agent.sessions (
    id            uuid primary key,
    dora_user_id  uuid not null references agent.users(dora_user_id),
    title         text,
    provider      text not null,
    model         text not null,
    created_at    timestamptz not null default now(),
    updated_at    timestamptz not null default now()
);
create index sessions_user_created_idx on agent.sessions (dora_user_id, created_at desc);

create table agent.messages (
    id           bigserial primary key,
    session_id   uuid not null references agent.sessions(id) on delete cascade,
    seq          integer not null,
    role         text not null,
    content      text not null,
    tool_call_id text,
    tool_calls   jsonb,
    created_at   timestamptz not null default now(),
    unique (session_id, seq)
);

create table agent.audit_log (
    id           bigserial primary key,
    dora_user_id uuid not null,
    action       text not null,
    detail       jsonb not null default '{}'::jsonb,
    created_at   timestamptz not null default now()
);
create index audit_log_user_created_idx on agent.audit_log (dora_user_id, created_at desc);

-- 002_strategy_versioning.sql

create table agent.strategies (
    id                uuid primary key,
    dora_user_id      uuid not null references agent.users(dora_user_id),
    name              text not null,
    head_revision     uuid,
    source_session_id uuid not null,
    created_at        timestamptz not null default now(),
    updated_at        timestamptz not null default now()
);
create unique index strategies_source_session_unique on agent.strategies (source_session_id);
create index strategies_user_created_idx on agent.strategies (dora_user_id, created_at desc);

create table agent.strategy_versions (
    revision         uuid primary key,
    strategy_id      uuid not null references agent.strategies(id) on delete cascade,
    parent_revision  uuid references agent.strategy_versions(revision),
    created_at       timestamptz not null default now(),
    provider         text not null,
    model            text not null,
    module_name      text not null,
    summary          text not null,
    rationale        text not null,
    validation       jsonb not null,
    image_ref        text
);
create index versions_strategy_created_idx on agent.strategy_versions (strategy_id, created_at desc);

create table agent.strategy_version_files (
    revision  uuid not null references agent.strategy_versions(revision) on delete cascade,
    path      text not null,
    sha256    bytea not null,
    primary key (revision, path)
);

create table agent.strategy_blobs (
    strategy_id  uuid not null references agent.strategies(id) on delete cascade,
    sha256       bytea not null,
    content      bytea not null,
    primary key (strategy_id, sha256)
);

create table agent.strategy_capture_pending (
    session_id   uuid primary key references agent.sessions(id) on delete cascade,
    dora_user_id uuid not null references agent.users(dora_user_id),
    provider     text not null,
    model        text not null,
    module_name  text not null,
    summary      text not null,
    rationale    text not null,
    validation   jsonb not null,
    files        jsonb not null,
    created_at   timestamptz not null default now(),
    image_ref    text
);

-- 004_backtesting.sql

create table agent.backtests (
    id              uuid primary key,
    strategy_id     uuid not null references agent.strategies(id) on delete cascade,
    version_id      uuid not null references agent.strategy_versions(revision) on delete restrict,
    user_id         uuid not null references agent.users(dora_user_id) on delete cascade,
    status          text not null check (status in
                       ('queued','running','succeeded','failed','cancelled')),
    requested_at    timestamptz not null default now(),
    started_at      timestamptz,
    finished_at      timestamptz,
    error_message   text,
    window_start    timestamptz not null,
    window_end      timestamptz not null,
    resolution      text not null,
    params          jsonb not null default '{}'::jsonb,
    summary         jsonb,
    fill_count      integer,
    container_id    text,
    image_ref       text not null,
    order_book_id   text
);
create index backtests_strategy_idx on agent.backtests (strategy_id, requested_at desc);
create unique index backtests_user_active_uidx
    on agent.backtests (user_id)
    where status in ('queued','running');

create table agent.backtest_fills (
    id              uuid primary key,
    backtest_id     uuid not null references agent.backtests(id) on delete cascade,
    timestamp       timestamptz not null,
    side            text not null check (side in ('buy','sell')),
    quantity        numeric(20,8) not null,
    price           numeric(20,8) not null,
    order_id        text not null,
    simulated_at    timestamptz not null
);
create index backtest_fills_backtest_idx on agent.backtest_fills (backtest_id, timestamp);

-- 006_safety_caps.sql

create table agent.user_caps (
    user_id uuid primary key references agent.users(dora_user_id) on delete cascade,
    max_open_orders integer not null default 50,
    max_notional_per_order numeric(20, 8) not null default 100000,
    max_total_notional numeric(20, 8) not null default 1000000,
    max_orders_per_minute integer not null default 60,
    updated_at timestamptz not null default now()
);

create table agent.user_kill_switches (
    user_id uuid primary key references agent.users(dora_user_id) on delete cascade,
    halted boolean not null default false,
    halted_at timestamptz,
    halted_reason text
);

-- 007_safety_order_counter.sql -- test scaffolding.

create table agent.safety_order_counter (
    user_id uuid not null,
    window_start timestamptz not null,
    cnt integer not null
);
create index safety_order_counter_user_idx
    on agent.safety_order_counter (user_id, window_start desc);

-- 008_strategy_versions_wasm.sql
--
-- Adds wasm columns to strategy_versions, plus three new tables
-- (wasm_artifacts, wasm_manifests, running_live_strategies). The
-- agent's target CHECK constraint was relaxed by migration 015;
-- we apply only the relaxed default here.

alter table agent.strategy_versions
    add column wasm_ref text,
    add column manifest_hash text,
    add column target text not null default 'go-wasm';

create index strategy_versions_wasm_idx on agent.strategy_versions (wasm_ref)
    where wasm_ref is not null;

create table agent.wasm_artifacts (
    hash        text primary key,
    size_bytes  bigint not null,
    created_at  timestamptz not null default now(),
    path        text not null
);

create table agent.wasm_manifests (
    hash          text primary key,
    artifact_hash text not null references agent.wasm_artifacts(hash),
    manifest      jsonb not null,
    created_at    timestamptz not null default now()
);

create table agent.running_live_strategies (
    strategy_id              uuid not null references agent.strategies(id) on delete cascade,
    version_id               uuid not null references agent.strategy_versions(revision) on delete restrict,
    user_id                  uuid not null references agent.users(dora_user_id) on delete cascade,
    instance_id              uuid not null,
    subprocess_pid           integer,
    started_at               timestamptz not null default now(),
    last_restart_at          timestamptz,
    restart_count_in_window  integer not null default 0,
    restart_window_started_at timestamptz not null default now(),
    status                   text not null check (status in ('running','crashed','halted','paused')),
    crashed_reason           text,
    primary key (strategy_id, version_id)
);
create index running_live_strategies_user_idx
    on agent.running_live_strategies (user_id)
    where status in ('running','paused');

-- 009_server_admin_dora_key.sql (no DEK column; single api_key bytea).

create table agent.server_dora_credentials (
    id                          integer primary key check (id = 0),
    api_key                     bytea not null,
    updated_at                  timestamptz not null default now()
);

-- 010_session_summary.sql

alter table agent.sessions add column summary text;

-- 011_deployments.sql

create table agent.deployments (
    id                        uuid primary key,
    strategy_id               uuid not null references agent.strategies(id) on delete cascade,
    revision                  uuid not null references agent.strategy_versions(revision),
    user_id                   uuid not null references agent.users(dora_user_id) on delete cascade,
    params                    jsonb not null default '{}',
    status                    text not null check (status in ('running','stopped','crashed','halted')),
    instance_id               uuid,
    started_at                timestamptz,
    stopped_at                timestamptz,
    stopped_reason            text,
    restart_count             integer not null default 0,
    hotswapped_at             timestamptz,
    hotswapped_from_revision  uuid references agent.strategy_versions(revision),
    created_at                timestamptz not null default now(),
    updated_at                timestamptz not null default now()
);
create unique index deployments_one_active
    on agent.deployments (strategy_id)
    where status = 'running';
create index deployments_user_idx on agent.deployments (user_id, created_at desc);
create index deployments_strategy_idx on agent.deployments (strategy_id, created_at desc);

-- 012_deployment_order_book_id.sql

alter table agent.deployments add column order_book_id text;
alter table agent.deployments add column resolution text;

-- 013_deployment_candle_count.sql

alter table agent.deployments add column candle_count bigint not null default 0;

-- 014_deployments_warmup_candles.sql

alter table agent.deployments add column warmup_candles integer not null default 0;
