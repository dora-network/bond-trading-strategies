create table if not exists strategy_runs (
    id uuid primary key,
    strategy_type text not null,
    status text not null,
    config jsonb not null,
    created_at timestamptz not null,
    updated_at timestamptz not null,
    stopped_at timestamptz,
    error text not null default '',
    check (status in ('running', 'paused', 'stopped'))
);

---- create above / drop below ----

drop table if exists strategy_runs;
