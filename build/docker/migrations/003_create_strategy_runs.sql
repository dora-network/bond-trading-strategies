create table if not exists strategy_runs (
    id uuid primary key,
    strategy_type text not null,
    status text not null,
    config jsonb not null,
    created_at timestamp not null,
    updated_at timestamp not null,
    stopped_at timestamp,
    error text not null default '',
    check (status in ('running', 'paused', 'stopped'))
);
