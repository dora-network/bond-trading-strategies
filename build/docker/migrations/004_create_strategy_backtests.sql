create table if not exists strategy_backtests (
    id uuid primary key,
    dora_user_id text not null,
    strategy_type text not null,
    status text not null,
    config jsonb not null,
    start timestamp not null,
    "end" timestamp not null,
    created_at timestamp not null,
    completed_at timestamp,
    error text not null default '',
    result jsonb,
    check (status in ('running', 'completed', 'failed', 'cancelled'))
);

create index if not exists idx_strategy_backtests_user_id on strategy_backtests(dora_user_id);
