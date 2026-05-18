create table if not exists strategy_backtests (
    id uuid primary key,
    dora_user_id text not null,
    strategy_type text not null,
    status text not null,
    config jsonb not null,
    start timestamptz not null,
    "end" timestamptz not null,
    created_at timestamptz not null,
    completed_at timestamptz,
    error text not null default '',
    result jsonb,
    check (status in ('running', 'completed', 'failed', 'cancelled'))
);

create index if not exists idx_strategy_backtests_user_id on strategy_backtests(dora_user_id);

---- create above / drop below ----

drop table if exists strategy_backtests;
