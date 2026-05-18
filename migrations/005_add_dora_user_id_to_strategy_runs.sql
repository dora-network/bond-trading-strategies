alter table strategy_runs
    add column if not exists dora_user_id text not null default '';

create index if not exists idx_strategy_runs_user_id on strategy_runs(dora_user_id);

---- create above / drop below ----

drop index if exists idx_strategy_runs_user_id;

alter table strategy_runs
    drop column if exists dora_user_id;
