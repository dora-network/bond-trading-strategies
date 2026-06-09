-- 009_create_notification_log.sql
-- Persists notification events for Last-Event-ID replay across reconnects.
create table if not exists notification_log (
    id          uuid primary key,
    user_id     text not null,
    type        text not null,
    run_id      uuid,
    backtest_id uuid,
    payload     jsonb not null,
    created_at  timestamp not null default now()
);

create index if not exists notification_log_user_id_created_at_idx
    on notification_log (user_id, created_at desc);

create index if not exists notification_log_user_id_id_idx
    on notification_log (user_id, id);
