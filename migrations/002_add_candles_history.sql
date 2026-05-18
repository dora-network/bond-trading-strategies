-- Write your migrate up statements here
create table if not exists candles_history (
    order_book_id uuid not null,
    start_timestamp timestamp not null,
    open numeric(42, 18),
    high numeric(42, 18),
    low numeric(42, 18),
    close numeric(42, 18),
    volume numeric(42, 18),
    primary key (order_book_id, start_timestamp)
);

---- create above / drop below ----

-- Write your migrate down statements here. If this migration is irreversible
-- Then delete the separator line above.

drop table if exists candles_history;
