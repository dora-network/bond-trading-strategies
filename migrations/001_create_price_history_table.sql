create table price_history(
    asset_id uuid,
    price numeric(42, 18),
    ytm numeric(42, 18),
    timestamp timestamp,
    primary key (asset_id, timestamp)
);

---- create above / drop below ----

drop table price_history;
