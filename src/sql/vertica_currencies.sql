drop table if exists VT2512182D1223__STAGING.currencies


create table VT2512182D1223__STAGING.currencies
(
    date_update                 timestamp,
    currency_code               int,
    currency_code_with          int,
    currency_with_div           numeric(5, 3)
)
order by date_update
segmented by hash(currency_code, date_update) all nodes
partition by date_update::date;
