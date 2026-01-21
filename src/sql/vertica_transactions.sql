drop table if exists VT2512182D1223__STAGING.transactions


create table VT2512182D1223__STAGING.transactions
(
    operation_id            varchar(60),
    account_number_from     int,
    account_number_to       int,
    currency_code           int,
    country                 varchar(30),
    status                  varchar(30),
    transaction_type        varchar(30),
    amount                  int,
    transaction_dt          timestamp
)
order by transaction_dt
segmented by hash(operation_id, transaction_dt) all nodes
partition by transaction_dt::date;
