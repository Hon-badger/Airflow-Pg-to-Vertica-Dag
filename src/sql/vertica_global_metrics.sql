drop table if exists VT2512182D1223__DWH.global_metrics;


create table VT2512182D1223__DWH.global_metrics
(
    date_update                     timestamp,
    currency_from                   int,
    amount_total                    float,
    cnt_transactions                int,
    avg_transactions_per_account    float,
    cnt_accounts_make_transactions  int
)
order by date_update
segmented by hash(currency_from, date_update) all nodes
partition by date_update::date;
