import pandas as pd
from airflow.providers.vertica.hooks.vertica import VerticaHook


def insert_data_into_dwh_global_metrics(ds:str) -> None:
    vertica_hook = VerticaHook(vertica_conn_id='VERTICA_CONNECTION')

    delete_sql = f"""DELETE FROM VT2512182D1223__DWH.global_metrics
                     WHERE date_update::date = '{ds}';"""

    print('Sucess delete in transactions')

    vertica_hook.run(delete_sql)
 
    sql = f"""INSERT INTO VT2512182D1223__DWH.global_metrics (date_update, currency_from, amount_total,
                                                             cnt_transactions, avg_transactions_per_account,
                                                             cnt_accounts_make_transactions)
            with
            only_dollar_currency as (
                select
                    date_update::date                                   as rate_date,
                    currency_code,
                    currency_with_div
                from
                    VT2512182D1223__STAGING.currencies
                where
                    currency_code_with = 420
                    and rate_date = '{ds}'
        ),
        transactions_usd as (
            select
                t.transaction_dt::date                                  as date_update,
                t.currency_code                                         as currency_from,
                t.account_number_from,
                t.operation_id,
                case
                    when t.country != 'usa'
                        then t.amount / odc.currency_with_div
                    else t.amount
                end                                                     as amount_usd
            from
                VT2512182D1223__STAGING.transactions t
            left join
                only_dollar_currency odc
            on
                t.transaction_dt::date = odc.rate_date
                and t.currency_code = odc.currency_code
            where
                date_update = '{ds}'
        )
            select
                date_update,
                currency_from,
                sum (amount_usd)                                        as amount_total,
                count (distinct operation_id)                           as cnt_transactions,
                count (distinct operation_id)
                    / nullif (count (distinct account_number_from), 0)  as avg_transactions_per_account,
                count (distinct account_number_from)                    as cnt_accounts_make_transactions
            from
                transactions_usd
            group by
                date_update,
                currency_from;"""

    # Получаем данные в DataFrame
    df = vertica_hook.run(sql)

    print('Sucessful Insert to Vertica dwh global_metrics')
    return