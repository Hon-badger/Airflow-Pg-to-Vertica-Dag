import pandas as pd
from airflow.providers.vertica.hooks.vertica import VerticaHook


def insert_transactions_to_vertica(csv_path: str, ds: str) -> None:
    vertica_hook = VerticaHook(vertica_conn_id='VERTICA_CONNECTION')

    delete_sql = f"""DELETE FROM VT2512182D1223__STAGING.transactions
                     WHERE transaction_dt::date = '{ds}';"""

    print('Sucess delete in transactions')

    vertica_hook.run(delete_sql)

    sql = f"""COPY VT2512182D1223__STAGING.transactions (operation_id, account_number_from,
                                                      account_number_to, currency_code, country,
                                                      status, transaction_type, amount, transaction_dt)
              FROM LOCAL '{csv_path}'
              DELIMITER ',';"""

    # Получаем данные в DataFrame
    df = vertica_hook.run(sql)

    print(f"Sucessful Insert to Vertica transactions '{ds}'")
    return
