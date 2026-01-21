import pandas as pd
from airflow.providers.vertica.hooks.vertica import VerticaHook


def insert_currencies_to_vertica(csv_path: str, ds: str) -> None:
    vertica_hook = VerticaHook(vertica_conn_id='VERTICA_CONNECTION')

    delete_sql = f"""DELETE FROM VT2512182D1223__STAGING.currencies
                     WHERE date_update::date = '{ds}';"""

    print('Sucess delete in currencies')

    vertica_hook.run(delete_sql)

    sql = f"""COPY VT2512182D1223__STAGING.currencies (date_update, currency_code,
                                                      currency_code_with, currency_with_div)
           FROM LOCAL '{csv_path}'
           DELIMITER ',';"""

    # Получаем данные в DataFrame
    df = vertica_hook.run(sql)

    print('Sucessful Insert to Vertica Currencies')
    return
