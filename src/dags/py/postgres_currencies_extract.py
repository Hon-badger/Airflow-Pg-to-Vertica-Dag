import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook


def extract_postgres_currencies_to_csv(ds: str) -> str:
    pg_hook = PostgresHook(postgres_conn_id='POSTGRES_CONNECTION')

    sql = f"""select
                *
              from
                public.currencies
              where
                date_update::date = '{ds}';"""

    # Получаем данные в DataFrame
    df = pg_hook.get_pandas_df(sql)

    # Сохраняем в CSV
    output_path = f'/opt/airflow/csv_data/currencies_{ds}.csv'
    df.to_csv(output_path, index=False)
    print(f"Get data{df.head(5)}")
    print(f"Data saved to {output_path}")
    return output_path
