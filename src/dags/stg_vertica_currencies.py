from airflow.decorators import dag, task
from airflow.models.variable import Variable
import pendulum
from py.postgres_currencies_extract import extract_postgres_currencies_to_csv
from py.vertica_currencies_insert import insert_currencies_to_vertica

START_DATE_IN_DATA = pendulum.datetime(2022, 10, 1, tz="UTC")
today = pendulum.today("UTC").date()
start_date=pendulum.datetime(2025, 12, 20, tz="UTC")

@dag(
    schedule_interval='0 22 * * *',
    start_date=pendulum.datetime(2025, 12, 20, tz="UTC"),
    catchup=False,
    tags=['stg', 'origin', 'project', 'final', 'new', 'currencies'],
    is_paused_upon_creation=True
)
def vertica_currencies_etl():

    delta_days = (today - start_date.date()).days
    processing_date = START_DATE_IN_DATA.add(days=delta_days).to_date_string()

    @task
    def extract_task():
        # 1. Extract data from postgres and save as .csv
        extract_postgres_currencies = extract_postgres_currencies_to_csv(ds=processing_date)
        return extract_postgres_currencies

    @task
    def insert_task(csv_data):
        # 2. Insert Data in Vertica as COPY code
        insert_data_to_vertica =  insert_currencies_to_vertica(csv_path=csv_data, ds=processing_date)

    extract_data = extract_task()
    insert_date = insert_task(extract_data)

stg_vertica_currencies = vertica_currencies_etl()
