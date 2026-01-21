from airflow.decorators import dag, task
from airflow.models.variable import Variable
import pendulum
from py.postgres_transactions_extract import extract_postgres_transactions_to_csv
from py.vertica_transactions_insert import insert_transactions_to_vertica

START_DATE_IN_DATA = pendulum.datetime(2022, 10, 1, tz="UTC")
today = pendulum.today("UTC").date()
start_date=pendulum.datetime(2025, 12, 20, tz="UTC")

@dag(
    schedule_interval='0 22 * * *',
    start_date=pendulum.datetime(2025, 12, 20, tz="UTC"),
    catchup=False,
    tags=['stg', 'origin', 'project', 'final', 'new', 'transactions'],
    is_paused_upon_creation=True
)
def vertica_transactions_etl():

    delta_days = (today - start_date.date()).days
    processing_date = START_DATE_IN_DATA.add(days=delta_days).to_date_string()
    
    @task()
    def extract_task():
        # 1. Extract data from postgres and save as .csv
        extract_postgres_transactions = extract_postgres_transactions_to_csv(ds=processing_date)
        return extract_postgres_transactions

    @task()
    def insert_task(csv_data):
        # 2. Insert Data in Vertica as COPY code
        insert_data_to_vertica =  insert_transactions_to_vertica(csv_path=csv_data, ds=processing_date)

    extract_data = extract_task()
    insert_date = insert_task(extract_data)

stg_transactions_dag = vertica_transactions_etl()
