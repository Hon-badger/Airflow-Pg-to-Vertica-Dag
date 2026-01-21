from airflow.decorators import dag, task
from airflow.models.variable import Variable
import pendulum
from py.vertica_global_metrics_insert import insert_data_into_dwh_global_metrics

START_DATE_IN_DATA = pendulum.datetime(2022, 10, 1, tz="UTC")
today = pendulum.today("UTC").date()
start_date=pendulum.datetime(2025, 12, 20, tz="UTC")

@dag(
    schedule_interval='0 22 * * *',
    start_date=pendulum.datetime(2025, 12, 20, tz="UTC"),
    catchup=False,
    tags=['dwh', 'origin', 'project', 'final', 'new', 'global_metrics'],
    is_paused_upon_creation=True
)
def vertica_global_metrics_etl():

    delta_days = (today - start_date.date()).days
    processing_date = START_DATE_IN_DATA.add(days=delta_days).to_date_string()

    @task
    def run_dwh_dag():
        insert_data_into_dwh = insert_data_into_dwh_global_metrics(ds=processing_date)

    run = run_dwh_dag()

    run

dwh_vertica_etl = vertica_global_metrics_etl()
