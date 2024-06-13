import pendulum

from airflow.models.dag import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils import *

with DAG(
    dag_id="test_dag_postgres_operator_v01",
    default_args=DefaultConfig.DEFAULT_DAG_ARGS,
    start_date=pendulum.datetime(2024, 6, 1, tz="UTC"),
    schedule_interval='@once',
    catchup=False,
    tags=["data_pipeline"],
) as dag:
    task1 = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_conn',
        sql="""
            create table if not exists dag_runs (
                dt data,
                dag_id chracter varying,
                primary key (dt, dag_id)
            )
        """
    )
    task1