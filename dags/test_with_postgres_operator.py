import pendulum

from airflow.models.dag import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': "sondt",
    'retries': 5,
    'retry_delay': pendulum.duration(minutes=5),
}

with DAG(
    dag_id="test_dag_postgres_operator_v01",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 6, 1, tz="UTC"),
    schedule_interval='@once',
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