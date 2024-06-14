import sys
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

import pendulum
import logging
from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator

from utils.config import *


with DAG(
    dag_id="mongo_load_data_with_context_manager_v01",
    default_args=DefaultConfig.DEFAULT_DAG_ARGS,
    start_date=pendulum.datetime(2024, 6, 1, tz="UTC"),
    schedule_interval='@once',  # Run on demand
    catchup=False,
    tags=["data_pipeline"],
) as dag:
    def load_mongo_data_with_context(**kwargs):
        mongo_hook = MongoHook(conn_id='mongodb_prod')

        with mongo_hook.get_conn() as client:
            db = client['conventional_gec_logs']
            collection = db['log_data']

            data = collection.find().limit(10)

            for document in data:
                logging.info(document)


    load_data_task = PythonOperator(
        task_id='load_mongo_data_with_context',
        python_callable=load_mongo_data_with_context,
    )

    load_data_task
