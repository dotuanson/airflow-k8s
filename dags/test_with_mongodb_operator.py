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


def load_mongo_data(**kwargs):
    mongo_hook = MongoHook(conn_id='mongodb_prod')

    with mongo_hook.get_conn() as client:
        db = client['conventional_gec_logs']
        collection = db['log_data']
        data = list(collection.find().limit(10))
        for doc in data:
            doc['_id'] = str(doc['_id'])
        kwargs['ti'].xcom_push(key='raw_data', value=data)


def transform_mongo_data(**kwargs):
    def transform_conversation_data(data):
        transformed_data_output = []
        for doc in data:
            if 'conversation' in doc:
                for conversation in doc['conversation']:
                    if conversation.get('role') == "user":
                        transformed_conversation = {
                            'orig_text': conversation.get('orig_text'),
                            'corr_text': conversation.get('corr_text')
                        }
                        transformed_data_output.append(transformed_conversation)
        return transformed_data_output

    raw_data = kwargs['ti'].xcom_pull(key='raw_data', task_ids='load_mongo_data')
    transformed_data = transform_conversation_data(raw_data)

    for document in transformed_data:
        logging.info(document)


def load_sheet_data(**kwargs): ...


with DAG(
        dag_id="grammaraide_conversation_gec_data_logs_mongo_v01",
        default_args=DefaultConfig.DEFAULT_DAG_ARGS,
        start_date=pendulum.datetime(2024, 6, 1, tz="UTC"),
        schedule_interval='@once',  # Run on demand
        catchup=False,
        tags=["grammaraide", "data pipeline"],
) as dag:
    load_mongo_task = PythonOperator(
        task_id='load_mongo_data',
        python_callable=load_mongo_data,
    )

    transform_task = PythonOperator(
        task_id='transform_mongo_data',
        python_callable=transform_mongo_data,
        provide_context=True,
    )

    load_mongo_task >> transform_task
