import sys
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

import pendulum
import logging
from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.operators.python import PythonOperator

from utils.config import *


def extract_mongo_data(**kwargs):
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
                            'corr_text': conversation.get('corr_text'),
                            '_id': doc.get('_id')
                        }
                        transformed_data_output.append(transformed_conversation)
        return transformed_data_output

    raw_data = kwargs['ti'].xcom_pull(key='raw_data', task_ids='extract_mongo_data')
    transformed_data = transform_conversation_data(raw_data)
    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)
    for document in transformed_data:
        logging.info(document)


def load_sheet_data(**kwargs):
    transformed_data = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transform_mongo_data')
    sheet_data = [[data["orig_text"], data["corr_text"]] for data in transformed_data]
    gsheet_hook = GSheetsHook(
        gcp_conn_id="google_cloud_sondo",
    )
    spreadsheet_id = '1FqP2iL_5yLXoTNUG4EOXJRypfu8ola_z6PaQabwsSaE'
    range_name = 'Sheet1!A1:A'
    gsheet_hook.append_values(
        spreadsheet_id=spreadsheet_id,
        range_=range_name,
        values=sheet_data,
        value_input_option='RAW'
    )


with DAG(
        dag_id="grammaraide_conversation_gec_data_logs_mongo_v01",
        default_args=DefaultConfig.DEFAULT_DAG_ARGS,
        start_date=pendulum.datetime(2024, 6, 1, tz="UTC"),
        schedule_interval='@once',  # Run on demand
        catchup=False,
        tags=["grammaraide", "data pipeline"],
) as dag:
    extract_task = PythonOperator(
        task_id='extract_mongo_data',
        python_callable=extract_mongo_data,
    )

    transform_task = PythonOperator(
        task_id='transform_mongo_data',
        python_callable=transform_mongo_data,
    )

    load_task = PythonOperator(
        task_id='load_sheet_data',
        python_callable=load_sheet_data,
    )

    load_task >> transform_task >> load_task
