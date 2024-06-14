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

        # Fetch data from MongoDB
        data = list(collection.find().limit(10))

        # Push data to XCom
        kwargs['ti'].xcom_push(key='raw_data', value=data)


def transform_mongo_data(**kwargs):
    # Pull data from XCom
    raw_data = kwargs['ti'].xcom_pull(key='raw_data', task_ids='load_mongo_data')

    def transform_conversation_data(data):
        """
        Transform 'conversation' data from MongoDB documents.

        Parameters:
            data (list): List of MongoDB documents (dictionaries).

        Returns:
            list: Transformed data.
        """
        transformed_data_output = []

        for document in data:
            if 'conversation' in document:
                conversation = document['conversation']
                # Example transformation logic
                transformed_conversation = {
                    'id': conversation.get('id'),
                    'message': conversation.get('message', '').upper(),  # Example transformation
                    'timestamp': conversation.get('timestamp')
                }
                transformed_data_output.append(transformed_conversation)

        return transformed_data_output

    # Transform data
    transformed_data = transform_conversation_data(raw_data)

    # Log or process the transformed data
    for document in transformed_data:
        logging.info(document)


with DAG(
        dag_id="mongo_load_data_with_context_manager_v02",
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

    transform_task = PythonOperator(
        task_id='transform_mongo_data',
        python_callable=transform_mongo_data,
        provide_context=True,
    )

    load_data_task >> transform_task
