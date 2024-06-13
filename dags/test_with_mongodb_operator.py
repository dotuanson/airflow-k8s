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

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id="mongo_load_data_with_context_manager_v01",
    default_args=DefaultConfig.DEFAULT_DAG_ARGS,
    start_date=pendulum.datetime(2024, 6, 1, tz="UTC"),
    schedule_interval='@once',  # Run on demand
    catchup=False,
    tags=["data_pipeline"],
) as dag:
    def load_mongo_data_with_context(**kwargs):
        # Initialize the MongoHook
        mongo_hook = MongoHook(conn_id='mongodb_prod')

        # Use context manager to handle the MongoDB connection
        with mongo_hook.get_conn() as client:
            # Access the specific database and collection
            db = client['conventional_gec_logs']
            collection = db['log_data']

            # Fetch the data (for demonstration, limit to first 10 documents)
            data = collection.find().limit(10)

            # Process the data (for demonstration, just log the documents)
            for document in data:
                logging.info(document)


    # Define the PythonOperator
    load_data_task = PythonOperator(
        task_id='load_mongo_data_with_context',
        python_callable=load_mongo_data_with_context,
    )

    # Set the task dependencies
    load_data_task
