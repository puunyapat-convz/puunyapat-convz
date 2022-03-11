from airflow                  import configuration, DAG
from airflow.operators.dummy  import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.decorators       import task
import airflow.contrib.operators.gcs_to_bq as gcs2bq

import datetime as dt
import logging

TEMP_PATH = configuration.get('core','dags_folder') + "/tmp/"
log       = logging.getLogger(__name__)

with DAG(
    dag_id="daily_gcs2gbq",
    schedule_interval=None,
    start_date=dt.datetime(2022, 3, 1),
    catchup=False
) as dag:
    start_task = DummyOperator(task_id="start_task")

    @task(task_id="load_to_staging")
    def load2stg():
        gcs2bq.GoogleCloudStorageToBigQueryOperator(
            
        )
        return

        # gs://ofm-data/ERP/daily/erp_tbdldetail/2022_03_10_1646955354604_0.jsonl