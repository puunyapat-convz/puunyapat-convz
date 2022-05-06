from airflow.providers.google.cloud.operators.gcs import *
from airflow.operators.python  import PythonOperator

from airflow      import configuration, DAG
from google.cloud import storage

import datetime as dt
import logging

path      = configuration.get('core','dags_folder')
MAIN_PATH = path + "/../data"

log       = logging.getLogger(__name__)

def _get_csv_header(blob):

    for line in blob_lines(blob):
        return line

def blob_lines(filename):
    BLOB_CHUNK_SIZE = 2560
    position = 0
    buff = []
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ofm-data')
    blob = storage.Blob(filename, bucket)

    while True:
        chunk = blob.download_as_bytes(
            start=position, 
            end=position + BLOB_CHUNK_SIZE,
        )

        if b'\n' in chunk:
            part1, part2 = chunk.split(b'\n', 1)
            buff.append(part1.decode('utf-8','replace'))
            yield ''.join(buff)
            parts = part2.split(b'\n')

            for part in parts[:-1]:
                yield part.decode('utf-8','replace')

            buff = [parts[-1].decode('utf-8','replace')]
            yield ''.join(buff)
            return
        else:
            buff.append(chunk.decode('utf-8','replace'))

        position += BLOB_CHUNK_SIZE + 1  # Blob chunk is downloaded using closed interval


with DAG(
    dag_id="a_gcs_list",
    schedule_interval=None,
    # schedule_interval="00 03 * * *",
    start_date=dt.datetime(2022, 5, 5),
    catchup=False,
    max_active_runs=1,
    tags=['convz', 'develop', 'mario'],
    render_template_as_native_obj=True,
    # default_args={
    #     'on_failure_callback': ofm_task_fail_slack_alert,
    #     'retries': 0
    # }
) as dag:

    # list_file = GCSListObjectsOperator(
    #     task_id = 'list_file',
    #     bucket = 'ofm-data', 
    #     prefix = 'Mercury/daily/tbproduct_content/2022_01_18', 
    #     delimiter = '.jsonl',
    #     gcp_conn_id = 'convz_dev_service_account'
    # )

    read_sample_1 = PythonOperator(
        task_id=f"read_sample_epro",
        python_callable=_get_csv_header,
        op_kwargs = {
            "blob" : "E-Procurement/daily/TBOrder/2022_05_05_1651791971953_0.jsonl"
        }
    )

    read_sample_2 = PythonOperator(
        task_id=f"read_sample_merc",
        python_callable=_get_csv_header,
        op_kwargs = {
            "blob" : "Mercury/daily/tbproduct_content/2022_05_05_1651791856272_0.jsonl"
        }
    )

    read_sample_1 >> read_sample_2