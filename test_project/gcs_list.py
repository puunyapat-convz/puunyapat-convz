from airflow.providers.google.cloud.operators.gcs import *
from airflow.operators.python  import PythonOperator

from airflow      import configuration, DAG
from google.cloud import storage

import datetime as dt

path      = configuration.get('core','dags_folder')
MAIN_PATH = path + "/../data"
BLOB_CHUNK_SIZE = 2560

def _get_csv_header(blob):

    output = f"{MAIN_PATH}/{blob.split('/')[-1]}"
    for line in blob_lines(blob):

        with open(file = output, mode='w') as f:
            f.write(line)

        return output

def blob_lines(filename):
    position = 0
    buff = []
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ofm-data')
    blob = storage.Blob(filename, bucket)

    while True:
        chunk = blob.download_as_text(
            start=position, 
            end=position + BLOB_CHUNK_SIZE, 
            encoding="utf-8"
        )

        if '\n' in chunk:
            part1, part2 = chunk.split('\n', 1)
            buff.append(part1)
            yield ''.join(buff)
            parts = part2.split('\n')
            for part in parts[:-1]:
                yield part
            buff = [parts[-1]]
        else:
            buff.append(chunk)

        position += BLOB_CHUNK_SIZE + 1  # Blob chunk is downloaded using closed interval

        if len(chunk) < BLOB_CHUNK_SIZE:
            yield ''.join(buff)
            return


with DAG(
    dag_id="a_gcs_list",
    schedule_interval=None,
    # schedule_interval="00 03 * * *",
    start_date=dt.datetime(2022, 4, 28),
    catchup=True,
    max_active_runs=1,
    tags=['convz', 'develop', 'airflow_style'],
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

    read_sample = PythonOperator(
        task_id=f"read_sample",
        python_callable=_get_csv_header,
        op_kwargs = {
            "blob" : "Mercury/daily/tbproduct_content/2022_01_18_1642488959528_0.jsonl"
        }
    )

    read_sample