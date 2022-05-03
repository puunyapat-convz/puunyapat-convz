from airflow.providers.google.cloud.operators.gcs import *
from airflow                   import configuration, DAG

import datetime as dt

path      = configuration.get('core','dags_folder')
MAIN_PATH = path + "/../data"

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

    list_file = GCSListObjectsOperator(
        task_id = 'list_file',
        bucket = 'ofm-data', 
        prefix = 'Mercury/daily/tbproduct_content/2022_01_18', 
        delimiter = '.jsonl',
        gcp_conn_id = 'convz_dev_service_account'
    )

    list_file