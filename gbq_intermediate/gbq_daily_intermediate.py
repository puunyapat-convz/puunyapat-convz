from airflow                   import configuration, DAG
from airflow.operators.python  import PythonOperator
from airflow.operators.dummy   import DummyOperator
from airflow.models            import Variable
from airflow.utils.task_group  import TaskGroup
from airflow.macros            import *
from utils.dag_notification    import *

from airflow.providers.google.cloud.hooks.gcs          import *
from airflow.providers.google.cloud.operators.bigquery import *
from airflow.providers.google.cloud.operators.gcs      import *

from airflow.providers.google.cloud.transfers.local_to_gcs    import *
from airflow.providers.google.cloud.transfers.gcs_to_local    import *
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import *

import datetime as dt
import logging

log       = logging.getLogger(__name__)
path      = configuration.get('core','dags_folder')
MAIN_PATH = path + "/../data"

BUCKET_NAME  = "ofm-data"
SOURCE_NAME  = "gbq_intermediate"
LOCATION     = "asia-southeast1" 

def _read_query(blobname, run_date):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(BUCKET_NAME)
    blob   = storage.Blob(blobname, bucket)
    query  = blob.download_as_bytes().decode()

    return query.replace("CURRENT_DATE",run_date)

def _gen_date(ds, offset):
    return ds_add(ds, offset)

with DAG(
    dag_id="gbq_daily_intermediate",
    schedule_interval=None,
    # schedule_interval="30 05 * * *",
    start_date=dt.datetime(2022, 5, 11),
    catchup=True,
    max_active_runs=1,
    tags=['convz', 'production', 'mario', 'daily_view', 'intermediate'],
    render_template_as_native_obj=True,
    # default_args={
    #     'on_failure_callback': ofm_task_fail_slack_alert,
    #     'retries': 0
    # }
) as dag:

    start_task = DummyOperator(task_id = "start_task")
    end_task   = DummyOperator(task_id = "end_task")

    CONFIG_VALUE = Variable.get(
        key='gbq_intermediate',
        default_var=['default_table'],
        deserialize_json=True
    )

    iterable_tables_list = CONFIG_VALUE.keys()

    with TaskGroup(
        f'run_table_tasks_group',
        prefix_group_id=False,
    ) as run_table_tasks_group:

        if iterable_tables_list:
            for index, tm1_table in enumerate(iterable_tables_list):

                PROJECT_DST, DATASET_DST = CONFIG_VALUE.get(tm1_table).split(".")

                create_ds = BigQueryCreateEmptyDatasetOperator(
                    task_id     = f"create_ds_{tm1_table}",
                    project_id  = PROJECT_DST,
                    dataset_id  = DATASET_DST,
                    location    = LOCATION,
                    gcp_conn_id = "convz_dev_service_account",
                    exists_ok   = True
                )

                with TaskGroup(
                    f'run_query_tasks_group_{tm1_table}',
                    prefix_group_id=False,
                ) as run_query_tasks_group:

                    for interval in range(0,10):

                        gen_date = PythonOperator(
                            task_id=f"gen_date_{tm1_table}_{interval}",
                            python_callable=_gen_date,
                            op_kwargs = {
                                "ds"    : '{{ ds }}',
                                "offset": -interval
                            }
                        )

                        read_query = PythonOperator(
                            task_id=f"read_query_{tm1_table}_{interval}",
                            python_callable=_read_query,
                            op_kwargs = {
                                "blobname": f'{SOURCE_NAME}/{PROJECT_DST}/{DATASET_DST}.{tm1_table}.sql',
                                "run_date": f'{{{{ ti.xcom_pull(task_ids="gen_date_{tm1_table}_{interval}") }}}}'
                            }
                        )

                        load_final = BigQueryInsertJobOperator( 
                            task_id = f"load_final_{tm1_table}_{interval}",
                            gcp_conn_id = "convz_dev_service_account",
                            configuration = {
                                "query": {
                                    "query": f'{{{{ ti.xcom_pull(task_ids="read_query_{tm1_table}_{interval}") }}}}',
                                    "destinationTable": {
                                        "projectId": PROJECT_DST,
                                        "datasetId": DATASET_DST,
                                        "tableId": f'{tm1_table.lower()}${{{{ ti.xcom_pull(task_ids="gen_date_{tm1_table}_{interval}").replace("-","") }}}}',
                                    },
                                    "createDisposition": "CREATE_IF_NEEDED",
                                    "writeDisposition": "WRITE_TRUNCATE",
                                    "useLegacySql": False,
                                    "timePartitioning": {
                                        "type":"DAY"
                                    },
                                }
                            }
                        )

                        gen_date >> read_query >> load_final                        

                create_ds >> run_query_tasks_group

    start_task >> run_table_tasks_group >> end_task