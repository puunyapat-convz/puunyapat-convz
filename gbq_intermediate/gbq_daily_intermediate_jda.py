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
import logging, arrow

log       = logging.getLogger(__name__)
path      = configuration.get('core','dags_folder')

MAIN_PATH = path + "/../data"
TIMEZONE  = 'Asia/Bangkok'

BUCKET_NAME  = "ofm-data"
SOURCE_NAME  = "gbq_intermediate"
LOCATION     = "asia-southeast1"

SHORTDATE_TB = [ 
    "central-cto-ofm-data-hub-prod.jda_b2s_landing_zone_views.b2s_jdaacstk_daily_v2",
    "central-cto-ofm-data-hub-prod.jda_ofm_landing_zone_views.ofm_jdaacstk_daily_v2"
]

def _read_query(blobname):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(BUCKET_NAME)
    blob   = storage.Blob(blobname, bucket)
    return blob.download_as_bytes().decode()

def _update_query(query, table, run_date):

    if table in SHORTDATE_TB:
        run_date = run_date.replace("-","")[2:]

    return query.replace("CURRENT_DATE",run_date)

def _gen_date(ds, offset):
    localtime = arrow.get(ds).to(TIMEZONE)
    log.info(f"UTC time: {ds}")
    log.info(f"{TIMEZONE} time: {localtime}")
    return ds_add(localtime.strftime("%Y-%m-%d"), offset)

with DAG(
    dag_id="gbq_daily_intermediate_jda",
    # schedule_interval=None,
    schedule_interval="00 23 * * *",
    start_date=dt.datetime(2022, 6, 6),
    catchup=True,
    max_active_runs=1,
    tags=['convz', 'production', 'mario', 'intermediate', 'jda'],
    render_template_as_native_obj=True,
    default_args={
        'on_failure_callback': ofm_task_fail_slack_alert,
        'retries': 0
    }
) as dag:

    start_task = DummyOperator(task_id = "start_task")
    end_task   = DummyOperator(task_id = "end_task")

    CONFIG_VALUE = Variable.get(
        key='gbq_intermediate_jda',
        default_var=['default_table'],
        deserialize_json=True
    )
    # CONFIG_VALUE = {
    #     "central-cto-ofm-data-hub-prod.jda_ofm_landing_zone_views.product_replenishment_table": [1,10]
    # }

    iterable_tables_list = CONFIG_VALUE.keys()

    with TaskGroup(
        f'run_table_tasks_group',
        prefix_group_id=False,
    ) as run_table_tasks_group:

        if iterable_tables_list:
            for index, table_fqdn in enumerate(iterable_tables_list):

                if table_fqdn[0] != '#':
                    PROJECT_DST, DATASET_DST, tm1_table = table_fqdn.split(".")
                    DATE_START, DATE_END = CONFIG_VALUE.get(table_fqdn)
                else:
                    continue

                create_ds = BigQueryCreateEmptyDatasetOperator(
                    task_id     = f"create_ds_{DATASET_DST}.{tm1_table}",
                    project_id  = PROJECT_DST,
                    dataset_id  = DATASET_DST,
                    location    = LOCATION,
                    gcp_conn_id = "convz_dev_service_account",
                    exists_ok   = True
                )

                read_query = PythonOperator(
                    task_id=f"read_query_{DATASET_DST}.{tm1_table}",
                    python_callable=_read_query,
                    op_kwargs = {
                        "blobname": f'{SOURCE_NAME}/{PROJECT_DST}/{DATASET_DST}.{tm1_table}.sql',
                    }
                )

                with TaskGroup(
                    f'run_query_tasks_group_{DATASET_DST}.{tm1_table}',
                    prefix_group_id=False,
                ) as run_query_tasks_group:

                    for interval in range(DATE_START,DATE_END+1):
                        interval = f"{interval:03d}"

                        gen_date = PythonOperator(
                            task_id=f"gen_date_{DATASET_DST}.{tm1_table}_{interval}",
                            python_callable=_gen_date,
                            op_kwargs = {
                                "ds"    : '{{ data_interval_end }}',
                                "offset": -int(interval)
                            }
                        )

                        update_query = PythonOperator(
                            task_id=f"update_query_{DATASET_DST}.{tm1_table}_{interval}",
                            python_callable=_update_query,
                            op_kwargs = {
                                "query"   : f'{{{{ ti.xcom_pull(task_ids="read_query_{DATASET_DST}.{tm1_table}") }}}}',
                                "table"   : table_fqdn,
                                "run_date": f'{{{{ ti.xcom_pull(task_ids="gen_date_{DATASET_DST}.{tm1_table}_{interval}") }}}}'
                            }
                        )

                        load_final = BigQueryInsertJobOperator( 
                            task_id = f"load_final_{DATASET_DST}.{tm1_table}_{interval}",
                            gcp_conn_id = "convz_dev_service_account",
                            configuration = {
                                "query": {
                                    "query": f'{{{{ ti.xcom_pull(task_ids="update_query_{DATASET_DST}.{tm1_table}_{interval}") }}}}',
                                    "destinationTable": {
                                        "projectId": PROJECT_DST,
                                        "datasetId": DATASET_DST,
                                        "tableId": f'{tm1_table}${{{{ ti.xcom_pull(task_ids="gen_date_{DATASET_DST}.{tm1_table}_{interval}").replace("-","") }}}}',
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

                        gen_date >> update_query >> load_final                        

                create_ds >> read_query >> run_query_tasks_group

    start_task >> run_table_tasks_group >> end_task