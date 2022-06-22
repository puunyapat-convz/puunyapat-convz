from airflow                   import configuration, DAG
from airflow.operators.python  import PythonOperator, BranchPythonOperator
from airflow.operators.bash    import BashOperator
from airflow.operators.dummy   import DummyOperator
from airflow.models            import Variable
from airflow.utils.task_group  import TaskGroup
from airflow.macros            import *
from utils.dag_notification    import *

from airflow.providers.google.cloud.operators.bigquery        import *

import datetime as dt
import os, logging

######### VARIABLES ###########

log       = logging.getLogger(__name__)
path      = configuration.get('core','dags_folder')
MAIN_PATH = path + "/../data"

PROJECT_ID  = 'central-cto-ofm-data-hub-dev'
BUCKET_TYPE = "prod"

MAIN_FOLDER = "ODP"
SUB_FOLDER  = ["JDA", "POS"]
FILE_EXT    = { "JDA": "dat", "POS": "TXT"  }

###############################

def _gen_date(ds, offset):
    return ds_add(ds, offset)

def _check_file(filename, tablename, branch_id):

    with open(filename) as f:
        count = sum(1 for _ in f)

    if count == 0:
        log.info(f"Table [ {tablename} ] has no file(s) for this run.")
        task_id = f"skip_table_{branch_id}"
    else:
        task_id = f"load_gbq_{branch_id}"

    os.remove(filename)
    return task_id  

with DAG(
    dag_id="gcs2gbq_daily_odp_dev",
    # schedule_interval=None,
    schedule_interval="00 02 * * *",
    start_date=dt.datetime(2022, 6, 15),
    catchup=True,
    max_active_runs=1,
    tags=['convz', 'development', 'mario', 'daily_data', 'odp'],
    render_template_as_native_obj=True,
    default_args={
        'on_failure_callback': ofm_task_fail_slack_alert,
        'retries': 0
    }
) as dag:

    start_task = DummyOperator(task_id = "start_task")
    end_task   = DummyOperator(task_id = "end_task")

    iterable_sources_list = Variable.get(
        key=f'sftp_folders',
        default_var=['default_table'],
        deserialize_json=True
    )
    # iterable_sources_list = {
    #   'ODP_JDA': ["BCH_JDA_DataPlatform_APADDR", "BCH_JDA_DataPlatform_SETDTL"],
    #   'ODP_POS': ["POS_DataPlatform_Txn_DiscountCoupon"]
    #  }

    with TaskGroup(
        f'load_{MAIN_FOLDER}_tasks_group',
        prefix_group_id=False,
    ) as load_source_tasks_group:

        for source in SUB_FOLDER:

            BUCKET_NAME = f"sftp-ofm-{source.lower()}-{BUCKET_TYPE}"
            DS_SUFFIX   = "_new" if source == "JDA" else ""
            DATASET_ID  = f"ofm_{source.lower()}_prod{DS_SUFFIX}"

            start_source = DummyOperator(task_id = f"start_task_{source}")

            with TaskGroup(
                f'load_{source}_tasks_group',
                prefix_group_id=False,
            ) as load_tables_tasks_group:

                for table in iterable_sources_list.get(f"{MAIN_FOLDER}_{source}"):

                    TABLE_ID = f'{table}'
                    PREFIX   = "JDA_" if source == "JDA" else ""
                    NULLMARK = "NULL" if table  == "POS_DataPlatform_Txn_Sales" else None

                    create_table = BigQueryCreateEmptyTableOperator(
                        task_id = f"create_table_{table}",
                        google_cloud_storage_conn_id = "convz_dev_service_account",
                        bigquery_conn_id = "convz_dev_service_account",
                        project_id = PROJECT_ID,
                        dataset_id = DATASET_ID,
                        table_id = TABLE_ID,
                        gcs_schema_object = f"gs://{BUCKET_NAME}/schema/{source}/{PREFIX}{table.replace('_DataPlatform','')}.schema",
                        time_partitioning = { "type": "DAY" },
                    )

                    with TaskGroup(
                        f'load_{table}_tasks_group',
                        prefix_group_id=False,
                    ) as load_interval_tasks_group:

                        range = [0,1] if source == "JDA" else [1,2]

                        for interval in range:

                            gen_date = PythonOperator(
                                task_id=f"gen_date_{table}_{interval}",
                                python_callable=_gen_date,
                                op_kwargs = {
                                    "ds"    : '{{ data_interval_end.strftime("%Y-%m-%d") }}',
                                    "offset": -interval
                                }
                            )

                            create_list = BashOperator(
                                task_id = f"create_list_{table}_{interval}",
                                cwd     = MAIN_PATH,
                                bash_command = f'temp=$(mktemp dev_{table}_{interval}.XXXXXXXX)'
                                                + f' && gsutil ls "gs://{BUCKET_NAME}/{MAIN_FOLDER}/{source}/{table}/*_'
                                                + f'{{{{ ti.xcom_pull(task_ids="gen_date_{table}_{interval}") }}}}.{FILE_EXT.get(source)}"'
                                                + f' > $temp; echo {MAIN_PATH}/$temp'
                            )

                            check_list = BranchPythonOperator(
                                task_id=f'check_list_{table}_{interval}',
                                python_callable=_check_file,
                                op_kwargs = { 
                                    'branch_id': f"{table}_{interval}",
                                    'tablename': table,
                                    'filename' : f'{{{{ ti.xcom_pull(task_ids="create_list_{table}_{interval}") }}}}',
                                }
                            )

                            skip_table = DummyOperator(task_id = f"skip_table_{table}_{interval}")

                            load_gbq = BigQueryInsertJobOperator( 
                                task_id = f"load_gbq_{table}_{interval}",
                                gcp_conn_id = "convz_dev_service_account",
                                configuration = {
                                    "load": {
                                        "sourceUris": [ f'gs://{BUCKET_NAME}/{MAIN_FOLDER}/{source}/{table}/*'
                                                        + f'{{{{ ti.xcom_pull(task_ids="gen_date_{table}_{interval}") }}}}.{FILE_EXT.get(source)}' ],
                                        "destinationTable": {
                                            "projectId": PROJECT_ID,
                                            "datasetId": DATASET_ID,
                                            "tableId"  : f'{TABLE_ID}${{{{ ti.xcom_pull(task_ids="gen_date_{table}_{interval}").replace("-","") }}}}'
                                        },
                                        "sourceFormat"   : "CSV",
                                        "fieldDelimiter" : "|",
                                        "nullMarker"     : NULLMARK,
                                        "skipLeadingRows": 1,
                                        "timePartitioning" : { "type": "DAY" },
                                        "createDisposition": "CREATE_IF_NEEDED",
                                        "writeDisposition" : "WRITE_TRUNCATE",
                                        "allowQuotedNewlines": True
                                    }
                                }
                            )

                            gen_date >> create_list >> check_list >> [ skip_table, load_gbq ]

                    create_table >> load_interval_tasks_group

            start_source >> load_tables_tasks_group

    start_task >> load_source_tasks_group >> end_task