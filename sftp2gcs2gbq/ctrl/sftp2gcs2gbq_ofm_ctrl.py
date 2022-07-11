from airflow                   import configuration, DAG
from airflow.operators.python  import PythonOperator, BranchPythonOperator
from airflow.operators.dummy   import DummyOperator
from airflow.models            import Variable
from airflow.utils.task_group  import TaskGroup
from airflow.macros            import *
from utils.dag_notification    import *

from airflow.providers.ssh.hooks.ssh                          import *
from airflow.providers.google.cloud.operators.bigquery        import *
from airflow.providers.google.cloud.transfers.local_to_gcs    import *
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import *

import datetime as dt
import shutil, pathlib, fnmatch, logging, arrow

######### VARIABLES ###########

log       = logging.getLogger(__name__)
path      = configuration.get('core','dags_folder')
MAIN_PATH = path + "/../data"

TIMEZONE  = 'Asia/Bangkok'
SSH_HOOK  = SSHHook(
    ssh_conn_id = "sftp-ofm-connection-id",
    banner_timeout = 120,
    conn_timeout   = 120,
    keepalive_interval = 15
)

PROJECT_ID  = 'central-cto-ofm-data-hub-prod'
SOURCE_TYPE = "daily"
BUCKET_TYPE = "prod"

MAIN_FOLDER = "ODP"
SUB_FOLDER  = ["JDA", "POS"]

FILE_EXT  = { "JDA": "ctrl", "POS": "LOG" }
SCHEMA    = {
                "POS": 
                [
                    {"name":"FileCreateDate", "type":"INTEGER", "mode":"NULLABLE"},
                    {"name":"FileCreateTime", "type":"INTEGER", "mode":"NULLABLE"},
                    {"name":"DataRowCount",   "type":"INTEGER", "mode":"NULLABLE"}
                ],
                "JDA" : 
                [
                    {"name":"IntefaceName", "type":"STRING", "mode":"NULLABLE"},
                    {"name":"BuCode",       "type":"STRING", "mode":"NULLABLE"},
                    {"name":"FileCount",    "type":"STRING", "mode":"NULLABLE"},
                    {"name":"TotalRec",     "type":"STRING", "mode":"NULLABLE"},
                    {"name":"BatchDate",    "type":"STRING", "mode":"NULLABLE"},
                    {"name":"Requester",    "type":"STRING", "mode":"NULLABLE"},
                    {"name":"DIHBatchID",   "type":"STRING", "mode":"NULLABLE"}
                ]
            }

###############################

def _gen_date(ds, offset):
    localtime = arrow.get(ds).to(TIMEZONE)
    log.info(f"UTC time: {ds}")
    log.info(f"{TIMEZONE} time: {localtime}")
    return ds_add(localtime.strftime("%Y-%m-%d"), offset)

def _get_sftp(ti, subfolder, tablename, branch_id, date_str):
    remote_path  = f"/{subfolder}/outbound/{tablename}/"
    archive_path = f"/{subfolder}/outbound/{tablename}/archive/"
    local_path   = f"{MAIN_PATH}/{MAIN_FOLDER}_{subfolder}/ctrl/{tablename}_{date_str}/"

    extension = FILE_EXT.get(subfolder)
    pattern   = f"*{date_str.replace('-','')}*.{extension}"
    matched   = []

    pathlib.Path(local_path).mkdir(parents=True, exist_ok=True)
    log.info(f"Remote path and file criteria: [{remote_path}{pattern}]")
    log.info(f"Remote archive path: [{archive_path}]")
    log.info(f"Local path: [{local_path}]")

    with SSH_HOOK.get_conn() as ssh_client:
        SFTP_HOOK = ssh_client.open_sftp()
        file_list = SFTP_HOOK.listdir(f"/{subfolder}/outbound/{tablename}/")
        
        for filename in file_list:
            if fnmatch.fnmatch(filename, pattern):
                log.info(f"Retrieving file: [{filename}]...")
                new_name = filename.replace(f'.{extension}', f'_{date_str}.{extension}')

                ## download from SFTP and save as new_name
                SFTP_HOOK.get(remote_path + filename, local_path + new_name)

                ## upload local file to SFTP archive directory
                log.info(f"Archiving local file: [{filename.split('/')[-1]}] to SFTP ...")
                SFTP_HOOK.put(local_path + new_name, archive_path + filename)

                ## remove sftp file on source path after move it to archive
                SFTP_HOOK.remove(remote_path + filename)
                matched.append(local_path + new_name)

        ## disconnect when done with all files
        SFTP_HOOK.close()

    log.info(f"Total files: [{len(matched)}]")

    if matched:
        ti.xcom_push(key='upload_list', value=matched)
        return f"save_gcs_{branch_id}"
    else:
        shutil.rmtree(local_path)
        return f"skip_table_{branch_id}"

def _remove_local(subfolder, tablename, date_str):
    local_path   = f"{MAIN_PATH}/{MAIN_FOLDER}_{subfolder}/ctrl/{tablename}_{date_str}/"

    ## remove local temp directory
    log.info(f"Removing local directory: [{local_path}] ...")
    shutil.rmtree(local_path)

with DAG(
    dag_id="sftp2gcs2gbq_ofm_ctrl",
    # schedule_interval=None,
    schedule_interval="30 22,11,16 * * *",
    start_date=dt.datetime(2022, 7, 6),
    catchup=True,
    max_active_runs=1,
    tags=['convz', 'production', 'mario', 'daily_ctrl', 'sftp', 'ofm'],
    description='SFTP to GCS and GBQ for ODP JDA/POS control files',
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
    #   'ODP_JDA': ["BCH_JDA_DataPlatform_APADDR"],
    #   'ODP_POS': ["POS_DataPlatform_Txn_DiscountCoupon"]
    # }

    with TaskGroup(
        f'load_{MAIN_FOLDER}_tasks_group',
        prefix_group_id=False,
    ) as load_source_tasks_group:

        for source in SUB_FOLDER:

            BUCKET_NAME = f"sftp-ofm-{source.lower()}-{BUCKET_TYPE}"
            DATASET_ID  = f"{source.lower()}_ofm_daily_ctrlfiles"

            start_source = DummyOperator(task_id = f"start_task_{source}")

            with TaskGroup(
                f'load_{source}_tasks_group',
                prefix_group_id=False,
            ) as load_tables_tasks_group:

                for index, table in enumerate(iterable_sources_list.get(f"{MAIN_FOLDER}_{source}")):

                    TABLE_ID = f'{table}'

                    create_table = BigQueryCreateEmptyTableOperator(
                        task_id = f"create_table_{table}",
                        google_cloud_storage_conn_id = "convz_dev_service_account",
                        bigquery_conn_id = "convz_dev_service_account",
                        project_id = PROJECT_ID,
                        dataset_id = DATASET_ID,
                        table_id = TABLE_ID,
                        schema_fields = SCHEMA.get(source),
                        time_partitioning = { "type": "DAY" },
                    )

                    with TaskGroup(
                        f'load_{table}_tasks_group',
                        prefix_group_id=False,
                    ) as load_interval_tasks_group:

                        range = [0,1] if source == "JDA" else [1]

                        for interval in range:

                            gen_date = PythonOperator(
                                task_id=f"gen_date_{table}_{interval}",
                                python_callable=_gen_date,
                                op_kwargs = {
                                    "ds"    : '{{ data_interval_end }}',
                                    "offset": -interval
                                }
                            )

                            get_sftp = BranchPythonOperator(
                                task_id=f'get_sftp_{table}_{interval}',
                                python_callable=_get_sftp,
                                pool='sftp_connect_pool',
                                op_kwargs = {
                                    'subfolder': source,
                                    'tablename': table,
                                    'branch_id': f'{table}_{interval}',
                                    'date_str' : f'{{{{ ti.xcom_pull(task_ids="gen_date_{table}_{interval}") }}}}'
                                }
                            )

                            skip_table = DummyOperator(task_id = f"skip_table_{table}_{interval}")

                            save_gcs   = LocalFilesystemToGCSOperator(
                                task_id = f"save_gcs_{table}_{interval}",
                                gcp_conn_id ='convz_dev_service_account',
                                src = f'{{{{ ti.xcom_pull(key = "upload_list", task_ids="get_sftp_{table}_{interval}") }}}}',
                                dst = f"{MAIN_FOLDER}/{source}/{TABLE_ID}/",
                                bucket = BUCKET_NAME
                            )

                            remove_local = PythonOperator(
                                task_id=f'remove_local_{table}_{interval}',
                                python_callable=_remove_local,
                                op_kwargs = {
                                    'subfolder': source,
                                    'tablename': table,
                                    'date_str' : f'{{{{ ti.xcom_pull(task_ids="gen_date_{table}_{interval}") }}}}'
                                }
                            )

                            load_gbq = BigQueryInsertJobOperator( 
                                task_id = f"load_gbq_{table}_{interval}",
                                gcp_conn_id = "convz_dev_service_account",
                                configuration = {
                                    "load": {
                                        "sourceUris": [ f'gs://{BUCKET_NAME}/{MAIN_FOLDER}/{source}/{TABLE_ID}/*'
                                                            + f'{{{{ ti.xcom_pull(task_ids="gen_date_{table}_{interval}") }}}}.{FILE_EXT.get(source)}' ],
                                        "destinationTable": {
                                            "projectId": PROJECT_ID,
                                            "datasetId": DATASET_ID,
                                            "tableId"  : f'{TABLE_ID}${{{{ ti.xcom_pull(task_ids="gen_date_{table}_{interval}").replace("-","") }}}}'
                                        },
                                        "sourceFormat"   : "CSV",
                                        "fieldDelimiter" : "|",
                                        "skipLeadingRows": 1,
                                        "timePartitioning" : { "type": "DAY" },
                                        "createDisposition": "CREATE_IF_NEEDED",
                                        "writeDisposition" : "WRITE_TRUNCATE"
                                    }
                                }
                            )

                            gen_date >> get_sftp >> [ skip_table, save_gcs ]
                            save_gcs >> [ load_gbq, remove_local ]

                    create_table >> load_interval_tasks_group
            start_source >> load_tables_tasks_group

    start_task >> load_source_tasks_group >> end_task