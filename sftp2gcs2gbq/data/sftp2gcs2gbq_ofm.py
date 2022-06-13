from airflow                   import configuration, DAG
from airflow.operators.python  import PythonOperator, BranchPythonOperator
from airflow.operators.dummy   import DummyOperator
from airflow.models            import Variable
from airflow.utils.task_group  import TaskGroup
from airflow.macros            import *
from utils.dag_notification    import *

from airflow.providers.sftp.hooks.sftp                        import *
from airflow.providers.google.cloud.operators.bigquery        import *
from airflow.providers.google.cloud.transfers.local_to_gcs    import *
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import *

import datetime as dt
import shutil, pathlib, fnmatch, logging
import time, random

######### VARIABLES ###########

log       = logging.getLogger(__name__)
path      = configuration.get('core','dags_folder')
MAIN_PATH = path + "/../data"
SFTP_HOOK = SFTPHook(ssh_conn_id="sftp-odp-connection", banner_timeout=30.0)

PROJECT_ID  = 'central-cto-ofm-data-hub-prod'
SOURCE_TYPE = "daily"
BUCKET_TYPE = "prod"

MAIN_FOLDER = "ODP"
SUB_FOLDER  = ["JDA", "POS"]
FILE_EXT    = { "JDA": "dat", "POS": "TXT"  }

###############################

def _list_file(subfolder, tablename):
    ## put random delay to prevent EOFerror
    delay = round(random.uniform(0, 10), 3)
    log.info(f"Waiting with delay {delay} seconds...")
    time.sleep(delay)

    file_list = SFTP_HOOK.list_directory(f"/{subfolder}/outbound/{tablename}/")
    SFTP_HOOK.close_conn()
    return file_list

def _gen_date(ds, offset):
    return ds_add(ds, offset)

def _get_sftp(ti, subfolder, tablename, branch_id, date_str, sftp_list):
    remote_path = f"/{subfolder}/outbound/{tablename}/"
    local_path  = f"{MAIN_PATH}/{MAIN_FOLDER}_{subfolder}/data/{tablename}_{date_str}/"

    extension = FILE_EXT.get(subfolder)
    pattern   = f"*{date_str.replace('-','')}*.{extension}"
    matched   = []

    pathlib.Path(local_path).mkdir(parents=True, exist_ok=True)
    log.info(f"Remote path and file criteria: [{remote_path}{pattern}]")
    log.info(f"Local path: [{local_path}]")
    
    for filename in sftp_list:
        if fnmatch.fnmatch(filename, pattern):
            log.info(f"Retrieving file: [{filename}]...")
            new_name = filename.replace(f'.{extension}', f'_{date_str}.{extension}')

            ## download file from SFTP and save as new_name
            SFTP_HOOK.retrieve_file(remote_path + filename, local_path + new_name)
            matched.append(local_path + new_name)

    ## close session to prevent SFTP overload
    log.info(f"Total files: [{len(matched)}]")
    SFTP_HOOK.close_conn()

    if matched:
        ti.xcom_push(key='upload_list', value=matched)
        return f"save_gcs_{branch_id}"
    else:
        shutil.rmtree(local_path)
        return f"skip_table_{branch_id}"

def _archive_sftp(subfolder, tablename, date_str, file_list):
    local_path   = f"{MAIN_PATH}/{MAIN_FOLDER}_{subfolder}/data/{tablename}_{date_str}/"
    remote_path  = f"/{subfolder}/outbound/{tablename}/"
    archive_path = f"/{subfolder}/outbound/{tablename}/archive/"
    extension    = FILE_EXT.get(subfolder)

    log.info(f"Local path: [{local_path}]")
    log.info(f"SFTP archive path: [{archive_path}]")

    for filename in file_list:
        new_name = filename.split('/')[-1].replace(f'_{date_str}.{extension}', f'.{extension}')

        ## upload local file to SFTP archive directory
        log.info(f"Archiving local file: [{filename.split('/')[-1]}] to SFTP ...")
        SFTP_HOOK.store_file(archive_path + new_name, filename)

        ## remove sftp file on source path after move it to archive
        log.info(f"Removing SFTP file: [{remote_path + new_name}] ...")
        SFTP_HOOK.delete_file(remote_path + new_name)

    ## close session to prevent SFTP overload
    log.info(f"Total files: [{len(file_list)}]")
    SFTP_HOOK.close_conn()

    ## remove local temp directory
    log.info(f"Removing local directory: [{local_path}] ...")
    shutil.rmtree(local_path)

with DAG(
    dag_id="sftp2gcs2gbq_ofm",
    # schedule_interval=None,
    schedule_interval="30 00,11,17 * * *",
    start_date=dt.datetime(2022, 5, 19),
    catchup=True,
    max_active_runs=1,
    tags=['convz', 'production', 'mario', 'daily_data', 'sftp', 'ofm'],
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
    #  }

    with TaskGroup(
        f'load_{MAIN_FOLDER}_tasks_group',
        prefix_group_id=False,
    ) as load_source_tasks_group:

        for source in SUB_FOLDER:

            BUCKET_NAME = f"sftp-ofm-{source.lower()}-{BUCKET_TYPE}"
            DATASET_ID  = f"{source.lower()}_ofm_daily_source"

            start_source = DummyOperator(task_id = f"start_task_{source}")

            with TaskGroup(
                f'load_{source}_tasks_group',
                prefix_group_id=False,
            ) as load_tables_tasks_group:

                for table in iterable_sources_list.get(f"{MAIN_FOLDER}_{source}"):

                    TABLE_ID = f'{table}'
                    PREFIX   = "JDA_" if source == "JDA" else ""

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

                    list_file = PythonOperator(
                        task_id=f'list_file_{table}',
                        python_callable=_list_file,
                        op_kwargs = {
                            'subfolder': source,
                            'tablename': table
                        }
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
                                    "ds"    : '{{ data_interval_end.strftime("%Y-%m-%d") }}',
                                    "offset": -interval
                                }
                            )

                            get_sftp = BranchPythonOperator(
                                task_id=f'get_sftp_{table}_{interval}',
                                python_callable=_get_sftp,
                                op_kwargs = {
                                    'subfolder': source,
                                    'tablename': table,
                                    'branch_id': f'{table}_{interval}',
                                    'date_str' : f'{{{{ ti.xcom_pull(task_ids="gen_date_{table}_{interval}") }}}}',
                                    'sftp_list': f'{{{{ ti.xcom_pull(task_ids="list_file_{table}") }}}}'
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

                            archive_sftp = PythonOperator(
                                task_id=f'archive_sftp_{table}_{interval}',
                                python_callable=_archive_sftp,
                                op_kwargs = {
                                    'subfolder': source,
                                    'tablename': table,
                                    'date_str' : f'{{{{ ti.xcom_pull(task_ids="gen_date_{table}_{interval}") }}}}',
                                    'file_list': f'{{{{ ti.xcom_pull(key = "upload_list", task_ids="get_sftp_{table}_{interval}") }}}}'
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
                                        "nullMarker"     : "NULL",
                                        "skipLeadingRows": 1,
                                        "timePartitioning" : { "type": "DAY" },
                                        "createDisposition": "CREATE_IF_NEEDED",
                                        "writeDisposition" : "WRITE_TRUNCATE",
                                        "allowQuotedNewlines": True
                                    }
                                }
                            )

                            gen_date >> get_sftp >> [ skip_table, save_gcs ]
                            save_gcs >> [ archive_sftp, load_gbq ]

                    [ create_table, list_file ] >> load_interval_tasks_group  

            start_source >> load_tables_tasks_group
                
    start_task >> load_source_tasks_group >> end_task

