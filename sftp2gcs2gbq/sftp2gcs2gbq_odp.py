from airflow                   import configuration, DAG
from airflow.operators.python  import PythonOperator, BranchPythonOperator
from airflow.operators.bash    import BashOperator
from airflow.operators.dummy   import DummyOperator
from airflow.models            import Variable
from airflow.utils.task_group  import TaskGroup
from airflow.macros            import *
from utils.dag_notification    import *

from airflow.providers.sftp.hooks.sftp                        import *
from airflow.providers.google.cloud.operators.bigquery        import *
from airflow.providers.google.cloud.transfers.sftp_to_gcs     import *
from airflow.providers.google.cloud.transfers.gcs_to_sftp     import *
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import *

import datetime as dt
import pathlib
import fnmatch
import logging

######### VARIABLES ###########

log       = logging.getLogger(__name__)
path      = configuration.get('core','dags_folder')
MAIN_PATH = path + "/../data"

PROJECT_ID  = 'central-cto-ofm-data-hub-prod'
SOURCE_TYPE = "daily"
BUCKET_TYPE = "prod"

MAIN_FOLDER = "ODP"
SUB_FOLDER  = ["JDA", "POS"]
FILE_EXT    = { "JDA": "dat", "POS": "TXT"  }

###############################

def _list_file(subfolder, tablename):
    hook = SFTPHook(ssh_conn_id="sftp-odp-connection")
    return hook.list_directory(f"/{subfolder}/outbound/{tablename}/archive/")

def _gen_date(ds, offset):
    return ds_add(ds, offset)

def _filter_file(ti, subfolder, tablename, branch_id, date_str):
    hook = SFTPHook(ssh_conn_id="sftp-odp-connection")
    sftp_list   = []
    
    remote_path = f"/{subfolder}/outbound/{tablename}/archive/"
    local_path  = f"{MAIN_PATH}/{MAIN_FOLDER}_{subfolder}/{tablename}/"

    extension   = FILE_EXT.get(subfolder)
    pattern     = f"*{date_str.replace('-','')}*.{extension}"

    pathlib.Path(local_path).mkdir(parents=True, exist_ok=True)
    log.info(f"Local path: [{local_path}]")
    log.info(f"Filename criteria: [{pattern}]")
    
    for filename in hook.list_directory(remote_path):
        if fnmatch.fnmatch(filename, pattern):
            new_name = filename.replace(f'.{extension}', f'_{date_str}.{extension}')
            log.info(f"Retrieving file: [{new_name}]...")
            hook.retrieve_file(remote_path + filename, local_path + new_name)
            sftp_list.append(new_name)

    if sftp_list:
        ti.xcom_push(key='upload_path', value=local_path)
        return f"save_gcs_{branch_id}"
    else:
        return f"skip_table_{branch_id}"

with DAG(
    dag_id="sftp2gcs2gbq_odp",
    schedule_interval=None,
    # schedule_interval="10 00 * * *",
    start_date=dt.datetime(2022, 5, 17),
    catchup=False,
    max_active_runs=1,
    tags=['convz', 'production', 'mario', 'daily_data', 'odp'],
    render_template_as_native_obj=True,
    # default_args={
    #     'on_failure_callback': ofm_task_fail_slack_alert,
    #     'retries': 0
    # }
) as dag:

    start_task = DummyOperator(task_id = "start_task")
    end_task   = DummyOperator(task_id = "end_task")

    # iterable_sources_list = Variable.get(
    #     key=f'sftp_folders',
    #     default_var=['default_table'],
    #     deserialize_json=True
    # )
    iterable_sources_list = {
      'ODP_JDA': ["BCH_JDA_DataPlatform_APADDR"],
      'ODP_POS': ["POS_DataPlatform_Txn_DiscountCoupon"]
     }

    with TaskGroup(
        f'load_{MAIN_FOLDER}_tasks_group',
        prefix_group_id=False,
    ) as load_source_tasks_group:

        for source in SUB_FOLDER:

            BUCKET_NAME = f"sftp-ofm-{source.lower()}-{BUCKET_TYPE}"
            DATASET_ID  = f"{source.lower()}_ofm_daily_source"

            with TaskGroup(
                f'load_{source}_tasks_group',
                prefix_group_id=False,
            ) as load_tables_tasks_group:

                for table in iterable_sources_list.get(f"{MAIN_FOLDER}_{source}"):

                    TABLE_ID = f'test_{table}_daily_source'
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
                                    "ds"    : '{{ ds }}',
                                    "offset": -interval
                                }
                            )

                            filter_file = BranchPythonOperator(
                                task_id=f'filter_file_{table}_{interval}',
                                python_callable=_filter_file,
                                op_kwargs = {
                                    'subfolder': source,
                                    'tablename': table,
                                    'branch_id': f'{table}_{interval}',
                                    'date_str' : f'{{{{ ti.xcom_pull(task_ids="gen_date_{table}_{interval}") }}}}',
                                }
                            )

                            skip_table = DummyOperator(task_id = f"skip_table_{table}_{interval}")
                            save_gcs   = DummyOperator(task_id = f"save_gcs_{table}_{interval}")

                            # save_gcs = SFTPToGCSOperator(
                            #     task_id = f"save_gcs_{table}_{interval}",
                            #     source_path = f'/{source}/outbound/{table}/archive/{{{{ ti.xcom_pull(key = "upload_path", task_ids="filter_file_{table}_{interval}") }}}}', 
                            #     destination_bucket = BUCKET_NAME, 
                            #     destination_path = f"{MAIN_FOLDER}/{source}/test_{table}", 
                            #     gcp_conn_id = 'convz_dev_service_account', 
                            #     sftp_conn_id = 'sftp-odp-connection',
                            #     move_object = False, ## Set as True when testing is done
                            # )

                            # archive_file = GCSToSFTPOperator(
                            #     task_id = f"archive_file_{table}_{interval}",
                            #     source_bucket = BUCKET_NAME, 
                            #     source_object = f'{MAIN_FOLDER}/{source}/test_{table}/{{{{ ti.xcom_pull(key = "upload_path", task_ids="filter_file_{table}_{interval}") }}}}', 
                            #     destination_path = f'/{source}/outbound/{table}/archive/',
                            #     keep_directory_structure = False,
                            #     gcp_conn_id = 'convz_dev_service_account', 
                            #     sftp_conn_id = 'sftp-odp-connection',
                            #     move_object = False,
                            # )

                            # load_gbq = BigQueryInsertJobOperator( 
                            #     task_id = f"load_gbq_{table}_{interval}",
                            #     gcp_conn_id = "convz_dev_service_account",
                            #     configuration = {
                            #         "load": {
                            #             "sourceUris": [ f'gs://{BUCKET_NAME}/{MAIN_FOLDER}/{source}/test_{table}/{{{{ ti.xcom_pull(key = "upload_path", task_ids="filter_file_{table}_{interval}") }}}}.{FILE_EXT.get(source)}' ],
                            #             "destinationTable": {
                            #                 "projectId": PROJECT_ID,
                            #                 "datasetId": DATASET_ID,
                            #                 "tableId"  : f'{TABLE_ID}${{{{ ti.xcom_pull(task_ids="gen_date_{table}_{interval}") }}}}'
                            #             },
                            #             "sourceFormat"   : "CSV",
                            #             "fieldDelimiter" : "|",
                            #             "skipLeadingRows": 1,
                            #             "timePartitioning" : { "type": "DAY" },
                            #             "createDisposition": "CREATE_IF_NEEDED",
                            #             "writeDisposition" : "WRITE_TRUNCATE"
                            #         }
                            #     }
                            # )

                            gen_date >> filter_file >> [ skip_table, save_gcs ] 
                            # save_gcs >> [ archive_file, load_gbq ]

                    create_table >> load_interval_tasks_group

    start_task >> load_source_tasks_group >> end_task

