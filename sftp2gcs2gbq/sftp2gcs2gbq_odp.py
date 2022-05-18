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
from airflow.providers.google.cloud.operators.gcs             import *
from airflow.providers.google.cloud.transfers.sftp_to_gcs     import *
from airflow.providers.google.cloud.transfers.gcs_to_sftp     import *
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import *

import datetime as dt
import pathlib
import logging
import json

log       = logging.getLogger(__name__)
path      = configuration.get('core','dags_folder')
MAIN_PATH = path + "/../data"

MAIN_FOLDER = "ODP"
SUB_FOLDER  = ["JDA", "POS"]
BUCKET_TYPE = "prod"

def _gen_date(ds, offset):
    return ds_add(ds, offset).replace("-","")

def _list_file(subfolder, tablename):
    hook = SFTPHook(ssh_conn_id="sftp-odp-connection")
    return hook.list_directory(f"/{subfolder}/outbound/{tablename}/archive/")

def _filter_file(ti, branch_id, date_str, sftp_list):
    file_prefix = ""

    for file in sftp_list:
        if str(date_str) in file:
            file_prefix = '_'.join(file.split('_')[:2]) + f'_{date_str}*'
            break

    if file_prefix:
        ti.xcom_push(key='sftp_prefix', value=file_prefix)
        return f"save_gcs_{branch_id}"
    else:
        return f"skip_table_{branch_id}"

with DAG(
    dag_id="sftp2gcs2gbq_odp",
    schedule_interval=None,
    # schedule_interval="20 01 * * *",
    start_date=dt.datetime(2022, 5, 12),
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
      'ODP_POS': ["POS_DataPlatform_Master_Discount"]
     }

    with TaskGroup(
        f'load_{MAIN_FOLDER}_tasks_group',
        prefix_group_id=False,
    ) as load_source_tasks_group:

        for source in SUB_FOLDER:

            BUCKET_NAME = f"sftp-ofm-{source.lower()}-{BUCKET_TYPE}"

            with TaskGroup(
                f'load_{source}_tasks_group',
                prefix_group_id=False,
            ) as load_tables_tasks_group:

                for table in iterable_sources_list.get(f"{MAIN_FOLDER}_{source}"):

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
                                    "ds"    : '{{ ds }}',
                                    "offset": -interval
                                }
                            )

                            filter_file = BranchPythonOperator(
                                task_id=f'filter_file_{table}_{interval}',
                                python_callable=_filter_file,
                                op_kwargs = {
                                    'branch_id': f'{table}_{interval}',
                                    'date_str' : f'{{{{ ti.xcom_pull(task_ids="gen_date_{table}_{interval}") }}}}',
                                    'sftp_list': f'{{{{ ti.xcom_pull(task_ids="list_file_{table}") }}}}'
                                }
                            )

                            skip_table = DummyOperator(task_id = f"skip_table_{table}_{interval}")

                            save_gcs = SFTPToGCSOperator(
                                task_id = f"save_gcs_{table}_{interval}",
                                source_path = f'/{source}/outbound/{table}/archive/{{{{ ti.xcom_pull(key = "sftp_prefix", task_ids="filter_file_{table}_{interval}") }}}}', 
                                destination_bucket = BUCKET_NAME, 
                                destination_path = f"{MAIN_FOLDER}/{source}/test_{table}", 
                                gcp_conn_id = 'convz_dev_service_account', 
                                sftp_conn_id = 'sftp-odp-connection',
                                move_object = True, ## Set to True when testing is done
                            )

                            archive_file = GCSToSFTPOperator(
                                task_id = f"archive_file_{table}_{interval}",
                                source_bucket = BUCKET_NAME, 
                                source_object = f'{MAIN_FOLDER}/{source}/test_{table}/{{{{ ti.xcom_pull(key = "sftp_prefix", task_ids="filter_file_{table}_{interval}") }}}}', 
                                destination_path = f'/{source}/outbound/{table}/archive/',
                                keep_directory_structure = False,
                                gcp_conn_id = 'convz_dev_service_account', 
                                sftp_conn_id = 'sftp-odp-connection',
                                move_object = False,
                            )

                            gen_date >> filter_file >> [ skip_table, save_gcs ] 
                            save_gcs >> archive_file

                    list_file >> load_interval_tasks_group

    start_task >> load_source_tasks_group >> end_task

