from airflow                   import configuration, DAG
from airflow.operators.python  import PythonOperator
from airflow.operators.dummy   import DummyOperator
from airflow.models            import Variable
from airflow.utils.task_group  import TaskGroup

from airflow.providers.sftp.hooks.sftp  import *

import datetime as dt
import logging

######### VARIABLES ###########

log       = logging.getLogger(__name__)
path      = configuration.get('core','dags_folder')
MAIN_PATH = path + "/../data"

PROJECT_ID  = 'central-cto-ofm-data-hub-prod'
SOURCE_TYPE = "daily"
BUCKET_TYPE = "prod"

MAIN_FOLDER = ["ODP", "B2S"]
SUB_FOLDER  = "POS"

###############################

def _list_file(hookname, mainfolder, subfolder, tablename):
    SFTP_HOOK = SFTPHook(ssh_conn_id=hookname, keepalive_interval=10)
    file_list = SFTP_HOOK.list_directory(f"/{subfolder}/outbound/{tablename}/")
    SFTP_HOOK.close_conn()
    return file_list

with DAG(
    dag_id="sftp2gcs2gbq_list",
    schedule_interval=None,
    # schedule_interval="50 00 * * *",
    start_date=dt.datetime(2022, 5, 25),
    catchup=True,
    max_active_runs=1,
    tags=['convz', 'production', 'mario', 'daily_data', 'sftp', 'pos', 'adhoc'],
    render_template_as_native_obj=True,
    # default_args={
    #     'on_failure_callback': ofm_task_fail_slack_alert,
    #     'retries': 0
    # }
) as dag:

    start_task = DummyOperator(task_id = "start_task")
    end_task   = DummyOperator(task_id = "end_task")

    iterable_sources_list = Variable.get(
        key=f'sftp_folders',
        default_var=['default_table'],
        deserialize_json=True
    )
    # iterable_sources_list = {
    #   "ODP_POS": ["POS_DataPlatform_Txn_Translator"],
    #   "B2S_POS": ["POS_DataPlatform_Txn_Translator"]
    # }

    with TaskGroup(
        f'load_{SUB_FOLDER}_tasks_group',
        prefix_group_id=False,
    ) as load_source_tasks_group:

        for source in MAIN_FOLDER:

            start_source = DummyOperator(task_id = f"start_{source}")

            if source == "ODP": source = "OFM"

            BUCKET_NAME = f"sftp-{source.lower()}-{SUB_FOLDER.lower()}-{BUCKET_TYPE}"
            DATASET_ID  = f"{SUB_FOLDER.lower()}_{source.lower()}_daily_source"

            if source == "OFM": source = "ODP"

            with TaskGroup(
                f'load_{source}_tasks_group',
                prefix_group_id=False,
            ) as load_tables_tasks_group:

                for table in iterable_sources_list.get(f"{source}_{SUB_FOLDER}"):

                    TABLE_ID = f'{table}'

                    list_file = PythonOperator(
                        task_id=f'list_file_{source}_{table}',
                        python_callable=_list_file,
                        op_kwargs = {
                            'hookname'  : f"sftp-{source.lower()}-connection",
                            'mainfolder': source,
                            'subfolder' : SUB_FOLDER,
                            'tablename' : table
                        }
                    )

                    start_source >> load_tables_tasks_group

    start_task >> load_source_tasks_group >> end_task
