from airflow                   import configuration, DAG
from airflow.operators.python  import PythonOperator, BranchPythonOperator
from airflow.operators.bash    import BashOperator
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
import pandas   as pd
import pathlib
import tempfile
import logging
import json

log       = logging.getLogger(__name__)
path      = configuration.get('core','dags_folder')
MAIN_PATH = path + "/../data"

MAIN_FOLDER = "ODP"
SUB_FOLDER  = ["JDA", "POS"]
BUCKET_TYPE = "prod"

def _gen_date(ds, offset):
    return ds_add(ds, offset)

with DAG(
    dag_id="sftp2gcs2gcq_odp",
    schedule_interval=None,
    # schedule_interval="20 01 * * *",
    start_date=dt.datetime(2022, 5, 12),
    catchup=False,
    max_active_runs=1,
    tags=['convz', 'production', 'mario', 'daily_data', 'jda'],
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

            BUCKET_NAME = f"sftp-ofm-{source.lower()}"

            with TaskGroup(
                f'load_{source}_tasks_group',
                prefix_group_id=False,
            ) as load_tables_tasks_group:

                for table in iterable_sources_list.get(f"{MAIN_FOLDER}_{source}"):

                    with TaskGroup(
                        f'load_{table}_tasks_group',
                        prefix_group_id=False,
                    ) as load_interval_tasks_group:

                        for interval in [0,1]:

                            gen_date = PythonOperator(
                                task_id=f"gen_date_{table}_{interval}",
                                python_callable=_gen_date,
                                op_kwargs = {
                                    "ds"    : '{{ ds }}',
                                    "offset": -interval
                                }
                            )

                            fetch_file = DummyOperator(task_id = f"fetch_file_{table}_{interval}")

                            gen_date >> fetch_file

                    # dummy_task >> load_interval_tasks_group

    start_task >> load_source_tasks_group >> end_task

