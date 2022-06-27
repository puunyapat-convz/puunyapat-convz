from airflow                   import configuration, DAG
from airflow.operators.python  import PythonOperator, BranchPythonOperator
from airflow.operators.bash    import BashOperator
from airflow.operators.dummy   import DummyOperator
from airflow.models            import Variable
from airflow.utils.task_group  import TaskGroup
from airflow.macros            import *

from airflow.providers.google.cloud.operators.bigquery        import *

import datetime as dt
import os, logging

######### VARIABLES ###########

log       = logging.getLogger(__name__)
path      = configuration.get('core','dags_folder')
MAIN_PATH = path + "/../data"

PROJECT_ID  = 'central-cto-ofm-data-hub-dev'
BUCKET_TYPE = "prod"

MAIN_FOLDER = "B2S"
SUB_FOLDER  = ["JDA", "POS"]
FILE_EXT    = { "JDA": "dat", "POS": "TXT"  }

SQL_COMMAND = { 
    "COUNT"  : "SELECT ",
    "COMPARE": ""
}

with DAG(
    dag_id="row_compare",
    schedule_interval=None,
    # schedule_interval="00 02 * * *",
    start_date=dt.datetime(2022, 6, 24),
    catchup=False,
    max_active_runs=1,
    tags=['convz', 'production', 'mario', 'utility'],
    render_template_as_native_obj=True,
    # default_args={
    #     'on_failure_callback': ofm_task_fail_slack_alert,
    #     'retries': 0
    # }
) as dag:

    start_task = DummyOperator(task_id = "start_task")
    end_task   = DummyOperator(task_id = "end_task")

