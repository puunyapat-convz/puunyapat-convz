from airflow                   import configuration, DAG
from airflow.operators.python  import PythonOperator
from airflow.operators.bash    import BashOperator
from airflow.models            import Variable
from airflow.operators.dummy   import DummyOperator
# from airflow.utils.task_group  import TaskGroup

import datetime as dt
import logging

######### VARIABLES ###########

log       = logging.getLogger(__name__)
path      = configuration.get('core','dags_folder')
MAIN_PATH = path + "/../data"

PROJECT_ID   = "central-cto-ofm-data-hub-dev"
DATASET_ID   = "test_airflow"
LOCATION     = "asia-southeast1" 

BUCKET_NAME  = "ofm-data"
SOURCE_NAME  = [ "ERP", "MDS" ] 
SOURCE_TYPE  = "daily"

###############################

def _read_table(source, filename):
    with open(filename) as f:
        lines = f.read().splitlines()

    Variable.set(
        key   = f'{source}_tables',
        value = lines,
        serialize_json = True
    )    

with DAG(
    dag_id="gcs2gbq_create_var",
    # schedule_interval="00 00 * * *",
    schedule_interval=None,
    start_date=dt.datetime(2022, 3, 27),
    catchup=False,
    tags=['convz_prod_airflow_style']
) as dag:

    start_task = DummyOperator(task_id = "start_task")
    end_task = DummyOperator(task_id = "end_task")

    for SOURCE in SOURCE_NAME:

        get_table_names = BashOperator(
            task_id  = f"get_{SOURCE}_tables",
            cwd      = MAIN_PATH,
            bash_command = f"gsutil ls gs://{BUCKET_NAME}/{SOURCE}/{SOURCE_TYPE}"
                            + f" | grep -v erp_ | sed '1d' | cut -d'/' -f6 > {SOURCE}_tm1_folders; cat {SOURCE}_tm1_folders;"
                            + f" echo {MAIN_PATH}/{SOURCE}_tm1_folders"
        )

        read_table_list = PythonOperator(
            task_id = f"read_{SOURCE}_list",
            python_callable = _read_table,
            op_kwargs = {
                'source'   : SOURCE,
                'filename' : f'{{{{ ti.xcom_pull(task_ids="get_{SOURCE}_tables") }}}}',
                # 'filename' : '/Users/oH/airflow/dags/ERP_tm1_folders'
            },
        )

        remove_table_list = BashOperator(
            task_id  = f"remove_{SOURCE}_list",
            cwd      = MAIN_PATH,
            trigger_rule = 'all_done',
            bash_command = "rm -f {{ ti.xcom_pull(task_ids='get_{SOURCE}_tables') }}"
        )

        ## loop level dependencies
        start_task >> get_table_names >> read_table_list >> remove_table_list >> end_task