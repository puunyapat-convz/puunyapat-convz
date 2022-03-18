from airflow                   import configuration, DAG
from airflow.operators.python  import PythonOperator
from airflow.utils.decorators  import apply_defaults
from airflow.operators.bash    import BashOperator
from airflow.models            import Variable
from airflow.utils.task_group  import TaskGroup

import datetime as dt
import logging

from pandas import value_counts

######### VARIABLES ###########

log       = logging.getLogger(__name__)
MAIN_PATH = configuration.get('core','dags_folder')

SCHEMA_FILE    = f"{MAIN_PATH}/schemas/OFM-B2S_Source_Datalake_20211020-live-version.xlsx"
SCHEMA_SHEET   = "Field-ERP"
SCHEMA_COLUMNS = ["TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE"] # Example value ["TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE"]

PROJECT_ID   = "central-cto-ofm-data-hub-dev"
DATASET_ID   = "test_airflow"
LOCATION     = "asia-southeast1" 

BUCKET_NAME  = "ofm-data"
SOURCE_NAME  = "ERP"
SOURCE_TYPE  = "daily"

PAYLOAD_NAME = "_airbyte_data"

###############################

def _read_table(filename):
    with open(filename) as f:
        lines = f.read().splitlines()
    return lines
    # return [ "tbadjusthead", "tblocationareamaster" ]

def _process_list(ti, task_id, var_name, **kwargs):
    if 'value' in kwargs:
        data_from_file = kwargs.get("value")
    else:
        data_from_file = ti.xcom_pull(task_ids = task_id)

    Variable.set(
        key   = var_name,
        value = data_from_file,
        serialize_json = True
    )

def _create_var(var_name):
    Variable.set(
        key   = var_name,
        value = "",
        serialize_json = True
    )

with DAG(
    dag_id="gcs2gbq_create_var",
    schedule_interval="43 09 * * *",
    # schedule_interval=None,
    start_date=dt.datetime(2022, 3, 16),
    catchup=False,
    tags=['convz_prod_airflow_style']
) as dag:

    get_table_names = BashOperator(
        task_id  = "get_table_names",
        cwd      = MAIN_PATH,
        bash_command = f"[ -d tmp ] || mkdir tmp; gsutil ls gs://{BUCKET_NAME}/{SOURCE_NAME}/{SOURCE_TYPE}"
                        + f" | grep -v erp_ | sed '1d' | cut -d'/' -f6 > tmp/{SOURCE_NAME}_tm1_folders;"
                        + f" echo {MAIN_PATH}/tmp/{SOURCE_NAME}_tm1_folders"
    )

    read_table_list = PythonOperator(
        task_id = 'read_table_list',
        python_callable = _read_table,
        op_kwargs={ 
            'filename' : '{{ ti.xcom_pull(task_ids="get_table_names") }}',
            # 'filename' : '/Users/oH/airflow/dags/ERP_tm1_folders'
        },
    )

    table_variable = PythonOperator(
        task_id = 'table_variable',
        python_callable = _process_list,
        op_kwargs = {
            'task_id'  : 'read_table_list',
            'var_name' : f'{SOURCE_NAME}_tables'
        }
    )

    iterable_tables_list = Variable.get(
        key=f'{SOURCE_NAME}_tables',
        default_var=['default_table'],
        deserialize_json=True
    )

    with TaskGroup(
        'load_tm1_folders_tasks_group',
        prefix_group_id=False,
    ) as load_folders_tasks_group:

        if iterable_tables_list:
            for index, tm1_table in enumerate(iterable_tables_list):               

                file_variables = PythonOperator(
                    task_id = f'file_variables_{tm1_table}',
                    python_callable = _create_var,
                    op_kwargs = {
                        'var_name' : f'{SOURCE_NAME}_{tm1_table}_files'
                    }
                )

                # TaskGroup load_folders_tasks_group level dependencies
                file_variables

    # DAG level dependencies
    get_table_names >> read_table_list >> table_variable >> load_folders_tasks_group