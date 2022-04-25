from airflow                   import configuration, DAG
from airflow.operators.python  import PythonOperator
from airflow.operators.bash    import BashOperator
from airflow.models            import Variable
from airflow.operators.dummy   import DummyOperator

import datetime as dt
import logging

######### VARIABLES ###########

log       = logging.getLogger(__name__)
path      = configuration.get('core','dags_folder')
MAIN_PATH = path + "/../data"

BUCKET_NAME  = "ofm-data"
SOURCE_NAME  = [ "ERP", "MDS", "officemate", "Mercury", "TMS" ] 
SOURCE_TYPE  = { "daily" : "| egrep -iv '^$|^erp_|^mercury_'", "intraday": "" }

###############################

def _read_table(name, type, filename):
    with open(filename) as f:
        lines = f.read().splitlines()

    if len(lines) > 0:
        Variable.set(
            key   = f'{name}_{type}',
            value = lines,
            serialize_json = True
        )
    else:
        log.info(f"No folders in gs://{BUCKET_NAME}/{name}/{type}")

with DAG(
    dag_id="gcs2gbq_create_var",
    # schedule_interval="00 00 * * *",
    schedule_interval=None,
    start_date=dt.datetime(2022, 4, 15),
    catchup=False,
    tags=['convz_prod_airflow_style']
) as dag:

    start_task = DummyOperator(task_id = "start_task")
    end_task = DummyOperator(task_id = "end_task")

    for NAME in SOURCE_NAME:
        for TYPE in SOURCE_TYPE.keys():

            get_table_names = BashOperator(
                task_id  = f"get_{NAME}_{TYPE}",
                cwd      = MAIN_PATH,
                bash_command = f"gsutil ls gs://{BUCKET_NAME}/{NAME}/{TYPE}" \
                                + f" | cut -d'/' -f6 {SOURCE_TYPE.get(TYPE)} > {NAME}_{TYPE}_folders;" \
                                + f" echo {MAIN_PATH}/{NAME}_{TYPE}_folders"
            )

            read_table_list = PythonOperator(
                task_id = f"read_{NAME}_{TYPE}",
                python_callable = _read_table,
                op_kwargs = {
                    'name' : NAME,
                    'type' : TYPE,
                    'filename' : f'{{{{ ti.xcom_pull(task_ids="get_{NAME}_{TYPE}") }}}}',
                    # 'filename' : '/Users/oH/airflow/dags/ERP_tm1_folders'
                },
            )

            remove_table_list = BashOperator(
                task_id  = f"remove_{NAME}_{TYPE}",
                cwd      = MAIN_PATH,
                trigger_rule = 'all_done',
                bash_command = f"rm -f {{{{ ti.xcom_pull(task_ids='get_{NAME}_{TYPE}') }}}}"
            )

            ## loop level dependencies
            start_task >> get_table_names >> read_table_list >> remove_table_list >> end_task