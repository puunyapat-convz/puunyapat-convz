from airflow                   import configuration, DAG
from airflow.operators.python  import PythonOperator, BranchPythonOperator
from airflow.operators.dummy   import DummyOperator
from airflow.operators.bash    import BashOperator
from airflow.models            import Variable
from airflow.utils.task_group  import TaskGroup
from airflow.macros            import *
from utils.dag_notification    import *

import datetime as dt
import logging, os

######### VARIABLES ###########

log       = logging.getLogger(__name__)
path      = configuration.get('core','dags_folder')
MAIN_PATH = path + "/../data"
FILE_TYPE = ["TXT", "LOG"]

###############################

def _gen_date(ds, offset):
    ds_nodash = ds_add(ds, offset).replace('-','')
    return [ ds_add(ds, offset), f'{ds_nodash[0:4]}/{ds_nodash[0:6]}/' ]

def _check_file(filename, tablename, branch_id):

    with open(filename) as f:
        count = sum(1 for _ in f)

    if count == 0:
        log.info(f"Table [ {tablename} ] has no file(s) for this run.")
        task_id = f"skip_table_{branch_id}"
    else:
        task_id = f"move_file_{branch_id}"

    os.remove(filename)
    return task_id    

with DAG(
    dag_id="sftp_pos_hk",
    # schedule_interval=None,
    schedule_interval="00 14 * * *",
    start_date=dt.datetime(2022, 3, 7),
    catchup=True,
    max_active_runs=1,
    tags=['convz', 'production', 'mario', 'sftp', 'utility'],
    render_template_as_native_obj=True,
    description='Housekeeper for POS_Txn files on GCS',
    default_args={
        'on_failure_callback': ofm_task_fail_slack_alert,
        'retries': 0
    }
) as dag:

    start_task = DummyOperator(task_id = "start_task")
    end_task   = DummyOperator(task_id = "end_task")

    iterable_sources_list = Variable.get(
        key=f'sftp_pos_hk',
        deserialize_json=True
    )
    # iterable_sources_list = {
    #     "sftp-b2s-pos-prod": 
    #         { "B2S/POS/POS_DataPlatform_Txn_Translator": [-7,-7] },
    #     "sftp-ofm-pos-prod": 
    #         { "ODP/POS/POS_DataPlatform_Txn_Translator": [-7,-7] }
    # }

    with TaskGroup(
        'sftp_pos_hk_tasks_group',
        prefix_group_id=False,
    ) as gcs_hk_tasks_group:

        for BUCKET_NAME in iterable_sources_list.keys():           
            source = "_".join(BUCKET_NAME.split('-')[1:3])

            start_source = DummyOperator(task_id = f"start_{source}")

            with TaskGroup(
                f'move_{source}_tasks_group',
                prefix_group_id=False,
            ) as move_bucket_tasks_group:

                for BUCKET_PATH in iterable_sources_list.get(BUCKET_NAME).keys():

                    path_id   = f"{source}_" + "_".join(BUCKET_PATH.split('_')[-2::1])
                    tablename = BUCKET_PATH.split('/')[-1]
                    MIN_DATE, MAX_DATE = iterable_sources_list.get(BUCKET_NAME).get(BUCKET_PATH)

                    with TaskGroup(
                        f'move_{path_id}_tasks_group',
                        prefix_group_id=False,
                    ) as move_tables_tasks_group:

                        for interval in range(MIN_DATE, MAX_DATE+1):

                            int_id = "{:02d}".format(abs(interval))

                            gen_date = PythonOperator(
                                task_id = f"gen_date_{path_id}_{int_id}",
                                python_callable = _gen_date,
                                op_kwargs = {
                                    "ds"    : '{{ data_interval_end.strftime("%Y-%m-%d") }}',
                                    "offset": interval
                                }
                            )

                            create_list = BashOperator(
                                task_id = f"create_list_{path_id}_{int_id}",
                                cwd     = MAIN_PATH,
                                bash_command = f'temp=$(mktemp {path_id}_{int_id}.XXXXXXXX)'
                                                + f' && gsutil ls "gs://{BUCKET_NAME}/{BUCKET_PATH}/*_{{{{ ti.xcom_pull(task_ids="gen_date_{path_id}_{int_id}")[0] }}}}.*"'
                                                + f' > $temp; echo {MAIN_PATH}/$temp'
                            )

                            check_list = BranchPythonOperator(
                                task_id=f'check_list_{path_id}_{int_id}',
                                python_callable=_check_file,
                                op_kwargs = { 
                                    'branch_id': f"{path_id}_{int_id}",
                                    'tablename': tablename,
                                    'filename' : f'{{{{ ti.xcom_pull(task_ids="create_list_{path_id}_{int_id}") }}}}',
                                }
                            )

                            move_file = BashOperator(
                                task_id = f"move_file_{path_id}_{int_id}",
                                cwd     = MAIN_PATH,
                                bash_command = f'gsutil -m mv "gs://{BUCKET_NAME}/{BUCKET_PATH}/*_{{{{ ti.xcom_pull(task_ids="gen_date_{path_id}_{int_id}")[0] }}}}.*"'
                                                 + f' gs://{BUCKET_NAME}/{BUCKET_PATH}/{{{{ ti.xcom_pull(task_ids="gen_date_{path_id}_{int_id}")[1] }}}}'
                            )

                            skip_table = DummyOperator(task_id = f"skip_table_{path_id}_{int_id}")

                            gen_date >> create_list >> check_list >> [ move_file, skip_table ]

            start_source >> move_bucket_tasks_group

    start_task >> gcs_hk_tasks_group >> end_task