from shutil import move
from airflow                   import configuration, DAG
from airflow.operators.python  import PythonOperator, BranchPythonOperator
from airflow.operators.dummy   import DummyOperator
from airflow.models            import Variable
from airflow.utils.task_group  import TaskGroup
from airflow.macros            import *
from utils.dag_notification    import *

from airflow.providers.google.cloud.transfers.gcs_to_gcs import *

import datetime as dt
import logging

######### VARIABLES ###########

log       = logging.getLogger(__name__)
path      = configuration.get('core','dags_folder')
MAIN_PATH = path + "/../data"
FILE_TYPE = ["TXT", "LOG"]

###############################

def _gen_date(ds, offset):
    ds_nodash = ds_add(ds, offset).replace('-','')
    return [ ds_add(ds, offset), f'{ds_nodash[0:4]}/{ds_nodash[0:6]}/' ]

with DAG(
    dag_id="sftp_pos_hk",
    schedule_interval=None,
    # schedule_interval="00 14 * * *",
    start_date=dt.datetime(2022, 2, 28),
    # end_date=dt.datetime(2022, 3, 8),
    catchup=False,
    max_active_runs=1,
    tags=['convz', 'production', 'mario', 'sftp', 'utility'],
    render_template_as_native_obj=True,
    # default_args={
    #     'on_failure_callback': ofm_task_fail_slack_alert,
    #     'retries': 0
    # }
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
            source = "-".join(BUCKET_NAME.split('-')[1:3])

            start_source = DummyOperator(task_id = f"start_{source}")

            with TaskGroup(
                f'move_{source}_tasks_group',
                prefix_group_id=False,
            ) as move_bucket_tasks_group:

                for BUCKET_PATH in iterable_sources_list.get(BUCKET_NAME).keys():

                    path_id = f"{source}-" + "-".join(BUCKET_PATH.split('_')[-2::1])
                    MIN_DATE, MAX_DATE = iterable_sources_list.get(BUCKET_NAME).get(BUCKET_PATH)

                    with TaskGroup(
                        f'move_{path_id}_tasks_group',
                        prefix_group_id=False,
                    ) as move_tables_tasks_group:

                        for interval in range(MIN_DATE, MAX_DATE+1):

                            int_id = "{:02d}".format(abs(interval))

                            gen_date = PythonOperator(
                                task_id = f"gen_date_{path_id}-{int_id}",
                                python_callable = _gen_date,
                                op_kwargs = {
                                    "ds"    : '{{ data_interval_end.strftime("%Y-%m-%d") }}',
                                    "offset": interval
                                }
                            )

                            with TaskGroup(
                                f'move_{path_id}_{interval}_tasks_group',
                                prefix_group_id=False,
                            ) as move_ext_tasks_group:

                                for type in FILE_TYPE:

                                    move_file = GCSToGCSOperator(
                                        task_id = f"move_{type}_{path_id}-{int_id}",
                                        source_bucket = BUCKET_NAME,
                                        source_object = BUCKET_PATH + f'/*_{{{{ ti.xcom_pull(task_ids="gen_date_{path_id}-{int_id}")[0] }}}}.{type}',
                                        # source_object=BUCKET_PATH + '/BI',
                                        # delimiter = f'_{{{{ ti.xcom_pull(task_ids="gen_date_{path_id}-{int_id}")[0] }}}}.{type}',
                                        destination_bucket = None, 
                                        destination_object = BUCKET_PATH + f'/{{{{ ti.xcom_pull(task_ids="gen_date_{path_id}-{int_id}")[1] }}}}',
                                        gcp_conn_id ='convz_dev_service_account',
                                        move_object = False, 
                                        replace = True
                                    )

                                    gen_date >> move_file

            start_source >> move_bucket_tasks_group

    start_task >> gcs_hk_tasks_group >> end_task