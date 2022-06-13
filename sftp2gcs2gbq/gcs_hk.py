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
FILE_TYPE = [ "TXT", "LOG"]

###############################

def _gen_date(ds, offset):
    ds_nodash = ds_add(ds, offset).replace('-','')
    return [ ds_add(ds, offset), f'{ds_nodash[0:4]}/{ds_nodash[0:6]}/' ]

with DAG(
    dag_id="gcs_hk",
    # schedule_interval=None,
    schedule_interval="30 02 * * *",
    start_date=dt.datetime(2022, 6, 11),
    end_date=dt.datetime(2022, 6, 12),
    catchup=True,
    max_active_runs=1,
    tags=['convz', 'production', 'mario', 'gcs'],
    render_template_as_native_obj=True,
    # default_args={
    #     'on_failure_callback': ofm_task_fail_slack_alert,
    #     'retries': 0
    # }
) as dag:

    start_task = DummyOperator(task_id = "start_task")
    end_task   = DummyOperator(task_id = "end_task")

    iterable_sources_list = Variable.get(
        key=f'gcs_hk',
        deserialize_json=True
    )
    # iterable_sources_list = {
    #     "sftp-b2s-pos-prod:B2S/POS/POS_DataPlatform_Txn_DiscountCoupon": [10,7]
    # }

    with TaskGroup(
        'gcs_hk_tasks_group',
        prefix_group_id=False,
    ) as gcs_hk_tasks_group:

        for gcs_path in iterable_sources_list.keys():

            BUCKET_NAME, BUCKET_PATH = gcs_path.split(':')
            MIN_DATE, MAX_DATE = iterable_sources_list.get(gcs_path)
            bucket_id          = BUCKET_PATH.replace('/','_').replace('POS_DataPlatform_','')

            with TaskGroup(
                f'move_{BUCKET_NAME}_tasks_group',
                prefix_group_id=False,
            ) as move_bucket_tasks_group:

                for interval in range(MIN_DATE, MAX_DATE+1):

                    int_id = "{:02d}".format(abs(interval))

                    gen_date = PythonOperator(
                        task_id=f"gen_date_{bucket_id}-{int_id}",
                        python_callable=_gen_date,
                        op_kwargs = {
                            "ds"    : '{{ data_interval_end.strftime("%Y-%m-%d") }}',
                            "offset": interval
                        }
                    )

                    with TaskGroup(
                        f'move_ext_tasks_group',
                        prefix_group_id=False,
                    ) as move_ext_tasks_group:

                        for type in FILE_TYPE:

                            move_file = GCSToGCSOperator(
                                task_id=f"move_{type}_{bucket_id}-{int_id}",
                                source_bucket=BUCKET_NAME,
                                source_object=BUCKET_PATH + f'/*_{{{{ ti.xcom_pull(task_ids="gen_date_{bucket_id}-{int_id}")[0] }}}}.{type}',
                                destination_bucket=None, 
                                destination_object=BUCKET_PATH + f'/{{{{ ti.xcom_pull(task_ids="gen_date_{bucket_id}-{int_id}")[1] }}}}', 
                                move_object=False, 
                                replace=True, 
                                gcp_conn_id='convz_dev_service_account', 
                            )

                            gen_date >> move_file

    start_task >> gcs_hk_tasks_group >> end_task