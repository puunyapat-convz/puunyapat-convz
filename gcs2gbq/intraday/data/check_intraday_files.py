from airflow                   import configuration, DAG
from airflow.operators.python  import BranchPythonOperator
from airflow.operators.bash    import BashOperator
from airflow.operators.dummy   import DummyOperator
from airflow.models            import Variable
from airflow.utils.task_group  import TaskGroup
from dateutil                  import parser
from utils.dag_notification    import *

import datetime as dt
import logging

log  = logging.getLogger(__name__)
path = configuration.get('core','dags_folder')

MAIN_PATH   = path + "/../data"
BUCKET_NAME = "ofm-data"
SOURCE_TYPE = "intraday"

def _count_file(ti, source, table, filename, gcs_path, total, ts):

    found    = 0
    ts_split = ts.split('T')

    start_epoch = parser.parse(f"{ts_split[0]}T00:00:00.000Z").timestamp()    
    end_epoch   = parser.parse(f"{ts}:59:59.999Z").timestamp() ## Minute must be very end of hour period of DAG interval

    with open(filename) as f:
        lines = f.read().splitlines()

    for line in lines:
        file_epoch = int(line.split(",")[2].split("/")[-1].split("_")[3])/1000

        if file_epoch >= start_epoch and file_epoch <= end_epoch:
            found += 1

    hour   = int(ts_split[1])+1
    expect = (int(total/24))*hour

    if found < expect:
        log.warning(f"Table [ {table} ] has files lower than expected.")
        ti.xcom_push(key='gcs_uri', value=gcs_path)
        ti.xcom_push(key='filecount', value=f"`{found}` from `{expect}`")
        return f"send_alert_{source}_{table}"
    else:
        return f"no_alert_{source}_{table}"

with DAG(
    dag_id="check_intraday_files",
    # schedule_interval=None,
    schedule_interval="59 7-23/8 * * *",
    start_date=dt.datetime(2022, 5, 10, 23, 59),
    # end_date=dt.datetime(2022, 5, 11, 15, 59),
    catchup=True,
    max_active_runs=1,
    tags=['convz', 'production', 'mario', 'alert', 'gcs'],
    description='Alert for missing intraday files on GCS',
    render_template_as_native_obj=True,
    default_args={
        'on_failure_callback': ofm_task_fail_slack_alert,
        'retries': 0
    }
) as dag:

    start_task = DummyOperator(task_id = "start_task")
    end_task   = DummyOperator(task_id = "end_task", trigger_rule='none_failed')

    iterable_sources_list = Variable.get(
        key='intraday_alert',
        default_var=['default_source'],
        deserialize_json=True
    )
    # iterable_sources_list = { "TMS":"24" }

    with TaskGroup(
        'source_tasks_group',
        prefix_group_id=False,
    ) as check_source_tasks_group:
    
        if iterable_sources_list.keys():
            for index, source_name in enumerate(iterable_sources_list.keys()):

                iterable_tables_list = Variable.get(
                    key=f'{source_name}_intraday',
                    default_var=['default_table'],
                    deserialize_json=True
                )
                # iterable_tables_list = [ "TMS_TBDeliveryConfirm" ]

                with TaskGroup(
                    f'{source_name}_tables_tasks_group',
                    prefix_group_id=False,
                ) as check_tables_tasks_group:

                    if iterable_tables_list:
                        for index, tm1_table in enumerate(iterable_tables_list):

                            create_list = BashOperator(
                                task_id = f"create_list_{source_name}_{tm1_table}",
                                cwd     = MAIN_PATH,
                                bash_command = f"temp=$(mktemp {source_name}_check.XXXXXXXX)" 
                                                + f' && gsutil du "gs://{BUCKET_NAME}/{source_name}/intraday/{tm1_table}/{{{{ ds.replace("-","_") }}}}*.jsonl"'
                                                                                            ## use yesterday_ds for manual run ^
                                                + f" | tr -s ' ' ',' | sed 's/^/{tm1_table},/g' | sort -t, -k2n > $temp;"
                                                + f' echo "{MAIN_PATH}/$temp"'
                            )

                            count_file = BranchPythonOperator(
                                task_id = f'count_file_{source_name}_{tm1_table}',
                                python_callable = _count_file,
                                op_kwargs={ 
                                    'source'   : source_name,
                                    'table'    : tm1_table,
                                    'filename' : f'{{{{ ti.xcom_pull(task_ids="create_list_{source_name}_{tm1_table}") }}}}',
                                    'gcs_path' : f"gs://{BUCKET_NAME}/{source_name}/{SOURCE_TYPE}/{tm1_table}/{{{{ ds.replace('-','_') }}}}*.jsonl",
                                    'total'    : f'{{{{ var.json.intraday_alert.{source_name} }}}}',
                                    'ts'       : '{{ ts.split(":")[0] }}'
                                },
                            )

                            send_alert = DummyOperator(
                                task_id = f"send_alert_{source_name}_{tm1_table}",
                                on_success_callback = ofm_missing_intraday_file_slack_alert
                            )

                            no_alert   = DummyOperator(task_id = f"no_alert_{source_name}_{tm1_table}")

                            remove_file_list = BashOperator(
                                task_id  = f"remove_file_list_{source_name}_{tm1_table}",
                                cwd      = MAIN_PATH,
                                trigger_rule = 'all_done',
                                bash_command = f"rm -f {{{{ ti.xcom_pull(task_ids='create_list_{source_name}_{tm1_table}') }}}}"
                            )
                    
                            ## loop level dependencies
                            create_list >> count_file >> [ send_alert, no_alert ] >> remove_file_list

    ## dag level dependencies
    start_task >> check_source_tasks_group >> end_task