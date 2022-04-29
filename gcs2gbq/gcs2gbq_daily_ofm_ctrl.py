from airflow                   import configuration, DAG
from airflow.operators.python  import PythonOperator, BranchPythonOperator
from airflow.operators.bash    import BashOperator
from airflow.operators.dummy   import DummyOperator
from airflow.models            import Variable
from airflow.utils.task_group  import TaskGroup
from dateutil                  import parser
from utils.dag_notification    import *

from airflow.providers.google.cloud.operators.bigquery import *

import datetime as dt
import logging

######### VARIABLES ###########

log       = logging.getLogger(__name__)
path      = configuration.get('core','dags_folder')
MAIN_PATH = path + "/../data"

PROJECT_ID   = "central-cto-ofm-data-hub-prod"
DATASET_ID   = "officemate_ofm_daily_ctrlfiles"
LOCATION     = "asia-southeast1" 

BUCKET_NAME  = "ofm-data"
SOURCE_NAME  = "officemate"
SOURCE_TYPE  = "daily"

###############################

def _create_pattern(run_date):
    run_date = f'{run_date}T00:00:00Z'
    epoch    = int(parser.parse(run_date).timestamp())

    timestr = str(epoch)
    prefix  = int(timestr[:len(timestr)-5])

    Variable.set(
        key   = f'{SOURCE_NAME}_{SOURCE_TYPE}_epoch',
        value = [ prefix, prefix+1 ],
        serialize_json = True
    )

def _check_list(ti, tablename, filename, run_date):
    with open(filename) as f:
        lines    = f.read().splitlines()
        gcs_list = []

        for line in lines:
            split_line = line.split("/")
            file_epoch = split_line[-1].split("_")[-1].split(".")[0]

            if dt.datetime.utcfromtimestamp(int(file_epoch)).strftime('%Y-%m-%d') == run_date:
                gcs_list.append(line)
                log.info(f"Added control file [{split_line[-1]}] to list.")
            else:
                log.warning(f"Skipped file [{split_line[-1]}] with incorrect epoch timestamp.")

    if len(gcs_list) == 0:
        log.info(f"Table [ {tablename} ] has no control file(s) for this run.")
        return [ f"skip_table_{tablename}", f"remove_list_{tablename}" ]
    else:
        ti.xcom_push(key='gcs_uri', value=gcs_list)
        return [ f"create_table_{tablename}", f"remove_list_{tablename}" ]

def _remove_var():
    Variable.delete(key = f'{SOURCE_NAME}_{SOURCE_TYPE}_epoch')

with DAG(
    dag_id="gcs2gbq_daily_ofm_ctrl",
    # schedule_interval=None,
    schedule_interval="00 03 * * *",
    start_date=dt.datetime(2022, 4, 28),
    catchup=True,
    max_active_runs=1,
    tags=['convz', 'production', 'airflow_style', 'daily', 'officemate', 'control'],
    render_template_as_native_obj=True,
    # default_args={
    #     'on_failure_callback': ofm_task_fail_slack_alert,
    #     'retries': 0
    # }
) as dag:

    start_task = DummyOperator(task_id = "start_task")
    end_task   = DummyOperator(task_id = "end_task")

    create_ds_final = BigQueryCreateEmptyDatasetOperator(
        task_id     = "create_ds_final",
        dataset_id  = DATASET_ID,
        project_id  = PROJECT_ID,
        location    = LOCATION,
        gcp_conn_id = "convz_dev_service_account",
        exists_ok   = True
    )

    create_epoch = PythonOperator(
        task_id = f'create_epoch',
        python_callable = _create_pattern,
        op_kwargs={ 
            'run_date' : '{{ ds }}' ## use yesterday_ds for manual run
        },
    )

    remove_var = PythonOperator(
        task_id = f"remove_var",
        trigger_rule = 'none_failed',
        python_callable = _remove_var,
    )

    iterable_tables_list = Variable.get(
        key=f'{SOURCE_NAME}_{SOURCE_TYPE}',
        default_var=['default_table'],
        deserialize_json=True
    )
    # iterable_tables_list = [ "tbaccount_segment" ]

    with TaskGroup(
        'load_ctrl_folders_tasks_group',
        prefix_group_id=False,
    ) as load_folders_tasks_group:

        if iterable_tables_list:
            for index, tm1_table in enumerate(iterable_tables_list):

                iterable_epoch_list = Variable.get(
                    key=f'{SOURCE_NAME}_{SOURCE_TYPE}_epoch',
                    default_var=['default_epoch'],
                    deserialize_json=True
                )

                with TaskGroup(
                    f'load_ctrl_epoch_{tm1_table}',
                    prefix_group_id=False,
                ) as load_epoch_tasks_group:

                    if iterable_tables_list:
                        for epoch in iterable_epoch_list:

                            list_file = BashOperator(
                                task_id = f"list_file_{tm1_table}_{epoch}",
                                cwd     = MAIN_PATH,
                                bash_command = f"temp={SOURCE_NAME}_{SOURCE_TYPE}_{tm1_table}_ctrl.{epoch};" 
                                                + f' gsutil ls "gs://{BUCKET_NAME}/{SOURCE_NAME}/{SOURCE_TYPE}/{tm1_table}/{SOURCE_NAME}_{tm1_table}_{epoch}?????.ctrl" > $temp;'
                                                + f' echo {MAIN_PATH}/$temp'
                            )

                            list_file

                merge_list = BashOperator(
                    task_id = f"merge_list_{tm1_table}",
                    cwd     = MAIN_PATH,
                    bash_command = f'epoch=($(echo "{{{{ var.value.{SOURCE_NAME}_{SOURCE_TYPE}_epoch.replace("\n","") }}}}" | tr -dc "[:alnum:]," | tr "," " "));'
                                    + f' temp=$(mktemp {SOURCE_NAME}_{SOURCE_TYPE}_ctrl.XXXXXXXX); prefix={SOURCE_NAME}_{SOURCE_TYPE}_{tm1_table}_ctrl'
                                    + f" && cat $prefix.${{epoch[0]}} $prefix.${{epoch[1]}} > $temp"
                                    + f" && rm -f $prefix.${{epoch[0]}} $prefix.${{epoch[1]}}"
                                    + f' && echo {MAIN_PATH}/$temp'
                )

                check_list = BranchPythonOperator(
                    task_id=f'check_list_{tm1_table}',
                    python_callable=_check_list,
                    op_kwargs = { 
                        'tablename' : tm1_table,
                        'filename' : f'{{{{ ti.xcom_pull(task_ids="merge_list_{tm1_table}") }}}}',
                        'run_date' : '{{ ds }}' ## use yesterday_ds for manual run
                    }
                )

                skip_table = DummyOperator(
                    task_id = f"skip_table_{tm1_table}",
                    # on_success_callback = ofm_missing_daily_file_slack_alert
                )

                remove_list = BashOperator(
                    task_id  = f"remove_list_{tm1_table}",
                    cwd      = MAIN_PATH,
                    bash_command = f"rm -f {{{{ ti.xcom_pull(task_ids='merge_list_{tm1_table}') }}}}"
                )

                create_table = BigQueryCreateEmptyTableOperator(
                    task_id = f"create_table_{tm1_table}",
                    google_cloud_storage_conn_id = "convz_dev_service_account",
                    bigquery_conn_id = "convz_dev_service_account",
                    project_id = PROJECT_ID,
                    dataset_id = DATASET_ID,
                    table_id = f"{tm1_table.lower()}_{SOURCE_TYPE}",
                    gcs_schema_object = f'gs://{BUCKET_NAME}/{SOURCE_NAME}/schemas/control_files.json',
                    time_partitioning = { "field":"created_at", "type":"DAY" },
                )

                load_file = BigQueryInsertJobOperator( 
                    task_id = f"load_file_{tm1_table}",
                    gcp_conn_id = "convz_dev_service_account",
                    trigger_rule = 'all_success',
                    configuration = {
                        "load": {
                            "sourceUris": f'{{{{ ti.xcom_pull(task_ids="check_list_{tm1_table}", key="gcs_uri") }}}}',
                            "destinationTable": {
                                "projectId": PROJECT_ID,
                                "datasetId": DATASET_ID,
                                "tableId": f"{tm1_table.lower()}_{SOURCE_TYPE}"
                            },
                            "sourceFormat": "CSV",
                            "fieldDelimiter": "|",
                            "skipLeadingRows": 1,
                            "timePartitioning": { "field":"created_at", "type":"DAY" },
                            "createDisposition": "CREATE_IF_NEEDED",
                            "writeDisposition": "WRITE_TRUNCATE"
                        }
                    }
                )

                # TaskGroup load_folders_tasks_group level dependencies
                load_epoch_tasks_group >> merge_list >> check_list >> [ remove_list, skip_table, create_table ]
                create_table >> load_file

    # DAG level dependencies
    start_task >> create_ds_final >> create_epoch >> load_folders_tasks_group >> remove_var >> end_task
