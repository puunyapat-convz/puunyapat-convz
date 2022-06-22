from airflow                   import configuration, DAG
from airflow.operators.python  import PythonOperator, BranchPythonOperator
from airflow.operators.dummy   import DummyOperator
from airflow.models            import Variable
from airflow.utils.task_group  import TaskGroup

from airflow.providers.google.cloud.operators.bigquery import *
from airflow.providers.google.cloud.operators.gcs      import *

from airflow.providers.google.cloud.transfers.local_to_gcs    import *
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import *

import datetime as dt
import logging
import json
import re

######### VARIABLES ###########

log       = logging.getLogger(__name__)
path      = configuration.get('core','dags_folder')
MAIN_PATH = path + "/../data"

PROJECT_DST  = "central-cto-ofm-data-hub-dev"
DATASET_DST  = "b2s_jda_prod_new"
TYPE_DST     = "daily"

PROJECT_SRC  = "central-cto-ofm-data-hub-dev"
DATASET_SRC  = "b2s_jda_prod_new"
TYPE_SRC     = "daily"

LOCATION     = "asia-southeast1" 
BUCKET_NAME  = "sftp-b2s-jda-prod"
SOURCE_NAME  = "B2S"

###############################

def _create_var(table_name, data):
    new_data = [ value[0] for value in data ]
    Variable.set(
        key   = f'{SOURCE_NAME}_{table_name}_report_date',
        value = new_data,
        serialize_json = True
    )

def _remove_var(table_name):
    Variable.delete(key = f'{SOURCE_NAME}_{table_name}_report_date')

def _prepare_list(source_list):
    json_data  = json.loads(json.dumps(source_list))
    table_list = list(map(lambda datum: datum['tableId'], json_data))
    final_list = [ name for name in table_list if not re.match(f"^test_", name) ]

    return final_list

def _check_table(table_name, source_list):
    if f"{table_name}" in source_list:
        return f"list_report_dates_{table_name}"
    else:
        return f"skip_table_{table_name}"

with DAG(
    dag_id="migrate_daily_jda",
    schedule_interval=None,
    # schedule_interval="40 00 * * *",
    start_date=dt.datetime(2022, 6, 1),
    catchup=False,
    tags=['convz', 'production', 'migrate', 'daily_data', 'jda', 'mario'],
    render_template_as_native_obj=True,
    default_args={
        'retries': 1,
        'retry_delay': dt.timedelta(seconds=5),
    #     'depends_on_past': False,
    #     'email': ['airflow@example.com'],
    #     'email_on_failure': False,
    #     'email_on_retry': False,
    #     'queue': 'bash_queue',
    #     'pool': 'backfill',
    #     'priority_weight': 10,
    #     'end_date': datetime(2016, 1, 1),
    #     'wait_for_downstream': False,
    #     'sla': timedelta(hours=2),
    #     'execution_timeout': timedelta(seconds=300),
    #     'on_failure_callback': some_function,
    #     'on_success_callback': some_other_function,
    #     'on_retry_callback': another_function,
    #     'sla_miss_callback': yet_another_function,
    #     'trigger_rule': 'all_success'
    },
) as dag:

    start_task = DummyOperator(task_id = "start_task")
    end_task   = DummyOperator(task_id = "end_task")

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id     = "create_dataset",
        dataset_id  = DATASET_DST,
        project_id  = PROJECT_DST,
        location    = LOCATION,
        gcp_conn_id = "convz_dev_service_account",
        exists_ok   = True
    )

    get_source_tb = BigQueryGetDatasetTablesOperator(
        task_id     = "get_source_tb",
        project_id  = PROJECT_SRC,
        dataset_id  = DATASET_SRC,
        gcp_conn_id = "convz_dev_service_account",
    )

    prepare_list = PythonOperator(
        task_id   = "prepare_list",
        python_callable = _prepare_list,
        op_kwargs = { 
            'source_list': '{{ ti.xcom_pull(task_ids="get_source_tb") }}'
        },
    )

    # iterable_tables_list = Variable.get(
    #     key=f'{SOURCE_NAME}_{TYPE_SRC}',
    #     default_var=['default_table'],
    #     deserialize_json=True
    # )

    # iterable_tables_list = [
    #     "LG_CALENDAR",
    #     "LG_CODE_RATE",
    #     "LG_EMPLOYEE_HIP",
    #     "LG_EMPLOYEE_MASTER",
    #     "LG_FLEET_TYPE",
    #     "LG_FLEET_TYPE_SUB",
    #     "LG_MASTER_SUBCLUSTER",
    #     "LG_VEHICLE_MASTER"
    # ]

    iterable_tables_list = [ "BCH_JDA_DataPlatform_JDASAL" ]

    with TaskGroup(
        'migrate_historical_tasks_group',
        prefix_group_id=False,
    ) as migrate_tasks_group:

        if iterable_tables_list:
            for index, tm1_table in enumerate(iterable_tables_list):

                check_source_tb = BranchPythonOperator(
                    task_id = f'check_source_tb_{tm1_table}',
                    python_callable = _check_table,
                    op_kwargs = {
                        'table_name'  : tm1_table,
                        'source_list' : '{{ ti.xcom_pull(task_ids="prepare_list") }}'
                    }
                )

                skip_table = DummyOperator(task_id = f"skip_table_{tm1_table}")

                list_report_dates = BigQueryInsertJobOperator( 
                    task_id = f"list_report_dates_{tm1_table}",
                    gcp_conn_id = "convz_dev_service_account",
                    configuration = {
                        "query": {
                            "query": f"SELECT DISTINCT DATE(_PARTITIONTIME) as `report_date` "
                                        + f"FROM `{PROJECT_SRC}.{DATASET_SRC}.{tm1_table}` ORDER BY 1 ASC",
                            "destinationTable": {
                                "projectId": PROJECT_SRC,
                                "datasetId": DATASET_SRC,
                                "tableId": f"{tm1_table}_report_date",
                            },
                            "createDisposition": "CREATE_IF_NEEDED",
                            "writeDisposition": "WRITE_TRUNCATE",
                            "useLegacySql": False,
                        }
                    }
                )

                get_list = BigQueryGetDataOperator(
                    task_id     = f'get_list_{tm1_table}',
                    dataset_id  = f'{DATASET_SRC}',
                    table_id    = f"{tm1_table}_report_date",
                    gcp_conn_id = 'convz_dev_service_account',
                    max_results = 1000,
                    selected_fields='report_date'
                )

                create_var = PythonOperator(
                    task_id=f'create_var_{tm1_table}',
                    python_callable = _create_var,
                    op_kwargs={ 
                        'table_name' : tm1_table,
                        'data'       : f'{{{{ ti.xcom_pull(task_ids="get_list_{tm1_table}") }}}}'
                    },
                )

                drop_list = BigQueryDeleteTableOperator(
                    task_id = f"drop_list_{tm1_table}",
                    gcp_conn_id  = "convz_dev_service_account",
                    ignore_if_missing = True,
                    deletion_dataset_table = f"{PROJECT_SRC}.{DATASET_SRC}.{tm1_table}_report_date",
                )

                TABLE_DST = f'test_{tm1_table}'

                create_prod_table = BigQueryCreateEmptyTableOperator(
                    task_id = f"create_final_{tm1_table}",
                    google_cloud_storage_conn_id = "convz_dev_service_account",
                    bigquery_conn_id = "convz_dev_service_account",
                    project_id = PROJECT_DST,
                    dataset_id = DATASET_DST,
                    table_id = TABLE_DST,
                    gcs_schema_object = f"gs://{BUCKET_NAME}/schema/JDA/JDA_{tm1_table.replace('_DataPlatform','')}.schema",
                    time_partitioning = { "type":"DAY" },
                )

                remove_var = PythonOperator(
                    task_id = f"remove_var_{tm1_table}",
                    trigger_rule = 'all_success',
                    python_callable = _remove_var,
                    op_kwargs = { 'table_name' : tm1_table }
                )

                iterable_date_list = Variable.get(
                    key=f'{SOURCE_NAME}_{tm1_table}_report_date',
                    default_var=['2000-01-01'],
                    deserialize_json=True
                )
                # iterable_date_list = [ "2022-05-12" ]

                with TaskGroup(
                    f'migrate_{tm1_table}_tasks_group',
                    prefix_group_id=False,
                ) as migrate_date_group:

                    if iterable_date_list:
                        for index, report_date in enumerate(iterable_date_list):

                            extract_to_prod = BigQueryInsertJobOperator( 
                                task_id = f"extract_to_prod_{tm1_table}_{report_date}",
                                gcp_conn_id = "convz_dev_service_account",
                                configuration = {
                                    "query": {
                                        "query": "SELECT * EXCEPT (XTDAM4 ,XTDAM7),\n"
                                                    +"  CAST(XTDAM4 AS float64) as XTDAM4,\n"
                                                    +"  CAST(XTDAM7 AS float64) as XTDAM7 \n"
                                                    + f"FROM {PROJECT_SRC}.{DATASET_SRC}.{tm1_table}\n" 
                                                    + f"WHERE DATE(_PARTITIONTIME) = '{report_date}'",
                                        "destinationTable": {
                                            "projectId": PROJECT_DST,
                                            "datasetId": DATASET_DST,
                                            "tableId"  : f"{TABLE_DST}${report_date.replace('-','')}",
                                        },
                                        "createDisposition": "CREATE_IF_NEEDED",
                                        "writeDisposition" : "WRITE_TRUNCATE",
                                        "timePartitioning" : { "type":"DAY" },
                                        "useLegacySql": False,
                                    }
                                }
                            )

                            # Date level dependencies
                            extract_to_prod 

                # Table level dependencies
                check_source_tb >> [ skip_table, list_report_dates ]
                list_report_dates >> get_list >> create_var >> [ drop_list, create_prod_table ]
                create_prod_table >> migrate_date_group >> remove_var
                
    # DAG level dependencies
    start_task >> [create_dataset, get_source_tb] >> prepare_list >> migrate_tasks_group >> end_task
