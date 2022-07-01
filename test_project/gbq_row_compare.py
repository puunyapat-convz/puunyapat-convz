from airflow                   import configuration, DAG
from airflow.operators.python  import PythonOperator
from airflow.operators.dummy   import DummyOperator
from airflow.models            import Variable
from airflow.utils.task_group  import TaskGroup
from airflow.macros            import *

from airflow.providers.google.cloud.operators.bigquery        import *

import datetime as dt
import logging

######### VARIABLES ###########

log       = logging.getLogger(__name__)
path      = configuration.get('core','dags_folder')
MAIN_PATH = path + "/../data"

SQL_COMMAND ="""SELECT
    *,
    `count_dev`-`count_prod` AS diff,
    `count_dev`/`count_prod` AS dup
FROM (
    SELECT
        [partition] AS date_dev,
        COUNT(*) AS count_dev
    FROM
        `[dev_fqdn]`
    GROUP BY
        1)
    FULL OUTER JOIN (
    SELECT
        [partition] AS date_prod,
        COUNT(*) AS count_prod
    FROM
        `[prod_fqdn]`
    GROUP BY
        1)
ON
    date_prod = date_dev
ORDER BY
    date_prod"""

def _create_query(prd_fqdn, dev_fqdn, dataset):

    for item in dataset:
        if "pos" in item or "jda" in item:
            partition = "DATE(_PARTITIONTIME)"
        else:
            partition = "DATE(`report_date`)"

    query = SQL_COMMAND.replace("[partition]",partition)
    query = query.replace("[dev_fqdn]",dev_fqdn).replace("[prod_fqdn]",prd_fqdn)

    return query


with DAG(
    dag_id="gbq_row_compare",
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

    iterable_list = Variable.get(
        key=f'gbq_compare_pos_txn',
        deserialize_json=True
    )

    with TaskGroup(
        f'compare_rows_tasks_group',
        prefix_group_id=False,
    ) as compare_tasks_group:

        for compare_item in iterable_list:
            PRD_FQDN, DEV_FQDN = compare_item
            dataset = []
            table   = ""

            if PRD_FQDN != "": 
                PRD_PROJECT, PRD_DATASET, PRD_TABLE = PRD_FQDN.split(".")
                table = PRD_TABLE
                dataset.append(PRD_DATASET)

            if DEV_FQDN != "":
                DEV_PROJECT, DEV_DATASET, DEV_TABLE = DEV_FQDN.split(".")
                if table == "": table = DEV_TABLE 
                dataset.append(DEV_DATASET)            

            create_query = PythonOperator(
                task_id=f'create_query_{PRD_DATASET}_{table}',
                python_callable=_create_query,
                op_kwargs = {
                    'prd_fqdn': PRD_FQDN,
                    'dev_fqdn': DEV_FQDN,
                    'dataset' : dataset
                }
            )

            run_query = BigQueryInsertJobOperator( 
            task_id = f"run_query_{PRD_DATASET}_{table}",
            gcp_conn_id = "convz_dev_service_account",
            configuration = {
                "query": {
                    "query": f'{{{{ ti.xcom_pull(task_ids="create_query_{PRD_DATASET}_{table}") }}}}',
                    "destinationTable": {
                        "projectId": "central-cto-ofm-data-hub-dev",
                        "datasetId": "airflow_test_mds",
                        "tableId": f"compare_{PRD_DATASET}_{table}",
                    },
                    "createDisposition": "CREATE_IF_NEEDED",
                    "writeDisposition": "WRITE_TRUNCATE",
                    "useLegacySql": False,
                    }
                }
            )

            save_file = BigQueryInsertJobOperator( 
            task_id = f"save_file_{PRD_DATASET}_{table}",
            gcp_conn_id = "convz_dev_service_account",
            configuration = {
                "extract": {
                    "destinationUris": [
                        f"gs://ofm-data/test/{PRD_DATASET}_{table}.csv"
                    ],
                    "sourceTable": {
                        "projectId": "central-cto-ofm-data-hub-dev",
                        "datasetId": "airflow_test_mds",
                        "tableId": f"compare_{PRD_DATASET}_{table}",
                    },                            
                    "printHeader": True,
                    "fieldDelimiter": ",",
                    "destinationFormat": "CSV",
                    "compression": "None",
                    "useAvroLogicalTypes": False,
                    }
                }
            )

            create_query >> run_query >> save_file
            
    start_task >> compare_tasks_group >> end_task
