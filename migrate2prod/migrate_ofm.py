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
import pandas   as pd
import tempfile
import logging
import json

######### VARIABLES ###########

## BQ_DTYPE
## Data type mappings from Excel to GBQ schema
## The first element is targeted GBQ data type
## The others are source data type from Excel you need to map them into GBQ

BQ_DTYPE = [
    [ "STRING", "string", "char", "nchar", "nvarchar", "varchar", "sysname", "text", "uniqueidentifier" ],
    [ "INT64", "integer", "int", "tinyint", "smallint", "bigint" ],
    [ "FLOAT64", "float", "numeric", "decimal", "money" ],
    [ "BOOLEAN", "bit", "boolean" ],
    [ "DATE", "date" ],
    [ "TIME", "time" ],
    [ "DATETIME", "datetime", "datetime2", "smalldatetime" ],
    [ "TIMESTAMP", "timestamp" ]
]

log       = logging.getLogger(__name__)
path      = configuration.get('core','dags_folder')
MAIN_PATH = path + "/../data"

SCHEMA_FILE    = f"{MAIN_PATH}/schemas/OFM-B2S_Source_Datalake_20211020-live-version.xlsx"
SCHEMA_SHEET   = "Field-Officemate"
SCHEMA_COLUMNS = ["TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE"] # Example value ["TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE"]

PROJECT_DST  = "central-cto-ofm-data-hub-prod"
DATASET_DST  = "officemate_ofm_daily"

PROJECT_SRC  = "central-cto-ofm-data-hub-dev"
DATASET_SRC  = "officemate_source"

LOCATION     = "asia-southeast1" 
BUCKET_NAME  = "ofm-data"
SOURCE_NAME  = "officemate"
SOURCE_TYPE  = "daily"

## specify airbyte header field name which contains data here
FIELD_PREFIX = ""
# FIELD_PREFIX = "_airbyte_data."

###############################

class ContentToGoogleCloudStorageOperator(BaseOperator):

    template_fields = ('content', 'dst', 'bucket')

    def __init__(self,
                 content,
                 dst,
                 bucket,
                 gcp_conn_id='google_cloud_default',
                 mime_type='application/octet-stream',
                 delegate_to=None,
                 gzip=False,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        
        self.content = content
        self.dst = dst
        self.bucket = bucket
        self.gcp_conn_id = gcp_conn_id
        self.mime_type = mime_type
        self.delegate_to = delegate_to
        self.gzip = gzip

    def execute(self, context):

        hook = GCSHook(
            google_cloud_storage_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to
        )

        json_data = json.dumps(self.content, indent=4)

        with tempfile.NamedTemporaryFile(prefix="gcs-local") as file:
            file.write(json_data.encode('utf-8'))
            file.flush()
            hook.upload(
                bucket_name=self.bucket,
                object_name=self.dst,
                mime_type=self.mime_type,
                filename=file.name,
                gzip=self.gzip,
        )
        return f'gs://{self.bucket}/{self.dst}'

def _generate_schema(table_name, report_date, run_date):
    
    schema = []
    query  = f"SELECT\n"

    schema_df = pd.read_excel(SCHEMA_FILE, sheet_name = SCHEMA_SHEET, usecols = SCHEMA_COLUMNS)

    # Rename all Excel columns to script usable names
    new_columns = {
        SCHEMA_COLUMNS[0] : "TABLE_NAME",
        SCHEMA_COLUMNS[1] : "COLUMN_NAME",
        SCHEMA_COLUMNS[2] : "DATA_TYPE",
        SCHEMA_COLUMNS[3] : "IS_NULLABLE"
    }
    schema_df.rename(columns=new_columns, inplace=True)
    
    schema_df['TABLE_NAME']  = schema_df['TABLE_NAME'].str.lower()
    schema_df['COLUMN_NAME'] = schema_df['COLUMN_NAME'].str.lower()       

    # filter by table name
    filtered_schema_df = schema_df.loc[schema_df['TABLE_NAME'] == table_name.lower()]
    
    # Generate the GBQ table schema
    for index, rows in filtered_schema_df.iterrows():
        src_data_type  = filtered_schema_df.loc[filtered_schema_df.COLUMN_NAME == rows.COLUMN_NAME, "DATA_TYPE"].values[0].lower()
        gbq_data_mode  = filtered_schema_df.loc[filtered_schema_df.COLUMN_NAME == rows.COLUMN_NAME, "IS_NULLABLE"].values[0]
        gbq_field_mode = "REQUIRED" if str(gbq_data_mode) in [ "0", "0.0", "NO" ] else "NULLABLE"

        # Map GBQ data type and exit at first match
        gbq_data_type  = ""

        for line in BQ_DTYPE:
            if src_data_type.strip() in line:
                gbq_data_type = line[0]
                break

        if gbq_data_type == "":
            log.error(f"Cannot map field '{rows.COLUMN_NAME}' with data type: '{src_data_type}'") 
       
        schema.append({"name":rows.COLUMN_NAME, "type":gbq_data_type.upper(), "mode":gbq_field_mode })
        query = f"{query}\tCAST ({FIELD_PREFIX}`{rows.COLUMN_NAME}` AS {gbq_data_type.upper()}) AS `{rows.COLUMN_NAME}`,\n"

    # Add time partitioned field
    schema.append({"name":"report_date", "type":"DATE", "mode":"REQUIRED"})
    schema.append({"name":"run_date", "type":"DATE", "mode":"REQUIRED"})

    query = f"{query}\tDATE('{report_date}') AS `report_date`,\n"
    query = f"{query}\tDATE('{run_date}') AS `run_date`\n"

    query = f"{query}FROM `{PROJECT_SRC}.{DATASET_SRC}.{SOURCE_TYPE}_{table_name}`\n"
    query = f"{query}WHERE report_date = '{report_date}'"
    # query = f"{query}LIMIT 10"

    return schema, query

def _create_var(table_name, data):
    new_data = [ value[0] for value in data ]
    Variable.set(
        key   = f'{SOURCE_NAME}_{table_name}_report_date',
        value = new_data,
        serialize_json = True
    )

def _remove_var(table_name):
    Variable.delete(key = f'{SOURCE_NAME}_{table_name}_report_date')

def _update_query(report_date, run_date, sql):
    sql_lines = sql.splitlines(True)
    sql_lines[-1] = sql_lines[-1].replace(run_date, report_date)
    sql_lines[-4] = sql_lines[-4].replace(run_date, report_date)
    sql = ''.join(sql_lines)

    return sql

def _check_table(table_name, source_list):
    json_data  = json.loads(json.dumps(source_list))
    table_list = list(map(lambda datum: datum['tableId'], json_data))

    if f"daily_{table_name}" in table_list:
        return f"list_report_dates_{table_name}"
    else:
        return f"skip_table_{table_name}"

with DAG(
    dag_id="migrate_ofm",
    schedule_interval=None,
    # schedule_interval="40 00 * * *",
    start_date=dt.datetime(2022, 3, 30),
    catchup=False,
    tags=['convz_prod_migration'],
    render_template_as_native_obj=True,
) as dag:

    start_task = DummyOperator(task_id = "start_task")

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

    end_task   = DummyOperator(task_id = "end_task")

    iterable_tables_list = Variable.get(
        key=f'{SOURCE_NAME}_tables',
        default_var=['default_table'],
        deserialize_json=True
    )
    # iterable_tables_list = [ "tbaccount_segment_master" ]

    with TaskGroup(
        'migrate_historical_tasks_group',
        prefix_group_id=False,
    ) as migrate_tasks_group:

        if iterable_tables_list:
            for index, tm1_table in enumerate(iterable_tables_list):

                check_source_tb = BranchPythonOperator(
                    task_id = f'check_source_tb_{tm1_table}',
                    trigger_rule = 'all_success',
                    python_callable = _check_table,
                    op_kwargs = {
                        'table_name'  : tm1_table,
                        'source_list' : '{{ ti.xcom_pull(task_ids="get_source_tb") }}'
                    }
                )

                skip_table = DummyOperator(task_id = f"skip_table_{tm1_table}")

                list_report_dates = BigQueryExecuteQueryOperator(
                    task_id  = f"list_report_dates_{tm1_table}",
                    location = LOCATION,
                    sql      = f"SELECT DISTINCT CAST(report_date AS STRING) AS report_date "
                                + f"FROM `{PROJECT_SRC}.{DATASET_SRC}.{SOURCE_TYPE}_{tm1_table}` ORDER BY 1 ASC",
                    destination_dataset_table = f"{PROJECT_SRC}.{DATASET_SRC}.{tm1_table}_report_date",
                    write_disposition = "WRITE_TRUNCATE",
                    bigquery_conn_id  = 'convz_dev_service_account',
                    use_legacy_sql    = False,
                )

                get_list = BigQueryGetDataOperator(
                    task_id     = f'get_list_{tm1_table}',
                    dataset_id  = f'{DATASET_SRC}',
                    table_id    = f"{tm1_table}_report_date",
                    gcp_conn_id = 'convz_dev_service_account',
                    max_results = 100,
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

                create_schema = PythonOperator(
                    task_id=f'create_schema_{tm1_table}',
                    python_callable=_generate_schema,
                    op_kwargs={ 
                        'table_name' : tm1_table,
                        'report_date': '{{ ds }}',
                        'run_date'   : '{{ ds }}'
                    },
                )

                schema_to_gcs = ContentToGoogleCloudStorageOperator(
                    task_id = f'schema_to_gcs_{tm1_table}',
                    content = f'{{{{ ti.xcom_pull(task_ids="create_schema_{tm1_table}")[0] }}}}',
                    dst     = f'{SOURCE_NAME}/schemas/{tm1_table}.json',
                    bucket  = BUCKET_NAME,
                    gcp_conn_id = "convz_dev_service_account"
                )

                create_prod_table = BigQueryCreateEmptyTableOperator(
                    task_id = f"create_final_{tm1_table}",
                    google_cloud_storage_conn_id = "convz_dev_service_account",
                    bigquery_conn_id = "convz_dev_service_account",
                    project_id = PROJECT_DST,
                    dataset_id = DATASET_DST,
                    table_id = f"{tm1_table.lower()}_daily_source",
                    gcs_schema_object = f'gs://{BUCKET_NAME}/{SOURCE_NAME}/schemas/{tm1_table}.json',
                    time_partitioning = { "field":"report_date", "type":"DAY" },
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
                # iterable_date_list = [ "2022-01-21" ]

                with TaskGroup(
                    f'migrate_{tm1_table}_tasks_group',
                    prefix_group_id=False,
                ) as migrate_date_group:

                    if iterable_date_list:
                        for index, report_date in enumerate(iterable_date_list):

                            update_query = PythonOperator(
                                task_id=f'update_query_{tm1_table}_{report_date}',
                                python_callable = _update_query,
                                op_kwargs={ 
                                    'report_date' : report_date,
                                    'run_date'    : '{{ ds }}',
                                    'sql'         : f'{{{{ ti.xcom_pull(task_ids="create_schema_{tm1_table}")[1] }}}}'
                                },
                            )

                            extract_to_prod = BigQueryExecuteQueryOperator(
                                task_id  = f"extract_to_prod_{tm1_table}_{report_date}",
                                location = LOCATION,
                                sql      = f'{{{{ ti.xcom_pull(task_ids="update_query_{tm1_table}_{report_date}") }}}}',
                                destination_dataset_table = f"{PROJECT_DST}.{DATASET_DST}.{tm1_table.lower()}_{SOURCE_TYPE}_source$" 
                                                            + report_date.replace("-",''),
                                time_partitioning = { "field":"report_date", "type":"DAY" },
                                write_disposition = "WRITE_TRUNCATE",
                                bigquery_conn_id  = 'convz_dev_service_account',
                                use_legacy_sql    = False,
                            )

                            # Date level dependencies
                            update_query >> extract_to_prod 

                # Table level dependencies
                check_source_tb >> [ skip_table, list_report_dates ]
                list_report_dates >> get_list >> create_var >> [ drop_list, create_schema ]
                create_schema >> schema_to_gcs >> create_prod_table >> migrate_date_group >> remove_var
                

    # DAG level dependencies
    start_task >> [create_dataset, get_source_tb] >> migrate_tasks_group >> end_task
