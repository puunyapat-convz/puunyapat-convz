from airflow                   import configuration, DAG
from airflow.operators.python  import PythonOperator, BranchPythonOperator
from airflow.operators.bash    import BashOperator
from airflow.operators.dummy   import DummyOperator
from airflow.models            import Variable
from airflow.utils.task_group  import TaskGroup

from airflow.providers.google.cloud.hooks.gcs          import *
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
SCHEMA_SHEET   = "Field-MDS"
SCHEMA_COLUMNS = ["Table_Name", "Field_Name", "Field_Type", "IS_NULLABLE"] # Example value ["TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE"]

PROJECT_ID   = "central-cto-ofm-data-hub-dev"
DATASET_ID   = "airflow_test_mds"
LOCATION     = "asia-southeast1" 

BUCKET_NAME  = "ofm-data"
SOURCE_NAME  = "MDS"
SOURCE_TYPE  = "daily"

## airbyte header which contains data
PAYLOAD_NAME = "_airbyte_data"

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
        query = f"{query}\tCAST ({PAYLOAD_NAME}.{rows.COLUMN_NAME} AS {gbq_data_type.upper()}) AS `{rows.COLUMN_NAME}`,\n"

    # Add time partitioned field
    schema.append({"name":"report_date", "type":"DATE", "mode":"REQUIRED"})
    schema.append({"name":"run_date", "type":"DATE", "mode":"REQUIRED"})

    query = f"{query}\tDATE('{report_date}') AS report_date,\n"
    query = f"{query}\tDATE('{run_date}') AS run_date\n"

    query = f"{query}FROM `{PROJECT_ID}.{DATASET_ID}.{SOURCE_TYPE}_{table_name}_stg`\n"
    # query = f"{query}WHERE DATE(_PARTITIONTIME) = '{report_date}'"
    # query = f"{query}LIMIT 10"

    return schema, query

def _check_file(tablename, filename):
    result = False

    with open(filename) as f:
        lines  = f.readlines()

        for line in lines: 
            print(len(line.strip().split(",")))
            if len(line.strip().split(",")) == 3:
                result = True
                break
            else:
                result = False
                break

        if not result:
            log.info(f"Table [ {tablename} ] has no T-1 file(s) for this run.")
            return f"skip_table_{tablename}"
        else:
            return f"read_tm1_list_{tablename}"

def _read_file(filename):
    with open(filename) as f:
        lines    = f.read().splitlines()
        gcs_list = []

        ## Each valid line contains
        ## [0] = tablename
        ## [1] = filesize
        ## [2] = GCS URI

        for line in lines:
            split_line    = line.split(",")
            split_line[1] = int(split_line[1])
            split_line[2] = split_line[2].replace(f"gs://{BUCKET_NAME}/", "")

            if split_line[1] == 0:
                log.warning(f"Skipped file [{split_line[2]}] which has size {split_line[1]} byte.")
            else:
                ## Create file size > 0 list
                log.info(f"Adding file [{split_line[2]}] with size {split_line[1]} byte to list.")
                gcs_list.append(split_line[2])

        return gcs_list

def _check_xcom(table_name, tm1_varible):
    if len(tm1_varible) == 0:
        log.info(f"Table [ {table_name} ] has no T-1 file to load.")        
        return f"skip_load_{table_name}"
    else:
        return [ f"drop_temp_{table_name}", f"create_schema_{table_name}" ]

with DAG(
    dag_id="gcs2gbq_daily_mds",
    # schedule_interval=None,
    schedule_interval="20 00 * * *",
    start_date=dt.datetime(2022, 3, 28),
    catchup=False,
    tags=['convz_prod_airflow_style'],
    render_template_as_native_obj=True,
) as dag:

    start_task = DummyOperator(task_id = "start_task")

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id     = "create_dataset",
        dataset_id  = DATASET_ID,
        project_id  = PROJECT_ID,
        location    = LOCATION,
        gcp_conn_id = "convz_dev_service_account",
        exists_ok   = True
    )

    end_task   = DummyOperator(task_id = "end_task")

    iterable_tables_list = Variable.get(
        key=f'{SOURCE_NAME}_tables',
        default_var=['default_table'],
        deserialize_json=True
    )
    # iterable_tables_list = [ "COL_STORE_MASTER" ]

    with TaskGroup(
        'load_tm1_folders_tasks_group',
        prefix_group_id=False,
    ) as load_folders_tasks_group:

        if iterable_tables_list:
            for index, tm1_table in enumerate(iterable_tables_list):

                create_tm1_list = BashOperator(
                    task_id  = f"create_tm1_list_{tm1_table}",
                    cwd      = MAIN_PATH,
                    bash_command = "yesterday=$(sed 's/-/_/g' <<< {{ yesterday_ds }});"
                                    + f' gsutil du "gs://{BUCKET_NAME}/{SOURCE_NAME}/{SOURCE_TYPE}/{tm1_table}/$yesterday*.jsonl"'
                                    + f" | tr -s ' ' ',' | sed 's/^/{tm1_table},/g' | sort -t, -k2n > {SOURCE_NAME}_{tm1_table}_tm1_files;"
                                    + f' echo "{MAIN_PATH}/{SOURCE_NAME}_{tm1_table}_tm1_files"'
                )

                check_tm1_list = BranchPythonOperator(
                    task_id=f'check_tm1_list_{tm1_table}',
                    python_callable=_check_file,
                    op_kwargs = { 
                        'tablename' : tm1_table,
                        'filename' : f'{{{{ ti.xcom_pull(task_ids="create_tm1_list_{tm1_table}") }}}}'
                    }
                )

                skip_table = DummyOperator(task_id = f"skip_table_{tm1_table}")

                read_tm1_list = PythonOperator(
                    task_id = f'read_tm1_list_{tm1_table}',
                    python_callable = _read_file,
                    op_kwargs={ 
                        'filename' : f'{{{{ ti.xcom_pull(task_ids="create_tm1_list_{tm1_table}") }}}}'
                        # 'filename' : '/Users/oH/airflow/dags/ERP_tm1_files'
                    },
                )

                remove_file_list = BashOperator(
                    task_id  = f"remove_file_list_{tm1_table}",
                    cwd      = MAIN_PATH,
                    trigger_rule = 'all_done',
                    bash_command = f"rm -f {{{{ ti.xcom_pull(task_ids='create_tm1_list_{tm1_table}') }}}}"
                )

                check_variable = BranchPythonOperator(
                    task_id=f'check_xcom_{tm1_table}',
                    python_callable=_check_xcom,
                    op_kwargs = {
                        'table_name'  : tm1_table,
                        'tm1_varible' : f'{{{{ ti.xcom_pull(task_ids="read_tm1_list_{tm1_table}") }}}}'
                    }
                )

                skip_load = DummyOperator(task_id = f"skip_load_{tm1_table}")

                create_schema = PythonOperator(
                    task_id=f'create_schema_{tm1_table}',
                    provide_context=True,
                    dag=dag,
                    python_callable=_generate_schema,
                    op_kwargs={ 
                        'table_name' : tm1_table,
                        'report_date': '{{ yesterday_ds }}',
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

                create_final_table = BigQueryCreateEmptyTableOperator(
                    task_id = f"create_final_{tm1_table}",
                    google_cloud_storage_conn_id = "convz_dev_service_account",
                    bigquery_conn_id = "convz_dev_service_account",
                    dataset_id = DATASET_ID,
                    table_id = f"daily_{tm1_table}",
                    project_id = PROJECT_ID,
                    gcs_schema_object = f'{{{{ ti.xcom_pull(task_ids="schema_to_gcs_{tm1_table}") }}}}',
                    time_partitioning = { "report_date": "DAY" },
                )

                drop_temp_tables = BigQueryDeleteTableOperator(
                    task_id  = f"drop_temp_{tm1_table}",
                    location = LOCATION,
                    gcp_conn_id = 'convz_dev_service_account',
                    ignore_if_missing = True,
                    deletion_dataset_table = f'{PROJECT_ID}.{DATASET_ID}.daily_{tm1_table}_stg'
                )

                load_tm1_files = GCSToBigQueryOperator(
                    task_id = f"load2stg_{tm1_table}",
                    google_cloud_storage_conn_id = "convz_dev_service_account",
                    bigquery_conn_id = "convz_dev_service_account",
                    bucket = BUCKET_NAME,
                    source_objects = f'{{{{ ti.xcom_pull(task_ids="read_tm1_list_{tm1_table}") }}}}',
                    source_format  = 'NEWLINE_DELIMITED_JSON',
                    destination_project_dataset_table = f"{PROJECT_ID}.{DATASET_ID}.daily_{tm1_table}_stg",
                    autodetect = True,
                    time_partitioning = { "run_date": "DAY" },
                    write_disposition = "WRITE_TRUNCATE",
                )

                extract_to_final = BigQueryExecuteQueryOperator(
                    task_id  = f"extract_to_final_{tm1_table}",
                    location = LOCATION,
                    sql      = f'{{{{ ti.xcom_pull(task_ids="create_schema_{tm1_table}")[1] }}}}',
                    destination_dataset_table = f"{PROJECT_ID}.{DATASET_ID}.daily_{tm1_table}${{{{ yesterday_ds_nodash }}}}",
                    time_partitioning = { "report_date": "DAY" },
                    write_disposition = "WRITE_TRUNCATE",
                    bigquery_conn_id  = 'convz_dev_service_account',
                    use_legacy_sql    = False,
                    trigger_rule      = 'all_success'
                )

                # TaskGroup load_files_tasks_group level dependencies
                create_tm1_list >> check_tm1_list >> [ skip_table, read_tm1_list ] >> remove_file_list
                read_tm1_list >> check_variable >> [ skip_load, create_schema, drop_temp_tables ]

                drop_temp_tables >> load_tm1_files >> extract_to_final
                create_schema >> schema_to_gcs >> create_final_table >> extract_to_final
                extract_to_final >> end_task                

    # DAG level dependencies
    start_task >> create_dataset >> load_folders_tasks_group >> end_task

## TO DO
## 1. Change BigQueryExecuteQueryOperator to BigQueryInsertJobOperator
