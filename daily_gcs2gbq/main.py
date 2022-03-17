from airflow                   import configuration, DAG
from airflow.operators.python  import PythonOperator, BranchPythonOperator
from airflow.utils.decorators  import apply_defaults
from airflow.operators.bash    import BashOperator
from airflow.operators.dummy   import DummyOperator
from airflow.models            import Variable
from airflow.utils.task_group  import TaskGroup
from airflow.exceptions        import AirflowFailException

from airflow.providers.google.cloud.hooks.gcs          import *
from airflow.providers.google.cloud.operators.bigquery import *
from airflow.providers.google.cloud.operators.gcs      import *

from airflow.providers.google.cloud.transfers.local_to_gcs    import *
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import *

import datetime as dt
import pandas   as pd
import tempfile
import logging
import re

######### VARIABLES ###########

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
MAIN_PATH = configuration.get('core','dags_folder')

SCHEMA_FILE    = f"{MAIN_PATH}/schemas/OFM-B2S_Source_Datalake_20211020-live-version.xlsx"
SCHEMA_SHEET   = "Field-ERP"
SCHEMA_COLUMNS = ["TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE"] # Example value ["TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE"]

PROJECT_ID   = "central-cto-ofm-data-hub-dev"
DATASET_ID   = "test_airflow"
LOCATION     = "asia-southeast1" 

BUCKET_NAME  = "ofm-data"
SOURCE_NAME  = "ERP"
SOURCE_TYPE  = "daily"

PAYLOAD_NAME = "_airbyte_data"

###############################

class ContentToGoogleCloudStorageOperator(BaseOperator):

    template_fields = ('content', 'dst', 'bucket')

    @apply_defaults
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

        with tempfile.NamedTemporaryFile(prefix="gcs-local") as file:
            file.write(self.content.replace('\'','\"').encode('utf-8'))
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
    query  = f"\tSELECT\n"

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
        query = f"{query}\t\tCAST ({PAYLOAD_NAME}_{rows.COLUMN_NAME} AS {gbq_data_type.upper()}) AS {rows.COLUMN_NAME},\n"

    # Add time partitioned field
    schema.append({"name":"report_date", "type":"DATE", "mode":"REQUIRED"})
    schema.append({"name":"run_date", "type":"DATE", "mode":"REQUIRED"})

    query = f"{query}\t\tDATE('{report_date}') AS report_date,\n"
    query = f"{query}\t\tDATE('{run_date}') AS run_date\n"

    query = f"{query}\tFROM `{PROJECT_ID}.{DATASET_ID}.{SOURCE_TYPE}_{table_name}_flat`\n"
    query = f"{query}\tLIMIT 10"

    return schema, query

def _read_file(filename):
    with open(filename) as f:
        lines     = f.read().splitlines()
        tm1_files = []

        for line in lines:
            split_line    = line.split(",")
            split_line[1] = int(split_line[1])
            split_line[2] = split_line[2].replace(f"gs://{BUCKET_NAME}/","")
            tm1_files.append(split_line)

        # return tm1_files
        return [[ "tbadjusthead", 187796, "ERP/daily/tbadjusthead/2022_03_15_1647381759787_0.jsonl" ], [ "tblocationareamaster", 0, "ERP/daily/tblocationareamaster/2022_03_15_1647381823246_0.jsonl" ]]

def _process_list(ti, task_id):
    tm1_file_list = ti.xcom_pull(task_ids = task_id)
    Variable.set(
        key   = f'{SOURCE_NAME}_tm1_files',
        value = tm1_file_list,
        serialize_json = True
    )

def _check_size(tm1_file):
    print(f"File {tm1_file[2]} has size {tm1_file[1]} byte(s).")
    if tm1_file[1] == 0:
        return f"skip_file_{tm1_file[0]}" 
    else:
        return [ f"load2stg_{tm1_file[0]}", f"create_schema_{tm1_file[0]}" ]

with DAG(
    dag_id="daily_gcs2gbq",
    schedule_interval=None,
    start_date=dt.datetime(2022, 3, 1),
    catchup=False,
    tags=['convz_prod_airflow_code'],
) as dag:

    # get_table_names = BashOperator(
    #     task_id  = "get_table_names",
    #     cwd      = MAIN_PATH,
    #     bash_command = f"gsutil ls gs://{BUCKET_NAME}/{SOURCE_NAME}/{SOURCE_TYPE}"
    #                     + f" | grep -v erp_ | sed '1d' | cut -d'/' -f6 > {SOURCE_NAME}_tm1_folders;"
    #                     + f" echo {MAIN_PATH}/{SOURCE_NAME}_tm1_folders"
    # )
    
    # create_tm1_list = BashOperator(
    #     task_id  = "create_tm1_list",
    #     cwd      = MAIN_PATH,
    #     bash_command = f"gsutil cp -P gs://{BUCKET_NAME}/{SOURCE_NAME}/schemas/scan_tm1_files.bash .;"
    #                     + f" ./scan_tm1_files.bash {SOURCE_NAME} {BUCKET_NAME}/{SOURCE_NAME}/{SOURCE_TYPE} { '{{ yesterday_ds }}' }"
    # )
    
    read_tm1_list = PythonOperator(
        task_id = 'read_tm1_list',
        python_callable = _read_file,
        op_kwargs={ 
            'filename' : '/Users/oH/airflow/dags/ERP_tm1_files'
            # 'filename' : '{{ ti.xcom_pull(task_ids="create_tm1_list") }}'
        },
    )

    prepare_variables = PythonOperator(
        task_id = 'prepare_variables',
        python_callable = _process_list,
        op_kwargs = { 'task_id' : 'read_tm1_list' }
    )

    iterable_list = Variable.get(
        key=f'{SOURCE_NAME}_tm1_files',
        default_var=['default_tm1_file'],
        deserialize_json=True
    )

    with TaskGroup(
        'load_to_stg_tasks_group',
        prefix_group_id=False,
    ) as load_to_stg_tasks_group:

        if iterable_list:
            for index, tm1_file in enumerate(iterable_list):

                check_size = BranchPythonOperator(
                    task_id=f'check_size_{tm1_file[0]}',
                    python_callable=_check_size,
                    op_kwargs = { 'tm1_file' : tm1_file }
                )

                skip_file = DummyOperator(task_id = f"skip_file_{tm1_file[0]}")

                create_schema = PythonOperator(
                    task_id=f'create_schema_{tm1_file[0]}',
                    provide_context=True,
                    dag=dag,
                    python_callable=_generate_schema,
                    op_kwargs={ 
                        'table_name' : tm1_file[0],
                        'report_date': '{{ yesterday_ds }}',
                        'run_date'   : '{{ ds }}',
                    },
                )

                schema_to_gcs = ContentToGoogleCloudStorageOperator(
                    task_id = f'schema_to_gcs_{tm1_file[0]}', 
                    content = f'{{{{ ti.xcom_pull(task_ids="create_schema_{tm1_file[0]}")[0] }}}}', 
                    dst     = f'{SOURCE_NAME}/schemas/{tm1_file[0]}.json', 
                    bucket  = BUCKET_NAME, 
                    gcp_conn_id = "convz_dev_service_account"
                )

                create_final_table = BigQueryCreateEmptyTableOperator(
                    task_id = f"create_final_{tm1_file[0]}",
                    google_cloud_storage_conn_id = "convz_dev_service_account",
                    bigquery_conn_id = "convz_dev_service_account",
                    dataset_id = DATASET_ID,
                    table_id = f"daily_{tm1_file[0]}",
                    project_id = PROJECT_ID,
                    gcs_schema_object = f'{{{{ ti.xcom_pull(task_ids="schema_to_gcs_{tm1_file[0]}") }}}}',
                    time_partitioning = { "report_date": "DAY" },
                )

                load_tm1_file = GCSToBigQueryOperator(
                    task_id = f"load2stg_{tm1_file[0]}",
                    google_cloud_storage_conn_id = "convz_dev_service_account",
                    bigquery_conn_id = "convz_dev_service_account",
                    bucket = BUCKET_NAME,
                    source_objects = [ tm1_file[2] ],
                    source_format  = 'NEWLINE_DELIMITED_JSON',
                    destination_project_dataset_table = f"{PROJECT_ID}:{DATASET_ID}.daily_{tm1_file[0]}_stg${ '{{ yesterday_ds_nodash }}' }",
                    autodetect = True,
                    time_partitioning = { "run_date": "DAY" },
                    write_disposition = 'WRITE_TRUNCATE',
                )

                flatten_rows = BigQueryExecuteQueryOperator(
                    task_id  = f"flatten_rows_{tm1_file[0]}",
                    location = LOCATION,
                    sql      = f"SELECT * FROM [{PROJECT_ID}:{DATASET_ID}.daily_{tm1_file[0]}_stg]",
                    destination_dataset_table = f"{PROJECT_ID}:{DATASET_ID}.daily_{tm1_file[0]}_flat${ '{{ yesterday_ds_nodash }}' }",
                    time_partitioning = { "report_date": "DAY" },
                    write_disposition = 'WRITE_TRUNCATE',
                    bigquery_conn_id  = 'convz_dev_service_account',
                    flatten_results   = True,
                    use_legacy_sql    = True,        
                )

                extract_to_final = BigQueryExecuteQueryOperator(
                    task_id  = f"extract_to_final_{tm1_file[0]}",
                    location = LOCATION,
                    sql      = f'{{{{ ti.xcom_pull(task_ids="create_schema_{tm1_file[0]}")[1] }}}}',
                    destination_dataset_table = f"{PROJECT_ID}:{DATASET_ID}.daily_{tm1_file[0]}${ '{{ yesterday_ds_nodash }}' }",
                    time_partitioning = { "report_date": "DAY" },
                    write_disposition = 'WRITE_TRUNCATE',
                    bigquery_conn_id  = 'convz_dev_service_account',
                    use_legacy_sql    = False,
                    trigger_rule      = 'all_success'
                )

                # TaskGroup level dependencies               
                check_size >> [ skip_file, load_tm1_file, create_schema ] 
                load_tm1_file >> flatten_rows >> extract_to_final
                create_schema >> schema_to_gcs >> create_final_table >> extract_to_final

    # DAG level dependencies
    read_tm1_list >> prepare_variables >> load_to_stg_tasks_group