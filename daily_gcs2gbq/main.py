from airflow                   import configuration, DAG
from airflow.operators.python  import PythonOperator, get_current_context
from airflow.decorators        import task
from airflow.utils.decorators  import apply_defaults
from airflow.operators.bash    import *

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
TABLE_NAME   = "ERP_TBDLDetail".lower()

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

def generate_schema(table_name, report_date, run_date):
    
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

with DAG(
    dag_id="daily_gcs2gbq",
    schedule_interval=None,
    start_date=dt.datetime(2022, 3, 1),
    catchup=False,
    tags=['convz_production_code'],
) as dag:

    # get_tm1_file = BashOperator(
    #     task_id  = "get_tm1_file",
    #     cwd      = MAIN_PATH,
    #     bash_command = f"yesterday=$(sed 's/-/_/g' <<< { '{{ yesterday_ds }}' });"
    #                     + f"gsutil ls \"gs://{BUCKET_NAME}/{SOURCE_NAME}/{SOURCE_TYPE}/{TABLE_NAME}/$yesterday*.jsonl\""
    #                     + f" | sed 's#gs://{BUCKET_NAME}/##g' > {TABLE_NAME}_tm1; "
    #                     + f"echo {MAIN_PATH}/{TABLE_NAME}_tm1"
    # )
    
    @task(task_id="load_all_to_staging")

    def call_load_all_to_staging():
        context = get_current_context()
        target_dataset = f"{PROJECT_ID}:{DATASET_ID}.daily_{TABLE_NAME}"

        with open(f'{MAIN_PATH}/{TABLE_NAME}_tm1', 'r') as infile:
            tm1_files = infile.read().splitlines()
                    
        for tm1_file in tm1_files:
            file_name = re.split('[/.]', tm1_file)[2]
            print(f"loading file [{tm1_file}] into [{target_dataset}${get_date(context)}]...")

            GCSToBigQueryOperator(
                task_id = f"load_to_staging_{file_name}",
                google_cloud_storage_conn_id = "convz_dev_service_account",
                bigquery_conn_id = "convz_dev_service_account",
                bucket = BUCKET_NAME,
                source_objects = [ tm1_file ],
                source_format  = 'NEWLINE_DELIMITED_JSON',
                destination_project_dataset_table = f"{target_dataset}${ '{{ yesterday_ds }}' }",
                autodetect = True,
                time_partitioning = { "run_date": "DAY" },
                write_disposition = 'WRITE_TRUNCATE',
                dag = dag
            )

    def get_date(context):
        return context['yesterday_ds']

    load_all_to_staging = call_load_all_to_staging()

    create_schema = PythonOperator(
        task_id='create_schema',
        provide_context=True,
        dag=dag,
        python_callable=generate_schema,
        op_kwargs={ 
            'table_name' : TABLE_NAME,
            'report_date': '{{ yesterday_ds }}',
            'run_date'   : '{{ ds }}',
        },
    )

    schema_to_gcs = ContentToGoogleCloudStorageOperator(
        task_id = 'schema_to_gcs', 
        content = '{{ ti.xcom_pull(task_ids="create_schema")[0] }}', 
        dst     = f'{SOURCE_NAME}/schemas/{TABLE_NAME}.json', 
        bucket  = BUCKET_NAME, 
        gcp_conn_id = "convz_dev_service_account"
    )

    flatten_rows = BigQueryExecuteQueryOperator(
        task_id  = "flatten_rows",
        location = LOCATION,
        sql      = f"SELECT * FROM [{PROJECT_ID}:{DATASET_ID}.daily_{TABLE_NAME}_stg]",
        destination_dataset_table = f"{PROJECT_ID}:{DATASET_ID}.daily_{TABLE_NAME}_flat${ '{{ yesterday_ds_nodash }}' }",
        time_partitioning = { "report_date": "DAY" },
        write_disposition = 'WRITE_TRUNCATE',
        bigquery_conn_id  = 'convz_dev_service_account',
        flatten_results   = True,
        use_legacy_sql    = True,        
    )

    create_final_table = BigQueryCreateEmptyTableOperator(
        task_id = "create_final_table",
        bigquery_conn_id = "convz_dev_service_account",
        dataset_id = DATASET_ID,
        table_id = f"daily_{TABLE_NAME}",
        project_id = PROJECT_ID,
        gcs_schema_object = '{{ ti.xcom_pull(task_ids="schema_to_gcs") }}',
        time_partitioning = { "report_date": "DAY" },
        dag = dag
    )

    extract_to_final = BigQueryExecuteQueryOperator(
        task_id  = "extract_to_final",
        location = LOCATION,
        sql      = '{{ ti.xcom_pull(task_ids="create_schema")[1] }}',
        destination_dataset_table = f"{PROJECT_ID}:{DATASET_ID}.daily_{TABLE_NAME}${ '{{ yesterday_ds_nodash }}' }",
        time_partitioning = { "report_date": "DAY" },
        write_disposition = 'WRITE_TRUNCATE',
        bigquery_conn_id  = 'convz_dev_service_account',
        use_legacy_sql    = False,
        trigger_rule      = 'all_success'
    )
    load_all_to_staging >> flatten_rows >> extract_to_final
    create_schema >> schema_to_gcs >> create_final_table >> extract_to_final