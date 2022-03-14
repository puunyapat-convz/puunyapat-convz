from airflow                  import configuration, DAG
from airflow.operators.dummy  import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash   import BashOperator
from airflow.decorators       import task

import datetime as dt
import pandas   as pd
import logging
import airflow.contrib.operators.gcs_to_bq as gcs2bq
import airflow.providers.google.cloud.operators.bigquery as af_bq

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
BUCKET_NAME  = "ofm-data"
TABLE_NAME   = "ERP_TBDLDetail".lower()
LOCATION     = "asia-southeast1" 

PAYLOAD_NAME = "_airbyte_data"

###############################

def generate_schema(table_name):
    
    schema = []
    query_insert = f"INSERT INTO `{PROJECT_ID}.{DATASET_ID}.daily_{table_name}`\n\t(\n"
    query_select = f"\tSELECT\n"

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
       
        schema.append({"mode":gbq_field_mode, "name":rows.COLUMN_NAME, "type":gbq_data_type.upper()})
        query_insert = f"{query_insert}\t\t{rows.COLUMN_NAME},\n"
        query_select = f"{query_select}\t\tCAST ({PAYLOAD_NAME}.{rows.COLUMN_NAME} AS {gbq_data_type.upper()}) AS {rows.COLUMN_NAME},\n"

    # Add time partitioned field
    schema.append({"name":"report_date", "type":"DATE","mode":"REQUIRED"})
    schema.append({"name":"run_date",    "type":"DATE","mode":"REQUIRED"})

    query_insert = f"{query_insert}\t\treport_date,\n"
    query_insert = f"{query_insert}\t\trun_date\n\t)\n"

    query_select = f"{query_select}\t\tDATE('2022-03-13') AS report_date,\n"
    query_select = f"{query_select}\t\tDATE('2022-03-14') AS run_date\n"

    query = f"{query_insert}{query_select}\tFROM `{PROJECT_ID}.{DATASET_ID}.daily_{table_name}_stg`\n"
    query = f"{query}\tLIMIT 10"

    return schema, query

with DAG(
    dag_id="daily_gcs2gbq",
    schedule_interval=None,
    start_date=dt.datetime(2022, 3, 1),
    catchup=False,
    tags=['convz_production_code'],
) as dag:

    start_task = DummyOperator(task_id="start_task")

    create_schema = PythonOperator(
        task_id='create_schema',
        provide_context=True,
        dag=dag,
        python_callable=generate_schema,
        op_kwargs={ 'table_name': TABLE_NAME },
    )

    # load_to_staging = af_bq.GoogleCloudStorageToBigQueryOperator(
    #     task_id = "load_to_staging",
    #     google_cloud_storage_conn_id = "convz_dev_service_account",
    #     bigquery_conn_id= "convz_dev_service_account",
    #     bucket = BUCKET_NAME,
    #     source_objects = [f'ERP/daily/{TABLE_NAME.lower()}/2022_03_10_1646955354604_0.jsonl'],
    #     source_format= 'NEWLINE_DELIMITED_JSON',
    #     destination_project_dataset_table = f'{PROJECT_ID}:{DATASET_ID}.daily_{TABLE_NAME}_stg',
    #     autodetect = True,
    #     time_partitioning = { "run_date": "DAY" },
    #     write_disposition = 'WRITE_TRUNCATE',
    #     dag = dag
    # )

    test_jinja = BashOperator(
        task_id='bash_op',
        bash_command="echo '{{ task_instance.xcom_pull(task_ids=\"create_schema\")[1] }}'",
        dag=dag,
    )

    # @task(task_id="print_schema")
    # def fetch_schema(my_task_id, **kwargs):
    #     ti = kwargs['ti']
    #     print(ti.xcom_pull(task_ids=my_task_id))

    # print_schema = fetch_schema('create_schema')

    # pull = PythonOperator(
    #     task_id='puller',
    #     provide_context=True,
    #     dag=dag,
    #     python_callable=puller_dynamic,
    #     op_kwargs={'my_task_id': 'generate_schema'},
    # )

    # create_final_table = af_bq.BigQueryCreateEmptyTableOperator(
    #     task_id = "create_final_table",
    #     bigquery_conn_id = "convz_dev_service_account",
    #     dataset_id = DATASET_ID,
    #     table_id = f"daily_{TABLE_NAME}",
    #     project_id = PROJECT_ID,
    #     schema_fields = "{{ ti.xcom_pull(task_ids='create_schema')[0] }}",
    #     time_partitioning = { "report_date": "DAY" },
    #     dag = dag
    # )

    # extract_to_final = af_bq.BigQueryExecuteQueryOperator(
    #     task_id  = "extract_to_final",
    #     sql      = "{{ ti.xcom_pull(task_ids='create_schema')[1] }}",
    #     location = LOCATION,
    #     bigquery_conn_id = 'convz_dev_service_account',
    #     use_legacy_sql   = False,        
    # )
    
    end_task = DummyOperator(task_id="end_task")

    start_task >> create_schema >> test_jinja >> end_task