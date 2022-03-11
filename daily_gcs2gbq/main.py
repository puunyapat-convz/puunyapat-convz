from airflow                  import configuration, DAG
from airflow.operators.dummy  import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.decorators       import task
from google.cloud             import bigquery

import datetime as dt
import pandas   as pd
import logging
import airflow.contrib.operators.gcs_to_bq as gcs2bq

BQ_DTYPE = [
    [ "STRING", "string", "char", "nchar", "nvarchar", "varchar", "sysname", "text", "uniqueidentifier" ],
    [ "INTEGER", "integer", "int", "tinyint", "smallint", "bigint" ],
    [ "FLOAT", "float", "numeric", "decimal", "money" ],
    [ "BOOLEAN", "bit", "boolean" ],
    [ "DATE", "date" ],
    [ "TIME", "time" ],
    [ "DATETIME", "datetime", "datetime2", "smalldatetime" ],
    [ "TIMESTAMP", "timestamp" ]
]

PD_DTYPE = {
    "INTEGER"  : "int64",
    "STRING"   : "object",
    "FLOAT"    : "float64",
    "BOOLEAN"  : "bool",
    "DATE"     : "datetime64",
    "TIME"     : "datetime64",
    "DATETIME" : "datetime64",
    "TIMESTAMP": "datetime64"
}

log       = logging.getLogger(__name__)
MAIN_PATH = configuration.get('core','dags_folder')

# PATH FOR EXCEL SCHEMA FILE
SCHEMAS_PATH   = f"{MAIN_PATH}/schemas"
# SCHEMA MAPPING FROM EXCEL
SCHEMA_FILE    = f"{SCHEMAS_PATH}/OFM-B2S_Source_Datalake_20211020-live-version.xlsx"
# Sheet name in schema file. For examples: 'Field-Officemate', 'Field-ERP'
SCHEMA_SHEET   = "Field-ERP"
# Specify column names from schema file for TABLE_NAME, FIELD_NAME, DATA_TYPE and IS_NULLABLE respectively.
SCHEMA_COLUMNS = ["TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE"] # Example value ["TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE"]

def generate_schema(table_name):
    
    schema    = []
    schema_df = pd.read_excel(SCHEMA_FILE, sheet_name = SCHEMA_SHEET, usecols = SCHEMA_COLUMNS)

    new_columns = {
        SCHEMA_COLUMNS[0] : "TABLE_NAME",
        SCHEMA_COLUMNS[1] : "COLUMN_NAME",
        SCHEMA_COLUMNS[2] : "DATA_TYPE",
        SCHEMA_COLUMNS[3] : "IS_NULLABLE"
    }
    # Rename all Excel columns to script usable names
    schema_df.rename(columns=new_columns, inplace=True)
    
    # Slice only required columns
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
            log.error(f"!!Cannot map field '{rows.COLUMN_NAME}' with data type: '{src_data_type}'") 
       
        schema.append(bigquery.SchemaField(rows.COLUMN_NAME, gbq_data_type, mode = gbq_field_mode))    
    return schema

with DAG(
    dag_id="daily_gcs2gbq",
    schedule_interval=None,
    start_date=dt.datetime(2022, 3, 1),
    catchup=False
) as dag:
    start_task = DummyOperator(task_id="start_task")

    @task(task_id="load_to_staging")
    def load2stg():
        schema = generate_schema("ERP_TBDLDetail")
        for rows in schema:
            print(rows)
        # gcs2bq.GoogleCloudStorageToBigQueryOperator()
        return 0

        # gs://ofm-data/ERP/daily/erp_tbdldetail/2022_03_10_1646955354604_0.jsonl