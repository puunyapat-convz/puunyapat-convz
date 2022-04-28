from airflow                   import configuration, DAG
from airflow.operators.python  import PythonOperator, BranchPythonOperator
from airflow.operators.bash    import BashOperator
from airflow.operators.dummy   import DummyOperator
from airflow.models            import Variable
from airflow.utils.task_group  import TaskGroup
from utils.dag_notification    import *

from airflow.providers.google.cloud.hooks.gcs          import *
from airflow.providers.google.cloud.operators.bigquery import *
from airflow.providers.google.cloud.operators.gcs      import *

from airflow.providers.google.cloud.transfers.local_to_gcs    import *
from airflow.providers.google.cloud.transfers.gcs_to_local    import *
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import *

import datetime as dt
import pandas   as pd
import pathlib
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
    [ "BYTES", "varbinary" ],
    [ "DATE", "date" ],
    [ "TIME", "time" ],
    [ "DATETIME", "datetime", "datetime2", "smalldatetime" ],
    [ "TIMESTAMP", "timestamp" ]
]

DATE_FORMAT = {
    "DATE" : "%FT%R:%E*SZ",
    "TIME" : "%T",
    "DATETIME"  : "%FT%R:%E*SZ",
    "TIMESTAMP" : "%FT%R:%E*SZ"
}

log       = logging.getLogger(__name__)
path      = configuration.get('core','dags_folder')
MAIN_PATH = path + "/../data"

SCHEMA_FILE    = f"{MAIN_PATH}/schemas/OFM-B2S_Source_Datalake_20211020-live-version.xlsx"
SCHEMA_SHEET   = "Field-TMS"
SCHEMA_COLUMNS = ["TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE"] 
# Example value ["TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE"]

PROJECT_ID   = "central-cto-ofm-data-hub-prod"
DATASET_ID   = "tms_ofm_daily"
LOCATION     = "asia-southeast1" 

BUCKET_NAME  = "ofm-data"
SOURCE_NAME  = "TMS"
SOURCE_TYPE  = "daily"

## airbyte header which contains data
FIELD_PREFIX = "_airbyte_data."

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
        else:
            gbq_data_type = gbq_data_type.upper()
       
        if gbq_data_type in ["DATE", "TIME", "DATETIME", "TIMESTAMP"]:
            if gbq_field_mode == "NULLABLE":
                method = f"IF   ({FIELD_PREFIX}`{rows.COLUMN_NAME}` IS NULL," \
                            + f" CAST(PARSE_TIMESTAMP('{DATE_FORMAT.get(gbq_data_type)}', {FIELD_PREFIX}`{rows.COLUMN_NAME}`) AS {gbq_data_type}), NULL)"
            else:
                method = f"CAST (PARSE_TIMESTAMP('{DATE_FORMAT.get(gbq_data_type)}', {FIELD_PREFIX}`{rows.COLUMN_NAME}`) AS {gbq_data_type})"
        else:
            method = f"CAST ({FIELD_PREFIX}`{rows.COLUMN_NAME}` AS {gbq_data_type})"

        query = f"{query}\t{method} AS `{rows.COLUMN_NAME}`,\n"
        schema.append({"name":rows.COLUMN_NAME, "type":gbq_data_type, "mode":gbq_field_mode })

    # Add time partitioned field
    schema.append({"name":"report_date", "type":"DATE", "mode":"REQUIRED"})
    schema.append({"name":"run_date", "type":"DATE", "mode":"REQUIRED"})

    query = f"{query}\tDATE('{report_date}') AS `report_date`,\n"
    query = f"{query}\tDATE('" + f"{run_date.strftime('%Y-%m-%d')}') AS `run_date`\n"
    query = f"{query}FROM `{PROJECT_ID}.{DATASET_ID}_stg.{table_name}_{SOURCE_TYPE}_stg`\n"

    return schema, query

def _check_file(ti, tablename, filename, gcs_path):
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
            ti.xcom_push(key='gcs_uri', value=gcs_path)
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
        path = pathlib.Path(f'{MAIN_PATH}/{SOURCE_NAME}/{table_name}')
        path.mkdir(parents=True, exist_ok=True)
        return [ f"drop_temp_{table_name}", f"create_schema_{table_name}", f"get_sample_{table_name}" ]

def _get_schema(table_name):
    hook = BigQueryHook(bigquery_conn_id="convz_dev_service_account")
    result = hook.get_schema(
        project_id=PROJECT_ID, 
        dataset_id=f"{DATASET_ID}_stg", 
        table_id  =table_name
    )
    return json.dumps(result)

def _update_schema(stg_schema, fin_schema):
    json_schema = stg_schema

    for index in range(len(json_schema)):
        if json_schema["fields"][index]["name"] == FIELD_PREFIX[0:-1]:
            stg_schema = json_schema["fields"][index]["fields"]
            break

    stg_fields = [ fields["name"].lower() for fields in stg_schema ]
    fin_fields = [ fields["name"].lower() for fields in fin_schema if fields["name"].lower() not in ["report_date","run_date"] ]

    for field_name in fin_fields:
        if field_name not in stg_fields:
            stg_fields.append(field_name)
            stg_schema.append(fin_schema[fin_fields.index(field_name)])

        if fin_schema[fin_fields.index(field_name)]["type"] in ["INT64", "FLOAT64"]:
            stg_schema[stg_fields.index(field_name)]["type"] = "FLOAT64"
        else:
            stg_schema[stg_fields.index(field_name)]["type"] = "STRING"

        stg_schema[stg_fields.index(field_name)]["mode"] = "NULLABLE"

    json_schema["fields"][index]["fields"] = stg_schema
    return json.dumps(json_schema, indent=4)

with DAG(
    dag_id="gcs2gbq_daily_tms",
    # schedule_interval=None,
    schedule_interval="30 01 * * *",
    start_date=dt.datetime(2022, 4, 25),
    catchup=True,
    max_active_runs=1,
    tags=['convz', 'production', 'airflow_style', 'daily', 'tms'],
    render_template_as_native_obj=True,
    default_args={
        'on_failure_callback': ofm_task_fail_slack_alert,
        'retries': 0
    }
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

    create_ds_stg = BigQueryCreateEmptyDatasetOperator(
        task_id     = "create_ds_stg",
        dataset_id  = f"{DATASET_ID}_stg",
        project_id  = PROJECT_ID,
        location    = LOCATION,
        gcp_conn_id = "convz_dev_service_account",
        exists_ok   = True
    )

    iterable_tables_list = Variable.get(
        key=f'{SOURCE_NAME}_{SOURCE_TYPE}',
        default_var=['default_table'],
        deserialize_json=True
    )
    # iterable_tables_list = [ "tbcodtypemaster" ]

    with TaskGroup(
        'load_tm1_folders_tasks_group',
        prefix_group_id=False,
    ) as load_folders_tasks_group:

        if iterable_tables_list:
            for index, tm1_table in enumerate(iterable_tables_list):

                create_tm1_list = BashOperator(
                    task_id = f"create_tm1_list_{tm1_table}",
                    cwd     = MAIN_PATH,
                    trigger_rule = 'all_success',
                    bash_command = f"temp=$(mktemp {SOURCE_NAME}_{SOURCE_TYPE}.XXXXXXXX)" 
                                    + f' && gsutil du "gs://{BUCKET_NAME}/{SOURCE_NAME}/{SOURCE_TYPE}/{tm1_table}/{{{{ ds.replace("-","_") }}}}*.jsonl"'
                                                                                    ## use yesterday_ds for manual run ^
                                    + f" | tr -s ' ' ',' | sed 's/^/{tm1_table},/g' | sort -t, -k2n > $temp;"
                                    + f' echo "{MAIN_PATH}/$temp"'
                )

                check_tm1_list = BranchPythonOperator(
                    task_id=f'check_tm1_list_{tm1_table}',
                    python_callable=_check_file,
                    op_kwargs = { 
                        'tablename' : tm1_table,
                        'filename' : f'{{{{ ti.xcom_pull(task_ids="create_tm1_list_{tm1_table}") }}}}',
                        'gcs_path' : f"gs://{BUCKET_NAME}/{SOURCE_NAME}/{SOURCE_TYPE}/{tm1_table}/{{{{ ds.replace('-','_') }}}}*.jsonl"
                    }
                )

                skip_table = DummyOperator(
                    task_id = f"skip_table_{tm1_table}",
                    on_success_callback = ofm_missing_daily_file_slack_alert
                )

                read_tm1_list = PythonOperator(
                    task_id = f'read_tm1_list_{tm1_table}',
                    python_callable = _read_file,
                    op_kwargs={ 
                        'filename' : f'{{{{ ti.xcom_pull(task_ids="create_tm1_list_{tm1_table}") }}}}'
                    },
                )

                remove_file_list = BashOperator(
                    task_id  = f"remove_file_list_{tm1_table}",
                    cwd      = MAIN_PATH,
                    trigger_rule = 'all_done',
                    bash_command = f"rm -f {{{{ ti.xcom_pull(task_ids='create_tm1_list_{tm1_table}') }}}}"
                )

                check_xcom = BranchPythonOperator(
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
                        'report_date': '{{ ds }}',
                        'run_date'   : '{{ data_interval_end }}'
                    },
                )

                schema_to_gcs = ContentToGoogleCloudStorageOperator(
                    task_id = f'schema_to_gcs_{tm1_table}',
                    content = f'{{{{ ti.xcom_pull(task_ids="create_schema_{tm1_table}")[0] }}}}',
                    dst     = f'{SOURCE_NAME}/schemas/{SOURCE_TYPE}_{tm1_table}.json',
                    bucket  = BUCKET_NAME,
                    gcp_conn_id = "convz_dev_service_account"
                )

                create_final_table = BigQueryCreateEmptyTableOperator(
                    task_id = f"create_final_{tm1_table}",
                    google_cloud_storage_conn_id = "convz_dev_service_account",
                    bigquery_conn_id = "convz_dev_service_account",
                    dataset_id = DATASET_ID,
                    table_id = f"{tm1_table.lower()}_{SOURCE_TYPE}_source",
                    project_id = PROJECT_ID,
                    gcs_schema_object = f'{{{{ ti.xcom_pull(task_ids="schema_to_gcs_{tm1_table}") }}}}',
                    time_partitioning = { "field":"report_date", "type":"DAY" },
                )

                drop_temp = BigQueryDeleteTableOperator(
                    task_id  = f"drop_temp_{tm1_table}",
                    location = LOCATION,
                    gcp_conn_id = 'convz_dev_service_account',
                    ignore_if_missing = True,
                    deletion_dataset_table = f"{PROJECT_ID}.{DATASET_ID}_stg.{tm1_table}_{SOURCE_TYPE}_stg"
                )

                get_sample = BashOperator(
                    task_id = f"get_sample_{tm1_table}",
                    cwd     = f"{MAIN_PATH}/{SOURCE_NAME}/{tm1_table}",
                    bash_command = f'gsutil cp {{{{ ti.xcom_pull(task_ids="read_tm1_list_{tm1_table}")[0] }}}} .'
                                        + f' && data_file=$(basename {{{{ ti.xcom_pull(task_ids="read_tm1_list_{tm1_table}")[0] }}}} | cut -d. -f1)'
                                        + " && head -1 $data_file.jsonl > $data_file-sample.jsonl"
                                        + " && echo $PWD/$data_file-sample.jsonl"
                )

                load_sample = BashOperator(
                    task_id = f"load_sample_{tm1_table}",
                    cwd     = f"{MAIN_PATH}/{SOURCE_NAME}",
                    trigger_rule = 'all_success',
                    bash_command = "bq load --autodetect --source_format=NEWLINE_DELIMITED_JSON"
                                    + f" {PROJECT_ID}:{DATASET_ID}_stg.{tm1_table}_{SOURCE_TYPE}_stg"
                                    + f' {{{{ ti.xcom_pull(task_ids="get_sample_{tm1_table}") }}}}'
                                    + f' && rm -rf {tm1_table}'
                )

                get_schema = PythonOperator(
                    task_id=f"get_schema_{tm1_table}",
                    provide_context=True,
                    python_callable=_get_schema,
                    op_kwargs = {
                        "table_name" : f"{tm1_table}_{SOURCE_TYPE}_stg"
                    }
                )

                update_schema = PythonOperator(
                    task_id=f"update_schema_{tm1_table}",
                    provide_context=True,
                    python_callable=_update_schema,
                    op_kwargs = {
                        "stg_schema": f'{{{{ ti.xcom_pull(task_ids="get_schema_{tm1_table}") }}}}',
                        "fin_schema": f'{{{{ ti.xcom_pull(task_ids="create_schema_{tm1_table}")[0] }}}}'
                    }
                )

                drop_sample = BigQueryDeleteTableOperator(
                    task_id  = f"drop_sample_{tm1_table}",
                    location = LOCATION,
                    gcp_conn_id = 'convz_dev_service_account',
                    ignore_if_missing = True,
                    deletion_dataset_table = f"{PROJECT_ID}.{DATASET_ID}_stg.{tm1_table}_{SOURCE_TYPE}_stg"
                )

                load_stg = BigQueryInsertJobOperator( 
                    task_id = f"load_stg_{tm1_table}",
                    gcp_conn_id = "convz_dev_service_account",
                    trigger_rule = 'all_success',
                    configuration = {
                        "load": {
                            "sourceUris": f'{{{{ ti.xcom_pull(task_ids="read_tm1_list_{tm1_table}") }}}}',
                            "destinationTable": {
                                "projectId": PROJECT_ID,
                                "datasetId": f"{DATASET_ID}_stg",
                                "tableId": f"{tm1_table}_{SOURCE_TYPE}_stg${{{{ ds_nodash }}}}"
                            },
                            "schema": f'{{{{ ti.xcom_pull(task_ids="update_schema_{tm1_table}") }}}}',
                            "timePartitioning": { "type": "DAY" },
                            "createDisposition": "CREATE_IF_NEEDED",
                            "writeDisposition": "WRITE_TRUNCATE",
                            "sourceFormat": "NEWLINE_DELIMITED_JSON",
                            "schemaUpdateOptions": ["ALLOW_FIELD_ADDITION"]
                        }
                    }
                )

                load_final = BigQueryInsertJobOperator( 
                    task_id = f"load_final_{tm1_table}",
                    gcp_conn_id = "convz_dev_service_account",
                    trigger_rule = 'all_success',
                    configuration = {
                        "query": {
                            "query": f'{{{{ ti.xcom_pull(task_ids="create_schema_{tm1_table}")[1] }}}}',
                            "destinationTable": {
                                "projectId": PROJECT_ID,
                                "datasetId": DATASET_ID,
                                "tableId": f"{tm1_table.lower()}_{SOURCE_TYPE}_source${{{{ ds_nodash }}}}"
                            },
                            "createDisposition": "CREATE_IF_NEEDED",
                            "writeDisposition": "WRITE_TRUNCATE",
                            "useLegacySql": False,
                            "timePartitioning": {
                                "field":"report_date",
                                "type":"DAY"
                            },
                        }
                    }
                )

                # TaskGroup load_files_tasks_group level dependencies
                create_tm1_list >> check_tm1_list >> [ skip_table, read_tm1_list ] >> remove_file_list
                read_tm1_list >> check_xcom >> [ skip_load, create_schema, drop_temp, get_sample ]

                [ drop_temp, get_sample ] >> load_sample >> get_schema >> [ update_schema, drop_sample ] >> load_stg >> load_final
                create_schema >> schema_to_gcs >> create_final_table >> load_final

    # DAG level dependencies
    start_task >> [ create_ds_final, create_ds_stg ] >> load_folders_tasks_group
    load_folders_tasks_group >> end_task
