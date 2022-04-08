from airflow                   import configuration, DAG
from airflow.models            import Variable
from airflow.operators.python  import PythonOperator
from airflow.operators.bash    import BashOperator
from airflow.providers.google.cloud.hooks.bigquery import *
from airflow.providers.google.cloud.transfers.gcs_to_local import *
from airflow.providers.google.cloud.operators.bigquery import *
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import *

import datetime as dt
import tempfile
import json

PROJECT_ID   = "central-cto-ofm-data-hub-prod"
DATASET_ID   = "erp_ofm_daily_stg"
LOCATION     = "asia-southeast1" 

BUCKET_NAME  = "ofm-data"
SOURCE_NAME  = "ERP"
SOURCE_TYPE  = "daily"

path      = configuration.get('core','dags_folder')
MAIN_PATH = path + "/../data"

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

def _update_schema(schema_data):
    # schema_data = '{ "fields": [ { "name": "_airbyte_data", "type": "RECORD", "mode": "NULLABLE", "fields": [ { "name": "_airbyte_emitted_at", "type": "TIMESTAMP", "mode": "NULLABLE", "description": "bq-datetime" }, { "name": "shippingreasoncode", "type": "STRING", "mode": "NULLABLE" }, { "name": "customernameshort", "type": "STRING", "mode": "NULLABLE" }, { "name": "customeraddress2", "type": "STRING", "mode": "NULLABLE" }, { "name": "customeraddress1", "type": "STRING", "mode": "NULLABLE" }, { "name": "shippinglabelno", "type": "INTEGER", "mode": "NULLABLE" }, { "name": "shippingboxtype", "type": "STRING", "mode": "NULLABLE" }, { "name": "shippinggroup", "type": "STRING", "mode": "NULLABLE" }, { "name": "shippingboxno", "type": "INTEGER", "mode": "NULLABLE" }, { "name": "textfilename", "type": "STRING", "mode": "NULLABLE" }, { "name": "customername", "type": "STRING", "mode": "NULLABLE" }, { "name": "canceledqty", "type": "FLOAT", "mode": "NULLABLE" }, { "name": "personname", "type": "STRING", "mode": "NULLABLE" }, { "name": "shippingplandate", "type": "INTEGER", "mode": "NULLABLE" }, { "name": "phonenumber", "type": "INTEGER", "mode": "NULLABLE" }, { "name": "expirydate", "type": "STRING", "mode": "NULLABLE" }, { "name": "pickedqty", "type": "FLOAT", "mode": "NULLABLE" }, { "name": "faxnumber", "type": "INTEGER", "mode": "NULLABLE" }, { "name": "inventoryassignmentstartno", "type": "STRING", "mode": "NULLABLE" }, { "name": "status", "type": "STRING", "mode": "NULLABLE" }, { "name": "updateby", "type": "STRING", "mode": "NULLABLE" }, { "name": "updateon", "type": "TIMESTAMP", "mode": "NULLABLE", "description": "bq-datetime" }, { "name": "pickedtime", "type": "INTEGER", "mode": "NULLABLE" }, { "name": "qcstatus", "type": "INTEGER", "mode": "NULLABLE" }, { "name": "sectionname", "type": "STRING", "mode": "NULLABLE" }, { "name": "msgerror", "type": "STRING", "mode": "NULLABLE" }, { "name": "itemcode", "type": "INTEGER", "mode": "NULLABLE" }, { "name": "createon", "type": "TIMESTAMP", "mode": "NULLABLE", "description": "bq-datetime" }, { "name": "ID", "type": "FLOAT", "mode": "NULLABLE" }, { "name": "customercode", "type": "INTEGER", "mode": "NULLABLE" }, { "name": "createby", "type": "STRING", "mode": "NULLABLE" }, { "name": "zipcode", "type": "INTEGER", "mode": "NULLABLE" }, { "name": "customeraddress3", "type": "STRING", "mode": "NULLABLE" }, { "name": "specifiedlot", "type": "STRING", "mode": "NULLABLE" }, { "name": "detailno", "type": "INTEGER", "mode": "NULLABLE" }, { "name": "saleorderno", "type": "STRING", "mode": "NULLABLE" }, { "name": "ownercode", "type": "STRING", "mode": "NULLABLE" }, { "name": "lot", "type": "STRING", "mode": "NULLABLE" }, { "name": "systemid", "type": "STRING", "mode": "NULLABLE" }, { "name": "_airbyte_tbshippinglabelinformation_hashid", "type": "STRING", "mode": "NULLABLE" }, { "name": "remarks", "type": "STRING", "mode": "NULLABLE" }, { "name": "memo1", "type": "STRING", "mode": "NULLABLE" }, { "name": "pickeddate", "type": "TIMESTAMP", "mode": "NULLABLE", "description": "bq-datetime" }, { "name": "memo2", "type": "STRING", "mode": "NULLABLE" }, { "name": "itemname", "type": "STRING", "mode": "NULLABLE" }, { "name": "subcode", "type": "INTEGER", "mode": "NULLABLE" } ] }, { "name": "_airbyte_emitted_at", "type": "INTEGER", "mode": "NULLABLE" }, { "name": "_airbyte_ab_id", "type": "STRING", "mode": "NULLABLE" } ] }'
    # json_schema = json.loads(schema_data)
    json_schema = schema_data

    for index in range(len(json_schema)):
        if json_schema['fields'][index]['name'] == "_airbyte_data":
            stg_schema = json_schema['fields'][index]['fields']
            break
            
    # stg_schema = next(data['fields'] for data in schema_data['fields'] if data['name'] == '_airbyte_data')
    fin_schema = [{'name': 'test_add', 'type': 'STRING', 'mode': 'REQUIRED'},{'name': 'id', 'type': 'INT64', 'mode': 'REQUIRED'}, {'name': 'shippinglabelno', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'systemid', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'shippingreasoncode', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'saleorderno', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'detailno', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'ownercode', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'itemcode', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'subcode', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'qcstatus', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'lot', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'expirydate', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'pickedqty', 'type': 'INT64', 'mode': 'REQUIRED'}, {'name': 'canceledqty', 'type': 'INT64', 'mode': 'REQUIRED'}, {'name': 'shippingplandate', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'shippinggroup', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'inventoryassignmentstartno', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'customercode', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'customername', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'customernameshort', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'customeraddress1', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'customeraddress2', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'customeraddress3', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'zipcode', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'phonenumber', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'faxnumber', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'sectionname', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'personname', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'memo1', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'memo2', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'specifiedlot', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'remarks', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'itemname', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'pickeddate', 'type': 'DATETIME', 'mode': 'REQUIRED'}, {'name': 'pickedtime', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'shippingboxno', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'shippingboxtype', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'textfilename', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'status', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'updateon', 'type': 'DATETIME', 'mode': 'REQUIRED'}, {'name': 'updateby', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'createon', 'type': 'DATETIME', 'mode': 'REQUIRED'}, {'name': 'createby', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'msgerror', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'report_date', 'type': 'DATE', 'mode': 'REQUIRED'}, {'name': 'run_date', 'type': 'DATE', 'mode': 'REQUIRED'}]

    stg_fields = [ fields['name'].lower() for fields in stg_schema ]
    fin_fields = [ fields['name'].lower() for fields in fin_schema if fields['name'].lower() not in ['report_date','run_date'] ]

    for field_name in fin_fields:
        if field_name in stg_fields:
            if fin_schema[fin_fields.index(field_name)]['type'] in [ 'INT64', 'FLOAT64']:
                stg_schema[stg_fields.index(field_name)]['type'] = 'FLOAT64'
            else:
                stg_schema[stg_fields.index(field_name)]['type'] = 'STRING'

    json_schema['fields'][index]['fields'] = stg_schema
    return json.dumps(json_schema['fields'], indent=4)

def _get_schema(table_name):
    hook = BigQueryHook(bigquery_conn_id="convz_dev_service_account")
    result = hook.get_schema(
        project_id=PROJECT_ID, 
        dataset_id=DATASET_ID, 
        table_id  =table_name
    )
    return json.dumps(result)

with DAG(
    dag_id="a_test",
    # schedule_interval="05 00 * * *",
    schedule_interval=None,
    start_date=dt.datetime(2022, 4, 5),
    catchup=False,
    render_template_as_native_obj=True,
) as dag:

    # load2local = GCSToLocalFilesystemOperator(
    #     task_id="load2local",
    #     bucket=BUCKET_NAME,
    #     object_name='ERP/daily/tbshippinglabelinformation/2022_04_07_1649370599999_0.jsonl', 
    #     filename=MAIN_PATH + '/tbshippinglabelinformation/2022_04_07_1649370599999_0.jsonl', 
    #     gcp_conn_id='convz_dev_service_account',
    # )

    # load_sample = BashOperator(
    #     task_id  = f"load_sample",
    #     cwd      = MAIN_PATH + '/tbshippinglabelinformation/',
    #     bash_command = "head -1 2022_04_07_1649370599999_0.jsonl > 2022_04_07_1649370599999_0_sample.jsonl" \
    #                     # + ' && rm -f 2022_04_07_1649370599999_0.jsonl'
    #                     + ' && bq load --autodetect --source_format=NEWLINE_DELIMITED_JSON' \
    #                     + ' central-cto-ofm-data-hub-prod:erp_ofm_daily_stg.tbshippinglabelinformation_daily_stg' \
    #                     + ' 2022_04_07_1649370599999_0_sample.jsonl'
    # )

    # get_schema = PythonOperator(
    #     task_id="get_schema",
    #     provide_context=True,
    #     python_callable=_get_schema,
    #     op_kwargs = {
    #         'table_name' : "tbshippinglabelinformation_daily_stg"
    #     }
    # )

    # update_schema = PythonOperator(
    #     task_id="update_schema",
    #     provide_context=True,
    #     python_callable=_update_schema,
    #     op_kwargs = {
    #         'schema_data' : '{{ ti.xcom_pull(task_ids="get_schema") }}'
    #     }
    # )

    schema_to_gcs = ContentToGoogleCloudStorageOperator(
        task_id = f'schema_to_gcs',
        content = '{{ ti.xcom_pull(task_ids="update_schema") }}',
        dst     = f'{SOURCE_NAME}/schemas/tbshippinglabelinformation_stg.json',
        bucket  = BUCKET_NAME,
        gcp_conn_id = "convz_dev_service_account"
    )

    drop_stg_tables = BigQueryDeleteTableOperator(
        task_id  = f"drop_stg_tables",
        location = LOCATION,
        gcp_conn_id = 'convz_dev_service_account',
        ignore_if_missing = True,
        deletion_dataset_table = f"{PROJECT_ID}.{DATASET_ID}.tbshippinglabelinformation_daily_stg"
    )

    recreate_stg = BigQueryCreateEmptyTableOperator(
        task_id = f"recreate_stg",
        google_cloud_storage_conn_id = "convz_dev_service_account",
        bigquery_conn_id = "convz_dev_service_account",
        project_id = PROJECT_ID,
        dataset_id = DATASET_ID,
        table_id = "tbshippinglabelinformation_daily_stg",
        gcs_schema_object = '{{ ti.xcom_pull(task_ids="schema_to_gcs") }}'
    )

    ## change to BigQueryInsertJobOperator
    reload2stg = GCSToBigQueryOperator(
        task_id = f"reload2stg",
        google_cloud_storage_conn_id = "convz_dev_service_account",
        bigquery_conn_id = "convz_dev_service_account",
        bucket = BUCKET_NAME,
        source_objects = ['ERP/daily/tbshippinglabelinformation/2022_04_07_1649370599999_0.jsonl'],
        source_format  = 'NEWLINE_DELIMITED_JSON',
        destination_project_dataset_table = f"{PROJECT_ID}.{DATASET_ID}.tbshippinglabelinformation_daily_stg",
        schema_object = '{{ ti.xcom_pull(task_ids="schema_to_gcs") }}'
    )

    schema_to_gcs >> drop_stg_tables >> recreate_stg >> reload2stg
    # get_schema >> update_schema >> schema_to_gcs >> drop_stg_tables >> recreate_stg
    # load2local #>> get_schema >> update_schema
