# from airflow import DAG, macros
# from airflow.operators.python  import PythonOperator
# from airflow.providers.google.cloud.hooks.bigquery import *
import datetime as dt
import json

PROJECT_ID   = "central-cto-ofm-data-hub-prod"
DATASET_ID   = "erp_ofm_daily_stg"
LOCATION     = "asia-southeast1" 

payload      = '{ "fields": [ { "name": "_airbyte_data", "type": "RECORD", "mode": "NULLABLE", "fields": [ { "name": "_airbyte_emitted_at", "type": "TIMESTAMP", "mode": "NULLABLE", "description": "bq-datetime" }, { "name": "shippingreasoncode", "type": "STRING", "mode": "NULLABLE" }, { "name": "customernameshort", "type": "STRING", "mode": "NULLABLE" }, { "name": "customeraddress2", "type": "STRING", "mode": "NULLABLE" }, { "name": "customeraddress1", "type": "STRING", "mode": "NULLABLE" }, { "name": "shippinglabelno", "type": "INTEGER", "mode": "NULLABLE" }, { "name": "shippingboxtype", "type": "STRING", "mode": "NULLABLE" }, { "name": "shippinggroup", "type": "STRING", "mode": "NULLABLE" }, { "name": "shippingboxno", "type": "INTEGER", "mode": "NULLABLE" }, { "name": "textfilename", "type": "STRING", "mode": "NULLABLE" }, { "name": "customername", "type": "STRING", "mode": "NULLABLE" }, { "name": "canceledqty", "type": "FLOAT", "mode": "NULLABLE" }, { "name": "personname", "type": "STRING", "mode": "NULLABLE" }, { "name": "shippingplandate", "type": "INTEGER", "mode": "NULLABLE" }, { "name": "phonenumber", "type": "INTEGER", "mode": "NULLABLE" }, { "name": "expirydate", "type": "STRING", "mode": "NULLABLE" }, { "name": "pickedqty", "type": "FLOAT", "mode": "NULLABLE" }, { "name": "faxnumber", "type": "INTEGER", "mode": "NULLABLE" }, { "name": "inventoryassignmentstartno", "type": "STRING", "mode": "NULLABLE" }, { "name": "status", "type": "STRING", "mode": "NULLABLE" }, { "name": "updateby", "type": "STRING", "mode": "NULLABLE" }, { "name": "updateon", "type": "TIMESTAMP", "mode": "NULLABLE", "description": "bq-datetime" }, { "name": "pickedtime", "type": "INTEGER", "mode": "NULLABLE" }, { "name": "qcstatus", "type": "INTEGER", "mode": "NULLABLE" }, { "name": "sectionname", "type": "STRING", "mode": "NULLABLE" }, { "name": "msgerror", "type": "STRING", "mode": "NULLABLE" }, { "name": "itemcode", "type": "INTEGER", "mode": "NULLABLE" }, { "name": "createon", "type": "TIMESTAMP", "mode": "NULLABLE", "description": "bq-datetime" }, { "name": "ID", "type": "FLOAT", "mode": "NULLABLE" }, { "name": "customercode", "type": "INTEGER", "mode": "NULLABLE" }, { "name": "createby", "type": "STRING", "mode": "NULLABLE" }, { "name": "zipcode", "type": "INTEGER", "mode": "NULLABLE" }, { "name": "customeraddress3", "type": "STRING", "mode": "NULLABLE" }, { "name": "specifiedlot", "type": "STRING", "mode": "NULLABLE" }, { "name": "detailno", "type": "INTEGER", "mode": "NULLABLE" }, { "name": "saleorderno", "type": "STRING", "mode": "NULLABLE" }, { "name": "ownercode", "type": "STRING", "mode": "NULLABLE" }, { "name": "lot", "type": "STRING", "mode": "NULLABLE" }, { "name": "systemid", "type": "STRING", "mode": "NULLABLE" }, { "name": "_airbyte_tbshippinglabelinformation_hashid", "type": "STRING", "mode": "NULLABLE" }, { "name": "remarks", "type": "STRING", "mode": "NULLABLE" }, { "name": "memo1", "type": "STRING", "mode": "NULLABLE" }, { "name": "pickeddate", "type": "TIMESTAMP", "mode": "NULLABLE", "description": "bq-datetime" }, { "name": "memo2", "type": "STRING", "mode": "NULLABLE" }, { "name": "itemname", "type": "STRING", "mode": "NULLABLE" }, { "name": "subcode", "type": "INTEGER", "mode": "NULLABLE" } ] }, { "name": "_airbyte_emitted_at", "type": "INTEGER", "mode": "NULLABLE" }, { "name": "_airbyte_ab_id", "type": "STRING", "mode": "NULLABLE" } ] }'

json_payload = json.loads(payload)
payload_idx  = 0

for i in range(len(json_payload)):
    if json_payload['fields'][i]['name'] == "_airbyte_data":
        payload_idx = i
        break

stg_schema = json_payload['fields'][payload_idx]['fields']
fin_schema  = [{'name': 'test_add', 'type': 'STRING', 'mode': 'REQUIRED'},{'name': 'id', 'type': 'INT64', 'mode': 'REQUIRED'}, {'name': 'shippinglabelno', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'systemid', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'shippingreasoncode', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'saleorderno', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'detailno', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'ownercode', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'itemcode', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'subcode', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'qcstatus', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'lot', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'expirydate', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'pickedqty', 'type': 'INT64', 'mode': 'REQUIRED'}, {'name': 'canceledqty', 'type': 'INT64', 'mode': 'REQUIRED'}, {'name': 'shippingplandate', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'shippinggroup', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'inventoryassignmentstartno', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'customercode', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'customername', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'customernameshort', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'customeraddress1', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'customeraddress2', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'customeraddress3', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'zipcode', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'phonenumber', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'faxnumber', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'sectionname', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'personname', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'memo1', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'memo2', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'specifiedlot', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'remarks', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'itemname', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'pickeddate', 'type': 'DATETIME', 'mode': 'REQUIRED'}, {'name': 'pickedtime', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'shippingboxno', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'shippingboxtype', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'textfilename', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'status', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'updateon', 'type': 'DATETIME', 'mode': 'REQUIRED'}, {'name': 'updateby', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'createon', 'type': 'DATETIME', 'mode': 'REQUIRED'}, {'name': 'createby', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'msgerror', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'report_date', 'type': 'DATE', 'mode': 'REQUIRED'}, {'name': 'run_date', 'type': 'DATE', 'mode': 'REQUIRED'}]

stg_fields = [ fields['name'].lower() for fields in stg_schema ]
fin_fields = [ fields['name'].lower() for fields in fin_schema if fields['name'].lower() not in ['report_date','run_date'] ]

print("before")
print(f"stg fields = {len(stg_schema)}")
print(f"fin fields = {len(fin_schema)}")
print()

for field_name in fin_fields:
    if field_name in stg_fields:
        stg_schema[stg_fields.index(field_name)]['type'] = fin_schema[fin_fields.index(field_name)]['type']
    else:
        print(f"added {field_name} in staging")
        stg_fields.append(field_name)
        stg_schema.append(fin_schema[fin_fields.index(field_name)])
        stg_schema[stg_fields.index(field_name)]['mode'] = 'NULLABLE'

print()
print("after")
print(f"stg fields = {len(stg_schema)}")
print(f"fin fields = {len(fin_schema)}")

json_payload['fields'][payload_idx]['fields'] = stg_schema

print()
print(json_payload)

# def _get_schema():
#     hook = BigQueryHook(bigquery_conn_id="convz_dev_service_account")
#     result = hook.get_schema(project_id=PROJECT_ID, dataset_id=DATASET_ID, table_id="tbshippinglabelinformation_daily_stg")
#     json_data = json.dumps(result, indent=4)
#     return json_data

# with DAG(
#     dag_id="a_test",
#     # schedule_interval="05 00 * * *",
#     schedule_interval=None,
#     start_date=dt.datetime(2022, 4, 5),
#     catchup=True
# ) as dag:

#     get_schema = PythonOperator(
#         task_id="get_schema",
#         provide_context=True,
#         python_callable=_get_schema,
#     )

#     get_schema
