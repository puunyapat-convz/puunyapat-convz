from google.oauth2 import service_account
from google.cloud import bigquery
import json
import os

# set path to script location
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# GOOGLE SERVICE ACCOUNT
GOOGLE_CRED = "credentials/central-cto-ofm-data-hub-dev-17796d07812b.json"

# GOOGLE BIGQUERY INFO, still in test output table
GBQ_PROJECT_ID = "central-cto-ofm-data-hub-dev"
GBQ_DATASET_ID = "bi_oracle" 
GBQ_TABLE      = {  "Inventory" : [ "test_bi_oracle_ofm_inv", "schemas/test_bi_oracle_ofm_inv.json" ], 
                    "OBS"       : [ "test_bi_oracle_ofm_obs", "schemas/test_bi_oracle_ofm_obs.json" ] }

# Construct a BigQuery client object.
credentials = service_account.Credentials.from_service_account_file(GOOGLE_CRED, scopes=["https://www.googleapis.com/auth/cloud-platform"])
client = bigquery.Client(credentials=credentials, project=GBQ_PROJECT_ID,)

def open_schema(key):

    with open(GBQ_TABLE.get(key)[1]) as json_file:
        schema_dict = json.load(json_file)
        table = bigquery.Table(table_id, schema=schema_dict)
        table = client.create_table(table)
    
if __name__ == "__main__":

    for key in GBQ_TABLE.keys():
        table_id = GBQ_PROJECT_ID+"."+GBQ_DATASET_ID+"."+GBQ_TABLE.get(key)[0]
        open_schema(key)
        print("Created table {}".format(table_id)) 

# table_type=GBQ_SCHEMA.get("OBS")
# print(GBQ_SCHEMA.get("OBS"))
# with open(table_type) as json_file:
#     schema_GBQ = json.load(json_file)
