from google.oauth2 import service_account
from google.cloud  import bigquery
from google.cloud  import storage
import json, os ,re

# set path to script location
os.chdir(os.path.dirname(os.path.abspath(__file__)))
script_home = os.getcwd()

# GOOGLE SERVICE ACCOUNT
GOOGLE_CRED = "../credentials/central-cto-ofm-data-hub-dev-17796d07812b.json"

# GOOGLE BIGQUERY INFO, still in test output table
PROJECT_ID  = "central-cto-ofm-data-hub-dev"
DATASET_ID  = "officemate_new_schema" 
BUCKET_NAME = "ofm-data"

credentials    = service_account.Credentials.from_service_account_file(GOOGLE_CRED, scopes=["https://www.googleapis.com/auth/cloud-platform"])

def create_folder_list(bucket, path):
    # Get all bucket list
    storage_client = storage.Client(credentials=credentials, project=PROJECT_ID,)
    bucket_list    = storage_client.get_bucket(bucket)
    folder_list    = bucket_list.list_blobs(prefix= path, delimiter="/", max_results=1)

    # Force blobs to load.
    next(folder_list, ...) 

    # Create subfolders as list
    all_subfolders = []

    for subfolder in folder_list.prefixes:
        all_subfolders.append(str(subfolder).split('/')[2])
    
    return all_subfolders

def open_schema(filepath):

    with open(script_home+"/"+filepath) as json_file:
        schema_dict = json.load(json_file)
        table = bigquery.Table(table_id, schema=schema_dict)
        table.time_partitioning = bigquery.Table.TimePartitioning(field="report_date")
        table = client.create_table(table)

if __name__ == "__main__":

    # Construct a BigQuery client object.
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID,)
    
    print()
    print("Create tables name from GCS ...")
    # all_subfolders = create_folder_list(BUCKET_NAME, 'officemate/daily/')

    # This is the output from create_folder_list function
    all_subfolders = [
                        'tbinvoicecancelreason', 'arsetlmt', 'ivtxdetl', 'tbpromotion_premium', 'tbpromotion_coupon_code',
                        'ofm_tbeo_head', 'tbproductstatic_history', 'tbpromotion_campaign', 'cmspaytype', 'tbpromotion_account', 
                        'tbcontact_information', 'ofm_pohead', 'tbproductskucode', 'ofm_tbso_detail', 'tbtitle_master', 'ofm_tbteam_master', 
                        'tbcontact_master', 'tbproductcontrolstatic_history', 'tbproductdimension', 'tbvendoronchannel', 'tbcatalogmaster', 
                        'glactmst', 'ivtxhead', 'tbarprovision', 'tbcustprices', 'tbarchqmst', 'tbpromotion_channel', 'tbholiday', 
                        'tbproductwarehouse_omthistory', 'cmpcategory', 'cmprovince', 'arbatdtl', 'tbinvoice_address', 'tbaccount_class_master', 
                        'tbpromotion_quantity_discount', 'tbcompanydepartments', 'tbinvoice_government', 'arbildtl', 'tbtmsservicemaster', 'tbsoremark', 
                        'potxdetl', 'arbilhdr', 'tbaccount_credit', 'tbproductunit_history', 'ofm_soivhead', 'tbcreditcardmaster', 'tbaccount_additional', 
                        'tbaccount_credit_trans', 'tbseriesmaster', 'tbfixprice_doc_detail', 'tbpromotion_absorb', 'tbtsic_master', 'ofm_podetl', 'tbdvmapzone', 
                        'tbdeptmasterjda', 'ofm_tbproductwarehouse', 'tbdeptmaster', 'tbaccount_lock_telesale', 'cmpsubcategory', 'ofm_tbproductmaster', 
                        'ofm_tbsopricebydate', 'tbstore_branch', 'tbproductcatalog', 'ofm_tbproductstatic', 'gldptmst', 'tbpromotionpricemaster', 
                        'tbwarehousemaster', 'tbsale_channel', 'tbofinaccmaster', 'tbcmspaymenttype', 'ofm_sodnhead', 'tbpromotion_owner_campaign', 
                        'tbaccount_segment_master', 'ofm_tdsystem', 'tbsumprodmst', 'tbproductmaster_history', 'tbcmprospectactivity', 'glactmstmapofin', 
                        'arvattrn', 'ofm_tbeo_detail', 'tbbusiness_type_master', 'tbstore_master', 'tbcontact_mapping', 'tbobs_nonstore_monthly', 
                        'tbpromotion_installment_payment', 'ofm_tbproductprintingmaster', 'tbaccount_group_master', 'tbcmcity', 'tbcustpayment', 
                        'tbarsetlmtdtl', 'tbcmfeedbackgroup', 'tbabbreviationglaccount', 'ofm_tballocate_stock_bychannel', 'ofm_soivdetl', 'ofm_sodndetl', 
                        'ofm_tbuser_master', 'glvchtyp', 'ofm_tbproductcontrolstatic', 'tbaccount_fixprice', 'tbaccount_master', 'ofm_tbsoreceiptstore', 
                        'cmeemp', 'tbshipping_address', 'tbsale_method', 'artrndtl', 'potxhead', 'tbfixprice_doc_head', 'ofm_tbso_head', 'ofm_tbproductunit', 
                        'cmcreasonmiss', 'tbpromotion_product', 'tbfixprice_doc_accountgroup', 'tbproductchannel', 'tbproductbrandmaster', 
                        'tbproductwarehouse_colhistory', 'cmssuppcontact', 'tbpromotion_condition', 'cmssupplier', 'artrnmst', 'tbcmprospectgroup'
                     ]

    for subdir, dirs, files in os.walk("schemas"):
        for file in files:
            # print os.path.join(subdir, file)
            filepath = subdir + os.sep + file

            if filepath.endswith(".json"):
                # print(filepath)
                table_name = re.sub("^OFM_", "", filepath.split("/")[-2])                
                pattern    = re.compile(f"(?i){table_name}")

                if any((match := pattern.search(x)) for x in all_subfolders):
                    table_name = "daily_" + match.group(0)
                    table_id  = PROJECT_ID+"."+DATASET_ID+"."+table_name
                    #print(table_id)

                    # delete before create
                    #client.delete_table(table_id, not_found_ok=True)
                    print(f"  - Deleted >> {table_id}.")
                    #open_schema(filepath)
                    print(f"  + Created >> {table_id}.") 

                else:
                    print(f" !! table: {table_name} not found on GCS folders.")                
