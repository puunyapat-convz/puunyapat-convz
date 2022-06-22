from dateutil import parser

import json, re
import datetime as dt

# now_string=datetime.datetime.now().strftime("%Y-%m-%dT%H:00:00") #local time, add Z for UTC 
# print(now_string)
# get timestamp from {{ ts }}

# now_string = '2022-04-25T00:00:00Z'
# epoch   = parser.parse(now_string).timestamp()
# print(int(epoch))

# timestr = str(int(epoch))
# prefix  = int(timestr[:len(timestr)-5])

# print(f'1st: {prefix}') # first pattern
# print(f'2nd: {prefix+1}') # second pattern

## run parallel in Airflow

# test = "ERP_tbshippinglabelinformation_1651123838.ctrl".split("_")[-1].split(".")[0]
# epochtime = dt.datetime.utcfromtimestamp(int(test)).strftime('%Y-%m-%d')

# print(epochtime)

# def test(**kwargs):
#     if kwargs.get("pretty"):
#         print('true')

# test(pretty = True)

jda_tables = [
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_APADDR'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_APPSUPR'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_APSUPP'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_APSUPX'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_APTERM'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_APVATR'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_FACTAG'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_FACTAGE'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_INVCAL'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_INVDPT'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_INVMFG'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_INVUMR'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_JDAACSTK'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_JDABAR'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_JDAPCALW'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_JDAPCDSQ'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_JDAPCDTL'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_JDAPCFGP'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_JDAPCHDR'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_JDAPHC'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_JDAPOALW'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_JDAPODSQ'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_JDAPODTL'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_JDAPOFGP'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_JDAPOHDR'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_JDAPRC'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_JDASAL'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_JDASKU'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_JDASTK'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_JDATRN'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_MGRBVD'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_MGROBS1'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_MSTFLD'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_MSTITA'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_MSTTYP'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_RPLPRF'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_RPLPRM'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_RPLSDT'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_RPLSHD'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_SETDTL'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_TAXTRT'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_TBLCNT'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_TBLCOL'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_TBLDIS'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_TBLDST'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_TBLFIN'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_TBLFLD'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_TBLPRV'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_TBLREG'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_TBLSIZ'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'BCH_JDA_DataPlatform_TBLSTR'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_APADDR'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_APPSUPR'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_APSUPP'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_APSUPX'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_APTERM'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_APVATR'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_FACTAG'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_FACTAGE'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_INVDPT'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_INVMFG'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_INVUMR'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_JDAACSTK'}, 
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_JDABAR'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_JDAPHC'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_JDAPRC'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_JDASKU'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_JDASTK'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_JDATRN'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_MGRBVD'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_MGROBS1'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_MSTTYP'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_RPLPRF'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_RPLPRM'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_RPLSDT'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_RPLSHD'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_SETDTL'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_TAXTRT'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_TBLCNT'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_TBLCOL'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_TBLDIS'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_TBLDST'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_TBLFIN'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_TBLFLD'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_TBLPRV'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_TBLREG'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_TBLSIZ'},
    {'projectId': 'central-cto-ofm-data-hub-dev', 'datasetId': 'ofm_jda_prod_new', 'tableId': 'test_BCH_JDA_DataPlatform_TBLSTR'}
 ]

json_data  = json.loads(json.dumps(jda_tables))
table_list = list(map(lambda datum: datum['tableId'], json_data))
final_list = [ name for name in table_list if not re.match(f"^test_", name) ]

print(final_list)

# iterable_tables_list = [ "BCH_JDA_DataPlatform_JDASAL", "BCH_JDA_DataPlatform_APADDR" ]

# final_list = []

# for ft_name in iterable_tables_list:
#     if ft_name in jda_tables: final_list.append(ft_name)

# print(final_list)
