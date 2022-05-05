import datetime as dt
from dateutil import parser
import json

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

def test(**kwargs):
    if kwargs.get("pretty"):
        print('true')

test( pretty = True)

json_data = json.dumps({'_airbyte_ab_id': '0a3eb0d8-3222-4040-b0e6-b414430e22af',
 '_airbyte_data': {'_airbyte_arbildtl_hashid': '5039c46c754b7d977f300bd50f774274',
                   '_airbyte_emitted_at': '2022-05-04T21:05:39.950000Z',
                   'billdtlprvstatus': 'P',
                   'billdtlstatus': 'P',
                   'billno': 'BI-220501495',
                   'createby': '9003867',
                   'custrefno': 'TILS-P22050001',
                   'invamt': 599.99,
                   'invdate': '2022-05-03T00:00:00.000000Z',
                   'lastupdateby': '9003867',
                   'referenceno': 'SS2205000022',
                   'referenceno1': '',
                   'trantype': 'IV',
                   'updateon': '2022-05-04T13:13:49.417000Z'},
 '_airbyte_emitted_at': 1651699187154}, separators=(',', ':'))
print(json_data)