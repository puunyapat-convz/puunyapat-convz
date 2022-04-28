import datetime
from dateutil import parser

# now_string=datetime.datetime.now().strftime("%Y-%m-%dT%H:00:00") #local time, add Z for UTC 
# print(now_string)
# get timestamp from {{ ts }}

now_string = '2022-04-25T00:00:00Z'
epoch   = parser.parse(now_string).timestamp()
print(int(epoch))

timestr = str(int(epoch))
prefix  = int(timestr[:len(timestr)-5])

print(f'1st: {prefix}') # first pattern
print(f'2nd: {prefix+1}') # second pattern

## run parallel in Airflow