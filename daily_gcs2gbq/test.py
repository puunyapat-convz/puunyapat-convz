BUCKET_NAME  = "ofm-data"
SOURCE_NAME  = [ "ERP", "MDS", "officemate" ] 
SOURCE_TYPE  = { "daily" : "| grep '^tb'",
                 "intraday": "| grep '^erp_'" }

for NAME in SOURCE_NAME:
    for TYPE in SOURCE_TYPE.keys():
        print(f"gsutil ls gs://{BUCKET_NAME}/{NAME}/{TYPE}" \
                + f" | cut -d'/' -f6 {SOURCE_TYPE.get(TYPE)} > {NAME}_{TYPE}_folders; cat {NAME}_{TYPE}_folders;" \
                + f" echo {NAME}_{TYPE}_folders")
