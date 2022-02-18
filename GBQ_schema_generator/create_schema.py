import pandas as pd
import json, os, shutil

# set path to script location
os.chdir(os.path.dirname(os.path.abspath(__file__)))

BQ_DTYPE = [
    [ "STRING", "char", "nchar", "nvarchar", "varchar", "sysname", "text", "uniqueidentifier" ],
    [ "INTEGER", "int", "tinyint", "smallint", "bigint" ],
    [ "FLOAT", "numeric", "decimal", "money" ],
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


def mkDir(path,jump):
    if not os.path.exists(path):
        os.makedirs(path)
    if jump:
        os.chdir(path)

def convertType(data_type):

    for line in BQ_DTYPE:
        if data_type.strip() in line:
            gbq_data_type = line[0]
            break

    return gbq_data_type

# read Excel file
my_sheet = 'Field-MDS'
file_name = 'OFM-B2S_Source_Datalake_20211020-live-version.xlsx'
df = pd.read_excel(file_name, sheet_name = my_sheet, usecols = ["Source","Table_Name","Field_Name","Field_Type","IS_NULLABLE"])

mkDir("schemas",True)

# Create source as main folder
source_name = df.iat[0,0]

# Remove existing source folder first
print()
print(f"Removing existing folder: {source_name}")
shutil.rmtree(source_name)

mkDir(source_name,True)
print(f"Converting source: {source_name}")

exclude_name = []
all_excel_tb = [ "V_PROD_CLUSTER_2" ]
# all_excel_tb = df.iloc[:,1].unique()

for table_name in all_excel_tb:
    
    if not pd.isna(table_name) and table_name not in exclude_name:
        mkDir(table_name,True)

        print(f"  >> {table_name} ...")
        all_fields = [ {"mode":"NULLABLE", "name":"report_date", "type":"DATE"} ]
        all_jsons  = json.loads('{}')

        tables_fields = df.loc[df.iloc[:,1] == table_name]
        #print(tables_fields.iloc[:,[2,3]])

        for row in tables_fields.itertuples(index=True, name='Pandas'):
            gbq_field_mode = "REQUIRED" if str(row[5]) in [ "0", "0.0", "NO" ] else "NULLABLE"
            new_type = convertType(str(row[4]).lower())
            all_fields.append({"mode":gbq_field_mode, "name":str(row[3]), "type":new_type.upper()})
        
        #print(all_fields)
        #print()

        with open(f'{table_name}.json', 'w', encoding='utf-8') as f:
            json.dump(all_fields, f, ensure_ascii=False, indent=4)

        os.chdir("..")

os.chdir("../..")
#print(os.getcwd())

