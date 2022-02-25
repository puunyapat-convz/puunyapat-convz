#!/bin/bash

       YEAR=${1}
 TABLE_NAME=${2}
SCHEMA_FILE=${3}

for MONTH in ${YEAR}{01..12}
#for MONTH in ${YEAR}{04..04}
do
	echo
	echo "loading ${MONTH} of table ${TABLE_NAME}..."

	bq load \
	  --source_format=CSV \
	  --field_delimiter='|' \
	  --skip_leading_rows=1 \
	  --schema=${SCHEMA_FILE} \
	  central-cto-ofm-data-hub-dev:b2s_pos.${TABLE_NAME} \
	  "gs://sftp-b2s-pos-prod/B2S/POS/${TABLE_NAME}/${YEAR}/${MONTH}/*.TXT"
done

echo
