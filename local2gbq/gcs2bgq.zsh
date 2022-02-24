#!/bin/zsh

       YEAR=${1}
 TABLE_NAME=${2}
SCHEMA_FILE=${3}

for MONTH in ${YEAR}{01..12}
do
	echo
	echo "loading ${MONTH} of table ${TABLE_NAME}..."

	bq load \
	  --source_format=CSV \
	  --field_delimiter='|' \
	  --skip_leading_rows=1 \
	  --schema=${SCHEMA_FILE} \
	  central-cto-ofm-data-hub-dev:b2s_pos_prod.${TABLE_NAME}\
	  "gs://sftp-ofm-pos-prod/ODP/POS/${TABLE_NAME}/${YEAR}/${MONTH}/*.TXT"
done

echo
