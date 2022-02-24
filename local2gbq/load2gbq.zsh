#!/bin/zsh

[ -z ${1} ] || [ -z ${2} ] && help

 TBNAME=$(cut -d'/' -f1 <<< ${1})
 LOGDIR=logs
LOGFILE=${LOGDIR}/bq_load_${TBNAME}_$(date +'%Y%m%d_%H%M').log

[ -d ${LOGDIR} ] || mkdir ${LOGDIR}

function help
{
	echo "    Usage : $0 <directory name> <file suffix>"
	exit
}

function writeLog
{
	local LEVEL

	case ${1} in
		0)	LEVEL="INFO"
			;;
		1)	LEVEL="ERROR"
			;;
	esac
	
	echo "$(date +'%F %T')|${LEVEL}|${2}" >> ${LOGFILE}
}

for DIRNAME in ${1}
do
	OIFS=$IFS
	 IFS=$'\r\n'
	LIST=($(find ${DIRNAME}/*.${2} -type f))
	 IFS=$OIFS

	for FILE in "${LIST[@]}"
	do
		echo -n "  Loading ${FILE}..."
		bq load \
			--quiet=true \
			--source_format=CSV \
	 		--field_delimiter='|' \
			--skip_leading_rows=1 \
			--schema=/Users/oH/Downloads/POS_POS_DataPlatform_Txn_DiscountCoupon_schema_POS_TXN_DISCOUNTCOUPON.schema \
			central-cto-ofm-data-hub-dev:b2s_pos_prod.POS_DataPlatform_Txn_DiscountCoupon \
			${FILE}

		if [ $? -eq 0 ]
		then
			writeLog 0 "${FILE} success"
		else
			writeLog 1 "${FILE} failed"
		fi
	done
done
