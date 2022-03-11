#!/bin/zsh

OUTPUT=$(mktemp)

echo "[" > $OUTPUT

while read line
do
	
	echo "  {"
	echo "     \"name\" : \"$(cut -d, -f1 <<< $line)\","
	echo "     \"type\" : \"$(cut -d, -f2 <<< $line)\","
	echo "     \"mode\" : \"NULLABLE\""
	echo "  },"

done < $1 >> $OUTPUT

echo "]" >> $OUTPUT

LAST=$(wc -l < $OUTPUT)
sed "$((--LAST))s/,//" $OUTPUT > ${1}.json
