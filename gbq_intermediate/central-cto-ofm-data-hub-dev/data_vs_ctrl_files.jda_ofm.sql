SELECT
  IntefaceName,
  parse_DATE("%d%m%Y",SUBSTRING(BatchDate,0,8)) AS ctrl_date,
  CAST(TotalRec AS int64) AS ctrl_row_count,
  table_name AS table_name_frm_data,
  data_date,
  data_row_count,
  "ofm" AS brand
FROM
  `central-cto-ofm-data-hub-prod.jda_ofm_daily_ctrlfiles.BCH_JDA_DataPlatform_*`
LEFT JOIN (
  SELECT
    _TABLE_SUFFIX AS table_name,
    DATE(_PARTITIONTIME) AS data_date,
    COUNT(*) AS data_row_count
  FROM
    `central-cto-ofm-data-hub-prod.jda_ofm_daily_source.BCH_JDA_DataPlatform_*`
  WHERE
    DATE(_PARTITIONTIME) = "CURRENT_DATE"
  GROUP BY
    1,
    2)
ON
  (IntefaceName=table_name
    AND parse_DATE("%d%m%Y",
      SUBSTRING(BatchDate,0,8))=data_date)
WHERE
  DATE(_PARTITIONTIME) = "CURRENT_DATE"
ORDER BY
  1,
  2