SELECT
  * EXCEPT(rnk)
FROM (
  SELECT
    *,
    DATE_SUB(DATE(_PARTITIONTIME), INTERVAL 1 day) AS EOD_Date,
    ROW_NUMBER() OVER (PARTITION BY INUMBR, ISTORE ORDER BY DATE(_PARTITIONTIME) DESC ) AS rnk
  FROM
    `central-cto-ofm-data-hub-dev.ofm_jda_prod.BCH_JDA_DataPlatform_RPLPRF`
  WHERE
    DATE_SUB(DATE(_PARTITIONTIME), INTERVAL 1 day) <= "CURRENT_DATE"
  ) AS a
WHERE
  rnk = 1