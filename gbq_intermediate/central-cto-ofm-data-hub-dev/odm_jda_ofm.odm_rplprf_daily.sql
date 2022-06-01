SELECT
  * EXCEPT(rnk)
FROM (
  SELECT
    *,
    DATE(_PARTITIONTIME) AS EOD_Date,
    ROW_NUMBER() OVER (PARTITION BY INUMBR, ISTORE ORDER BY DATE(_PARTITIONTIME) DESC ) AS rnk
  FROM
    `central-cto-ofm-data-hub-dev.ofm_jda_prod_new.BCH_JDA_DataPlatform_RPLPRF`
  WHERE
    DATE(_PARTITIONTIME) <= "CURRENT_DATE"
  ) AS a
WHERE
  rnk = 1