WITH
  tmp AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY INSTOR, INSKU ORDER BY INTRND DESC, DATE(_PARTITIONTIME) DESC) rnk,
    DATE(_PARTITIONTIME) AS partition_date
  FROM
    `central-cto-ofm-data-hub-dev.b2s_jda_prod_new.BCH_JDA_DataPlatform_JDAACSTK`
  WHERE
    PARSE_DATE("%y%m%d", CAST(INTRND AS STRING)) <= "CURRENT_DATE"
  )
SELECT
  * EXCEPT (rnk,
    partition_date)
FROM
  tmp
WHERE
  rnk =1