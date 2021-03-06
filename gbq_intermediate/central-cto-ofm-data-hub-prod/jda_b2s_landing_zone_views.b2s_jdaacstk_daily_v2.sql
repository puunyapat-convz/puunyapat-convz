WITH
  tmp AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY INSTOR, INSKU ORDER BY INTRND DESC, DATE(_PARTITIONTIME) DESC) rnk,
    DATE(_PARTITIONTIME) AS partition_date
  FROM
    `central-cto-ofm-data-hub-prod.jda_b2s_daily_source.BCH_JDA_DataPlatform_JDAACSTK`
  WHERE
    INTRND <= CURRENT_DATE )
SELECT
  * EXCEPT (rnk,
    partition_date)
FROM
  tmp
WHERE
  rnk =1