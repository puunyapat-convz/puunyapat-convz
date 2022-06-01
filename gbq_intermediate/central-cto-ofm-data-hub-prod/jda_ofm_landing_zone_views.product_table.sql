WITH
  base AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY INUMBR ORDER BY EODDAT DESC) AS rnk
  FROM
    `central-cto-ofm-data-hub-prod.jda_ofm_daily_source.BCH_JDA_DataPlatform_JDASKU`
  WHERE
    DATE_SUB(DATE(_PARTITIONTIME), INTERVAL 1 day) <= "CURRENT_DATE"
  )
SELECT
  * EXCEPT(rnk)
FROM
  base
WHERE
  rnk = 1