WITH
  base AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY INUMBR ORDER BY EODDAT DESC) AS rnk
  FROM
    `central-cto-ofm-data-hub-dev.b2s_jda_prod_new.BCH_JDA_DataPlatform_JDASKU`
  WHERE
    DATE(_PARTITIONTIME) <= "CURRENT_DATE"
  )
SELECT
  * EXCEPT(rnk)
FROM
  base
WHERE
  rnk = 1