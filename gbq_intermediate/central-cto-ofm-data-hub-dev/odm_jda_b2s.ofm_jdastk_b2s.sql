WITH
  tmp AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY XSTORE, XSKU ORDER BY EODDAT DESC) AS rnk
  FROM
    `central-cto-ofm-data-hub-dev.ofm_jda_prod_new.BCH_JDA_DataPlatform_JDASTK`
  WHERE
    DATE(_PARTITIONTIME) <= "CURRENT_DATE"
  )
SELECT
  * EXCEPT (rnk)
FROM
  tmp
WHERE
  rnk =1