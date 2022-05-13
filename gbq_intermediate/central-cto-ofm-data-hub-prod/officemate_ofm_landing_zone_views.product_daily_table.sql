WITH
  tmp AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY pid ORDER BY updateon DESC ) AS rnk
  FROM
    `central-cto-ofm-data-hub-prod.officemate_ofm_daily.ofm_tbproductmaster_daily_source`
  WHERE
    report_date <= "CURRENT_DATE" )
SELECT
  * EXCEPT(rnk)
FROM
  tmp
WHERE
  rnk = 1