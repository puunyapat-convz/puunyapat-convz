WITH
  tmp AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY pid ORDER BY updateon DESC ) AS rnk
  FROM
    central-cto-ofm-data-hub-dev.officemate_source.daily_ofm_tbproductmaster
  WHERE
    report_date <= "CURRENT_DATE" )
SELECT
  * EXCEPT(rnk)
FROM
  tmp
WHERE
  rnk = 1