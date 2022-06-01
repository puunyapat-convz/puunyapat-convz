WITH
  product_base AS (
  SELECT
    PID,
    CAST(PIdOD AS string) AS PIDOD,
    CAST(DEPT_ID AS string) AS DEPT_ID,
    CAST(SUBDEPT_ID AS string) AS SUBDEPT_ID,
    CAST(CLASS AS string) AS CLASS,
    CAST(SUB_CLASS AS string) AS SUB_CLASS,
    CATEGORY_ID,
    SUB_CATEGORY_ID,
    BRAND_ID,
    SUPPLIER_ID,
    TRANS_DATE
  FROM
    `central-cto-ofm-data-hub-dev.ofm_one_times.DATA_PRODUCT_HISTORY`
  WHERE
    TRANS_DATE <= "CURRENT_DATE" --if run date = T then the date here will be equal to T-1
UNION ALL
SELECT
  pid,
  pidod AS PIDOD,
  dept AS DEPT_ID,
  sub_dpt AS SUBDEPT_ID,
  class AS CLASS,
  sub_cls AS SUB_CLASS,
  catid AS CATEGORY_ID,
  subcatid AS SUB_CATEGORY_ID,
  brandid AS BRAND_ID,
  lastsupplierid AS SUPPLIER_ID,
  CAST(updateon AS date) AS TRANS_DATE
FROM
  `central-cto-ofm-data-hub-prod.officemate_ofm_daily.ofm_tbproductmaster_daily_source`
WHERE
  CAST(updateon AS date) <= "CURRENT_DATE" --if run date = T then the date here will be equal to T-1)
)
 
,  latest_product_info AS (
  SELECT
    * EXCEPT(rnk)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY pid ORDER BY TRANS_DATE DESC) AS rnk
    FROM
      product_base )
  WHERE
    rnk = 1 )
 
, transaction_table as (
  SELECT
    PID,
    StoreID,
    WHID,
    DATE(_PARTITIONTIME ) AS file_date
  FROM
    `central-cto-ofm-data-hub-dev.ofm_landing_zone_views.daily_ofm_tbproductwarehouse_table`
  WHERE
    DATE(_PARTITIONTIME ) = "CURRENT_DATE" --if run date = T then the date here will be equal to T-1
  UNION ALL
  SELECT
    PID, 
    StoreID, 
    WHID,
    file_date
  FROM(
    SELECT
      PID,
      StoreID,
      WHID,
      LAST_DAY(parse_DATE("%Y%m%d",
          CONCAT(REGEXP_REPLACE(ymdata,"[^0-9]+",""),"01"))) AS file_date,
      ROW_NUMBER() OVER (PARTITION BY pid, storeid, LAST_DAY(parse_DATE("%Y%m%d", CONCAT(REGEXP_REPLACE(ymdata,"[^0-9]+",""),"01")))
      ORDER BY
        updateon DESC) AS rnk
    FROM
      `central-cto-ofm-data-hub-dev.ofm_landing_zone_views.daily_tbproductwarehouse_omthistory_view`
    WHERE
      LAST_DAY(parse_DATE("%Y%m%d",
          CONCAT(REGEXP_REPLACE(ymdata,"[^0-9]+",""),"01"))) = "CURRENT_DATE"
  ) 
  WHERE rnk = 1
)
 
SELECT
  t.*,
  p.PIDOD,
  p.DEPT_ID,
  p.SUBDEPT_ID,
  p.CLASS,
  p.SUB_CLASS,
  p.CATEGORY_ID,
  p.SUB_CATEGORY_ID,
  p.BRAND_ID,
  p.SUPPLIER_ID
FROM
  transaction_table AS t
LEFT JOIN
  latest_product_info AS p
ON
  p.pid = t.pid