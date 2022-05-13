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
    CAST(updateon AS date) <= "CURRENT_DATE" --if run date = T then the date here will be equal to T-1
    ),
  latest_product_info AS (
  SELECT
    * EXCEPT(rnk)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY pid ORDER BY TRANS_DATE DESC) AS rnk
    FROM
      product_base )
  WHERE
    rnk = 1 ),
  transaction_table AS (
  SELECT
    RDNo,
    Seqno,
    pid,
    CAST(createon AS date) AS createon
  FROM
    `central-cto-ofm-data-hub-prod.erp_ofm_daily.tbrddetail_daily_source`
  WHERE
    CAST(createon AS date) = "CURRENT_DATE" --if run date = T then the date here will be equal to T-1
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