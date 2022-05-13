WITH
  tmp AS (
  SELECT
    DISTINCT *
  FROM
    `central-cto-ofm-data-hub-prod.officemate_ofm_daily.ofm_tbproductwarehouse_daily_source`
  WHERE
    run_date >= '2022-03-10'
    AND report_date <= 'CURRENT_DATE' ),
  tmp2 AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY PID, StoreID, WHID ORDER BY updateon DESC ) AS rnk
  FROM
    tmp )
SELECT
  amt,
  pid,
  CAST(onpw AS int) AS onpw,
  CAST(onrd AS int) AS onrd,
  CAST(onrt AS int) AS onrt,
  unit,
  whid,
  CAST(onpic AS int) AS onpic,
  CAST(onplt AS int) AS onplt,
  CAST(onpru AS int) AS onpru,
  pname,
  CAST(onsale AS int) AS onsale,
  CAST(onhand AS int) AS onhand,
  whname,
  avgcost,
  storeid,
  createby,
  PARSE_DATETIME('%Y-%m-%d %H:%M:%S',
    LEFT(CAST(TIMESTAMP(createon) AS string),19)) AS createon,
  updateby,
  PARSE_DATETIME('%Y-%m-%d %H:%M:%S',
    LEFT(CAST(TIMESTAMP(updateon) AS string),19)) AS updateon,
  ordertype,
  storename,
  PARSE_DATETIME('%Y-%m-%d %H:%M:%S',
    LEFT(CAST(TIMESTAMP(lastrcvdate) AS string),19)) AS lastrcvdate,
  createbyname,
  updatebyname,
  report_date,
  run_date
FROM
  tmp2
WHERE
  rnk = 1