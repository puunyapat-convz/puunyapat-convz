with product_base as (
    SELECT  
        PID,
        cast(PIdOD as string) as PIDOD,
        cast(DEPT_ID as string) as DEPT_ID, 
        cast(SUBDEPT_ID as string) as SUBDEPT_ID, 
        cast(CLASS as string) as CLASS, 
        cast(SUB_CLASS as string) as SUB_CLASS, 
        CATEGORY_ID,  
        SUB_CATEGORY_ID,  
        BRAND_ID, 
        SUPPLIER_ID,    
        TRANS_DATE
    FROM `central-cto-ofm-data-hub-dev.ofm_one_times.DATA_PRODUCT_HISTORY` 
    where TRANS_DATE <=  "CURRENT_DATE" --if run date = T then the date here will be equal to T-1
UNION ALL
    SELECT
        pid, 
        pidod as PIDOD,
        dept as DEPT_ID,  
        sub_dpt as SUBDEPT_ID,
        class as CLASS, 
        sub_cls as SUB_CLASS,
        catid as CATEGORY_ID,
        subcatid as SUB_CATEGORY_ID,
        brandid as BRAND_ID,  
        lastsupplierid as SUPPLIER_ID,
        cast(updateon as date) as TRANS_DATE
    from `central-cto-ofm-data-hub-prod.officemate_ofm_daily.ofm_tbproductmaster_daily_source`
    where cast(updateon as date) <=  "CURRENT_DATE" --if run date = T then the date here will be equal to T-1)
)
 
, latest_product_info as (
Select * except(rnk)
from(
    select
        *, 
        row_number() over (partition by pid order by TRANS_DATE desc) as rnk
    from product_base
  )
where rnk = 1
)
 
, transaction_table as (
    SELECT
        PID, 
        StoreID, 
        WHID,
        DATE(_PARTITIONTIME ) as file_date
        --cast(createon as date) as createon
    FROM `central-cto-ofm-data-hub-dev.ofm_landing_zone_views.daily_ofm_tbproductwarehouse_table`
    where DATE(_PARTITIONTIME ) = "CURRENT_DATE" --if run date = T then the date here will be equal to T-1
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
            last_day(parse_date("%Y%m%d",concat(REGEXP_REPLACE(ymdata,"[^0-9]+",""),"01"))) as file_date,
        row_number() OVER (
            PARTITION BY pid,storeid,last_day(parse_date("%Y%m%d",concat(REGEXP_REPLACE(ymdata,"[^0-9]+",""),"01"))) 
            ORDER BY updateon desc
        ) as rnk
        FROM `central-cto-ofm-data-hub-dev.ofm_landing_zone_views.daily_tbproductwarehouse_omthistory_view` 
        where last_day(parse_date("%Y%m%d",concat(REGEXP_REPLACE(ymdata,"[^0-9]+",""),"01"))) = "CURRENT_DATE"
    ) 
    WHERE rnk = 1
)
 
select 
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
from transaction_table as t 
left join latest_product_info as p on p.pid = t.pid