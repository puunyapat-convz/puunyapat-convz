WITH
  DeliveryControl AS (
  SELECT
    t1.*
  FROM (
    SELECT
      ROW_NUMBER() OVER (PARTITION BY company, vehicleid, positionid, DeliveryDate ORDER BY UpdateOn DESC) rownum,
      *
    FROM
      `central-cto-ofm-data-hub-prod.tms_ofm_landing_zone_views.daily_tms_tbdeliverycontrol_view`
      --WHERE date(DeliveryDate) >= CURRENT-DATE()-10
      --where VehicleID='6153' and  date(DeliveryDate) >= '2022-04-11'
      ) t1
  WHERE
    t1.rownum=1
  )
SELECT
  DISTINCT
  --A.COMP_ID
  A.company AS COMPANYNAME,
  --CASE WHEN A.COMP_ID = 4 THEN 'OMT' WHEN A.COMP_ID = 2 THEN 'COL' END AS COMPANYNAME,  --missing comp_id
  A.TrackingNo AS BillDelivery,
  A.DocumentNo,
  A.DeliveryDate AS DeliveryDate,
  A.RefDocumentNo,
  A.RefDocumentNo2,
  A.RefDocumentNo3,
  A.CustomerID,
  A.CustomerName,
  A.JobRemark AS Remark,
  A.TotalAmount,
  YY.reptsale_id,
  A.Status,
  A.ClusterID,
  A.SubClusterID,
  A.VehicleID,
  IFNULL(W.LicenseNo,
    V.LicenseNo ) LicenseNo,
  IFNULL(W.VehicleName,
    V.VehicleName ) VehicleName,
  IFNULL(W.VehicleType,
    V.VehicleType ) VehicleType,
  C.GroupID,
  M2.GroupName,
  C.serviceName,
  ReDeliveryFlag,
  A.ISCustVIP,
  A.IsShipWithINV,
  CASE
    WHEN reptsale_id NOT IN ('BU3', 'BU4') THEN 'No'
  ELSE
  'Yes'
END
  AS IsCrossSale,
  IFNULL(YY.hamper_flag,
    'No') AS hamper_flag,
  A.VehiclePlanID,
  A.VehicleID AS Truck,
  B.ProviderName,
  B.ProviderShortName,
  A.ServiceType,
  C.DeliveryType,
  YY.order_type,
  LEFT(A.DocumentNo,2) AS Doc_Type,
  CASE
    WHEN IFNULL(AM.account_segment, '') = '' THEN 'Silver'
  ELSE
  AM.account_segment
END
  AS account_segment,
  CASE
    WHEN YY.sale_channel_id = 2 THEN 'Store'
  ELSE
  'Callcenter'
END
  ASSaleChannel,
  YY.sale_channel_name,
  YY.sale_method_name,
  IFNULL(E.ReasonFrom,'') AS ReasonFrom,
  IFNULL(E.ReasonName,'') AS ReasonName,
  --F.EmployeeID,
  CASE
    WHEN IFNULL(INF.CRCID, '') = '' THEN INF.EmployeeID
  ELSE
  INF.CRCID
END
  AS NavigatorCRC,
  F.EmployeeName AS NavigatorName,
  CASE
    WHEN IFNULL(INH.CRCID, '') = '' THEN INH.EmployeeID
  ELSE
  INH.CRCID
END
  AS StaffCRC,
  IFNULL(H.EmployeeName,'') AS StaffName,
  F.DMEmployeeID,
  M1.CRCID AS DMCRCID,
  F.DMEmployeeName,
  CASE
    WHEN A.Status IN ('Completed', 'CompletedPartial') THEN 'Yes'
  ELSE
  'No'
END
  AS IsOntime,
  CASE
    WHEN G.Sub_Cluster = 'BDC' THEN 'BDC'
  ELSE
  ''
END
  AS BDC, --Added this Column Now
  ZZ.WHID,
  CASE
    WHEN r1 = 1 THEN 'Yes'
  ELSE
  'No'
END
  AS ISLastAction,
  yy.telesale_id,
  yy.telesale_name,
  A.ParkCode AS ShipArea,
  A.ZoneID AS ShipSubArea,
  A.ParkCode,
  yy.ship_sub_area,
  yy.payment_code,
  yy.payment_type,
  yy.payment_name,
  D.ReasonID,
  D.ReasonName TMSReasonName,
  YY.franchise_order_type
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY DocumentNo ORDER BY DeliveryDate DESC ) AS r1
  FROM
    `central-cto-ofm-data-hub-prod.tms_ofm_landing_zone_views.daily_tms_tbdeliveryhead_view`
  WHERE
    --date(DeliveryDate) >=  CURRENT-DATE()-10 AND
    TrackingNo IS NOT NULL
    AND LEFT(DocumentNo,2) <>'SDO' ) A
LEFT JOIN
  `central-cto-ofm-data-hub-prod.erp_ofm_landing_zone_views.intraday_erp_tbssohead_view` AS ZZ
ON
  A.DocumentNo = ZZ.SSONo
  AND A.company = ZZ.company
LEFT JOIN
  `central-cto-ofm-data-hub-prod.officemate_ofm_landing_zone_views.ofm_tbso_head_daily_view` AS YY
ON
  ZZ.sono = YY.so_no
  AND ZZ.company = YY.company
LEFT JOIN
  `central-cto-ofm-data-hub-prod.tms_ofm_landing_zone_views.daily_tbprovidermaster_view` AS B
ON
  A.ProviderID = B.ProviderID
LEFT JOIN
  `central-cto-ofm-data-hub-prod.tms_ofm_landing_zone_views.daily_tbservicemaster_view` AS C
ON
  A.ServiceType = C.ServiceID
LEFT JOIN
  `central-cto-ofm-data-hub-prod.tms_ofm_landing_zone_views.daily_tms_tbdeliveryconfirm_view` AS D
ON
  A.TrackingNo = D.TrackingNo
  AND A.company = D.company
LEFT JOIN
  `central-cto-ofm-data-hub-prod.tms_ofm_landing_zone_views.daily_tbjobreasonmaster_view` AS E
ON
  D.ReasonID = E.ReasonID
LEFT JOIN
  `central-cto-ofm-data-hub-prod.officemate_ofm_landing_zone_views.ofm_tbuser_master_daily_view` AS IN1
ON
  YY.telesale_id = IN1.emp_id
LEFT JOIN
  DeliveryControl F
ON
  A.VehicleID = F.VehicleID
  AND A.DeliveryDate = F.DeliveryDate
  AND F.PositionID = '3'
  AND A.company = F.company
  --instead of comp_id used company
LEFT JOIN
  `central-cto-ofm-data-hub-prod.tms_ofm_landing_zone_views.daily_tbemployeemaster_view` AS INF
ON
  F.EmployeeID = INF.EmployeeID -- AND INF.PositionID = F.PositionID
LEFT JOIN
  DeliveryControl H
ON
  A.VehicleID = H.VehicleID
  AND A.DeliveryDate = H.DeliveryDate
  AND H.PositionID ='4'
  AND A.company = H.company
LEFT JOIN
  `central-cto-ofm-data-hub-prod.tms_ofm_landing_zone_views.daily_tbemployeemaster_view` AS INH
ON
  H.EmployeeID = INH.EmployeeID -- AND INH.PositionID = H.PositionID
LEFT JOIN
  `central-cto-ofm-data-hub-prod.tms_ofm_landing_zone_views.daily_tbemployeemaster_view` AS M1
ON
  F.DMEmployeeID = M1.EmployeeID
LEFT JOIN
  `central-cto-ofm-data-hub-prod.tms_ofm_landing_zone_views.daily_tbgroupservicemaster_view` AS M2
ON
  C.GROUPID = M2.GROUPID
LEFT JOIN (
  SELECT
    VehicleID,
    sub_cluster,
    ClusterID,
    SubClusterID
  FROM (
    SELECT
      ROW_NUMBER () OVER(PARTITION BY VehicleID ORDER BY Effective_Date DESC) AS ROW,
      VehicleID,
      sub_cluster,
      ClusterID,
      SubClusterID
    FROM
      `central-cto-ofm-data-hub-prod.mds_ofm_daily.lg_vehicle_master_daily_source` ) x
  WHERE
    ROW = 1 )G
ON
  A.VehicleID = G.VehicleID
  --LEFT JOIN DATASET.dbo.LG_VEHICLE_MASTER_LAST AS G WITH(NOLOCK) ON A.VehicleID = G.VehicleID COLLATE Thai_CI_AS  --missing table
LEFT JOIN (
  SELECT
    LicenseNo,
    VehicleID,
    VehicleName,
    VehicleType
  FROM
    `central-cto-ofm-data-hub-prod.tms_ofm_landing_zone_views.daily_tbvehiclemaster_view` ) V
ON
  A.VehicleID = V.VehicleID
LEFT JOIN (
  SELECT
    DeliveryDate,
    LicenseNo,
    VehicleID,
    FleetName VehicleName,
    FleetContract VehicleType
  FROM
    `central-cto-ofm-data-hub-prod.tms_ofm_landing_zone_views.daily_tbfleettransaction_view` ) W
ON
  A.VehicleID = W.VehicleID
  AND DATE(A.DeliveryDate) = DATE(W.DeliveryDate)
LEFT JOIN
  `central-cto-ofm-data-hub-prod.officemate_ofm_landing_zone_views.tbaccount_master_daily_view` AM
ON
  A.CustomerID = AM.account_id