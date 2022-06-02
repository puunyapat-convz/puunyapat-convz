SELECT
  s.sku_id,
  s.code,
  s.type,
  s.create_info_timestamp,
  s.update_info_timestamp,
  c.channel_id,
  c.channel_display_name_th,
  c.channel_display_name_en,
  --c.channel_sku_code,
  i.size,
  i.url,
  i.cdn_url,
  --i.sequence,
  p.price_type,
  p.price_exclude_vat,
  p.price_include_vat,
  MAX( s.report_date) AS report_date,
  MAX(s.run_date) AS run_date
FROM (
  SELECT
    *
  FROM
    `central-cto-ofm-data-hub-prod.pim_landing_zone_views.daily_productinformation_skucodes`
  WHERE
    sku_id IN ('Y040483','Y040481','Y040458','Y040486','Y005627','Y040455')) s
LEFT JOIN
  `central-cto-ofm-data-hub-prod.pim_landing_zone_views.daily_productinformation_channels` c
ON
  s.sku_id = c.sku_id
LEFT JOIN
  `central-cto-ofm-data-hub-prod.pim_landing_zone_views.daily_productinformation_images` i
ON
  s.sku_id = i.sku_id
LEFT JOIN
  `central-cto-ofm-data-hub-prod.pim_landing_zone_views.daily_productinformation_price` p
ON
  s.sku_id = p.sku_id
WHERE
  s.sku_id IS NOT NULL
  --and i.url is not null
GROUP BY
  1,2,3,4,5,6,7,8,9,10,11,12,13,14