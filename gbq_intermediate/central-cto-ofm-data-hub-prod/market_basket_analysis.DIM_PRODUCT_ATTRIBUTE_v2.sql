SELECT
  s.sku_id,
  s.code,
  s.type,
  --s.create_info_timestamp,
  --s.update_info_timestamp,
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
  p.price_include_vat
  --max(s.report_date) as report_date,
  --max(s.run_date) as run_date
FROM (
  SELECT
    *
  FROM
    `central-cto-ofm-data-hub-prod.pim_landing_zone_views.daily_productinformation_skucodes`
  ) s
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
  s.sku_id IS NOT NULL --and i.url is not null
GROUP BY
  s.sku_id,
  s.code,
  s.type,
  --s.create_info_timestamp,
  --s.update_info_timestamp,
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
  p.price_include_vat