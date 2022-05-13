WITH
  tmp2 AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY allocate_type, product_id, CAST (sale_channel_id AS string)
    ORDER BY
      update_date DESC ) AS rnk
  FROM
    {source_table}
  WHERE
    report_date <= "CURRENT_DATE" )
SELECT
  * EXCEPT(rnk)
FROM
  tmp2
WHERE
  rnk = 1