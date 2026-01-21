

DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;


DELETE FROM `{{params.gcp_project_id}}`.{{params.ip_forecast_t2_schema}}.idf_online_sale_hist
WHERE week_start_date >= {{params.start_date}} AND week_start_date <= {{params.end_date}};


INSERT INTO `{{params.gcp_project_id}}`.{{params.ip_forecast_t2_schema}}.idf_online_sale_hist
(
channel_brand,
selling_channel,
epm_choice_num,
week_num,
week_start_date,
last_updated_utc,
dma_cd,
order_line_quantity,
order_line_amount_usd,
dropship_order_line_quantity,
dropship_order_line_amount_usd,
bopus_order_line_quantity,
bopus_order_line_amount_usd,
merchandise_not_available_order_line_quantity,
merchandise_not_available_order_line_amount_usd,
dw_sys_load_tmstp
)
WITH wk AS (SELECT DISTINCT week_idnt AS week_num,
 CAST(day_date AS DATE) AS day_date,
 CAST(week_start_day_date AS DATE) AS week_start_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dcd
WHERE CAST(week_start_day_date AS DATE) BETWEEN {{params.start_date}} AND {{params.end_date}})
,sku AS (SELECT DISTINCT rms_sku_num,
 epm_choice_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim
WHERE LOWER(channel_country) = LOWER('US'))
SELECT CASE
  WHEN LOWER(oldf.source_channel_code) = LOWER('FULL_LINE')
  THEN 'NORDSTROM'
  WHEN LOWER(oldf.source_channel_code) = LOWER('RACK')
  THEN 'NORDSTROM_RACK'
  ELSE NULL
  END AS channel_brand,
 'ONLINE' AS selling_channel,
 sku.epm_choice_num,
 wk.week_num,
 wk.week_start_date,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS DATE) AS last_updated_utc,
 CAST(TRUNC(dma.us_dma_code) AS INTEGER) AS dma_cd,
 SUM(oldf.order_line_quantity) AS order_line_quantity,
 CAST(SUM(oldf.order_line_amount_usd) AS FLOAT64) AS order_line_amount_usd,
 SUM(CASE
   WHEN LOWER(oldf.last_released_node_type_code) = LOWER('DS')
   THEN oldf.order_line_quantity
   ELSE 0
   END) AS dropship_order_line_quantity,
 CAST(SUM(CASE
    WHEN LOWER(oldf.last_released_node_type_code) = LOWER('DS')
    THEN oldf.order_line_amount_usd
    ELSE 0
    END) AS FLOAT64) AS dropship_order_line_amount_usd,
 SUM(CASE
   WHEN LOWER(oldf.promise_type_code) LIKE LOWER('%PICKUP%')
   THEN oldf.order_line_quantity
   ELSE 0
   END) AS bopus_order_line_quantity,
 CAST(SUM(CASE
    WHEN LOWER(oldf.promise_type_code) LIKE LOWER('%PICKUP%')
    THEN oldf.order_line_amount_usd
    ELSE 0
    END) AS FLOAT64) AS bopus_order_line_amount_usd,
 SUM(CASE
   WHEN LOWER(oldf.cancel_reason_code) = LOWER('MERCHANDISE_NOT_AVAILABLE')
   THEN oldf.order_line_quantity
   ELSE 0
   END) AS merchandise_not_available_order_line_quantity,
 CAST(SUM(CASE
    WHEN LOWER(oldf.cancel_reason_code) = LOWER('MERCHANDISE_NOT_AVAILABLE')
    THEN oldf.order_line_amount_usd
    ELSE 0
    END) AS FLOAT64) AS merchandise_not_available_order_line_amount_usd,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM wk
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.order_line_detail_fact AS oldf ON wk.day_date = oldf.order_date_pacific
 INNER JOIN sku ON LOWER(oldf.rms_sku_num) = LOWER(sku.rms_sku_num)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.org_us_zip_dma AS dma ON LOWER(TRIM(SUBSTR(COALESCE(oldf.destination_zip_code, oldf.bill_zip_code
      ), 1, 10))) = LOWER(dma.us_zip_code)
WHERE (LOWER(oldf.cancel_reason_code) = LOWER('MERCHANDISE_NOT_AVAILABLE') OR oldf.cancel_reason_code IS NULL OR LOWER(oldf
     .cancel_reason_code) = LOWER(''))
 AND LOWER(oldf.source_channel_country_code) = LOWER('US')
 AND oldf.order_date_pacific <> DATE '4444-04-04'
GROUP BY channel_brand,
 selling_channel,
 sku.epm_choice_num,
 wk.week_num,
 wk.week_start_date,
 last_updated_utc,
 dma_cd,
 dw_sys_load_tmstp
HAVING sku.epm_choice_num IS NOT NULL;

