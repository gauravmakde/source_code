


DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;


DELETE FROM `{{params.gcp_project_id}}`.{{params.ip_forecast_t2_schema}}.idf_online_sale
WHERE week_start_date >= {{params.start_date}} AND week_start_date <= {{params.end_date}};



INSERT INTO `{{params.gcp_project_id}}`.{{params.ip_forecast_t2_schema}}.idf_online_sale
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
fc_fill_order_line_quantity,
fc_fill_order_line_amount_usd,
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
SELECT dem.banner AS channel_brand,
 'ONLINE' AS selling_channel,
 sku.epm_choice_num,
 wk.week_num,
 wk.week_start_date,
 CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS DATE) AS last_updated_utc,
 CAST(TRUNC(dma.us_dma_code) AS INTEGER) AS dma_cd,
 SUM(dem.demand_units) AS order_line_quantity,
 CAST(CAST(SUM(dem.jwn_reported_demand_usd_amt) AS FLOAT64) AS BIGNUMERIC) AS order_line_amount_usd,
 SUM(CASE
   WHEN LOWER(dem.fulfilled_from_location_type) LIKE LOWER('%Vendor%')
   THEN dem.demand_units
   ELSE 0
   END) AS dropship_order_line_quantity,
 CAST(CAST(SUM(CASE
    WHEN LOWER(dem.fulfilled_from_location_type) LIKE LOWER('%Vendor%')
    THEN dem.jwn_reported_demand_usd_amt
    ELSE 0
    END) AS FLOAT64) AS BIGNUMERIC) AS dropship_order_line_amount_usd,
 SUM(CASE
   WHEN LOWER(dem.delivery_method) = LOWER('BOPUS')
   THEN dem.demand_units
   ELSE 0
   END) AS bopus_order_line_quantity,
 CAST(CAST(SUM(CASE
    WHEN LOWER(dem.delivery_method) = LOWER('BOPUS')
    THEN dem.jwn_reported_demand_usd_amt
    ELSE 0
    END) AS FLOAT64) AS BIGNUMERIC) AS bopus_order_line_amount_usd,
 SUM(CASE
   WHEN LOWER(dem.fulfilled_from_method) = LOWER('FC Filled')
   THEN dem.demand_units
   ELSE 0
   END) AS fc_fill_order_line_quantity,
 CAST(CAST(SUM(CASE
    WHEN LOWER(dem.fulfilled_from_method) = LOWER('FC Filled')
    THEN dem.jwn_reported_demand_usd_amt
    ELSE 0
    END) AS FLOAT64) AS BIGNUMERIC) AS fc_fill_order_line_amount_usd,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM wk
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.jwn_demand_metric_vw AS dem ON wk.day_date = dem.demand_date
 INNER JOIN sku ON LOWER(dem.rms_sku_num) = LOWER(sku.rms_sku_num)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.org_us_zip_dma AS dma ON LOWER(TRIM(SUBSTR(COALESCE(dem.ship_zip_code, dem.bill_zip_code), 1
     , 10))) = LOWER(dma.us_zip_code)
WHERE LOWER(dem.channel_country) = LOWER('US')
 AND LOWER(dem.channel) = LOWER('DIGITAL')
 AND LOWER(TRIM(dem.jwn_reported_demand_ind)) = LOWER('Y')
GROUP BY channel_brand,
 selling_channel,
 sku.epm_choice_num,
 wk.week_num,
 wk.week_start_date,
 last_updated_utc,
 dma_cd,
 dma.dw_sys_load_tmstp
HAVING sku.epm_choice_num IS NOT NULL;


