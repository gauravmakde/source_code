
BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;

/*SET QUERY_BAND = 'App_ID=APP08073;
DAG_ID=item_demand_forecasting_dataprep_11521_ACE_ENG;
---     Task_Name=item_demand_forecasting_online_inventory;'*/
BEGIN
SET _ERROR_CODE  =  0;


DELETE FROM `{{params.gcp_project_id}}`.{{params.ip_forecast_t2_schema}}.idf_online_inv
WHERE week_start_date >= {{params.start_date}} AND week_start_date <= {{params.end_date}};

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.ip_forecast_t2_schema}}.idf_online_inv
(
channel_brand,
selling_channel,
epm_choice_num,
week_num,
week_start_date,
last_updated_utc,
sku_ct,
regular_price_amt,
boh,
boh_sku_ct,
boh_store,
boh_store_sku_ct,
boh_store_ct,
dw_sys_load_tmstp
)
WITH wk AS (SELECT DISTINCT week_idnt AS week_num,
 CAST(day_date AS DATE) AS day_date,
 CAST(week_start_day_date AS DATE) AS week_start_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dcd
WHERE CAST(week_start_day_date AS DATE) BETWEEN {{params.start_date}} AND {{params.end_date}})
,sku AS (SELECT DISTINCT psd.rms_sku_num,
 psd.epm_choice_num,
 price_fcst.regular_price_amt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim AS psd
 LEFT JOIN (SELECT rms_sku_num,
   MAX(regular_price_amt) AS regular_price_amt
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim
  WHERE LOWER(channel_country) = LOWER('US')
  GROUP BY rms_sku_num) AS price_fcst ON LOWER(psd.rms_sku_num) = LOWER(price_fcst.rms_sku_num)
WHERE LOWER(psd.channel_country) = LOWER('US'))
,store AS (SELECT store_num,
  CASE
  WHEN channel_num IN (110, 120)
  THEN 'NORDSTROM'
  WHEN channel_num IN (210, 250)
  THEN 'NORDSTROM_RACK'
  ELSE NULL
  END AS channel_brand,
  CASE
  WHEN channel_num IN (110, 210)
  THEN 'STORE'
  WHEN channel_num IN (120, 250)
  THEN 'ONLINE'
  ELSE NULL
  END AS selling_channel
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim
WHERE channel_num IN (110, 210, 120, 250))
,onlineinv AS (SELECT store.channel_brand,
 sku.epm_choice_num,
 wk1.week_num,
 wk1.week_start_date,
 SUM(inv.stock_on_hand_qty) AS boh,
 COUNT(DISTINCT inv.rms_sku_id) AS boh_sku_ct,
 SUM(CASE
   WHEN LOWER(store.selling_channel) = LOWER('STORE')
   THEN inv.stock_on_hand_qty
   ELSE 0
   END) AS boh_store,
 COUNT(DISTINCT CASE
   WHEN LOWER(store.selling_channel) = LOWER('STORE')
   THEN inv.rms_sku_id
   ELSE NULL
   END) AS boh_store_sku_ct,
 COUNT(DISTINCT CASE
   WHEN LOWER(store.selling_channel) = LOWER('STORE')
   THEN inv.location_id
   ELSE NULL
   END) AS boh_store_ct
FROM (SELECT week_num,
   DATE_SUB(MIN(day_date), INTERVAL 1 DAY) AS prev_week_end_date,
   week_start_date
  FROM wk
  GROUP BY week_num,
   week_start_date) AS wk1
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.inventory_stock_quantity_by_day_fact AS inv ON wk1.prev_week_end_date = inv.snapshot_date
 INNER JOIN sku ON LOWER(inv.rms_sku_id) = LOWER(sku.rms_sku_num)
 INNER JOIN store ON CAST(inv.location_id AS FLOAT64) = store.store_num
WHERE inv.stock_on_hand_qty > 0
 AND sku.epm_choice_num IS NOT NULL
GROUP BY store.channel_brand,
 sku.epm_choice_num,
 wk1.week_num,
 wk1.week_start_date)
,liveonsite AS (SELECT popid.channel_brand,
 sku.epm_choice_num,
 wk_start_end.week_num,
 wk_start_end.week_start_date,
 COUNT(DISTINCT popid.rms_sku_num) AS sku_ct,
 MAX(sku.regular_price_amt) AS regular_price_amt
FROM (SELECT week_num,
   MIN(day_date) AS week_start_date,
   MAX(day_date) AS week_end_date
  FROM wk
  WHERE day_date >= DATE '2021-06-13'
  GROUP BY week_num) AS wk_start_end
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_online_purchasable_item_dim AS popid ON wk_start_end.week_start_date <= CAST(popid.eff_end_tmstp AS DATE)
    AND wk_start_end.week_end_date >= CAST(popid.eff_begin_tmstp AS DATE)
 INNER JOIN sku ON LOWER(popid.rms_sku_num) = LOWER(sku.rms_sku_num)
WHERE LOWER(popid.is_online_purchasable) = LOWER('Y')
 AND LOWER(popid.channel_brand) IN (LOWER('NORDSTROM'), LOWER('NORDSTROM_RACK'))
 AND LOWER(popid.channel_country) = LOWER('US')
 AND sku.epm_choice_num IS NOT NULL
GROUP BY wk_start_end.week_num,
 wk_start_end.week_start_date,
 popid.channel_brand,
 sku.epm_choice_num)
,liveonsite_history AS (SELECT t2los.channel_brand,
 sku.epm_choice_num,
 wk1.week_num,
 wk1.week_start_date,
 COUNT(DISTINCT t2los.rms_sku_num) AS sku_ct,
 MAX(sku.regular_price_amt) AS regular_price_amt
FROM (SELECT DISTINCT week_num,
   week_start_date,
   day_date
  FROM wk
  WHERE day_date >= DATE '2018-11-25'
   AND day_date <= DATE '2021-06-12') AS wk1
 INNER JOIN `{{params.gcp_project_id}}`.t2dl_das_site_merch.live_on_site_historical AS t2los ON wk1.day_date = t2los.day_date
 INNER JOIN sku ON LOWER(t2los.rms_sku_num) = LOWER(sku.rms_sku_num)
WHERE LOWER(t2los.is_online_purchasable) = LOWER('Y')
 AND LOWER(t2los.channel_brand) IN (LOWER('NORDSTROM'), LOWER('NORDSTROM_RACK'))
 AND LOWER(t2los.channel_country) = LOWER('US')
 AND (wk1.day_date >= DATE '2018-11-25' AND wk1.day_date <= DATE '2021-06-12')
 AND sku.epm_choice_num IS NOT NULL
GROUP BY wk1.week_num,
 wk1.week_start_date,
 t2los.channel_brand,
 sku.epm_choice_num)

SELECT los.channel_brand,
 'ONLINE' AS selling_channel,
 los.epm_choice_num,
 los.week_num,
 los.week_start_date,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATE) AS last_updated_utc,
 los.sku_ct,
 CAST(CAST(los.regular_price_amt AS FLOAT64) AS NUMERIC) AS regular_price_amt,
 COALESCE(onlineinv.boh, 0) AS boh,
 COALESCE(onlineinv.boh_sku_ct, 0) AS boh_sku_ct,
 COALESCE(onlineinv.boh_store, 0) AS boh_store,
 COALESCE(onlineinv.boh_store_sku_ct, 0) AS boh_store_sku_ct,
 COALESCE(onlineinv.boh_store_ct, 0) AS boh_store_ct,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM (SELECT *
   FROM liveonsite
   UNION DISTINCT
   SELECT *
   FROM liveonsite_history) AS los
 LEFT JOIN onlineinv ON LOWER(los.channel_brand) = LOWER(onlineinv.channel_brand) AND los.epm_choice_num = onlineinv.epm_choice_num
     AND los.week_num = onlineinv.week_num;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

END;
