BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;

/*SET QUERY_BAND = 'App_ID=APP08073;
DAG_ID=item_demand_forecasting_dataprep_11521_ACE_ENG;
---     Task_Name=item_demand_forecasting_store_inventory;'*/

BEGIN
SET _ERROR_CODE  =  0;

DELETE FROM {{params.gcp_project_id}}.{{params.ip_forecast_t2_schema}}.idf_store_inv
WHERE week_start_date >= {{params.start_date}} AND week_start_date <= {{params.end_date}};

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;

INSERT INTO {{params.gcp_project_id}}.{{params.ip_forecast_t2_schema}}.idf_store_inv
(
channel_brand,
selling_channel,
epm_choice_num,
week_num,
week_start_date,
store_num,
last_updated_utc,
regular_price_amt,
boh,
boh_sku_ct,
dw_sys_load_tmstp
)
WITH wk AS (SELECT DISTINCT week_idnt AS week_num,
 CAST(week_start_day_date AS DATE) AS week_start_date,
 DATE_SUB(CAST(week_start_day_date AS DATE), INTERVAL 1 DAY) AS prev_week_end_date
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dcd
WHERE CAST(week_start_day_date AS DATE) BETWEEN {{params.start_date}} AND ({{params.end_date}}))

,store AS (SELECT store_num,
  CASE
  WHEN channel_num IN (110)
  THEN 'NORDSTROM'
  WHEN channel_num IN (210)
  THEN 'NORDSTROM_RACK'
  ELSE NULL
  END AS channel_brand,
 'STORE' AS selling_channel
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.store_dim
WHERE channel_num IN (110, 210))

,sku AS (SELECT DISTINCT psd.rms_sku_num,
 psd.epm_choice_num,
 price_fcst.regular_price_amt
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.product_sku_dim AS psd
 LEFT JOIN (SELECT rms_sku_num,
   MAX(regular_price_amt) AS regular_price_amt
  FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim
  WHERE LOWER(channel_country) = LOWER('US')
  GROUP BY rms_sku_num) AS price_fcst ON LOWER(psd.rms_sku_num) = LOWER(price_fcst.rms_sku_num)
WHERE LOWER(psd.channel_country) = LOWER('US'))

SELECT store.channel_brand,
 store.selling_channel,
 sku.epm_choice_num,
 wk.week_num,
 wk.week_start_date,
 store.store_num,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS DATE)
 AS last_updated_utc,
 MAX(sku.regular_price_amt) AS regular_price_amt,
 SUM(inv.stock_on_hand_qty) AS boh,
 COUNT(DISTINCT inv.rms_sku_id) AS boh_sku_ct,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM wk
 INNER JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.inventory_stock_quantity_by_day_fact AS inv ON wk.prev_week_end_date = inv.snapshot_date
 INNER JOIN store ON CAST(inv.location_id AS FLOAT64) = store.store_num
 INNER JOIN sku ON LOWER(inv.rms_sku_id) = LOWER(sku.rms_sku_num)
WHERE inv.stock_on_hand_qty > 0
 AND sku.epm_choice_num IS NOT NULL
GROUP BY store.channel_brand,
 store.selling_channel,
 sku.epm_choice_num,
 wk.week_num,
 wk.week_start_date,
 store.store_num,
 last_updated_utc,
 dw_sys_load_tmstp;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS  COLUMN (week_start_date), COLUMN (epm_choice_num), COLUMN (channel_brand), COLUMN (selling_channel), COLUMN (week_start_date, epm_choice_num, channel_brand, selling_channel) on t2dl_das_inv_position_forecast.IDF_STORE_INV;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
