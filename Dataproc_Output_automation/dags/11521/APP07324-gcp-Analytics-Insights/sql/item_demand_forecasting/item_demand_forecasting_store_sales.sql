BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP08073;
DAG_ID=item_demand_forecasting_dataprep_11521_ACE_ENG;
---     Task_Name=item_demand_forecasting_store_sales;'*/
BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM `{{params.gcp_project_id}}`.{{params.ip_forecast_t2_schema}}.idf_store_sale
WHERE week_start_date >= {{params.start_date}} AND week_start_date <= {{params.end_date}};
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.ip_forecast_t2_schema}}.idf_store_sale
(
channel_brand,
selling_channel,
epm_choice_num,
week_num,
week_start_date,
store_num,
last_updated_utc,
demand_quantity,
store_take_demand,
owned_demand,
employee_demand,
dw_sys_load_tmstp
)
WITH wk AS (SELECT DISTINCT week_idnt AS week_num,
 CAST(day_date AS DATE) AS day_date,
 CAST(week_start_day_date AS DATE) AS week_start_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dcd
WHERE CAST(week_start_day_date AS DATE) BETWEEN {{params.start_date}} AND {{params.end_date}})
,store AS (SELECT store_num,
  CASE
  WHEN channel_num IN (110)
  THEN 'NORDSTROM'
  WHEN channel_num IN (210)
  THEN 'NORDSTROM_RACK'
  ELSE NULL
  END AS channel_brand,
 'STORE' AS selling_channel
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim
WHERE channel_num IN (110, 210))
,sku AS (SELECT DISTINCT rms_sku_num,
 epm_choice_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim
WHERE LOWER(channel_country) = LOWER('US'))
SELECT store.channel_brand,
 store.selling_channel,
 sku.epm_choice_num,
 wk.week_num,
 wk.week_start_date,
 store.store_num,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS DATE) AS last_updated_utc,
 CAST(SUM(dem.demand_units) AS BIGNUMERIC) AS demand_quantity,
 CAST(SUM(CASE
    WHEN LOWER(dem.line_item_fulfillment_type) = LOWER('StoreTake')
    THEN dem.demand_units
    ELSE 0
    END) AS BIGNUMERIC) AS store_take_demand,
 CAST(SUM(CASE
    WHEN LOWER(dem.line_item_fulfillment_type) NOT LIKE LOWER('%DropShip')
    THEN dem.demand_units
    ELSE 0
    END) AS BIGNUMERIC) AS owned_demand,
 CAST(SUM(CASE
    WHEN LOWER(TRIM(dem.employee_discount_ind)) = LOWER('Y')
    THEN dem.demand_units
    ELSE 0
    END) AS BIGNUMERIC) AS employee_demand,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM wk
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.jwn_demand_metric_vw AS dem ON wk.day_date = dem.demand_date
 INNER JOIN sku ON LOWER(dem.rms_sku_num) = LOWER(sku.rms_sku_num)
 INNER JOIN store ON dem.intent_store_num = store.store_num
WHERE LOWER(dem.channel_country) = LOWER('US')
 AND LOWER(dem.channel) = LOWER('STORE')
 AND LOWER(TRIM(dem.jwn_reported_demand_ind)) = LOWER('Y')
 AND LOWER(TRIM(dem.line_item_activity_type_code)) = LOWER('S')
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
--COLLECT STATISTICS  COLUMN (week_start_date), COLUMN (epm_choice_num), COLUMN (channel_brand), COLUMN (selling_channel), COLUMN (week_start_date, epm_choice_num, channel_brand, selling_channel) on t2dl_das_inv_position_forecast.idf_store_sale;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;