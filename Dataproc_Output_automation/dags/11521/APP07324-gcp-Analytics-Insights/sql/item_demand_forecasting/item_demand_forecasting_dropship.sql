BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP08073;
DAG_ID=item_demand_forecasting_dataprep_11521_ACE_ENG;
---     Task_Name=item_demand_forecasting_dropship;'*/
BEGIN
SET _ERROR_CODE  =  0;


DELETE FROM `{{params.gcp_project_id}}`.{{params.ip_forecast_t2_schema}}.idf_dropship
WHERE week_start_date >= {{params.start_date}} AND week_start_date <= {{params.end_date}};


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;

SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.ip_forecast_t2_schema}}.idf_dropship
(
channel_brand,
epm_choice_num,
week_num,
week_start_date,
last_updated_utc,
dropship_boh,
dropship_sku_ct,
dw_sys_load_tmstp
)
WITH wk AS (SELECT DISTINCT week_idnt AS week_num,
 CAST(week_start_day_date AS DATE) AS week_start_date,
 DATE_SUB(CAST(week_start_day_date AS DATE), INTERVAL 1 DAY) AS prev_week_end_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dcd
WHERE CAST(week_start_day_date AS DATE) BETWEEN {{params.start_date}} AND {{params.end_date}})
,sku AS (SELECT DISTINCT rms_sku_num,
 epm_choice_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim
WHERE LOWER(channel_country) = LOWER('US'))
SELECT inv.channel_brand,
 sku.epm_choice_num,
 wk.week_num,
 wk.week_start_date,
 cast(CURRENT_DATETIME('GMT') as date) AS last_updated_utc,
 CAST(CAST(SUM(inv.stock_on_hand_qty) AS FLOAT64) AS BIGNUMERIC) AS dropship_boh,
 CAST(CAST(COUNT(DISTINCT inv.rms_sku_id) AS FLOAT64) AS BIGNUMERIC) AS dropship_sku_ct,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM wk
 INNER JOIN (SELECT CASE
    WHEN LOWER(location_type) = LOWER('DS_OP')
    THEN 'NORDSTROM_RACK'
    ELSE 'NORDSTROM'
    END AS channel_brand,
   snapshot_date,
   rms_sku_id,
   SUM(stock_on_hand_qty) AS stock_on_hand_qty
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.inventory_stock_quantity_by_day_fact
  WHERE LOWER(location_type) IN (LOWER('DS_OP'), LOWER('DS'))
  GROUP BY channel_brand,
   snapshot_date,
   rms_sku_id) AS inv ON wk.prev_week_end_date = inv.snapshot_date
 INNER JOIN sku ON LOWER(inv.rms_sku_id) = LOWER(sku.rms_sku_num)
WHERE sku.epm_choice_num IS NOT NULL
GROUP BY inv.channel_brand,
 sku.epm_choice_num,
 wk.week_num,
 wk.week_start_date,
 last_updated_utc,
 dw_sys_load_tmstp;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


--COLLECT STATISTICS  COLUMN (week_start_date), COLUMN (epm_choice_num), COLUMN (channel_brand), column (week_start_date, epm_choice_num, channel_brand) on {{params.ip_forecast_t2_schema}}.idf_dropship;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
