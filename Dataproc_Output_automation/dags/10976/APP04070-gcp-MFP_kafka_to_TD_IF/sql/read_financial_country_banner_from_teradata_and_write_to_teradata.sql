BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=mfpc_financial_country_banner_kafka_to_teradata_v4;
---Task_Name=job_financial_country_banner_third_exec_002;'*/
---FOR SESSION VOLATILE;
BEGIN
SET _ERROR_CODE  =  0;  
DELETE FROM {{params.gcp_project_id}}.{{params.database_name_fact}}.merch_financial_country_banner_plan_fct AS tgt
WHERE EXISTS (SELECT *
    FROM {{params.gcp_project_id}}.{{params.database_name_staging}}.merch_financial_country_banner_plan_ldg AS stg
    WHERE tgt.week_id = CAST(week_id AS FLOAT64) AND tgt.department_number = CAST(department_number AS FLOAT64) AND LOWER(tgt.channel_country) = LOWER(channel_country) AND LOWER(tgt.pricing_channel) = LOWER(pricing_channel) AND LOWER(last_updated_time_in_millis) <> LOWER('LAST_UPDATED_TIME_IN_MILLIS') AND tgt.last_updated_time_in_millis < CAST(CASE WHEN last_updated_time_in_millis = '' THEN '0' ELSE last_updated_time_in_millis END AS BIGINT));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM {{params.gcp_project_id}}.{{params.database_name_staging}}.merch_financial_country_banner_plan_ldg AS stg
WHERE EXISTS (SELECT *
    FROM {{params.gcp_project_id}}.{{params.database_name_fact}}.merch_financial_country_banner_plan_fct AS tgt
    WHERE CAST(stg.week_id AS FLOAT64) = week_id AND CAST(stg.department_number AS FLOAT64) = department_number AND LOWER(stg.channel_country) = LOWER(channel_country) AND LOWER(stg.pricing_channel) = LOWER(pricing_channel) AND LOWER(stg.last_updated_time_in_millis) <> LOWER('LAST_UPDATED_TIME_IN_MILLIS') AND CAST(CASE WHEN stg.last_updated_time_in_millis = '' THEN '0' ELSE stg.last_updated_time_in_millis END AS BIGINT) <= last_updated_time_in_millis);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO {{params.gcp_project_id}}.{{params.database_name_fact}}.merch_financial_country_banner_plan_fct (source_publish_time, source_publish_time_tz, approved_time, approved_time_tz, week_id,
 department_number, channel_country, pricing_channel, last_updated_time_in_millis, last_updated_time,last_updated_time_tz, financial_plan_version, alternate_inventory_model, active_beginning_inventory_dollar_currency_code,
 active_beginning_inventory_dollar_amount, active_beginning_inventory_units,
 active_beginning_period_inventory_target_units, inactive_beginning_period_inventory_dollar_currency_code,
 inactive_beginning_period_inventory_dollar_amount, inactive_beginning_period_inventory_units,
 discount_terms_cost_currency_code, discount_terms_cost_amount, active_ending_inventory_dollar_currency_code,
 active_ending_inventory_dollar_amount, active_ending_inventory_units, inactive_ending_inventory_dollar_currency_code,
 inactive_ending_inventory_dollar_amount, inactive_ending_inventory_units, active_inventory_in_dollar_currency_code,
 active_inventory_in_dollar_amount, active_inventory_in_units, active_inventory_out_dollar_currency_code,
 active_inventory_out_dollar_amount, active_inventory_out_units, inactive_inventory_in_dollar_currency_code,
 inactive_inventory_in_dollar_amount, inactive_inventory_in_units, inactive_inventory_out_dollar_currency_code,
 inactive_inventory_out_dollar_amount, inactive_inventory_out_units, other_inventory_in_dollar_currency_code,
 other_inventory_in_dollar_amount, other_inventory_in_units, other_inventory_out_dollar_currency_code,
 other_inventory_out_dollar_amount, other_inventory_out_units, racking_in_dollar_currency_code, racking_in_dollar_amount
 , racking_in_units, racking_out_dollar_currency_code, racking_out_dollar_amount, racking_out_units,
 merch_margin_retail_currency_code, merch_margin_retail_amount, active_mos_dollar_currency_code,
 active_mos_dollar_amount, active_mos_units, inactive_mos_dollar_currency_code, inactive_mos_dollar_amount,
 inactive_mos_units, active_receipts_dollar_currency_code, active_receipts_dollar_amount, active_receipts_units,
 inactive_receipts_dollar_currency_code, inactive_receipts_dollar_amount, inactive_receipts_units,
 reserve_receipts_dollar_currency_code, reserve_receipts_dollar_amount, reserve_receipts_units,
 reclass_in_dollar_currency_code, reclass_in_dollar_amount, reclass_in_units, reclass_out_dollar_currency_code,
 reclass_out_dollar_amount, reclass_out_units, active_rtv_dollar_currency_code, active_rtv_dollar_amount,
 active_rtv_units, inactive_rtv_dollar_currency_code, inactive_rtv_dollar_amount, inactive_rtv_units,
 vendor_funds_cost_currency_code, vendor_funds_cost_amount, inventory_movement_drop_ship_in_dollar_currency_code,
 inventory_movement_drop_ship_in_dollar_amount, inventory_movement_drop_ship_in_units,
 inventory_movement_drop_ship_out_dollar_currency_code, inventory_movement_drop_ship_out_dollar_amount,
 inventory_movement_drop_ship_out_units, other_inventory_adjustments_dollar_currency_code,
 other_inventory_adjustments_dollar_amount, other_inventory_adjustments_units, active_opentobuy_cost_currency_code,
 active_opentobuy_cost_amount, inactive_opentobuy_cost_currency_code, inactive_opentobuy_cost_amount,
 active_commitments_cost_currency_code, active_commitments_cost_amount, inactive_commitments_cost_currency_code,
 inactive_commitments_cost_amount, dw_sys_load_tmstp, dw_sys_load_tmstp_tz, dw_sys_updt_tmstp, dw_sys_updt_tmstp_tz)
(SELECT CAST(`{{params.gcp_project_id}}.NORD_UDF.ISO8601_TMSTP`(source_publish_time) AS TIMESTAMP) AS source_publish_time,
`{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(CAST(`{{params.gcp_project_id}}.NORD_UDF.ISO8601_TMSTP`(source_publish_time) AS TIMESTAMP) as string)) as source_publish_time_tz,
  CAST(approved_time AS TIMESTAMP) AS approved_time,
  `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(approved_time) as approved_time_tz,
  CAST(TRUNC(CAST(week_id AS FLOAT64)) AS INTEGER) AS week_id,
  CAST(TRUNC(CAST(department_number AS FLOAT64)) AS INTEGER) AS department_number,
  channel_country,
  pricing_channel,
  CAST(last_updated_time_in_millis AS BIGINT) AS last_updated_time_in_millis,
  CAST(`{{params.gcp_project_id}}.NORD_UDF.EPOCH_TMSTP`(CAST(CASE
      WHEN last_updated_time_in_millis = ''
      THEN '0'
      ELSE last_updated_time_in_millis
      END AS BIGINT)) AS TIMESTAMP) AS last_updated_time,
      `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(CAST(`{{params.gcp_project_id}}.NORD_UDF.EPOCH_TMSTP`(CAST(CASE
      WHEN last_updated_time_in_millis = ''
      THEN '0'
      ELSE last_updated_time_in_millis
      END AS BIGINT)) AS TIMESTAMP) AS STRING)) as last_updated_time_tz,
  financial_plan_version,
  alternate_inventory_model,
  active_beginning_inventory_dollar_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN active_beginning_inventory_dollar_units = ''
       THEN '0'
       ELSE active_beginning_inventory_dollar_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN active_beginning_inventory_dollar_nanos = ''
        THEN '0'
        ELSE active_beginning_inventory_dollar_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS active_beginning_inventory_dollar_amount,
  ROUND(CAST(active_beginning_inventory_units AS NUMERIC), 4) AS active_beginning_inventory_units,
  ROUND(CAST(active_beginning_period_inventory_target_units AS NUMERIC), 4) AS
  active_beginning_period_inventory_target_units,
  inactive_beginning_period_inventory_dollar_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN inactive_beginning_period_inventory_dollar_units = ''
       THEN '0'
       ELSE inactive_beginning_period_inventory_dollar_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN inactive_beginning_period_inventory_dollar_nanos = ''
        THEN '0'
        ELSE inactive_beginning_period_inventory_dollar_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS inactive_beginning_period_inventory_dollar_amount,
  ROUND(CAST(inactive_beginning_period_inventory_units AS NUMERIC), 4) AS inactive_beginning_period_inventory_units,
  discount_terms_cost_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN discount_terms_cost_units = ''
       THEN '0'
       ELSE discount_terms_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN discount_terms_cost_nanos = ''
        THEN '0'
        ELSE discount_terms_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS discount_terms_cost_amount,
  active_ending_inventory_dollar_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN active_ending_inventory_dollar_units = ''
       THEN '0'
       ELSE active_ending_inventory_dollar_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN active_ending_inventory_dollar_nanos = ''
        THEN '0'
        ELSE active_ending_inventory_dollar_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS active_ending_inventory_dollar_amount,
  ROUND(CAST(active_ending_inventory_units AS NUMERIC), 4) AS active_ending_inventory_units,
  inactive_ending_inventory_dollar_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN inactive_ending_inventory_dollar_units = ''
       THEN '0'
       ELSE inactive_ending_inventory_dollar_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN inactive_ending_inventory_dollar_nanos = ''
        THEN '0'
        ELSE inactive_ending_inventory_dollar_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS inactive_ending_inventory_dollar_amount,
  ROUND(CAST(inactive_ending_inventory_units AS NUMERIC), 4) AS inactive_ending_inventory_units,
  active_inventory_in_dollar_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN active_inventory_in_dollar_units = ''
       THEN '0'
       ELSE active_inventory_in_dollar_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN active_inventory_in_dollar_nanos = ''
        THEN '0'
        ELSE active_inventory_in_dollar_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS active_inventory_in_dollar_amount,
  ROUND(CAST(active_inventory_in_units AS NUMERIC), 4) AS active_inventory_in_units,
  active_inventory_out_dollar_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN active_inventory_out_dollar_units = ''
       THEN '0'
       ELSE active_inventory_out_dollar_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN active_inventory_out_dollar_nanos = ''
        THEN '0'
        ELSE active_inventory_out_dollar_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS active_inventory_out_dollar_amount,
  ROUND(CAST(active_inventory_out_units AS NUMERIC), 4) AS active_inventory_out_units,
  inactive_inventory_in_dollar_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN inactive_inventory_in_dollar_units = ''
       THEN '0'
       ELSE inactive_inventory_in_dollar_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN inactive_inventory_in_dollar_nanos = ''
        THEN '0'
        ELSE inactive_inventory_in_dollar_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS inactive_inventory_in_dollar_amount,
  ROUND(CAST(inactive_inventory_in_units AS NUMERIC), 4) AS inactive_inventory_in_units,
  inactive_inventory_out_dollar_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN inactive_inventory_out_dollar_units = ''
       THEN '0'
       ELSE inactive_inventory_out_dollar_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN inactive_inventory_out_dollar_nanos = ''
        THEN '0'
        ELSE inactive_inventory_out_dollar_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS inactive_inventory_out_dollar_amount,
  ROUND(CAST(inactive_inventory_out_units AS NUMERIC), 4) AS inactive_inventory_out_units,
  other_inventory_in_dollar_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN other_inventory_in_dollar_units = ''
       THEN '0'
       ELSE other_inventory_in_dollar_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN other_inventory_in_dollar_nanos = ''
        THEN '0'
        ELSE other_inventory_in_dollar_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS other_inventory_in_dollar_amount,
  ROUND(CAST(other_inventory_in_units AS NUMERIC), 4) AS other_inventory_in_units,
  other_inventory_out_dollar_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN other_inventory_out_dollar_units = ''
       THEN '0'
       ELSE other_inventory_out_dollar_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN other_inventory_out_dollar_nanos = ''
        THEN '0'
        ELSE other_inventory_out_dollar_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS other_inventory_out_dollar_amount,
  ROUND(CAST(other_inventory_out_units AS NUMERIC), 4) AS other_inventory_out_units,
  racking_in_dollar_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN racking_in_dollar_units = ''
       THEN '0'
       ELSE racking_in_dollar_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN racking_in_dollar_nanos = ''
        THEN '0'
        ELSE racking_in_dollar_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS racking_in_dollar_amount,
  ROUND(CAST(racking_in_units AS NUMERIC), 4) AS racking_in_units,
  racking_out_dollar_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN racking_out_dollar_units = ''
       THEN '0'
       ELSE racking_out_dollar_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN racking_out_dollar_nanos = ''
        THEN '0'
        ELSE racking_out_dollar_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS racking_out_dollar_amount,
  ROUND(CAST(racking_out_units AS NUMERIC), 4) AS racking_out_units,
  merch_margin_retail_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN merch_margin_retail_units = ''
       THEN '0'
       ELSE merch_margin_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN merch_margin_retail_nanos = ''
        THEN '0'
        ELSE merch_margin_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS merch_margin_retail_amount,
  active_mos_dollar_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN active_mos_dollar_units = ''
       THEN '0'
       ELSE active_mos_dollar_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN active_mos_dollar_nanos = ''
        THEN '0'
        ELSE active_mos_dollar_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS active_mos_dollar_amount,
  ROUND(CAST(active_mos_units AS NUMERIC), 4) AS active_mos_units,
  inactive_mos_dollar_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN inactive_mos_dollar_units = ''
       THEN '0'
       ELSE inactive_mos_dollar_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN inactive_mos_dollar_nanos = ''
        THEN '0'
        ELSE inactive_mos_dollar_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS inactive_mos_dollar_amount,
  ROUND(CAST(inactive_mos_units AS NUMERIC), 4) AS inactive_mos_units,
  active_receipts_dollar_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN active_receipts_dollar_units = ''
       THEN '0'
       ELSE active_receipts_dollar_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN active_receipts_dollar_nanos = ''
        THEN '0'
        ELSE active_receipts_dollar_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS active_receipts_dollar_amount,
  ROUND(CAST(active_receipts_units AS NUMERIC), 4) AS active_receipts_units,
  inactive_receipts_dollar_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN inactive_receipts_dollar_units = ''
       THEN '0'
       ELSE inactive_receipts_dollar_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN inactive_receipts_dollar_nanos = ''
        THEN '0'
        ELSE inactive_receipts_dollar_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS inactive_receipts_dollar_amount,
  ROUND(CAST(inactive_receipts_units AS NUMERIC), 4) AS inactive_receipts_units,
  reserve_receipts_dollar_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN reserve_receipts_dollar_units = ''
       THEN '0'
       ELSE reserve_receipts_dollar_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN reserve_receipts_dollar_nanos = ''
        THEN '0'
        ELSE reserve_receipts_dollar_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS reserve_receipts_dollar_amount,
  ROUND(CAST(reserve_receipts_units AS NUMERIC), 4) AS reserve_receipts_units,
  reclass_in_dollar_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN reclass_in_dollar_units = ''
       THEN '0'
       ELSE reclass_in_dollar_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN reclass_in_dollar_nanos = ''
        THEN '0'
        ELSE reclass_in_dollar_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS reclass_in_dollar_amount,
  ROUND(CAST(reclass_in_units AS NUMERIC), 4) AS reclass_in_units,
  reclass_out_dollar_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN reclass_out_dollar_units = ''
       THEN '0'
       ELSE reclass_out_dollar_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN reclass_out_dollar_nanos = ''
        THEN '0'
        ELSE reclass_out_dollar_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS reclass_out_dollar_amount,
  ROUND(CAST(reclass_out_units AS NUMERIC), 4) AS reclass_out_units,
  active_rtv_dollar_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN active_rtv_dollar_units = ''
       THEN '0'
       ELSE active_rtv_dollar_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN active_rtv_dollar_nanos = ''
        THEN '0'
        ELSE active_rtv_dollar_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS active_rtv_dollar_amount,
  ROUND(CAST(active_rtv_units AS NUMERIC), 4) AS active_rtv_units,
  inactive_rtv_dollar_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN inactive_rtv_dollar_units = ''
       THEN '0'
       ELSE inactive_rtv_dollar_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN inactive_rtv_dollar_nanos = ''
        THEN '0'
        ELSE inactive_rtv_dollar_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS inactive_rtv_dollar_amount,
  ROUND(CAST(inactive_rtv_units AS NUMERIC), 4) AS inactive_rtv_units,
  vendor_funds_cost_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN vendor_funds_cost_units = ''
       THEN '0'
       ELSE vendor_funds_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN vendor_funds_cost_nanos = ''
        THEN '0'
        ELSE vendor_funds_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS vendor_funds_cost_amount,
  inventory_movement_drop_ship_in_dollar_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN inventory_movement_drop_ship_in_dollar_units = ''
       THEN '0'
       ELSE inventory_movement_drop_ship_in_dollar_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN inventory_movement_drop_ship_in_dollar_nanos = ''
        THEN '0'
        ELSE inventory_movement_drop_ship_in_dollar_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS inventory_movement_drop_ship_in_dollar_amount,
  ROUND(CAST(inventory_movement_drop_ship_in_units AS NUMERIC), 4) AS inventory_movement_drop_ship_in_units,
  inventory_movement_drop_ship_out_dollar_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN inventory_movement_drop_ship_out_dollar_units = ''
       THEN '0'
       ELSE inventory_movement_drop_ship_out_dollar_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN inventory_movement_drop_ship_out_dollar_nanos = ''
        THEN '0'
        ELSE inventory_movement_drop_ship_out_dollar_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS inventory_movement_drop_ship_out_dollar_amount,
  ROUND(CAST(inventory_movement_drop_ship_out_units AS NUMERIC), 4) AS inventory_movement_drop_ship_out_units,
  other_inventory_adjustments_dollar_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN other_inventory_adjustments_dollar_units = ''
       THEN '0'
       ELSE other_inventory_adjustments_dollar_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN other_inventory_adjustments_dollar_nanos = ''
        THEN '0'
        ELSE other_inventory_adjustments_dollar_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS other_inventory_adjustments_dollar_amount,
  ROUND(CAST(other_inventory_adjustments_units AS NUMERIC), 4) AS other_inventory_adjustments_units,
  active_opentobuy_cost_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN active_opentobuy_cost_units = ''
       THEN '0'
       ELSE active_opentobuy_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN active_opentobuy_cost_nanos = ''
        THEN '0'
        ELSE active_opentobuy_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS active_opentobuy_cost_amount,
  inactive_opentobuy_cost_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN inactive_opentobuy_cost_units = ''
       THEN '0'
       ELSE inactive_opentobuy_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN inactive_opentobuy_cost_nanos = ''
        THEN '0'
        ELSE inactive_opentobuy_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS inactive_opentobuy_cost_amount,
  active_commitments_cost_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN active_commitments_cost_units = ''
       THEN '0'
       ELSE active_commitments_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN active_commitments_cost_nanos = ''
        THEN '0'
        ELSE active_commitments_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS active_commitments_cost_amount,
  inactive_commitments_cost_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN inactive_commitments_cost_units = ''
       THEN '0'
       ELSE inactive_commitments_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN inactive_commitments_cost_nanos = ''
        THEN '0'
        ELSE inactive_commitments_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS inactive_commitments_cost_amount,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS
  dw_sys_load_tmstp,
  `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS STRING)) as dw_sys_load_tmstp_tz,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS
  dw_sys_updt_tmstp,
  `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) as string)) as dw_sys_updt_tmstp_tz
 FROM {{params.gcp_project_id}}.{{params.database_name_staging}}.merch_financial_country_banner_plan_ldg
 WHERE LOWER(department_number) <> LOWER('DEPARTMENT_NUMBER'));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
