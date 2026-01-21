BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=rp_bkd_msrs_wrk_load;
---Task_Name=merch_rp_t1_bkd_msrs_wrk_load;'*/
---FOR SESSION VOLATILE;
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;

BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_rp_booked_measures_wrk;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;


-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_rp_booked_measures_wrk (rms_sku_num, channel_num, week_num, max_event_tmstp,max_event_tmstp_tz,
 booked_buffer_receipt_plan_units, booked_receipt_plan_units, booked_trunkclub_not_kept_units,
 booked_usable_returns_units, booked_other_units, unit_retail_amount, weighted_average_cost_amount, dw_batch_date,
 dw_sys_load_tmstp)
(SELECT p01.rms_sku_num,
  b01.channel_num,
  b01.week_num,
  MAX(b01.event_tmstp_utc) AS max_event_tmstp,
  any_value(b01.event_tmstp_tz) AS max_event_tmstp_tz,
  CAST(SUM(CASE
     WHEN LOWER(b01.booked_measure_type) = LOWER('BOOKED_BUFFER_RECEIPT_PLAN_UNITS')
     THEN b01.booked_measure_units
     ELSE 0
     END) AS NUMERIC) AS booked_buffer_receipt_plan_units,
  CAST(SUM(CASE
     WHEN LOWER(b01.booked_measure_type) = LOWER('BOOKED_RECEIPT_PLAN_UNITS')
     THEN b01.booked_measure_units
     ELSE 0
     END) AS NUMERIC) AS booked_receipt_plan_units,
  CAST(SUM(CASE
     WHEN LOWER(b01.booked_measure_type) = LOWER('BOOKED_TRUNKCLUB_NOT_KEPT_UNITS')
     THEN b01.booked_measure_units
     ELSE 0
     END) AS NUMERIC) AS booked_trunkclub_not_kept_units,
  CAST(SUM(CASE
     WHEN LOWER(b01.booked_measure_type) = LOWER('BOOKED_USABLE_RETURNS_UNITS')
     THEN b01.booked_measure_units
     ELSE 0
     END) AS NUMERIC) AS booked_usable_returns_units,
  CAST(SUM(CASE
     WHEN LOWER(b01.booked_measure_type) NOT IN (LOWER('BOOKED_BUFFER_RECEIPT_PLAN_UNITS'), LOWER('BOOKED_RECEIPT_PLAN_UNITS'), LOWER('BOOKED_TRUNKCLUB_NOT_KEPT_UNITS'), LOWER('BOOKED_USABLE_RETURNS_UNITS'))
     THEN b01.booked_measure_units
     ELSE 0
     END) AS NUMERIC) AS booked_other_units,
  MAX(COALESCE(f01.retail_price_amt, 0)) AS unit_retail_amount,
  ROUND(CAST(MAX(COALESCE(w01.weighted_average_cost, 0)) AS NUMERIC), 4) AS weighted_average_cost_amount,
  CURRENT_DATE('PST8PDT'),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_rp_booked_measures_plan_fact AS b01
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim AS p01 
    ON b01.sku_num = p01.epm_sku_num 
    AND LOWER(b01.channel_country) = LOWER(p01.channel_country) 
    AND LOWER(b01.sku_type) = LOWER('EPM')
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_week_cal_454_vw AS c01 
    ON b01.week_num = c01.week_idnt
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_wac_sku_channel_cur_dim AS w01 
    ON LOWER(p01.rms_sku_num) = LOWER(w01.rms_sku_num) 
    AND b01.channel_num = w01.channel_num
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_future_price_sku_channel_vw AS f01 
    ON LOWER(p01.rms_sku_num) = LOWER(f01.rms_sku_num) 
    AND b01.channel_num = f01.channel_num 
    AND c01.week_end_day_date >= f01.eff_begin_date 
    AND c01.week_end_day_date < f01.eff_end_date
 GROUP BY p01.rms_sku_num,
  b01.channel_num,
  b01.week_num);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;


--COLLECT STATS ON `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_STG.MERCH_RP_BOOKED_MEASURES_WRK;
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
-- /*SET QUERY_BAND = NONE FOR SESSION;*/

END;
END;
