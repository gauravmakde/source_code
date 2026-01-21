BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=mfp_rp_anticipated_receipt_load;
---Task_Name=rp_ant_spend_receipt_work_load;'*/
---FOR SESSION VOLATILE;
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_rp_anticipated_spend_receipt_wrk;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_rp_anticipated_spend_receipt_wrk
(SELECT mpssf.sku_num,
  mpssf.store_num,
  mrbmw.week_num,
  mrbmw.channel_num,
  mpssf.poreceipt_order_number AS po_order_number,
  mpssf.event_time_utc AS po_event_time,
  mpssf.event_time_tz AS po_event_time_tz,
  mrbmw.max_event_tmstp_utc AS rp_event_time,
  mrbmw.max_event_tmstp_tz AS rp_event_time_tz,
  CAST(mpssf.event_time AS DATE) AS po_event_date,
  CAST(mrbmw.max_event_tmstp AS DATE) AS rp_event_date,
  mpssf.receipts_units,
  mpssf.receipts_cost,
  mpssf.receipts_cost_currency_code,
  mpssf.receipts_retail,
  mpssf.receipts_retail_currency_code,
  mpssf.receipts_crossdock_units,
  mpssf.receipts_crossdock_cost,
  mpssf.receipts_crossdock_cost_currency_code,
  mpssf.receipts_crossdock_retail,
  mpssf.receipts_crossdock_retail_currency_code,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_rp_booked_measures_wrk_vw AS mrbmw
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_poreceipt_sku_store_fact AS mpssf 
    ON LOWER(mrbmw.rms_sku_num) = LOWER(mpssf.sku_num )
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim AS dcd 
    ON mpssf.tran_date = dcd.day_date AND mrbmw.week_num = dcd.week_idnt
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS sd 
    ON mpssf.store_num = sd.store_num 
    AND mrbmw.channel_num = sd.channel_num
 WHERE LOWER(mpssf.rp_ind) = LOWER('Y')
  AND CAST(mpssf.event_time AS DATE) < DATE_SUB(CAST(mrbmw.max_event_tmstp AS DATE), INTERVAL 1 DAY));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
-- --COLLECT STATS ON `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_STG.MERCH_RP_ANTICIPATED_SPEND_RECEIPT_WRK;
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
-- /*SET QUERY_BAND = NONE FOR SESSION;*/
END;
