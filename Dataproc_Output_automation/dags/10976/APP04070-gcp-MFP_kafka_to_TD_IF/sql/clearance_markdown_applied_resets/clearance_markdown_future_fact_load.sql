BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=clearance_markdown_applied_resets;
---Task_Name=clearance_markdown_future_fact_load;'*/
---FOR SESSION VOLATILE;
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_future_fact AS cmarf
WHERE EXISTS (SELECT 1
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS etl
        INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_week_cal_454_vw AS wk_cal 
    ON LOWER(etl.interface_code) = LOWER('SMD_CMKDN_WKLY') 
	AND wk_cal.week_idnt BETWEEN etl.start_rebuild_week_num AND etl.end_rebuild_week_num 
	AND cmarf.snapshot_date = wk_cal.week_end_day_date);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_future_fact (snapshot_date, rms_style_num, color_num, channel_country,
 channel_brand, selling_channel, future_clearance_markdown_date, future_clearance_price_amt,
 future_clearance_price_currency_code, dw_sys_load_tmstp, dw_sys_updt_tmstp)
(SELECT wk_cal.week_end_day_date AS snapshot_date,
  cmf.rms_style_num,
  cmf.color_num,
  cmf.channel_country,
  cmf.channel_brand,
  cmf.selling_channel,
  cmf.effective_begin_tmstp_utc,
  cmf.clearance_price_amt,
  cmf.clearance_price_currency_code,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_fact_backfilled_vw AS cmf
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS etl 
	ON LOWER(etl.interface_code) = LOWER('SMD_CMKDN_WKLY')
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_week_cal_454_vw AS wk_cal 
  ON wk_cal.week_idnt BETWEEN etl.start_rebuild_week_num AND etl.end_rebuild_week_num 
    AND CAST(cmf.effective_begin_tmstp AS DATE) <= DATE_ADD(wk_cal.week_end_day_date, INTERVAL 35 DAY) 
	AND CAST(cmf.effective_begin_tmstp AS DATE) > wk_cal.week_end_day_date 
	AND CAST(cmf.dw_sys_load_tmstp AS DATE) < wk_cal.week_end_day_date 
	AND LOWER(cmf.clearance_markdown_state) = LOWER('DECLARED') 
	AND cmf.rms_style_num IS NOT NULL 
	AND cmf.color_num IS NOT NULL
QUALIFY (ROW_NUMBER() OVER (PARTITION BY wk_cal.week_end_day_date, cmf.rms_style_num, cmf.color_num, cmf.selling_channel
       , cmf.channel_brand, cmf.channel_country ORDER BY cmf.effective_begin_tmstp)) = 1);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
--COLLECT STATS ON `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.CLEARANCE_MARKDOWN_FUTURE_FACT;
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
