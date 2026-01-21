BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=clearance_markdown_applied_resets;
---Task_Name=applied_resets_fact_load;'*/
---FOR SESSION VOLATILE;
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
BEGIN
SET _ERROR_CODE  =  0;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.clearance_markdown_gtt;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.clearance_markdown_gtt (snapshot_date, rms_style_num, color_num, channel_country, channel_brand
 , selling_channel, clearance_markdown_date, clearance_markdown_date_tz,clearance_price_amt, clearance_price_currency_code, dw_sys_load_tmstp,
 dw_sys_updt_tmstp)
(SELECT wk_cal.week_end_day_date AS snapshot_date,
  cmfbv.rms_style_num,
  cmfbv.color_num,
  cmfbv.channel_country,
  cmfbv.channel_brand,
  cmfbv.selling_channel,
  cmfbv.effective_begin_tmstp_utc AS clearance_markdown_date,
  cmfbv.effective_begin_tmstp_tz AS clearance_markdown_date_tz,
  cmfbv.clearance_price_amt,
  cmfbv.clearance_price_currency_code,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_fact_backfilled_vw AS cmfbv
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS etl 
    ON LOWER(etl.interface_code) = LOWER('SMD_CMKDN_WKLY')
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_week_cal_454_vw AS wk_cal 
    ON wk_cal.week_idnt BETWEEN etl.start_rebuild_week_num AND etl.end_rebuild_week_num
 WHERE CAST(cmfbv.effective_begin_tmstp AS DATE) <= wk_cal.week_end_day_date
  AND cmfbv.rms_style_num IS NOT NULL
  AND cmfbv.color_num IS NOT NULL);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.clearance_markdown_max_resets_gtt;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.clearance_markdown_max_resets_gtt (snapshot_date, rms_style_num, color_num, channel_country,
 channel_brand, selling_channel, markdown_reset_max_date, markdown_reset_max_date_tz, dw_sys_load_tmstp, dw_sys_updt_tmstp)
(SELECT wk_cal.week_end_day_date AS snapshot_date,
  cmrfbv.rms_style_num,
  cmrfbv.color_num,
  cmrfbv.channel_country,
  cmrfbv.channel_brand,
  cmrfbv.selling_channel,
  MAX(cmrfbv.effective_tmstp_utc) AS markdown_reset_max_date,
  MAX(cmrfbv.effective_tmstp_tz) AS markdown_reset_max_date_tz,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_reset_fact_backfilled_vw AS cmrfbv
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS etl 
    ON LOWER(etl.interface_code) = LOWER('SMD_CMKDN_WKLY')
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_week_cal_454_vw AS wk_cal 
    ON wk_cal.week_idnt BETWEEN etl.start_rebuild_week_num AND etl.end_rebuild_week_num
 WHERE CAST(cmrfbv.effective_tmstp AS DATE) <= wk_cal.week_end_day_date
  AND cmrfbv.rms_style_num IS NOT NULL
  AND cmrfbv.color_num IS NOT NULL
 GROUP BY snapshot_date,
  cmrfbv.rms_style_num,
  cmrfbv.color_num,
  cmrfbv.channel_country,
  cmrfbv.channel_brand,
  cmrfbv.selling_channel);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

--COLLECT STATS ON TEMPORARY `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_STG.CLEARANCE_MARKDOWN_GTT;
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
--COLLECT STATS ON TEMPORARY `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_STG.CLEARANCE_MARKDOWN_MAX_RESETS_GTT;
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;

BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_applied_resets_fact AS cmarf
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
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_applied_resets_fact (snapshot_date, rms_style_num, color_num, channel_country
 , channel_brand, selling_channel, clearance_markdown_date, clearance_markdown_date_tz, clearance_price_amt, clearance_price_currency_code,
 dw_sys_load_tmstp, dw_sys_updt_tmstp)
(SELECT cmg.snapshot_date,
  cmg.rms_style_num,
  cmg.color_num,
  cmg.channel_country,
  cmg.channel_brand,
  cmg.selling_channel,
   CASE
   WHEN cmmrg.markdown_reset_max_date > cmg.clearance_markdown_date
   THEN NULL
   ELSE cmg.clearance_markdown_date
   END AS clearance_markdown_date,
  cmg.clearance_markdown_date_tz AS clearance_markdown_date_tz,
  cmg.clearance_price_amt,
  cmg.clearance_price_currency_code,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.clearance_markdown_gtt AS cmg
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.clearance_markdown_max_resets_gtt AS cmmrg 
  ON LOWER(cmg.rms_style_num) = LOWER(cmmrg.rms_style_num ) 
  AND LOWER(cmg.color_num) = LOWER(cmmrg.color_num) 
  AND LOWER(cmg.channel_country) = LOWER(cmmrg.channel_country) 
  AND LOWER(cmg.channel_brand) = LOWER(cmmrg.channel_brand) 
  AND LOWER(cmg.selling_channel) = LOWER(cmmrg.selling_channel) AND cmg.snapshot_date = cmmrg.snapshot_date);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
--COLLECT STATS ON `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.CLEARANCE_MARKDOWN_APPLIED_RESETS_FACT;
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
/*SET QUERY_BAND = NONE FOR SESSION;*/
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
END;
