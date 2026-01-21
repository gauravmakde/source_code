BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=wac_sku_dim;
---Task_Name=wac_sku_dim_job_1;'*/
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
-- BEGIN
-- SET _ERROR_CODE  =  0;


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.merch_wac_sku_channel_cur_dim;

-- EXCEPTION WHEN ERROR THEN
-- SET _ERROR_CODE  =  1;
-- SET _ERROR_MESSAGE  =  @@error.message;
-- END;
BEGIN
SET _ERROR_CODE  =  0;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.merch_wac_sku_channel_cur_dim (channel_num, rms_sku_num, weighted_average_cost, eff_beg_dt_drvd
 , eff_begin_dt, eff_end_dt_drvd, eff_end_dt, dw_sys_load_tmstp)
(SELECT channel_num,
  sku_num,
  weighted_average_cost,
  eff_begin_dt AS eff_beg_dt_drvd,
  eff_begin_dt,
  eff_end_dt AS eff_end_dt_drvd,
  eff_end_dt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.weighted_average_cost_channel_current_dim AS wac_curr);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
-- /*SET QUERY_BAND = NONE FOR SESSION;*/
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
END;
