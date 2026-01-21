BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=clearance_markdown_applied_resets;
---Task_Name=wkly_batch_date_update;'*/
---FOR SESSION VOLATILE;
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
BEGIN
SET _ERROR_CODE  =  0;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup AS etl SET
    dw_batch_dt = CASE WHEN DATE_ADD(etl.dw_batch_dt, INTERVAL CAST(etl.interface_freq * CAST(cfg.config_value AS FLOAT64) AS INTEGER) DAY) > CURRENT_DATE('PST8PDT') THEN (SELECT MAX(week_end_day_date)
            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_week_cal_454_vw AS wk_cal 
				WHERE week_end_day_date < CURRENT_DATE('PST8PDT')) 
            ELSE DATE_ADD(etl.dw_batch_dt, INTERVAL CAST(etl.interface_freq * CAST(cfg.config_value AS FLOAT64) AS INTEGER) DAY) 
            END FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup AS cfg
  WHERE LOWER(etl.interface_code) = LOWER('SMD_CMKDN_WKLY') 
    AND LOWER(cfg.interface_code) = LOWER('SMD_CMKDN_WKLY') 
    AND LOWER(etl.interface_code) = LOWER(cfg.interface_code) 
    AND etl.dw_batch_dt < CURRENT_DATE('PST8PDT');

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
