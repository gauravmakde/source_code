BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=merch_sale_return_dept_channel_week_agg_fact_load;
---Task_Name=load_weeks_prepare;'*/
---FOR SESSION VOLATILE;

BEGIN
SET _ERROR_CODE  =  0;

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.merch_nap_aggregate_rebuild_lkup
WHERE LOWER(config_key) IN (LOWER('MERCH_SALE_RETURN_DEPT_CHANNEL_WEEK_REBUILD_WEEKS'));

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.merch_nap_aggregate_rebuild_lkup 
(interface_code, config_key, rebuild_week_num,
 process_status_ind, dw_sys_load_tmstp, dw_sys_updt_tmstp)
(SELECT 'MERCH_NAP_AGG_WKLY',
  'MERCH_SALE_RETURN_DEPT_CHANNEL_WEEK_REBUILD_WEEKS',
  CAST(TRIM(FORMAT('%11d', cal2.week_idnt)) AS INTEGER) AS week_num,
  'N',
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim AS cal
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup AS cfg1 
    ON LOWER(cfg1.interface_code) = LOWER('MERCH_NAP_AGG_WKLY') 
    AND LOWER(cfg1.config_key) = LOWER('MERCH_TRAN_REBUILD_YEARS')
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup AS cfg2 
    ON LOWER(cfg2.interface_code) = LOWER('MERCH_NAP_AGG_WKLY') 
    AND LOWER(cfg2.config_key) = LOWER('MERCH_TRAN_CUTOFF_YEAR')
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim AS cal2 
    ON cal2.fiscal_year_num >= cal.fiscal_year_num - CAST(cfg1.config_value AS FLOAT64)
 WHERE cal.day_date = CURRENT_DATE
  AND cal2.week_idnt <= cal.week_idnt
  AND cal2.fiscal_year_num >= CAST(cfg2.config_value AS FLOAT64)
 GROUP BY cfg1.config_key,
  cal2.week_idnt);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_sale_return_dept_channel_week_agg_fact;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
/*SET QUERY_BAND = NONE FOR SESSION;*/
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
END;
