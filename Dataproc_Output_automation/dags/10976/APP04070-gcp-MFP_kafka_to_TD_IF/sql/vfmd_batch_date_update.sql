BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=vfmd_columnar_view_data_load;
---Task_Name=vfmd_columnar_data_load_job_1;'*/
BEGIN
SET _ERROR_CODE  =  0;


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup
 SET
    dw_batch_dt = DATE_ADD(etl_batch_dt_lkup.dw_batch_dt, INTERVAL etl_batch_dt_lkup.interface_freq DAY)
WHERE LOWER(interface_code) = LOWER('MERCH_NAP_VFMD_DLY') AND dw_batch_dt < CURRENT_DATE('PST8PDT');

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;