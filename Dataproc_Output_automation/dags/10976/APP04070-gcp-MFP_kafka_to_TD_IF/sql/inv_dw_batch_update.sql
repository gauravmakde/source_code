-- BEGIN
-- DECLARE _ERROR_CODE INT64;
-- DECLARE _ERROR_MESSAGE STRING;
-- /*SET QUERY_BAND = '
-- App_ID=APP04070;
-- DAG_ID=inventory_apt_week_fact_load_dly;
-- ---Task_Name=inv_dw_batch_update;'*/
-- ---FOR SESSION VOLATILE;

-- BEGIN

-- SET ERROR_CODE  =  0;

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup SET
    dw_batch_dt = DATE_ADD(etl_batch_dt_lkup.dw_batch_dt, INTERVAL etl_batch_dt_lkup.interface_freq DAY),
    extract_start_dt = DATE_ADD(etl_batch_dt_lkup.extract_start_dt, INTERVAL etl_batch_dt_lkup.interface_freq DAY),
    extract_end_dt = DATE_ADD(etl_batch_dt_lkup.extract_end_dt, INTERVAL etl_batch_dt_lkup.interface_freq DAY)
WHERE LOWER(interface_code) = LOWER('MERCH_NAP_INV_DLY') AND dw_batch_dt < CURRENT_DATE('PST8PDT');



-- EXCEPTION WHEN ERROR THEN

-- RAISE USING MESSAGE = @@error.message;
-- /*SET QUERY_BAND = NONE FOR SESSION;*/
-- END;
-- END;
