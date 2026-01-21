BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=merch_nap_transaction_week_fact_load_completion_check;
---Task_Name=t1_update_nap_transaction_week_fact_load_flag;'*/
-- ---FOR SESSION VOLATILE;
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
BEGIN
SET _ERROR_CODE  =  0;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.config_lkup SET
    config_value = 'Y'
WHERE LOWER(interface_code) = LOWER('MERCH_NAP_AGG_WKLY') 
   AND LOWER(config_key) = LOWER('MERCH_TRAN_FACT_LOAD_FLAG');

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
