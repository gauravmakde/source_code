BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=spe_ap_supp_grp_fct;
---Task_Name=t2_spe_batch_updt;'*/
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
BEGIN
SET _ERROR_CODE  =  0;


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup SET
    dw_batch_dt = DATE_ADD(etl_batch_dt_lkup.dw_batch_dt, INTERVAL etl_batch_dt_lkup.interface_freq DAY)
WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SPE_DLY') 
AND dw_batch_dt < CURRENT_DATE('PST8PDT');

-- EXCEPTION WHEN ERROR THEN
-- SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

-- COMMIT TRANSACTION;

-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;


END;
