
BEGIN
DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=rp_batch_updt;
---Task_Name=merch_rp_t1_batch_updt;'*/

BEGIN TRANSACTION;

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup AS tgt
SET dw_batch_dt = DATE_ADD(tgt.dw_batch_dt, INTERVAL tgt.interface_freq DAY)
WHERE LOWER(interface_code) = LOWER('MERCH_NAP_RP_DLY') 
AND dw_batch_dt < CURRENT_DATE('PST8PDT');


/*SET QUERY_BAND = NONE FOR SESSION;*/
COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
ROLLBACK TRANSACTION;
RAISE USING MESSAGE = @@error.message;
END;
