

BEGIN
DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=apt_assortment_category_cluster_weekly_snapshot_load;
---Task_Name=load_weekly_snapshot;'*/


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.config_lkup 
SET config_value = 'Y'
WHERE LOWER(interface_code) = LOWER('MRIc_DAY_AGG_DLY') 
AND LOWER(config_key) = LOWER('MRCH_NAP_DAY_FACT_LOAD_FL');



EXCEPTION WHEN ERROR THEN
ROLLBACK TRANSACTION;
RAISE USING MESSAGE = @@error.message;
END;