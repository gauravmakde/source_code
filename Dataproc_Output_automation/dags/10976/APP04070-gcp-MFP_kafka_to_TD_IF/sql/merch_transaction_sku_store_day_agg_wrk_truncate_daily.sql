BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=merch_nap_mri_transaction_fact_to_day_aggregate_daily_load;
---Task_Name=merch_transaction_sku_store_day_agg_wrk_truncate;'*/

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_transaction_sku_store_day_agg_wrk;

/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
