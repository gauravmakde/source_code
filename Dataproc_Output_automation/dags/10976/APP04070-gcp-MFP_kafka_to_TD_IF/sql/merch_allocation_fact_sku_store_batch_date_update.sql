
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=merch_allocation_fact_sku_store_load;
---Task_Name=merch_allocation_fact_sku_store_batch_date_update;'*/
-- COMMIT TRANSACTION ;

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup
 SET
    dw_batch_dt = DATE_ADD(etl_batch_dt_lkup.dw_batch_dt, INTERVAL etl_batch_dt_lkup.interface_freq DAY)
WHERE LOWER(interface_code) = LOWER('MERCH_ALLOCATION_DLY');

-- COMMIT TRANSACTION ;
/*SET QUERY_BAND = NONE FOR SESSION;*/
-- COMMIT TRANSACTION ;