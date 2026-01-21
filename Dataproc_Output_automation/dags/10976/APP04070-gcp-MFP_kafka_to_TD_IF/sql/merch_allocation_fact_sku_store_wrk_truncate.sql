
-- /*SET QUERY_BAND = '
-- App_ID=APP04070;
-- DAG_ID=merch_allocation_fact_sku_store_load;
-- Task_Name=merch_allocation_fact_sku_store_wrk_truncate;'*/

-- commit transaction;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_allocation_fact_sku_store_wrk;


-- commit transaction;
-- /*SET QUERY_BAND = NONE FOR SESSION;*/
-- commit transaction;
