
BEGIN
DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=app04070;DAG_ID=clearance_markdown_stock_quantity_history_10976_tech_nap_merch;Task_Name=run_clear_inv_fact;'*/


BEGIN TRANSACTION;
SET ERROR_CODE  =  0;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_stock_quantity_end_of_wk_hist_fact;



/*SET QUERY_BAND = NONE FOR SESSION;*/
COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
ROLLBACK TRANSACTION;
RAISE USING MESSAGE = @@error.message;
END;
