BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=app04070;DAG_ID=inventory_sales_insights_history_10976_tech_nap_merch;Task_Name=execute_clear_clearance_hierarchy_dim;'*/
--FOR SESSION VOLATILE;

BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.clearance_markdown_hierarchy_dim;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


/*SET QUERY_BAND = NONE FOR SESSION;*/


END;
