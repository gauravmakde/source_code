-- SET QUERY_BAND = 'App_ID=app04070;DAG_ID=inventory_sales_insights_history_10976_tech_nap_merch;Task_Name=execute_delete_metrics;'
-- FOR SESSION VOLATILE;


BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.inventory_sales_insights_by_day_hist_fact
WHERE metrics_date BETWEEN (SELECT extract_start_dt
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
        WHERE LOWER(interface_code) = LOWER('SMD_INV_HIST_DLY')) AND (SELECT extract_end_dt
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
        WHERE LOWER(interface_code) = LOWER('SMD_INV_HIST_DLY'));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
-- COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
ROLLBACK TRANSACTION;
RAISE USING MESSAGE = @@error.message;
END;

-- SET QUERY_BAND = NONE FOR SESSION;


