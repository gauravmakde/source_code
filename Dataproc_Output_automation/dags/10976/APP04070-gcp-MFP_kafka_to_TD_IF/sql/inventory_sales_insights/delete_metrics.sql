-- SET QUERY_BAND = 'App_ID=app04070;DAG_ID=sales_amount_10976_tech_nap_merch;Task_Name=execute_delete_metrics;'
-- FOR SESSION VOLATILE;

-- ET;

-- Delete yesterday's metrics
DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.inventory_sales_insights_by_day_fact
WHERE metrics_date >= (SELECT extract_start_dt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
   WHERE LOWER(interface_code) = LOWER('SMD_INV_DLY')) AND metrics_date <= (SELECT extract_end_dt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
   WHERE LOWER(interface_code) = LOWER('SMD_INV_DLY'));

-- ET;

-- delete all records from INVENTORY_SALES_INSIGHTS_BY_DAY_FACT that are older than 24 months
DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.inventory_sales_insights_by_day_fact
WHERE metrics_date < DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 24 MONTH);

-- ET;

-- SET QUERY_BAND = NONE FOR SESSION;

-- ET;