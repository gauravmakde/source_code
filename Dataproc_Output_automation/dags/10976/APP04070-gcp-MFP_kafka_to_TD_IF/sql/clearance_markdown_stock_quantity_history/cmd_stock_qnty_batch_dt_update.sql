-- SET QUERY_BAND = 'App_ID=app04070;DAG_ID=smartmarkdown_insights_history_10976_tech_nap_merch;Task_Name=cmd_batch_dt_update;'
-- FOR SESSION VOLATILE;

-- ET;

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup SET
 dw_batch_dt = DATE_ADD(etl_batch_dt_lkup.dw_batch_dt, INTERVAL etl_batch_dt_lkup.interface_freq DAY),
 extract_start_dt = DATE_ADD(etl_batch_dt_lkup.extract_start_dt, INTERVAL etl_batch_dt_lkup.interface_freq DAY),
 extract_end_dt = DATE_ADD(etl_batch_dt_lkup.extract_end_dt, INTERVAL etl_batch_dt_lkup.interface_freq DAY),
 rcd_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
WHERE LOWER(interface_code) = LOWER('CMD_STOCK_QNTY_WKLY');
-- ET;

-- SET QUERY_BAND = NONE FOR SESSION;

-- ET;
