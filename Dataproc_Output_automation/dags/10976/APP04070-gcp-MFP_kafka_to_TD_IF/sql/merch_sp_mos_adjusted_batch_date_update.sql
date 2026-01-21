-- SET QUERY_BAND = '
-- App_ID=APP04070;
-- DAG_ID=merch_sp_mos_adjusted_week_agg_fact_load;
-- Task_Name=mos_agg_fact_load_job_3;'
-- FOR SESSION VOLATILE;

-- ET;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup SET
    dw_batch_dt = DATE_ADD(etl_batch_dt_lkup.dw_batch_dt, INTERVAL etl_batch_dt_lkup.interface_freq DAY),
    extract_start_dt = DATE_ADD(etl_batch_dt_lkup.extract_start_dt, INTERVAL etl_batch_dt_lkup.interface_freq DAY),
    extract_end_dt = DATE_ADD(etl_batch_dt_lkup.extract_end_dt, INTERVAL etl_batch_dt_lkup.interface_freq DAY)
WHERE LOWER(interface_code) = LOWER('MERCH_SP_MOS_ADJ_DLY') AND dw_batch_dt < CURRENT_DATE('PST8PDT');

-- ET;

-- SET QUERY_BAND = NONE FOR SESSION;

-- ET;