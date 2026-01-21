
CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_ACCRUED_SHRINK_SUBCLASS_STORE_WEEK_FACT',  '{{params.dbenv}}_NAP_FCT',  'accrued_shrink_fact_load',  'accr_shrink_load',  0,  'LOAD_START',  'Updating Config_value',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME()) AS DATETIME),  'ACCRUED_SHRINK');


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.config_lkup SET
    config_value = CASE WHEN (SELECT CAST(trunc(cast(CASE WHEN config_value = '' THEN '0' ELSE config_value END as float64)) AS INTEGER) AS config_value
                FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
                WHERE LOWER(interface_code) = LOWER('ACCRUED_SHRINK') AND LOWER(config_key) = LOWER('CURR_PROCESS_WEEK')) = 0 THEN FORMAT('%11d', (SELECT start_rebuild_week_num
                FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS tran_vw1
                WHERE LOWER(interface_code) = LOWER('ACCRUED_SHRINK'))) ELSE config_lkup.config_value END
WHERE LOWER(interface_code) = LOWER('ACCRUED_SHRINK') AND LOWER(config_key) = LOWER('CURR_PROCESS_WEEK');