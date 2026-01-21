UPDATE {{params.bq_project_id}}.{{params.DBJWNENV}}_NAP_JWN_METRICS_UTL.etl_batch_dt_lkup SET
 extract_start_dt = etl_batch_dt_lkup.extract_end_dt,
 extract_end_dt = CURRENT_DATE,
 dw_batch_dt = DATE_ADD(etl_batch_dt_lkup.dw_batch_dt, INTERVAL etl_batch_dt_lkup.interface_freq DAY),
 rcd_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_SALES');