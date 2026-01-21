
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup 
SET
 extract_start_dt = DATE_ADD(etl_batch_dt_lkup.extract_start_dt, INTERVAL 1 DAY),
 extract_end_dt = DATE_ADD(etl_batch_dt_lkup.extract_end_dt, INTERVAL 1 DAY),
 dw_batch_dt = DATE_ADD(etl_batch_dt_lkup.dw_batch_dt, INTERVAL 1 DAY),
 rcd_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
WHERE LOWER(interface_code) = LOWER('NAP_SCP_XREF_DLY') AND extract_end_dt <= CURRENT_DATE('PST8PDT') AND dw_batch_dt <
  CURRENT_DATE('PST8PDT');