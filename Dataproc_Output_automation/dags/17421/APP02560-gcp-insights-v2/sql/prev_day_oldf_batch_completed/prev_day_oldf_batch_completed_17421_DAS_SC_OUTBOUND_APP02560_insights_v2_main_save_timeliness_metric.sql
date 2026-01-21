BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD(
'ORDER_LINE_DETAIL_FACT',
  '{{params.dbenv}}_NAP_FCT',
  'prev_day_oldf_batch_completed_17421_DAS_SC_OUTBOUND_APP02560_insights_v2',
  'main_job_3_save_timeliness_metric',
  1,
  'LOAD_END',
  '',
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
  'ORDER_LINE_DETAIL_FACT');

END;


