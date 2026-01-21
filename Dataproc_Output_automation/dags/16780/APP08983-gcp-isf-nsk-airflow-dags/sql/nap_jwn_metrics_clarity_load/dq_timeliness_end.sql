--DECLARE _ERROR_CODE INT64;
--DECLARE _ERROR_MESSAGE STRING;

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('JWN_INVENTORY_SKU_LOC_DAY_FACT',  '{{params.dbenv}}_NAP_JWN_METRICS_FCT',  'nap_jwn_metrics_clarity_td_load',  'dq_timeliness_end',  1,  'LOAD_END',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME),  'NAP_ASCP_CLARITY_LOAD');


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('JWN_TRANSFERS_RMS_FACT',  '{{params.dbenv}}_NAP_JWN_METRICS_FCT',  'nap_jwn_metrics_clarity_td_load',  'dq_timeliness_end',  1,  'LOAD_END',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME),  'NAP_ASCP_CLARITY_LOAD');


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('JWN_EXTERNAL_IN_TRANSIT_FACT',  '{{params.dbenv}}_NAP_JWN_METRICS_FCT',  'nap_jwn_metrics_clarity_td_load',  'dq_timeliness_end',  1,  'LOAD_END',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME),  'NAP_ASCP_CLARITY_LOAD');


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('JWN_PURCHASE_ORDER_RECEIPTS_FACT',  '{{params.dbenv}}_NAP_JWN_METRICS_FCT',  'nap_jwn_metrics_clarity_td_load',  'dq_timeliness_end',  1,  'LOAD_END',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME),  'NAP_ASCP_CLARITY_LOAD');

