CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD(
    'JWN_INVENTORY_SKU_LOC_DAY_FACT',  
    '`{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_JWN_METRICS_FCT', 
     'nap_jwn_metrics_clarity_td_load',  
     'dq_timeliness_start',  0,  
     'LOAD_START',  '',  
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  
     'NAP_ASCP_CLARITY_LOAD');


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD(
    'JWN_TRANSFERS_RMS_FACT',  
    '`{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_JWN_METRICS_FCT',  
    'nap_jwn_metrics_clarity_td_load',  
    'dq_timeliness_start',  0,  
    'LOAD_START',  '',  
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  
    'NAP_ASCP_CLARITY_LOAD');


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD(
    'JWN_EXTERNAL_IN_TRANSIT_FACT',  
    '`{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_JWN_METRICS_FCT',  
    'nap_jwn_metrics_clarity_td_load',  
    'dq_timeliness_start',  0,  
    'LOAD_START',  '', 
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  
     'NAP_ASCP_CLARITY_LOAD');


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD(
    'JWN_PURCHASE_ORDER_RECEIPTS_FACT',  
    '`{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_JWN_METRICS_FCT',  
    'nap_jwn_metrics_clarity_td_load',  
    'dq_timeliness_start',  0,  
    'LOAD_START',  '',  
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  
    'NAP_ASCP_CLARITY_LOAD');