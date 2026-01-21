

CALL `{{params.gcp_project_id}}.{{params.dbenv}}_nap_utl.data_timeliness_metric_fact_ld`('PRODUCT_SKU_DIM',  '{{params.dbenv}}_NAP_DIM',  'PRODUCT_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1',  'dq_timeliness_start',  1,  'LOAD_START',  '',  CURRENT_DATETIME('PST8PDT'),  'NAP_PRODUCT_LOAD');


CALL `{{params.gcp_project_id}}.{{params.dbenv}}_nap_utl.data_timeliness_metric_fact_ld`('PRODUCT_STYLE_DIM',  '{{params.dbenv}}_NAP_DIM',  'PRODUCT_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1',  'dq_timeliness_start',  1,  'LOAD_START',  '',  CURRENT_DATETIME('PST8PDT'),  'NAP_PRODUCT_LOAD');


CALL `{{params.gcp_project_id}}.{{params.dbenv}}_nap_utl.data_timeliness_metric_fact_ld`('PRODUCT_CHOICE_DIM',  '{{params.dbenv}}_NAP_DIM',  'PRODUCT_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1',  'dq_timeliness_start',  1,  'LOAD_START',  '',  CURRENT_DATETIME('PST8PDT'),  'NAP_PRODUCT_LOAD');


CALL `{{params.gcp_project_id}}.{{params.dbenv}}_nap_utl.data_timeliness_metric_fact_ld`('PRODUCT_UPC_DIM',  '{{params.dbenv}}_NAP_DIM',  'PRODUCT_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1',  'dq_timeliness_start',  1,  'LOAD_START',  '',  CURRENT_DATETIME('PST8PDT'),  'NAP_PRODUCT_LOAD');


CALL `{{params.gcp_project_id}}.{{params.dbenv}}_nap_utl.data_timeliness_metric_fact_ld`('CASEPACK_SKU_XREF',  '{{params.dbenv}}_NAP_DIM',  'PRODUCT_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1',  'dq_timeliness_start',  1,  'LOAD_START',  '',  CURRENT_DATETIME('PST8PDT'),  'NAP_PRODUCT_LOAD');


