--TIMELINESS METRICS START FOR PRODUCT_SKU_DIM
  CALL `{{params.dataplex_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD
   ('PRODUCT_SKU_DIM',
    '{{params.dbenv}}_NAP_DIM',
    'PRODUCT_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1',
    'dq_timeliness_start',
    1,'LOAD_START', '',
    current_datetime('PST8PDT'),
    'NAP_PRODUCT_LOAD');

--TIMELINESS METRICS START FOR PRODUCT_STYLE_DIM
  CALL `{{params.dataplex_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD
   ('PRODUCT_STYLE_DIM',
    '{{params.dbenv}}_NAP_DIM',
    'PRODUCT_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1',
    'dq_timeliness_start',
    1,'LOAD_START', '',
    current_datetime('PST8PDT'),
    'NAP_PRODUCT_LOAD');

--TIMELINESS METRICS START FOR PRODUCT_CHOICE_DIM
  CALL `{{params.dataplex_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD
   ('PRODUCT_CHOICE_DIM',
    '{{params.dbenv}}_NAP_DIM',
    'PRODUCT_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1',
    'dq_timeliness_start',
    1,'LOAD_START', '',
    current_datetime('PST8PDT'),
    'NAP_PRODUCT_LOAD');

--TIMELINESS METRICS START FOR PRODUCT_UPC_DIM
  CALL `{{params.dataplex_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD
   ('PRODUCT_UPC_DIM',
    '{{params.dbenv}}_NAP_DIM',
    'PRODUCT_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1',
    'dq_timeliness_start',
    1,'LOAD_START', '',
    current_datetime('PST8PDT'),
    'NAP_PRODUCT_LOAD');

--TIMELINESS METRICS START FOR CASEPACK_SKU_XREF
  CALL `{{params.dataplex_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD
   ('CASEPACK_SKU_XREF',
    '{{params.dbenv}}_NAP_DIM',
    'PRODUCT_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1',
    'dq_timeliness_start',
    1,'LOAD_START', '',
    current_datetime('PST8PDT'),
    'NAP_PRODUCT_LOAD');

