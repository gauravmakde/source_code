CALL
  `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD
  ('MERCH_COST_VARIANCE_LEDGER_TRANSFER_WRK',
    '{{params.database_name_staging}}',
    'mfp_costvarianceledger_kafka_to_teradata',
    'load_to_transfer_fact_03',
    0,
    'LOAD_START',
    'Clearing WRK table',
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
    '');
TRUNCATE TABLE
  `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_cost_variance_ledger_transfer_wrk;
CALL
  `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_COST_VARIANCE_LEDGER_TRANSFER_WRK',
    '{{params.database_name_staging}}',
    'mfp_costvarianceledger_kafka_to_teradata',
    'load_to_transfer_fact_03',
    1,
    'INTERMEDIATE',
    'Populating WRK table',
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
    '');
INSERT INTO
  `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_cost_variance_ledger_transfer_wrk (location_id,
    last_updated_time_in_millis,
    last_updated_time,
    last_updated_time_tz,
    sku_id,
    tran_date,
    event_id,
    event_time,
    correlation_id,
    transfer_num,
    tran_code,
    quantity,
    dept_num,
    class_num,
    subclass_num,
    sku_type,
    total_cost_currcycd,
    total_cost_amt,
    total_retail_currcycd,
    total_retail_amt,
    dw_sys_load_tmstp,
    dw_sys_updt_tmstp) (
  SELECT
    location_id,
    CAST(CASE
        WHEN last_updated_time_in_millis = '' THEN '0'
        ELSE last_updated_time_in_millis
    END
      AS BIGINT) AS last_updated_time_in_millis,
    CAST(`{{params.gcp_project_id}}`.NORD_UDF.EPOCH_TMSTP(CAST(CASE
            WHEN last_updated_time_in_millis = '' THEN '0'
    
            ELSE last_updated_time_in_millis
        END
          AS BIGINT)) AS TIMESTAMP) AS last_updated_time,
    `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE( CAST(`{{params.gcp_project_id}}`.NORD_UDF.EPOCH_TMSTP(CAST(CASE
            WHEN last_updated_time_in_millis = '' THEN '0'
    
            ELSE last_updated_time_in_millis
        END
          AS BIGINT)) AS string)) AS last_updated_time_tz,
    sku_id,
    CAST(tran_date AS DATE) AS tran_date,
    event_id,
    CAST(`{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(event_time) AS DATETIME) AS event_time,
    correlation_id,
    reference_id AS transfer_num,
    CAST(CASE
        WHEN tran_code = '' THEN '0'
        ELSE tran_code
    END
      AS INTEGER) AS tran_code,
    CAST(TRUNC(CAST(CASE
        WHEN quantity = '' THEN '0'
        ELSE quantity 
    END AS float64))
      AS INTEGER) AS quantity,
    CAST(CASE
        WHEN dept_num = '' THEN '0'
        ELSE dept_num
    END
      AS INTEGER) AS dept_num,
    CAST(CASE
        WHEN class_num = '' THEN '0'
        ELSE class_num
    END
      AS INTEGER) AS class_num,
    CAST(CASE
        WHEN subclass_num = '' THEN '0'
        ELSE subclass_num
    END
      AS INTEGER) AS subclass_num,
    sku_type,
    total_cost_currcycd,
    ROUND(CAST(CAST(CASE
            WHEN total_cost_units = '' THEN '0'
            ELSE total_cost_units
        END
          AS INTEGER) + CAST(CASE
            WHEN total_cost_nanos = '' THEN '0'
            ELSE total_cost_nanos
        END
          AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_cost_amt,
    total_retail_currcycd,
    ROUND(CAST(CAST(CASE
            WHEN total_retail_units = '' THEN '0'
            ELSE total_retail_units
        END
          AS INTEGER) + CAST( trunc( cast(CASE
            WHEN total_retail_nanos = '' THEN '0'
            ELSE total_retail_nanos
        END as float64))
          AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_retail_amt,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_updt_tmstp
  FROM
    `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_cost_variance_ledger_ldg
  WHERE
    LOWER(event_type) = LOWER('TRANSFER'));



CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_COST_VARIANCE_LEDGER_TRANSFER_FCT',
    '{{params.database_name_fact}}',
    'mfp_costvarianceledger_kafka_to_teradata',
    'load_to_transfer_fact_03',
    2,
    'INTERMEDIATE',
    'Clearing fact table',
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
    '');


DELETE FROM `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_cost_variance_ledger_transfer_fct AS tgt
WHERE
  EXISTS (
  SELECT
    *
  FROM
    `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_cost_variance_ledger_transfer_wrk AS src
  WHERE
    LOWER(tgt.location_id) = LOWER(location_id)
    AND LOWER(tgt.sku_id) = LOWER(sku_id)
    AND tgt.tran_date = tran_date
    AND tgt.event_id = event_id);

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD
('MERCH_COST_VARIANCE_LEDGER_TRANSFER_FCT',
    '{{params.database_name_fact}}',
    'mfp_costvarianceledger_kafka_to_teradata',
    'load_to_transfer_fact_03',
    3,
    'INTERMEDIATE',
    'Filling fact table',
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
    '');

INSERT INTO
  `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_cost_variance_ledger_transfer_fct (location_id,
    last_updated_time_in_millis,
    last_updated_time,
    last_updated_time_tz,
    sku_id,
    tran_date,
    event_id,
    event_time,
    correlation_id,
    transfer_num,
    tran_code,
    quantity,
    dept_num,
    class_num,
    subclass_num,
    sku_type,
    total_cost_currcycd,
    total_cost_amt,
    total_retail_currcycd,
    total_retail_amt,
    dw_sys_load_tmstp,
    dw_sys_updt_tmstp) (
  SELECT
    *
  FROM
    `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_cost_variance_ledger_transfer_wrk);


CALL
  `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_COST_VARIANCE_LEDGER_TRANSFER_FCT',
    '{{params.database_name_fact}}',
    'mfp_costvarianceledger_kafka_to_teradata',
    'load_to_transfer_fact_03',
    4,
    'LOAD_END',
    '',
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
    '');