BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
-- SET QUERY_BAND = '
-- App_ID=APP04070;
-- DAG_ID=mfpc_mos_kafka_to_teradata;
-- Task_Name=job_mos_third_exec_2;'
-- FOR SESSION VOLATILE;

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_EXPENSE_TRANSFER_LEDGER_FACT',  '{{params.database_name_fact}}',  'mfpc_mos_kafka_to_teradata',  'job_mos_third_exec_2',  0,  'LOAD_START',  'Clearing fact table',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME()) AS DATETIME),  'MERCH_NAP_MOS_DLY');

-- This table to Test Teradata engine.
BEGIN
SET _ERROR_CODE  =  0;

DELETE FROM `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_expense_transfer_ledger_fact AS tgt
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_expense_transfer_ledger_ldg AS stg
    WHERE LOWER(tgt.general_ledger_reference_number) = LOWER(general_ledger_reference_number) AND LOWER(tgt.reason_code) = LOWER(reason_code) AND tgt.transaction_date = CAST(transaction_date AS DATE) AND LOWER(tgt.sku_num) = LOWER(sku_num) AND tgt.location_num = CAST(location_num AS FLOAT64) AND LOWER(last_updated_time_in_millis) <> LOWER('LAST_UPDATED_TIME_IN_MILLIS') AND tgt.last_updated_time_in_millis < CAST(CASE WHEN last_updated_time_in_millis = '' THEN '0' ELSE last_updated_time_in_millis END AS BIGINT));

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_EXPENSE_TRANSFER_LEDGER_LDG',  '{{params.database_name_staging}}',  'mfpc_mos_kafka_to_teradata',  'job_mos_third_exec_2',  1,  'INTERMEDIATE',  'Clearing stage table',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME()) AS DATETIME),  'MERCH_NAP_MOS_DLY');

BEGIN
SET _ERROR_CODE  =  0;

DELETE FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_expense_transfer_ledger_ldg AS stg
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_expense_transfer_ledger_fact AS tgt
    WHERE LOWER(stg.general_ledger_reference_number) = LOWER(general_ledger_reference_number) AND LOWER(stg.reason_code) = LOWER(reason_code) AND CAST(stg.transaction_date AS DATE) = transaction_date AND LOWER(stg.sku_num) = LOWER(sku_num) AND CAST(stg.location_num AS FLOAT64) = location_num AND LOWER(stg.last_updated_time_in_millis) <> LOWER('LAST_UPDATED_TIME_IN_MILLIS') AND CAST(CASE WHEN stg.last_updated_time_in_millis = '' THEN '0' ELSE stg.last_updated_time_in_millis END AS BIGINT) <= last_updated_time_in_millis);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_EXPENSE_TRANSFER_LEDGER_FACT',  '{{params.database_name_fact}}',  'mfpc_mos_kafka_to_teradata',  'job_mos_third_exec_2',  2,  'INTERMEDIATE',  'Filling fact table',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME()) AS DATETIME),  'MERCH_NAP_MOS_DLY');

BEGIN
SET _ERROR_CODE  =  0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_expense_transfer_ledger_fact (general_ledger_reference_number, reason_code,
 last_updated_time_in_millis, last_updated_time,last_updated_time_tz, event_time,event_time_tz, event_id, location_num, department_num, class_num,
 subclass_num, sku_type, sku_num, transaction_date, transaction_code, quantity, total_cost_currency_code,
 total_cost_amount, total_retail_currency_code, total_retail_amount, dw_sys_load_tmstp,dw_sys_load_tmstp_tz, dw_sys_updt_tmstp,dw_sys_updt_tmstp_tz)
(SELECT general_ledger_reference_number,
  reason_code,
  CAST(last_updated_time_in_millis AS BIGINT) AS last_updated_time_in_millis,
  CAST(`{{params.gcp_project_id}}`.NORD_UDF.EPOCH_TMSTP(CAST(CASE
      WHEN last_updated_time_in_millis = ''
      THEN '0'
      ELSE last_updated_time_in_millis
      END AS BIGINT)) AS TIMESTAMP) AS last_updated_time,
`{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(`{{params.gcp_project_id}}`.NORD_UDF.EPOCH_TMSTP(CAST(CASE
      WHEN last_updated_time_in_millis = ''
      THEN '0'
      ELSE last_updated_time_in_millis
      END AS BIGINT)) AS STRING)) as last_updated_time_tz ,
  CAST(`{{params.gcp_project_id}}`.NORD_UDF.ISO8601_TMSTP(event_time) AS TIMESTAMP) AS event_time,
`{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(NORD_UDF.ISO8601_TMSTP(event_time) AS STRING)) as event_time_tz,
  event_id,
  CAST(TRUNC(CAST(location_num AS FLOAT64)) AS INTEGER) AS location_num,
  CAST(TRUNC(CAST(department_num AS FLOAT64)) AS INTEGER) AS department_num,
  CAST(TRUNC(CAST(class_num AS FLOAT64)) AS INTEGER) AS class_num,
  CAST(TRUNC(CAST(subclass_num AS FLOAT64)) AS INTEGER) AS subclass_num,
  sku_type,
  sku_num,
  CAST(transaction_date AS DATE) AS transaction_date,
  transaction_code,
  ROUND(CAST(quantity AS NUMERIC), 0) AS quantity,
  total_cost_currency_code,
    ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN total_cost_units = ''
       THEN '0'
       ELSE total_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(cast(CASE
        WHEN total_cost_nanos = ''
        THEN '0'
        ELSE total_cost_nanos
        END as float64) ) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_cost_amount,
  total_retail_currency_code,
  ROUND(CAST(CAST(TRUNC(cast(CASE
       WHEN total_retail_units = ''
       THEN '0'
       ELSE total_retail_units
       END as float64) ) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN total_retail_nanos = ''
        THEN '0'
        ELSE total_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_retail_amount,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS dw_sys_load_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS  dw_sys_load_tmstp_tz,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS  dw_sys_updt_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS  dw_sys_updt_tmstp_tz
 FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_expense_transfer_ledger_ldg
 WHERE LOWER(event_id) <> LOWER('event_id')
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY general_ledger_reference_number, reason_code, transaction_date, sku_num,
       location_num, event_id ORDER BY CAST(LAST_UPDATED_TIME_IN_MILLIS AS BIGINT))) = 1);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_EXPENSE_TRANSFER_LEDGER_FACT',  '{{params.database_name_fact}}',  'mfpc_mos_kafka_to_teradata',  'job_mos_third_exec_2',  3,  'LOAD_END',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME()) AS DATETIME),  'MERCH_NAP_MOS_DLY');

-- SET QUERY_BAND = NONE FOR SESSION;
END;
