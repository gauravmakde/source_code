BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=project_mfp_reclass_kafka_to_td_tpt;
---Task_Name=read_reclass_from_kafka_and_write_to_teradata_job_3;'*/
---FOR SESSION VOLATILE;

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_RECLASS_LEDGER_FACT',  '{{params.dbenv}}_NAP_FCT',  'project_mfp_reclass_kafka_to_td_tpt',  'read_reclass_from_kafka_and_write_to_teradata_job_3',  0,  'LOAD_START',  'Delete delta data from fact table',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  'RECLASS');

BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_reclass_ledger_fact AS tgt
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_reclass_ledger_ldg AS stg
    WHERE LOWER(tgt.sku_num) = LOWER(sku_num) AND LOWER(sku_num_type) <> LOWER('sku_num_type') AND tgt.tran_date = CAST(tran_date AS DATE) AND tgt.last_updated_time_in_millis < CAST(CASE WHEN last_updated_time_in_millis = '' THEN '0' ELSE last_updated_time_in_millis END AS BIGINT));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_reclass_ledger_ldg AS stg
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_reclass_ledger_fact AS tgt
    WHERE LOWER(sku_num) = LOWER(stg.sku_num) AND LOWER(stg.sku_num_type) <> LOWER('sku_num_type') AND tran_date = CAST(stg.tran_date AS DATE) AND CAST(CASE WHEN stg.last_updated_time_in_millis = '' THEN '0' ELSE stg.last_updated_time_in_millis END AS BIGINT) <= last_updated_time_in_millis);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_reclass_ledger_fact (sku_num, sku_num_type, tran_date, last_updated_time_in_millis,
 last_updated_time, event_num, event_time, pack_indicator, store_num, dept_num, class_num, subclass_num,
 calendar_tran_date, tran_code, quantity, total_cost_currency_code, total_reclass_cost, dw_sys_load_tmstp,
 dw_sys_updt_tmstp)
(SELECT sku_num,
  sku_num_type,
  CAST(tran_date AS DATE) AS tran_date,
  CAST(last_updated_time_in_millis AS BIGINT) AS last_updated_time_in_millis,
  CAST(NORD_UDF.EPOCH_TMSTP(CAST(trunc(cast(
     CASE WHEN last_updated_time_in_millis = ''
      THEN '0'
      ELSE last_updated_time_in_millis
      END as float64)) AS INTEGER)) AS TIMESTAMP) AS last_updated_time,
  event_num,
  CAST(`{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(event_time) AS TIMESTAMP) AS event_time,
  pack_indicator,
  CAST(TRUNC(CAST(store_num AS FLOAT64)) AS INTEGER) AS store_num,
  CAST(TRUNC(CAST(dept_num AS FLOAT64)) AS INTEGER) AS dept_num,
  CAST(TRUNC(CAST(class_num AS FLOAT64)) AS INTEGER) AS class_num,
  subclass_num,
  calendar_tran_date,
  tran_code,
  quantity,
  total_cost_currency_code,
  ROUND(CAST(ROUND(CAST(CAST(trunc(cast(CASE
         WHEN total_cost_units = ''
         THEN '0'
         ELSE total_cost_units
         END AS INTEGER) + CAST(CASE
          WHEN total_cost_nanos = ''
          THEN '0'
          ELSE total_cost_nanos
          END as float64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS NUMERIC), 4) AS total_reclass_cost,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS
  dw_sys_load_tmstp,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS
  dw_sys_updt_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_reclass_ledger_ldg
 WHERE LOWER(sku_num_type) <> LOWER('sku_num_type')
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY sku_num, tran_date, event_num ORDER BY CAST(trunc(cast(CASE
         WHEN CAST(last_updated_time_in_millis AS STRING) = ''
         THEN '0'
         ELSE CAST(last_updated_time_in_millis AS STRING)
         END as float64)) AS BIGINT) DESC)) = 1);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_RECLASS_LEDGER_FACT',  '{{params.dbenv}}_NAP_FCT',  'project_mfp_reclass_kafka_to_td_tpt',  'read_reclass_from_kafka_and_write_to_teradata_job_3',  1,  'LOAD_END',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  'RECLASS');

/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
