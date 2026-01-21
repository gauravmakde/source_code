
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=mfpc_transferledger_kafka_to_teradata_10976_tech_nap_merch;
---Task_Name=mfpc_transferledger_kafka_to_teradata_10976_tech_nap_merch_job_0005;'*/


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_TRANSFER_LEDGER_FACT',  '{{params.dbenv}}_NAP_FCT',  'mfpc_transferledger_kafka_to_teradata',  'mfpc_transferledger_kafka_to_teradata_job_0005',  0,  'LOAD_START',  'Delete delta data from fact table',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  'TRANSFERLEDGER');


DELETE FROM `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_transfer_ledger_fact AS tgt
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_transfer_ledger_ldg AS stg
    WHERE LOWER(tgt.transfer_ledger_id) = LOWER(transfer_ledger_id) 
	AND LOWER(tgt.transfer_operation_type) = LOWER(transfer_operation_type) 
	AND LOWER(tgt.sku_num) = LOWER(sku_num) AND tgt.location_num = CAST(location_num AS FLOAT64) AND LOWER(tgt.sku_type) = LOWER(sku_type) AND tgt.event_id = event_id AND tgt.transaction_date = CAST(transaction_date AS DATE) AND LOWER(last_updated_time_in_millis) <> LOWER('LAST_UPDATED_TIME_IN_MILLIS') AND tgt.last_updated_time_in_millis < CAST(last_updated_time_in_millis AS BIGINT));

DELETE FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_transfer_ledger_ldg AS stg
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_transfer_ledger_fact AS tgt
    WHERE LOWER(stg.transfer_ledger_id) = LOWER(transfer_ledger_id) AND LOWER(stg.transfer_operation_type) = LOWER(transfer_operation_type) AND LOWER(stg.sku_type) = LOWER(sku_type) AND CAST(stg.location_num AS FLOAT64) = location_num AND LOWER(stg.sku_num) = LOWER(sku_num) AND stg.event_id = event_id AND CAST(stg.transaction_date AS DATE) = transaction_date AND LOWER(stg.last_updated_time_in_millis) <> LOWER('LAST_UPDATED_TIME_IN_MILLIS') AND CAST(stg.last_updated_time_in_millis  AS BIGINT) <= last_updated_time_in_millis);

INSERT INTO `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_transfer_ledger_fact (transfer_ledger_id, last_updated_time_in_millis, last_updated_time, last_updated_time_tz,
 transfer_operation_type, event_id, event_time, event_time_tz, shipment_num, event_type, location_num, department_num, class_num,
 subclass_num, sku_type, sku_num, transaction_date, transaction_code, quantity, total_cost_currency_code,
 total_cost_amount, total_retail_currency_code, total_retail_amount, dw_sys_load_tmstp, dw_sys_load_tmstp_tz, dw_sys_updt_tmstp, dw_sys_updt_tmstp_tz)
(SELECT transfer_ledger_id,
  CAST(TRUNC(CAST(last_updated_time_in_millis AS FLOAT64)) AS INT64),
  CAST(`{{params.gcp_project_id}}`.NORD_UDF.EPOCH_TMSTP(CAST(last_updated_time_in_millis AS BIGINT)) AS TIMESTAMP) AS last_updated_time,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(`{{params.gcp_project_id}}`.NORD_UDF.EPOCH_TMSTP(CAST(last_updated_time_in_millis AS BIGINT)) AS STRING)) AS   last_updated_time_tz, 
  transfer_operation_type,
  event_id,
  CAST(`{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(event_time) AS TIMESTAMP) AS event_time,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(`{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(event_time)) as event_time_tz,
  CAST(shipment_num AS BIGINT) AS shipment_num,
  event_type,
  CAST(TRUNC(CAST(location_num AS FLOAT64)) AS INTEGER) AS location_num,
  CAST(TRUNC(CAST(department_num AS FLOAT64)) AS INTEGER) AS department_num,
  CAST(TRUNC(CAST(class_num AS FLOAT64)) AS INTEGER) AS class_num,
  CAST(TRUNC(CAST(subclass_num AS FLOAT64)) AS INTEGER) AS subclass_num,
  sku_type,
  sku_num,
  CAST(transaction_date AS DATE) AS transaction_date,
  transaction_code,
  CAST(TRUNC(CAST(quantity AS FLOAT64)) AS INTEGER) AS quantity,
  total_cost_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(total_cost_units AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(total_cost_nanos AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_cost_amount,
  total_retail_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(total_retail_units AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(total_retail_nanos AS FLOAT64))
  AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_retail_amount,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS
  dw_sys_load_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as dw_sys_load_tmstp_tz,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS
  dw_sys_updt_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as dw_sys_updt_tmstp_tz,
 FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_transfer_ledger_ldg
 WHERE LOWER(event_id) <> LOWER('EVENT_ID')
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY transfer_ledger_id, transfer_operation_type, sku_num, sku_type, location_num,
       transaction_date, event_id ORDER BY CAST(last_updated_time_in_millis AS BIGINT) DESC)) = 1);


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_TRANSFER_LEDGER_FACT',  '{{params.dbenv}}_NAP_FCT',  'mfpc_transferledger_kafka_to_teradata',  'mfpc_transferledger_kafka_to_teradata_job_0005',  1,  'LOAD_END',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  'TRANSFERLEDGER');


