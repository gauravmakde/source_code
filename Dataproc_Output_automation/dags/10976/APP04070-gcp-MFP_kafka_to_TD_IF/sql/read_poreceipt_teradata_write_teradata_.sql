CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_PORECEIPT_LEDGER_FACT',  '{{params.dbenv}}_NAP_FCT',  'mfp_poreceipt_kafka_to_teradata_10976_tech_nap_merch',  'poreceipt_kafka_to_td_tpt_stage_1_job_2',  0,  'LOAD_START',  'Delete delta data from fact table',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  'PORECIEPT');


DELETE FROM `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_poreceipt_ledger_fact AS tgt
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_poreceipt_ledger_ldg AS stg
    WHERE LOWER(poreceipt_order_number) <> LOWER('PORECEIPT_ORDER_NUMBER') AND LOWER(tgt.poreceipt_order_number) = LOWER(poreceipt_order_number) AND LOWER(tgt.sku_num) = LOWER(sku_num) AND tgt.store_num = CAST(store_num AS FLOAT64) AND tgt.event_id = event_id AND tgt.tran_date = CAST(tran_date AS DATE) AND tgt.last_updated_time_in_millis < CAST(CASE WHEN last_updated_time_in_millis = '' THEN '0' ELSE last_updated_time_in_millis END AS BIGINT));




DELETE FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_poreceipt_ledger_ldg AS stg
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_poreceipt_ledger_fact AS tgt
    WHERE LOWER(SUBSTR(stg.poreceipt_order_number, 1, 30)) <> LOWER('PORECEIPT_ORDER_NUMBER') AND LOWER(stg.poreceipt_order_number) = LOWER(poreceipt_order_number) AND LOWER(stg.sku_num) = LOWER(sku_num) AND CAST(stg.store_num AS FLOAT64) = store_num AND stg.event_id = event_id AND CAST(stg.tran_date AS DATE) = tran_date AND CAST(CASE WHEN stg.last_updated_time_in_millis = '' THEN '0' ELSE stg.last_updated_time_in_millis END AS BIGINT) <= last_updated_time_in_millis);



INSERT INTO `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_poreceipt_ledger_fact (poreceipt_order_number, last_updated_time_in_millis,
 last_updated_time, event_id, event_time,event_time_tz, shipment_num, adjustment_type, store_num, department_num, class_num,
 subclass_num, sku_num_type, sku_num, tran_code, tran_date, quantity, total_cost_curr_code, total_cost_amount,
 total_retail_curr_code, total_retail_amount, dw_sys_load_tmstp, dw_sys_updt_tmstp)
(SELECT poreceipt_order_number,
  CAST(last_updated_time_in_millis AS BIGINT) AS last_updated_time_in_millis,
  CAST(`{{params.gcp_project_id}}`.NORD_UDF.EPOCH_TMSTP(CAST(TRUNC(CAST(CASE
      WHEN last_updated_time_in_millis = ''
      THEN '0'
      ELSE last_updated_time_in_millis
      END AS FLOAT64)) AS INTEGER)) AS DATETIME) AS last_updated_time,
  event_id,
  CAST(`{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(event_time) AS TIMESTAMP) AS event_time,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(`{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(event_time) AS STRING)) as event_time_tz,
  shipment_num,
  adjustment_type,
  CAST(TRUNC(CAST(store_num AS FLOAT64)) AS INTEGER) AS store_num,
  CAST(TRUNC(CAST(department_num AS FLOAT64)) AS INTEGER) AS department_num,
  CAST(TRUNC(CAST(class_num AS FLOAT64)) AS INTEGER) AS class_num,
  CAST(TRUNC(CAST(subclass_num AS FLOAT64)) AS INTEGER) AS subclass_num,
  sku_num_type,
  sku_num,
  CAST(TRUNC(CAST(tran_code AS FLOAT64)) AS INTEGER) AS tran_code,
  CAST(tran_date AS DATE) AS tran_date,
  CAST(TRUNC(CAST(quantity AS FLOAT64)) AS INTEGER) AS quantity,
  total_cost_curr_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN total_cost_units = ''
       THEN '0'
       ELSE total_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN total_cost_nanos = ''
        THEN '0'
        ELSE total_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_cost_amount,
  total_retail_curr_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN total_retail_units = ''
       THEN '0'
       ELSE total_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN total_retail_nanos = ''
        THEN '0'
        ELSE total_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_retail_amount,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_updt_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_poreceipt_ledger_ldg
 WHERE LOWER(poreceipt_order_number) <> LOWER('PORECEIPT_ORDER_NUMBER')
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY poreceipt_order_number, sku_num, store_num, tran_date, event_id
  ORDER BY CAST(CASE
         WHEN cast(last_updated_time_in_millis as string) = ''
         THEN '0'
         ELSE cast(last_updated_time_in_millis as string)
         END AS BIGINT) DESC)) = 1);



CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_PORECEIPT_LEDGER_FACT',  '{{params.dbenv}}_NAP_FCT',  'mfp_poreceipt_kafka_to_teradata_10976_tech_nap_merch',  'poreceipt_kafka_to_td_tpt_stage_1_job_2',  1,  'LOAD_END',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  'PORECIEPT');

