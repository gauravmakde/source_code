BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=mfp_poclaimallowanceledger_kafka_to_teradata;
---Task_Name=load_fact_table_04;'*/
---FOR SESSION VOLATILE;

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_PO_CLAIM_ALLOWANCE_LEDGER_WRK',  '{{params.database_name_staging}}',  'mfp_poclaimallowanceledger_kafka_to_teradata',  'load_fact_table_04',  0,  'LOAD_START',  'Delete from WRK table',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  '');

BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_po_claim_allowance_ledger_wrk;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_PO_CLAIM_ALLOWANCE_LEDGER_WRK',  '{{params.database_name_staging}}',  'mfp_poclaimallowanceledger_kafka_to_teradata',  'load_fact_table_04',  1,  'INTERMEDIATE',  'Filling WRK table',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  '');

BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_po_claim_allowance_ledger_wrk (po_num, invoice_num, location_id,
 last_updated_time_in_millis, last_updated_time, last_updated_time_tz, sku_id, tran_date, event_id, event_time, reference_num, tran_code,
 quantity, dept_num, class_num, subclass_num, sku_type, total_cost_currcycd, total_cost_amt, total_retail_currcycd,
 total_retail_amt, dw_sys_load_tmstp, dw_sys_updt_tmstp)
(SELECT po_num,
  invoice_num,
  location_id,
  CAST(CASE
    WHEN last_updated_time_in_millis = ''
    THEN '0'
    ELSE last_updated_time_in_millis
    END AS BIGINT) AS last_updated_time_in_millis,


  CAST(`{{params.gcp_project_id}}`.NORD_UDF.EPOCH_TMSTP(CAST(CASE
      WHEN last_updated_time_in_millis = ''
      THEN '0'
      ELSE last_updated_time_in_millis
      END AS BIGINT)) AS TIMESTAMP) AS last_updated_time,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(`{{params.gcp_project_id}}`.NORD_UDF.EPOCH_TMSTP(CAST(CASE
      WHEN last_updated_time_in_millis = ''
      THEN '0'
      ELSE last_updated_time_in_millis
      END AS BIGINT))AS STRING)) AS last_updated_time_tz,
  sku_id,
  CAST(tran_date AS DATE) AS tran_date,
  event_id,
  CAST(`{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(event_time) AS DATETIME) AS event_time,
  reference_id AS reference_num,
  CAST(TRUNC(CAST(CASE
    WHEN tran_code = ''
    THEN '0'
    ELSE tran_code
    END AS FLOAT64)) AS INTEGER) AS tran_code,
  CAST(TRUNC(CAST(CASE
    WHEN quantity = ''
    THEN '0'
    ELSE quantity
    END AS FLOAT64)) AS INTEGER) AS quantity,
  CAST(TRUNC(CAST(CASE
    WHEN dept_num = ''
    THEN '0'
    ELSE dept_num
    END AS FLOAT64)) AS INTEGER) AS dept_num,
  CAST(TRUNC(CAST(CASE
    WHEN class_num = ''
    THEN '0'
    ELSE class_num
    END AS FLOAT64)) AS INTEGER) AS class_num,
  CAST(TRUNC(CAST(CASE
    WHEN subclass_num = ''
    THEN '0'
    ELSE subclass_num
    END AS FLOAT64)) AS INTEGER) AS subclass_num,
  sku_type,
  total_cost_currcycd,
  ROUND(CAST(CAST(CASE
       WHEN total_cost_units = ''
       THEN '0'
       ELSE total_cost_units
       END AS INTEGER) + CAST(CASE
        WHEN total_cost_nanos = ''
        THEN '0'
        ELSE total_cost_nanos
        END AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_cost_amt,
  total_retail_currcycd,
  ROUND(CAST(CAST(CASE
       WHEN total_retail_units = ''
       THEN '0'
       ELSE total_retail_units
       END AS INTEGER) + CAST(CASE
        WHEN total_retail_nanos = ''
        THEN '0'
        ELSE total_retail_nanos
        END AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_retail_amt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_updt_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_po_claim_allowance_ledger_ldg
 WHERE LOWER(UPPER(event_id)) <> LOWER('EVENT_ID'));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS USING MAXVALUELENGTH 130 COLUMN(PO_NUM,INVOICE_NUM,LOCATION_ID,SKU_ID,TRAN_DATE,EVENT_ID) ON PRD_NAP_STG.MERCH_PO_CLAIM_ALLOWANCE_LEDGER_WRK;

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_PO_CLAIM_ALLOWANCE_LEDGER_FCT',  '{{params.database_name_fact}}',  'mfp_poclaimallowanceledger_kafka_to_teradata',  'load_fact_table_04',  2,  'INTERMEDIATE',  'Delete from fact table',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  '');

BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_po_claim_allowance_ledger_fct AS tgt
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_po_claim_allowance_ledger_wrk AS src
    WHERE LOWER(tgt.location_id) = LOWER(location_id) AND LOWER(tgt.sku_id) = LOWER(sku_id) AND tgt.tran_date = tran_date AND tgt.event_id = event_id AND LOWER(tgt.invoice_num) = LOWER(invoice_num) AND LOWER(tgt.po_num) = LOWER(po_num));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_PO_CLAIM_ALLOWANCE_LEDGER_FCT',  '{{params.database_name_fact}}',  'mfp_poclaimallowanceledger_kafka_to_teradata',  'load_fact_table_04',  3,  'INTERMEDIATE',  'Filling fact table',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  '');

BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_po_claim_allowance_ledger_fct (po_num, invoice_num, location_id,
 last_updated_time_in_millis, last_updated_time, last_updated_time_tz, sku_id, tran_date, event_id, event_time, reference_num, tran_code,
 quantity, dept_num, class_num, subclass_num, sku_type, total_cost_currcycd, total_cost_amt, total_retail_currcycd,
 total_retail_amt, dw_sys_load_tmstp, dw_sys_updt_tmstp)
(SELECT po_num,
  invoice_num,
  location_id,
  last_updated_time_in_millis,
  last_updated_time,
  last_updated_time_tz,
  sku_id,
  tran_date,
  event_id,
  event_time,
  reference_num,
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
  dw_sys_updt_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_po_claim_allowance_ledger_wrk
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY po_num, invoice_num, event_id ORDER BY CAST(last_updated_time_in_millis AS BIGINT)
       DESC)) = 1);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS USING MAXVALUELENGTH 130 COLUMN(PO_NUM,INVOICE_NUM,LOCATION_ID,SKU_ID,TRAN_DATE,EVENT_ID), COLUMN(TRAN_DATE), COLUMN(TRAN_DATE,PARTITION) ON PRD_NAP_FCT.MERCH_PO_CLAIM_ALLOWANCE_LEDGER_FCT;

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_PO_CLAIM_ALLOWANCE_LEDGER_FCT',  '{{params.database_name_fact}}',  'mfp_poclaimallowanceledger_kafka_to_teradata',  'load_fact_table_04',  4,  'LOAD_END',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  '');

/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
