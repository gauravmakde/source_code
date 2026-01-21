BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
-- /*SET QUERY_BAND = '
-- App_ID=APP04070;
-- DAG_ID=mfp_rtvcostadjustmentledger_kafka_to_teradata;
-- ---Task_Name=rtv_cost_adjustment_03_load_fact_table;'*/


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_RTV_COST_ADJUSTMENT_LEDGER_WRK',  '{{params.database_name_staging}}',  'mfp_rtvcostadjustmentledger_kafka_to_teradata',  'load_fact_table',  0,  'LOAD_START',  'Clearing WRK table',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  '');

-- COMMIT TRANSACTION;

BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_rtv_cost_adjustment_ledger_wrk;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
-- COMMIT TRANSACTION;


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_RTV_COST_ADJUSTMENT_LEDGER_WRK',  '{{params.database_name_staging}}',  'mfp_rtvcostadjustmentledger_kafka_to_teradata',  'load_fact_table',  1,  'INTERMEDIATE',  'Populating WRK table',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  '');

--COMMIT TRANSACTION;


BEGIN

SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_rtv_cost_adjustment_ledger_wrk (rtv_num, location_id, last_updated_time_in_millis,
 last_updated_time,last_updated_time_tz, sku_id, tran_date, event_id, event_time, correlation_id, reference_num, tran_code, quantity,
 dept_num, class_num, subclass_num, sku_type, total_cost_currcycd, total_cost_amt, total_retail_currcycd,
 total_retail_amt, dw_sys_load_tmstp, dw_sys_updt_tmstp)
(SELECT rtv_num,
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
      cast(`{{params.gcp_project_id}}`.NORD_UDF.EPOCH_TMSTP(CAST(CASE
      WHEN last_updated_time_in_millis = ''
      THEN '0'
      ELSE last_updated_time_in_millis
      END AS BIGINT)) as string) AS last_updated_time_tz,
  sku_id,
  CAST(tran_date AS DATE) AS tran_date,
  event_id,
 cast(`{{params.gcp_project_id}}`.NORD_UDF.EPOCH_TMSTP(cast(trunc(cast(event_time as float64)) as integer)) as datetime) AS event_time,
  correlation_id,
  reference_id AS reference_num,
  cast(trunc(cast(CASE WHEN tran_code = '' THEN '0' ELSE tran_code END  as float64)) as integer) AS tran_code,
  cast(trunc(cast(CASE WHEN quantity = '' THEN '0' ELSE quantity END  as float64)) as integer) AS quantity,
  cast(trunc(cast(CASE WHEN dept_num = '' THEN '0' ELSE dept_num END  as float64)) as integer) AS dept_num,
  cast(trunc(cast(CASE WHEN class_num = '' THEN '0' ELSE class_num END  as float64)) as integer) AS class_num,
  cast(trunc(cast(CASE WHEN subclass_num = '' THEN '0' ELSE subclass_num END  as float64)) as integer) AS subclass_num,
  sku_type,
  total_cost_currcycd,
  ROUND(CAST(cast(trunc(cast(CASE WHEN total_cost_units = '' THEN '0' ELSE total_cost_units END  as float64)) as integer)
  + cast(trunc(cast(CASE  WHEN total_cost_nanos = '' THEN '0' ELSE total_cost_nanos END  as float64)) as integer) / POWER(10, 9) AS NUMERIC), 9) AS total_cost_amt,
  total_retail_currcycd,
  ROUND(CAST(cast(trunc(cast(CASE WHEN total_retail_units = '' THEN '0' ELSE total_retail_units END  as float64)) as integer)
  + cast(trunc(cast(CASE WHEN total_retail_nanos = '' THEN '0' ELSE total_retail_nanos END  as float64)) as integer) / POWER(10, 9) AS NUMERIC), 9) AS total_retail_amt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_updt_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_rtv_cost_adjustment_ledger_ldg
 WHERE LOWER(UPPER(event_id)) <> LOWER('EVENT_ID'));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


--COLLECT STATISTICS USING MAXVALUELENGTH 130 COLUMN(RTV_NUM ,LOCATION_ID ,SKU_ID ,TRAN_DATE ,EVENT_ID) ON `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_STG.MERCH_RTV_COST_ADJUSTMENT_LEDGER_WRK;
--COMMIT TRANSACTION;


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_RTV_COST_ADJUSTMENT_LEDGER_FCT',  '{{params.database_name_fact}}',  'mfp_rtvcostadjustmentledger_kafka_to_teradata',  'load_fact_table',  2,  'INTERMEDIATE',  'Clearing fact table',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  '');

-- COMMIT TRANSACTION;

BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_rtv_cost_adjustment_ledger_fct AS tgt
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_rtv_cost_adjustment_ledger_wrk AS src
    WHERE LOWER(tgt.location_id) = LOWER(location_id) AND LOWER(tgt.sku_id) = LOWER(sku_id) AND tgt.tran_date = tran_date AND tgt.event_id = event_id AND LOWER(tgt.rtv_num) = LOWER(rtv_num));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COMMIT TRANSACTION;


 CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_RTV_COST_ADJUSTMENT_LEDGER_FCT',  '{{params.database_name_fact}}',  'mfp_rtvcostadjustmentledger_kafka_to_teradata',  'load_fact_table',  3,  'INTERMEDIATE',  'Filling fact table',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  '');

-- COMMIT TRANSACTION;

BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_rtv_cost_adjustment_ledger_fct (rtv_num, location_id, last_updated_time_in_millis,
 last_updated_time,last_updated_time_tz, sku_id, tran_date, event_id, event_time, correlation_id, reference_num, tran_code, quantity,
 dept_num, class_num, subclass_num, sku_type, total_cost_currcycd, total_cost_amt, total_retail_currcycd,
 total_retail_amt, dw_sys_load_tmstp, dw_sys_updt_tmstp)
(SELECT rtv_num,
  location_id,
  last_updated_time_in_millis,
  last_updated_time,
  last_updated_time_tz,
  sku_id,
  tran_date,
  event_id,
  event_time,
  correlation_id,
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
 FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_rtv_cost_adjustment_ledger_wrk);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COMMIT TRANSACTION;

--COLLECT STATISTICS USING MAXVALUELENGTH 130 COLUMN(RTV_NUM,LOCATION_ID,SKU_ID,TRAN_DATE,EVENT_ID), COLUMN(RTV_NUM), COLUMN(LOCATION_ID), COLUMN(SKU_ID), COLUMN(TRAN_DATE), COLUMN(EVENT_ID), COLUMN(TRAN_DATE,PARTITION) ON `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.MERCH_RTV_COST_ADJUSTMENT_LEDGER_FCT;
--COMMIT TRANSACTION;


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_RTV_COST_ADJUSTMENT_LEDGER_FCT',  '{{params.database_name_fact}}',  'mfp_rtvcostadjustmentledger_kafka_to_teradata',  'load_fact_table',  4,  'LOAD_END',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  '');

-- COMMIT TRANSACTION;

/*SET QUERY_BAND = NONE FOR SESSION;*/
-- COMMIT TRANSACTION;

END;
