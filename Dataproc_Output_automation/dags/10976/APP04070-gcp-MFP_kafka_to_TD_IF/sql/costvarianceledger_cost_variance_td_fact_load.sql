
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=mfp_costvarianceledger_kafka_to_teradata_10976_tech_nap_merch;
---Task_Name=cost_variance_ledger_03_load_to_cost_variance_fact_03;'*/
BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
BEGIN
SET _ERROR_CODE  =  0;

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_COST_VARIANCE_LEDGER_WRK',  '{{params.database_name_staging}}',  'mfp_costvarianceledger_kafka_to_teradata',  'load_to_cost_variance_fact_03',  0,  'LOAD_START',  'Clearing WRK table',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  '');


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_cost_variance_ledger_wrk;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_COST_VARIANCE_LEDGER_WRK',  '{{params.database_name_staging}}',  'mfp_costvarianceledger_kafka_to_teradata',  'load_to_cost_variance_fact_03',  1,  'INTERMEDIATE',  'Populating WRK table',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  '');

INSERT INTO
  `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_cost_variance_ledger_wrk ( 
    location_id,
    last_updated_time_in_millis,
    last_updated_time,last_updated_time_tz,
    sku_id,
    tran_date,
    event_id,
    event_time,
    correlation_id,
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
    )
(SELECT location_id,
  CAST(TRUNC(CAST(CASE WHEN last_updated_time_in_millis = '' THEN '0' ELSE last_updated_time_in_millis END AS FLOAT64)) AS INTEGER) AS last_updated_time_in_millis,
  CAST(`{{params.gcp_project_id}}`.NORD_UDF.EPOCH_TMSTP(cast(CAST(trunc(cast(CASE WHEN last_updated_time_in_millis = '' THEN '0' ELSE last_updated_time_in_millis END as float64)) AS BIGINT) as int64)) AS TIMESTAMP) AS last_updated_time,

  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(CAST(`{{params.gcp_project_id}}`.NORD_UDF.EPOCH_TMSTP(cast(CAST(trunc(cast(CASE WHEN last_updated_time_in_millis = '' THEN '0' ELSE last_updated_time_in_millis END as float64)) AS BIGINT) as int64)) AS TIMESTAMP) as string)) as last_updated_time_tz,

  sku_id,
  CAST(tran_date AS DATE) AS tran_date,
  event_id,
  CAST(`{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(event_time) AS DATETIME) AS event_time,
  correlation_id,
  CAST(TRUNC(CAST(CASE WHEN tran_code = '' THEN '0' ELSE tran_code END AS FLOAT64)) AS INTEGER) AS tran_code,
  CAST(TRUNC(CAST(CASE WHEN quantity = '' THEN '0' ELSE quantity END AS FLOAT64)) AS INTEGER) AS quantity,
  CAST(TRUNC(CAST(CASE WHEN dept_num = '' THEN '0' ELSE dept_num END AS FLOAT64)) AS INTEGER) AS dept_num,
  CAST(TRUNC(CAST(CASE WHEN class_num = '' THEN '0' ELSE class_num END AS FLOAT64)) AS INTEGER) AS class_num,
  CAST(TRUNC(CAST(CASE WHEN subclass_num = '' THEN '0' ELSE subclass_num END AS FLOAT64)) AS INTEGER) AS subclass_num,
  sku_type,
  total_cost_currcycd,
  ROUND(CAST(CAST(TRUNC(CAST(CASE WHEN total_cost_units = '' THEN '0' ELSE total_cost_units END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE WHEN total_cost_nanos = '' THEN '0' ELSE total_cost_nanos END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_cost_amt,
  total_retail_currcycd,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN total_retail_units = ''
       THEN '0'
       ELSE total_retail_units
       END AS FLOAT64)) AS INTEGER)+ CAST(TRUNC(CAST(CASE
        WHEN total_retail_nanos = ''
        THEN '0'
        ELSE total_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_retail_amt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_updt_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_cost_variance_ledger_ldg
 WHERE LOWER(event_type) = LOWER('COST_VARIANCE'));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

-- COLLECT STATISTICS USING MAXVALUELENGTH 130
--     COLUMN(LOCATION_ID,SKU_ID,TRAN_DATE,EVENT_ID) 
-- ON {database_name_staging}.MERCH_COST_VARIANCE_LEDGER_WRK;
-- ET;
CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_COST_VARIANCE_LEDGER_FCT',  '{{params.database_name_fact}}',  'mfp_costvarianceledger_kafka_to_teradata',  'load_to_cost_variance_fact_03',  2,  'INTERMEDIATE',  'Clearing fact table',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  '');


BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_cost_variance_ledger_fct AS tgt
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_cost_variance_ledger_wrk AS src
    WHERE LOWER(tgt.location_id) = LOWER(location_id) AND LOWER(tgt.sku_id) = LOWER(sku_id) AND tgt.tran_date = tran_date AND tgt.event_id = event_id);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

/*
--Do we need to include this condition while deleting from the Fact Table?
     WHERE tgt.RTV_ID = stg.RTV_ID
           and stg.LAST_UPDATED_TIME_IN_MILLIS <> 'LAST_UPDATED_TIME_IN_MILLIS'
           and tgt.LAST_UPDATED_TIME_IN_MILLIS < Cast(stg.LAST_UPDATED_TIME_IN_MILLIS AS BIGINT);

--Do we need to include this condition while deleting from the stage Table?
LOCK TABLE {database_name_fact}.MERCH_RETURN_TO_VENDOR_FCT FOR ACCESS
DELETE stg
      FROM {{params.database_name_fact}}.MERCH_RETURN_TO_VENDOR_LDG stg,
           {{params.database_name_fact}}.MERCH_RETURN_TO_VENDOR_FCT tgt
     WHERE stg.RTV_ID = tgt.RTV_ID
           and stg.LAST_UPDATED_TIME_IN_MILLIS <> 'LAST_UPDATED_TIME_IN_MILLIS'
           and Cast(stg.LAST_UPDATED_TIME_IN_MILLIS AS BIGINT)  <= tgt.LAST_UPDATED_TIME_IN_MILLIS;
*/
CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_COST_VARIANCE_LEDGER_FCT',  '{{params.database_name_fact}}',  'mfp_costvarianceledger_kafka_to_teradata',  'load_to_cost_variance_fact_03',  3,  'INTERMEDIATE',  'Filling fact table',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  '');

BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO
  `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_cost_variance_ledger_fct (location_id,
    last_updated_time_in_millis,
    last_updated_time,last_updated_time_tz,
    sku_id,
    tran_date,
    event_id,
    event_time,
    correlation_id,
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
    dw_sys_updt_tmstp)
(SELECT location_id,
    last_updated_time_in_millis,
    last_updated_time,
    `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(last_updated_time as string)) as last_updated_time_tz,
    sku_id,
    tran_date,
    event_id,
    event_time,
    correlation_id,
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
 FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_cost_variance_ledger_wrk);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

-- COLLECT STATISTICS USING MAXVALUELENGTH 130
--     COLUMN(LOCATION_ID,SKU_ID,TRAN_DATE,EVENT_ID),
-- 	COLUMN(TRAN_DATE),
--     COLUMN(TRAN_DATE,PARTITION)	
-- ON {database_name_fact}.MERCH_COST_VARIANCE_LEDGER_FCT;
-- ET;
CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_COST_VARIANCE_LEDGER_FCT',  '{{params.database_name_fact}}',  'mfp_costvarianceledger_kafka_to_teradata',  'load_to_cost_variance_fact_03',  4,  'LOAD_END',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  '');

END;
