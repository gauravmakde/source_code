--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File              : sales_return_logical_fact_table.sql
-- Author            : Tetiana Ivchyk
-- Description       : Insert into SALES_LOGICAL_FACT table the delta from the staging area
-- Source topic      : ascp-rms-salereturn-event-avro
-- Object model      : InventoryEvent
-- ETL Run Frequency : Every day
-- Version :         : 0.1
--*************************************************************************************************************************************
-- Change Log: Date Author     Description
--*************************************************************************************************************************************
-- 2023-09-22  Tetiana Ivchyk  FA-9655: sales_return_logical to ISF
--*************************************************************************************************************************************
-- 2024-02-20  Josh Roter  FA-11691: sales_return_logical to NSK
--*************************************************************************************************************************************

/***********************************************************************************
-- Load temp table with metrics
************************************************************************************/
BEGIN

CREATE TEMPORARY TABLE IF NOT EXISTS isf_dag_control_log_wrk 
 AS
SELECT isf_dag_nm,
step_nm,
batch_id,
tbl_nm,
metric_nm,
metric_value,
metric_tmstp_utc as metric_tmstp,
metric_tmstp_tz,
metric_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.isf_dag_control_log;


CREATE TEMPORARY TABLE IF NOT EXISTS sales_return_ldg_tpt_info AS
SELECT jobstarttime,
 jobendtime,
 rowsinserted
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.tpt_job_log
WHERE LOWER(job_name) = LOWER('{{params.dbenv}}_NAP_STG_SALES_RETURN_LOGICAL_LDG_JOB')
QUALIFY (ROW_NUMBER() OVER (PARTITION BY job_name ORDER BY rcd_load_tmstp DESC)) = 1;


INSERT INTO isf_dag_control_log_wrk
(
isf_dag_nm,
step_nm,
batch_id,
tbl_nm,
metric_nm,
metric_value,
metric_tmstp,
metric_tmstp_tz,
metric_date
)
WITH dummy AS (SELECT 1 AS num)
SELECT 'ascp_sales_return_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
 cast(NULL as string) AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR('{{params.dbenv}}_NAP_FCT.SALES_LOGICAL_FACT', 1, 100) AS tbl_nm,
 SUBSTR('TERADATA_JOB_START_TMSTP', 1, 100) AS metric_nm,
 SUBSTR(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS STRING), 1, 60) AS metric_value,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  'GMT' metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
 
FROM dummy
UNION ALL
SELECT 'ascp_sales_return_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
 cast(NULL as string) AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR('{{params.dbenv}}_NAP_STG.SALES_RETURN_LOGICAL_LDG', 1, 100) AS tbl_nm,
 SUBSTR('LOADED_TO_CSV_CNT', 1, 100) AS metric_nm,
 SUBSTR((SELECT metric_value
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.processed_data_spark_log
   WHERE LOWER(isf_dag_nm) = LOWER('ascp_sales_return_logical_16780_TECH_SC_NAP_insights')
    AND LOWER(tbl_nm) = LOWER('SALES_RETURN_LOGICAL_LDG')
    AND LOWER(metric_nm) = LOWER('PROCESSED_ROWS_CNT')), 1, 60) AS metric_value,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  'GMT' metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT 'ascp_sales_return_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
 cast(NULL as string) AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR(NULL, 1, 100) AS tbl_nm,
 SUBSTR('SPARK_PROCESSED_MESSAGE_CNT', 1, 100) AS metric_nm,
 SUBSTR((SELECT metric_value
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.processed_data_spark_log
   WHERE LOWER(isf_dag_nm) = LOWER('ascp_sales_return_logical_16780_TECH_SC_NAP_insights')
    AND LOWER(metric_nm) = LOWER('SPARK_PROCESSED_MESSAGE_CNT')), 1, 60) AS metric_value,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  'GMT' metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT 'ascp_sales_return_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
 cast(NULL as string) AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR('{{params.dbenv}}_NAP_STG.SALES_RETURN_LOGICAL_LDG', 1, 100) AS tbl_nm,
 SUBSTR('LOADED_TO_LDG_CNT', 1, 100) AS metric_nm,
 SUBSTR(CAST((SELECT rowsinserted
    FROM sales_return_ldg_tpt_info) AS STRING), 1, 60) AS metric_value,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  'GMT' AS  metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT 'ascp_sales_return_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
 cast(NULL as string) AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR('{{params.dbenv}}_NAP_STG.SALES_RETURN_LOGICAL_LDG', 1, 100) AS tbl_nm,
 SUBSTR('TPT_JOB_START_TMSTP', 1, 100) AS metric_nm,
 SUBSTR(CAST((SELECT jobstarttime
    FROM sales_return_ldg_tpt_info) AS STRING), 1, 60) AS metric_value,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  'GMT' AS  metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT 'ascp_sales_return_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
 cast(NULL as string) AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR('{{params.dbenv}}_NAP_STG.SALES_RETURN_LOGICAL_LDG', 1, 100) AS tbl_nm,
 SUBSTR('TPT_JOB_END_TMSTP', 1, 100) AS metric_nm,
 SUBSTR(CAST((SELECT jobendtime
    FROM sales_return_ldg_tpt_info) AS STRING), 1, 60) AS metric_value,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  'GMT' AS  metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy;

-- DATA_TIMELINESS_METRIC

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('SALES_LOGICAL_FACT',  '{{params.dbenv}}_NAP_FCT',  'ascp_sales_return_logical_16780_TECH_SC_NAP_insights',  'teradata_load_teradata',  0,  'LOAD_START',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  'NAP_ASCP_SALES_LOGICAL');


/***********************************************************************************
-- Insert to the target fact table
************************************************************************************/


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.sales_logical_fact 
(from_location_id, 
rms_sku_num, 
event_id, 
event_type, 
event_date,
event_timestamp,event_timestamp_tz, 
request_date, 
request_timestamp,request_timestamp_tz, 
from_location_type, 
from_logical_location_id,
from_logical_location_type, 
to_location_id, 
to_location_type, 
to_logical_location_id, 
to_logical_location_type, 
channel, 
tran_code, 
tran_date, 
program_name, 
adjustment_qty, 
from_disposition, 
to_disposition, 
reason_code, 
dw_batch_id,
dw_batch_date, 
dw_sys_load_tmstp, 
dw_sys_updt_tmstp)
(SELECT CAST(trunc(cast(TRIM(FORMAT('%20d', from_location_facility_id)) as float64)) AS INTEGER) AS from_location_id,
  TRIM(product_id) AS rms_sku_num,
  event_id,
  event_type,
  PARSE_DATE('%F', SUBSTR(event_timestamp, 1, 10)) AS event_date,
  CAST(parse_timestamp('%Y-%m-%d %H:%M:%E6S%Ez', concat(substr(replace(event_timestamp, 'T', ' '), 1, 23), '+00:00')) as TIMESTAMP) AS event_timestamp,
  '+00:00' AS event_timestamp_tz,
  PARSE_DATE('%F', SUBSTR(request_timestamp, 1, 10)) AS request_date,
  CAST(parse_timestamp('%Y-%m-%d %H:%M:%E6S%Ez', concat(substr(replace(request_timestamp, 'T', ' '), 1, 19), '+00:00')) AS TIMESTAMP) AS request_timestamp,
  '+00:00' AS request_timestamp_tz,
  from_location_facility_type AS from_location_type,
  from_location_logical_id AS from_logical_location_id,
  from_location_logical_type AS from_logical_location_type,
  to_location_facility_id AS to_location_id,
  to_location_facility_type AS to_location_type,
  to_location_logical_id AS to_logical_location_id,
  to_location_logical_type AS to_logical_location_type,
  channel,
  tran_code,
  CAST(parse_date('%Y-%m-%d', substr(CAST(date_add(DATE '1970-01-01', interval div(tran_code, 86400) DAY) + INTERVAL 1 SECOND * CAST(mod(tran_code, 86400) * NUMERIC '100' as INT64) / 100 as STRING), 1, 10)) AS DATE) AS tran_date,
  program_name,
  quantity AS adjustment_qty,
  from_disposition,
  to_disposition,
  reason_code,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS dw_batch_id,
   (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_updt_tmstp,
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.sales_return_logical_ldg);


-- COLLECT STATISTICS ON `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.SALES_LOGICAL_FACT COLUMN( PARTITION );
-- COLLECT STATISTICS ON `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.SALES_LOGICAL_FACT COLUMN( from_location_id, rms_sku_num );

/***********************************************************************************
-- Perform audit between stg and fct tables
************************************************************************************/
CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DQ_PIPELINE_AUDIT_V1
(
	'SALES_RETURN_LOGICAL_LDG_TO_BASE',
	'NAP_ASCP_SALES_LOGICAL',
	'{{params.dbenv}}_NAP_STG',
	'SALES_RETURN_LOGICAL_LDG',
	'{{params.dbenv}}_NAP_FCT',
	'SALES_LOGICAL_FACT',
	'Count_Distinct', 
	0, 
	'T-S', 
	'concat(event_id, to_location_facility_id, product_id)', 
	'concat(event_id, to_location_id, rms_sku_num)', 
	NULL,
	NULL, 
	'Y'
);

/***********************************************************************************
-- Update control metrics table with temp table
************************************************************************************/
INSERT INTO isf_dag_control_log_wrk
(SELECT 'ascp_sales_return_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
  CAST(NULL AS STRING) AS step_nm,
   (SELECT batch_id FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
  SUBSTR('{{params.dbenv}}_NAP_FCT.SALES_LOGICAL_FACT', 1, 100) AS tbl_nm,
  SUBSTR('TERADATA_JOB_END_TMSTP', 1, 100) AS metric_nm,
  SUBSTR(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS STRING), 1, 60) AS metric_value,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  'GMT' AS metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date);


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.isf_dag_control_log SET
 metric_value = wrk.metric_value,
 metric_tmstp = wrk.metric_tmstp,
 metric_tmstp_tz = wrk.metric_tmstp_tz 
 FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY isf_dag_nm, tbl_nm, metric_nm, batch_id ORDER BY metric_tmstp DESC) as rn
    FROM isf_dag_control_log_wrk
) AS wrk
WHERE LOWER(isf_dag_control_log.isf_dag_nm) = LOWER(wrk.isf_dag_nm) 
   AND LOWER(COALESCE(isf_dag_control_log.tbl_nm, '')) = LOWER(COALESCE(wrk.tbl_nm, '')) 
   AND LOWER(isf_dag_control_log.metric_nm) = LOWER(wrk.metric_nm) 
   AND isf_dag_control_log.batch_id = wrk.batch_id
   AND wrk.rn = 1; 

end
