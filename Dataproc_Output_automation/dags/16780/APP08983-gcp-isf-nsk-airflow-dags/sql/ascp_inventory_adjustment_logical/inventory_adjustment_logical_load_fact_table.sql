--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File              : inventory_adjustment_logical_load_fact_table.sql
-- Author            : Tetiana Ivchyk
-- Description       : Merge INVENTORY_ADJUSTMENT_LOGICAL_FACT table with delta from staging area
-- Source topic      : ascp-inventory-adjustment-logical-event-avro
-- Object model      : InventoryEvent
-- ETL Run Frequency : Every day
-- Version :         : 0.1
--*************************************************************************************************************************************
-- Change Log: Date Author     Description
--*************************************************************************************************************************************
-- 2023-07-31  Tetiana Ivchyk    FA-9651:  TECH_SC_NAP_Engg_Metamorph_inventory_adjustment_logical to ISF
-- 2023-10-05  Tetiana Ivchyk    FA-10201: Add metrics to released inventory_adjustment_logical
-- 2023-11-06  Andrew Ivchuk     FA-10496: App ID Migration- RMS 14 GG Inventory Adjustments - ISF Components.
-- 2024-02-19  Stefanos Stoikos  FA-11684: TECH_SC_NAP_Engg_Metamorph_inventory_adjustment_logical to NSK
--*************************************************************************************************************************************


/***********************************************************************************
-- Load temp table with metrics
************************************************************************************/



CREATE TEMPORARY TABLE IF NOT EXISTS isf_dag_control_log_wrk  AS
SELECT isf_dag_nm,
step_nm,
batch_id,
tbl_nm,
metric_nm,
metric_value,
metric_tmstp_utc,
metric_tmstp_tz,
metric_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.isf_dag_control_log;


CREATE TEMPORARY TABLE IF NOT EXISTS inventory_adjustment_logical_ldg_tpt_info AS
SELECT jobstarttime,
 jobendtime,
 rowsinserted
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.tpt_job_log
WHERE LOWER(job_name) = LOWER('{{params.dbenv}}_NAP_STG_INVENTORY_ADJUSTMENT_LOGICAL_LDG_JOB')
QUALIFY (ROW_NUMBER() OVER (PARTITION BY job_name ORDER BY rcd_load_tmstp DESC)) = 1;


INSERT INTO isf_dag_control_log_wrk
(
isf_dag_nm,
step_nm,
batch_id,
tbl_nm,
metric_nm,
metric_value,
metric_tmstp_utc,
metric_tmstp_tz,
metric_date
)
WITH dummy AS (SELECT 1 AS num)
SELECT 'ascp_inventory_adjustment_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
 cast(NULL as string) AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR('{{params.dbenv}}_NAP_FCT.INVENTORY_ADJUSTMENT_LOGICAL_FACT', 1, 100) AS tbl_nm,
 SUBSTR('TERADATA_JOB_START_TMSTP', 1, 100) AS metric_nm,
 SUBSTR(CAST(CAST(CURRENT_DATETIME('GMT') AS TIMESTAMP) AS STRING), 1, 60) AS metric_value,
 CAST(CURRENT_DATETIME('GMT') AS TIMESTAMP) AS metric_tmstp,
 'GMT' AS metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT 'ascp_inventory_adjustment_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
 cast(NULL as string) AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR('{{params.dbenv}}_NAP_STG.INVENTORY_ADJUSTMENT_LOGICAL_LDG', 1, 100) AS tbl_nm,
 SUBSTR('LOADED_TO_CSV_CNT', 1, 100) AS metric_nm,
 SUBSTR((SELECT metric_value
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.processed_data_spark_log
   WHERE LOWER(isf_dag_nm) = LOWER('ascp_inventory_adjustment_logical_16780_TECH_SC_NAP_insights')
    AND LOWER(tbl_nm) = LOWER('INVENTORY_ADJUSTMENT_LOGICAL_LDG')
    AND LOWER(metric_nm) = LOWER('PROCESSED_ROWS_CNT')), 1, 60) AS metric_value,
 CAST(CURRENT_DATETIME('GMT') AS TIMESTAMP) AS metric_tmstp,
 'GMT' AS metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT 'ascp_inventory_adjustment_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
 cast(NULL as string) AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR(NULL, 1, 100) AS tbl_nm,
 SUBSTR('SPARK_PROCESSED_MESSAGE_CNT', 1, 100) AS metric_nm,
 SUBSTR((SELECT metric_value
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.processed_data_spark_log
   WHERE LOWER(isf_dag_nm) = LOWER('ascp_inventory_adjustment_logical_16780_TECH_SC_NAP_insights')
    AND LOWER(metric_nm) = LOWER('SPARK_PROCESSED_MESSAGE_CNT')), 1, 60) AS metric_value,
 CAST(CURRENT_DATETIME('GMT') AS TIMESTAMP) AS metric_tmstp,
 'GMT' AS metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT 'ascp_inventory_adjustment_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
 cast(NULL as string) AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR('{{params.dbenv}}_NAP_STG.INVENTORY_ADJUSTMENT_LOGICAL_LDG', 1, 100) AS tbl_nm,
 SUBSTR('LOADED_TO_LDG_CNT', 1, 100) AS metric_nm,
 SUBSTR(CAST((SELECT rowsinserted
    FROM inventory_adjustment_logical_ldg_tpt_info) AS STRING), 1, 60) AS metric_value,
 CAST(CURRENT_DATETIME('GMT') AS TIMESTAMP) AS metric_tmstp,
 'GMT' AS metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT 'ascp_inventory_adjustment_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
 cast(NULL as string) AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR('{{params.dbenv}}_NAP_STG.INVENTORY_ADJUSTMENT_LOGICAL_LDG', 1, 100) AS tbl_nm,
 SUBSTR('TPT_JOB_START_TMSTP', 1, 100) AS metric_nm,
 SUBSTR(CAST((SELECT jobstarttime
    FROM inventory_adjustment_logical_ldg_tpt_info) AS STRING), 1, 60) AS metric_value,
 CAST(CURRENT_DATETIME('GMT') AS TIMESTAMP) AS metric_tmstp,
 'GMT' AS metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT 'ascp_inventory_adjustment_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
 cast(NULL as string) AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR('{{params.dbenv}}_NAP_STG.INVENTORY_ADJUSTMENT_LOGICAL_LDG', 1, 100) AS tbl_nm,
 SUBSTR('TPT_JOB_END_TMSTP', 1, 100) AS metric_nm,
 SUBSTR(CAST((SELECT jobendtime
    FROM inventory_adjustment_logical_ldg_tpt_info) AS STRING), 1, 60) AS metric_value,
 CAST(CURRENT_DATETIME('GMT') AS TIMESTAMP) AS metric_tmstp,
 'GMT' AS metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy;
/***********************************************************************************
-- Merge into the target fact table
************************************************************************************/


MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.inventory_adjustment_logical_fact AS fact
USING (SELECT TRIM(FORMAT('%20d', to_location_facility_id)) AS to_location_id,
  TRIM(product_id) AS rms_sku_num,
  event_id,
  event_type,
  PARSE_DATE('%F', SUBSTR(event_timestamp, 1, 10)) AS event_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(CONCAT(SUBSTR(REPLACE(event_timestamp, 'T', ' '), 1, 23), '+00:00') AS DATETIME)) AS TIMESTAMP)
  AS event_timestamp,
  PARSE_DATE('%F', SUBSTR(request_timestamp, 1, 10)) AS request_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(CONCAT(SUBSTR(REPLACE(request_timestamp, 'T', ' '), 1, 19), '+00:00') AS DATETIME)
    ) AS TIMESTAMP) AS request_timestamp,
  from_location_facility_id AS from_location_id,
  from_location_facility_type AS from_location_type,
  from_location_logical_id AS from_logical_location_id,
  from_location_logical_type AS from_logical_location_type,
  to_location_facility_type AS to_location_type,
  to_location_logical_id AS to_logical_location_id,
  to_location_logical_type AS to_logical_location_type,
  channel,
  tran_code,
  program_name,
  quantity AS adjustment_qty,
  from_disposition,
  to_disposition,
  reason_code,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('subject_area')) AS dw_batch_id,
   (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('subject_area')) AS dw_batch_date
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.inventory_adjustment_logical_ldg
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY event_id, product_id, to_location_facility_id ORDER BY PARSE_DATE('%F', SUBSTR(CAST(request_timestamp AS STRING)
        , 1, 10)) DESC)) = 1) AS ldg
ON ldg.event_date = fact.event_date 
AND CAST(ldg.to_location_id AS FLOAT64) = fact.to_location_id 
AND LOWER(ldg.rms_sku_num) = LOWER(fact.rms_sku_num) 
    AND LOWER(ldg.event_id) = LOWER(fact.event_id)
WHEN MATCHED THEN UPDATE SET
 event_type = ldg.event_type,
 event_timestamp = ldg.event_timestamp,
 event_timestamp_tz='+00:00',
 request_date = ldg.request_date,
 request_timestamp = ldg.request_timestamp,
 request_timestamp_tz= '+00:00',
 from_location_id = ldg.from_location_id,
 from_location_type = ldg.from_location_type,
 from_logical_location_id = ldg.from_logical_location_id,
 from_logical_location_type = ldg.from_logical_location_type,
 to_location_type = ldg.to_location_type,
 to_logical_location_id = ldg.to_logical_location_id,
 to_logical_location_type = ldg.to_logical_location_type,
 channel = ldg.channel,
 tran_code = ldg.tran_code,
 program_name = ldg.program_name,
 adjustment_qty = ldg.adjustment_qty,
 from_disposition = ldg.from_disposition,
 to_disposition = ldg.to_disposition,
 reason_code = ldg.reason_code,
 dw_batch_id = ldg.dw_batch_id,
 dw_batch_date = ldg.dw_batch_date,
 dw_sys_updt_tmstp = CURRENT_DATETIME('PST8PDT') 
WHEN NOT MATCHED THEN 
INSERT VALUES(CAST(TRUNC(CAST(ldg.to_location_id AS FLOAT64)) AS INTEGER), ldg.rms_sku_num, ldg.event_id, ldg.event_type,
 ldg.event_date, ldg.event_timestamp,'+00:00', ldg.request_date, ldg.request_timestamp,'+00:00', ldg.from_location_id, ldg.from_location_type
 , ldg.from_logical_location_id, ldg.from_logical_location_type, ldg.to_location_type, ldg.to_logical_location_id, ldg.to_logical_location_type
 , ldg.channel, ldg.tran_code, ldg.program_name, ldg.adjustment_qty, ldg.from_disposition, ldg.to_disposition, ldg.reason_code
 , ldg.dw_batch_id, ldg.dw_batch_date,CURRENT_DATETIME('PST8PDT') ,CURRENT_DATETIME('PST8PDT') 
 );


/***********************************************************************************
-- Collect stats on fact tables
************************************************************************************/


--COLLECT STATISTICS ON PRD_NAP_FCT.INVENTORY_ADJUSTMENT_LOGICAL_FACT COLUMN(PARTITION)


--COLLECT STATISTICS ON PRD_NAP_FCT.INVENTORY_ADJUSTMENT_LOGICAL_FACT COLUMN(to_location_id, rms_sku_num)


/***********************************************************************************
-- Perform audit between stg and fct tables
************************************************************************************/


-- Inventory adjustment logical


-- Audit_Type


-- Mtch_Threshold


-- Mtch_Type_Code


-- SRC_Key_Expr


-- TGT_Key_Expr


-- SRC_Filter


-- TGT_Filter


-- Mtch_Batch_Id_Ind


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DQ_PIPELINE_AUDIT_V1
(
	'INVENTORY_ADJUSTMENT_LOGICAL_LDG_TO_BASE',
	'{{params.subject_area}}',
	'{{params.dbenv}}_NAP_STG',
	'INVENTORY_ADJUSTMENT_LOGICAL_LDG',
	'{{params.dbenv}}_NAP_FCT',
	'INVENTORY_ADJUSTMENT_LOGICAL_FACT',
	'Count_Distinct', 
	0, 
	'T-S', 
	'concat(event_id, to_location_facility_id, product_id)', 
	'concat(event_id, to_location_id, rms_sku_num)', 
	null,
	null, 
	'Y'
);


/***********************************************************************************
-- Update control metrics table with temp table
************************************************************************************/


INSERT INTO isf_dag_control_log_wrk
(SELECT 'ascp_inventory_adjustment_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
  CAST(NULL as string) AS step_nm,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
  SUBSTR('{{params.dbenv}}_NAP_FCT.INVENTORY_ADJUSTMENT_LOGICAL_FACT', 1, 100) AS tbl_nm,
  SUBSTR('TERADATA_JOB_END_TMSTP', 1, 100) AS metric_nm,
  SUBSTR(CAST(CAST(CURRENT_DATETIME('GMT') AS TIMESTAMP) AS STRING), 1, 60) AS metric_value,
  CAST(CAST(CURRENT_DATETIME('GMT') AS TIMESTAMP) AS TIMESTAMP) AS
  metric_tmstp,
  'GMT' AS metric_tmstp_tz,
  CURRENT_DATE('GMT') AS metric_date);


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.isf_dag_control_log t0 SET
 t0.metric_value = wrk.metric_value,
 t0.metric_tmstp = wrk.metric_tmstp_utc,
 t0.metric_tmstp_tz = wrk.metric_tmstp_tz 
 FROM isf_dag_control_log_wrk AS wrk
WHERE LOWER(t0.isf_dag_nm) = LOWER(wrk.isf_dag_nm) 
AND LOWER(COALESCE(t0.tbl_nm, '')) = LOWER(COALESCE(wrk.tbl_nm, ''))
AND LOWER(t0.metric_nm) = LOWER(wrk.metric_nm) 
AND t0.batch_id = wrk.batch_id;