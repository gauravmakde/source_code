--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File              : rms_cost_transfers_load_fact_table.sql
-- Author            : Tetiana Ivchyk
-- Description       : Merge RMS_COST_TRANSFERS_FACT table with delta from staging area
-- Source topic      : inventory-merchandise-transfer-analytical-avro
-- Object model      : MerchandiseTransfer
-- ETL Run Frequency : Every day
-- Version :         : 0.1
--*************************************************************************************************************************************
-- Change Log: Date Author          Description
--*************************************************************************************************************************************
-- 2023-08-21  Tetiana Ivchyk       FA-9659:  TECH_SC_NAP_Engg_Metamorph_rms_cost_transfer to ISF
-- 2023-09-26  Joshua Roter         FA-8985:  Missing Transfers - Handling Cancel Events in NAP Semantic layer
-- 2023-10-04  Oleksandr Chaichenko FA 10284: Change source for DQ_PIPELINE_AUDIT_V1 to RMS_COST_TRANSFERS_DQ_VW_LDG
-- 2023-11-24  Alexis Ding          FA-10425: Updated isf_dag_nm to migrate to new APP ID
-- 2024-03-04  Maksym Pochepets     FA-11689: Migrate to NSK Airflow
--*************************************************************************************************************************************

/***********************************************************************************
-- Load temp table with metrics
************************************************************************************/
/************************************************************************************/
BEGIN



CREATE TEMPORARY TABLE IF NOT EXISTS isf_dag_control_log_wrk  AS
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


CREATE TEMPORARY TABLE IF NOT EXISTS rms_cost_transfers_ldg_tpt_info AS
SELECT jobstarttime,
 jobendtime,
 rowsinserted
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.tpt_job_log
WHERE LOWER(job_name) = LOWER('{{params.dbenv}}_NAP_STG_RMS_COST_TRANSFERS_LDG_JOB')
QUALIFY (ROW_NUMBER() OVER (PARTITION BY job_name ORDER BY rcd_load_tmstp DESC)) = 1;


CREATE TEMPORARY TABLE IF NOT EXISTS rms_cost_transfers_canceled_details_ldg_tpt_info AS
SELECT jobstarttime,
 jobendtime,
 rowsinserted
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.tpt_job_log
WHERE LOWER(job_name) = LOWER('{{params.dbenv}}_NAP_STG_RMS_COST_TRANSFERS_CANCELED_DETAILS_LDG_JOB')
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
SELECT '{{params.dag_name}}' AS isf_dag_nm,
 cast(NULL as STRING)  AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR('{{params.dbenv}}_fct_table_name', 1, 100) AS tbl_nm,
 SUBSTR('TERADATA_JOB_START_TMSTP', 1, 100) AS metric_nm,
 SUBSTR(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS STRING), 1, 60) AS metric_value,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS STRING)) metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT '{{params.dag_name}}' AS isf_dag_nm,
 cast(NULL as STRING)  AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR('{{params.dbenv}}_{ldg_table_name}', 1, 100) AS tbl_nm,
 SUBSTR('LOADED_TO_CSV_CNT', 1, 100) AS metric_nm,
 SUBSTR((SELECT metric_value
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.processed_data_spark_log
   WHERE LOWER(isf_dag_nm) = LOWER('{{params.dag_name}}')
    AND LOWER(tbl_nm) = LOWER('RMS_COST_TRANSFERS_LDG')
    AND LOWER(metric_nm) = LOWER('PROCESSED_ROWS_CNT')), 1, 60) AS metric_value,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS STRING)) metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT '{{params.dag_name}}' AS isf_dag_nm,
 cast(NULL as STRING)  AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR('{{params.dbenv}}_{ldg_table_name2}', 1, 100) AS tbl_nm,
 SUBSTR('LOADED_TO_CSV_CNT', 1, 100) AS metric_nm,
 SUBSTR((SELECT metric_value
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.processed_data_spark_log
   WHERE LOWER(isf_dag_nm) = LOWER('{{params.dag_name}}')
    AND LOWER(tbl_nm) = LOWER('RMS_COST_TRANSFERS_CANCELED_DETAILS_LDG')
    AND LOWER(metric_nm) = LOWER('PROCESSED_ROWS_CNT')), 1, 60) AS metric_value,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS STRING)) metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT '{{params.dag_name}}' AS isf_dag_nm,
 cast(NULL as STRING)  AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR(NULL, 1, 100) AS tbl_nm,
 SUBSTR('SPARK_PROCESSED_MESSAGE_CNT', 1, 100) AS metric_nm,
 SUBSTR((SELECT metric_value
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.processed_data_spark_log
   WHERE LOWER(isf_dag_nm) = LOWER('{{params.dag_name}}')
    AND LOWER(metric_nm) = LOWER('SPARK_PROCESSED_MESSAGE_CNT')), 1, 60) AS metric_value,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS STRING)) metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT '{{params.dag_name}}' AS isf_dag_nm,
 cast(NULL as STRING)  AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR('{{params.dbenv}}_{ldg_table_name}', 1, 100) AS tbl_nm,
 SUBSTR('LOADED_TO_LDG_CNT', 1, 100) AS metric_nm,
 SUBSTR(CAST((SELECT rowsinserted
    FROM rms_cost_transfers_ldg_tpt_info) AS STRING), 1, 60) AS metric_value,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS STRING)) metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT '{{params.dag_name}}' AS isf_dag_nm,
 cast(NULL as STRING)  AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR('{{params.dbenv}}_{ldg_table_name2}', 1, 100) AS tbl_nm,
 SUBSTR('LOADED_TO_LDG_CNT', 1, 100) AS metric_nm,
 SUBSTR(CAST((SELECT rowsinserted
    FROM rms_cost_transfers_canceled_details_ldg_tpt_info) AS STRING), 1, 60) AS metric_value,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS STRING)) metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT '{{params.dag_name}}' AS isf_dag_nm,
 cast(NULL as STRING)  AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR('{{params.dbenv}}_{ldg_table_name}', 1, 100) AS tbl_nm,
 SUBSTR('TPT_JOB_START_TMSTP', 1, 100) AS metric_nm,
 SUBSTR(CAST((SELECT jobstarttime
    FROM rms_cost_transfers_ldg_tpt_info) AS STRING), 1, 60) AS metric_value,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS STRING)) metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT '{{params.dag_name}}' AS isf_dag_nm,
 cast(NULL as STRING)  AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR('{{params.dbenv}}_{ldg_table_name2}', 1, 100) AS tbl_nm,
 SUBSTR('TPT_JOB_START_TMSTP', 1, 100) AS metric_nm,
 SUBSTR(CAST((SELECT jobstarttime
    FROM rms_cost_transfers_canceled_details_ldg_tpt_info) AS STRING), 1, 60) AS metric_value,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS STRING)) metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT '{{params.dag_name}}' AS isf_dag_nm,
 cast(NULL as STRING)  AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR('{{params.dbenv}}_{ldg_table_name}', 1, 100) AS tbl_nm,
 SUBSTR('TPT_JOB_END_TMSTP', 1, 100) AS metric_nm,
 SUBSTR(CAST((SELECT jobendtime
    FROM rms_cost_transfers_ldg_tpt_info) AS STRING), 1, 60) AS metric_value,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS STRING)) metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT '{{params.dag_name}}' AS isf_dag_nm,
   cast(NULL as STRING)  AS step_nm,
    (SELECT batch_id
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
    WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
   SUBSTR('{{params.dbenv}}_{ldg_table_name2}', 1, 100) AS tbl_nm,
   SUBSTR('TPT_JOB_END_TMSTP', 1, 100) AS metric_nm,
   SUBSTR(CAST((SELECT jobendtime FROM rms_cost_transfers_canceled_details_ldg_tpt_info) AS STRING), 1, 60) AS metric_value,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS STRING)) metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
  FROM dummy;

/***********************************************************************************
-- DATA_TIMELINESS_METRIC
************************************************************************************/
 CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('RMS_COST_TRANSFERS_FACT',  '{{params.dbenv}}_NAP_FCT',  'ascp_rms_cost_transfers',  'load_05_teradata',  0,  'LOAD_START',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  'NAP_ASCP_RMS_COST_TRANSFERS');


/***********************************************************************************
-- Merge into the target fact table
************************************************************************************/
-- CAST(OREPLACE(OREPLACE(details_ldg.created_time, 'Z', '+00:00'), 'T', ' ') AS TIMESTAMP WITH TIME ZONE FORMAT 'YYYY-MM-DDBHH:MI:SS.S(F)Z') AS create_time,


-- CAST(OREPLACE(OREPLACE(details_ldg.detail_latest_update_time, 'Z', '+00:00'), 'T', ' ') AS TIMESTAMP WITH TIME ZONE FORMAT 'YYYY-MM-DDBHH:MI:SS.S(F)Z') AS latest_update_time,


-- CAST(OREPLACE(OREPLACE(canceled_ldg.canceled_detail_latest_cancel_time, 'Z', '+00:00'), 'T', ' ') AS TIMESTAMP WITH TIME ZONE FORMAT 'YYYY-MM-DDBHH:MI:SS.S(F)Z') AS latest_cancel_time,


MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_{{params.fct_table_name}} AS fact
USING (SELECT TRIM(COALESCE(details_ldg.transfer_number, canceled_ldg.transfer_number)) AS transfer_num,
  TRIM(COALESCE(details_ldg.detail_product_id, canceled_ldg.canceled_detail_product_id)) AS rms_sku_num,
  NULLIF(TRIM(details_ldg.external_reference_number), '""') AS external_reference_num,
  NULLIF(TRIM(details_ldg.system_id), '""') AS system_id,
  NULLIF(TRIM(details_ldg.user_id), '""') AS user_id,
   CASE
   WHEN LOWER(details_ldg.from_location_facility) = LOWER('""')
   THEN NULL
   ELSE CAST(details_ldg.from_location_facility AS INTEGER)
   END AS from_location_facility,
   CASE
   WHEN LOWER(details_ldg.from_location_logical) = LOWER('""')
   THEN NULL
   ELSE CAST(details_ldg.from_location_logical AS INTEGER)
   END AS from_location_logical,
   CASE
   WHEN LOWER(details_ldg.to_location_facility) = LOWER('""')
   THEN NULL
   ELSE CAST(details_ldg.to_location_facility AS INTEGER)
   END AS to_location_facility,
   CASE
   WHEN LOWER(details_ldg.to_location_logical) = LOWER('""')
   THEN NULL
   ELSE CAST(details_ldg.to_location_logical AS INTEGER)
   END AS to_location_logical,
  TRIM(details_ldg.transfer_type) AS transfer_type,
  TRIM(details_ldg.transfer_context_type) AS transfer_context_type,
  TRIM(details_ldg.routing_code) AS routing_code,
  TRIM(details_ldg.freight_code) AS freight_code,
  CAST(details_ldg.delivery_date AS DATE) AS delivery_date,
  NULLIF(TRIM(details_ldg.comments), '""') AS comments,
  CAST(details_ldg.detail_quantity AS INTEGER) AS quantity,
  CAST(canceled_ldg.canceled_detail_quantity AS INTEGER) AS canceled_quantity,
  TRIM(details_ldg.detail_disposition) AS disposition,
  CAST(details_ldg.detail_supplier_pack_size AS INTEGER) AS supplier_pack_size,
    cast(parse_timestamp('%Y-%m-%d %H:%M:%E6S%Ez', replace(replace(details_ldg.created_time, 'Z', '+00:00'), 'T', ' ')) as TIMESTAMP) AS create_time,
    '+00:00' AS create_time_tz, 
    cast(parse_timestamp('%Y-%m-%d %H:%M:%E6S%Ez', replace(replace(details_ldg.detail_latest_update_time, 'Z', '+00:00'), 'T', ' ')) as TIMESTAMP) AS latest_update_time,
    '+00:00' AS latest_update_time_tz,
    cast(parse_timestamp('%Y-%m-%d %H:%M:%E6S%Ez', replace(replace(canceled_ldg.canceled_detail_latest_cancel_time, 'Z', '+00:00'), 'T', ' ')) as TIMESTAMP) AS latest_cancel_time,
    '+00:00' AS latest_cancel_time_tz,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS dw_batch_id,
   (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS dw_batch_date,
  timestamp(current_datetime('PST8PDT')) AS dw_sys_load_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS dw_sys_load_tmstp_tz, 
  timestamp(current_datetime('PST8PDT')) AS dw_sys_updt_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS dw_sys_updt_tmstp_tz
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.rms_cost_transfers_ldg AS details_ldg
  FULL JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.rms_cost_transfers_canceled_details_ldg AS canceled_ldg ON LOWER(details_ldg.transfer_number
     ) = LOWER(canceled_ldg.transfer_number) AND LOWER(details_ldg.detail_product_id) = LOWER(canceled_ldg.canceled_detail_product_id
     )
 WHERE (details_ldg.transfer_number IS NOT NULL OR canceled_ldg.transfer_number IS NOT NULL)
  AND (canceled_ldg.canceled_detail_product_id IS NOT NULL OR details_ldg.detail_product_id IS NOT NULL)) AS ldg
ON LOWER(ldg.transfer_num) = LOWER(fact.transfer_num) AND LOWER(ldg.rms_sku_num) = LOWER(fact.rms_sku_num)
WHEN MATCHED THEN UPDATE SET
 external_reference_num = ldg.external_reference_num,
 system_id = ldg.system_id,
 user_id = ldg.user_id,
 from_location_facility = ldg.from_location_facility,
 from_location_logical = ldg.from_location_logical,
 to_location_facility = ldg.to_location_facility,
 to_location_logical = ldg.to_location_logical,
 transfer_type = ldg.transfer_type,
 transfer_context_type = ldg.transfer_context_type,
 routing_code = ldg.routing_code,
 freight_code = ldg.freight_code,
 delivery_date = ldg.delivery_date,
 comments = ldg.comments,
 quantity = ldg.quantity,
 canceled_qty = ldg.canceled_quantity,
 disposition = ldg.disposition,
 supplier_pack_size = ldg.supplier_pack_size,
 create_time = ldg.create_time,
 create_time_tz = ldg.create_time_tz,
 latest_update_time = ldg.latest_update_time,
 latest_update_time_tz = ldg.latest_update_time_tz,
 latest_cancel_time = ldg.latest_cancel_time,
 latest_cancel_time_tz = ldg.latest_cancel_time_tz,
 dw_batch_id = ldg.dw_batch_id,
 dw_batch_date = ldg.dw_batch_date,
 dw_sys_updt_tmstp = ldg.dw_sys_updt_tmstp,
 dw_sys_updt_tmstp_tz = ldg.dw_sys_updt_tmstp_tz
WHEN NOT MATCHED THEN INSERT VALUES(ldg.transfer_num, ldg.rms_sku_num, ldg.external_reference_num, ldg.system_id, ldg.user_id
 , ldg.from_location_facility, ldg.from_location_logical, ldg.to_location_facility, ldg.to_location_logical, ldg.transfer_type
 , ldg.transfer_context_type, ldg.routing_code, ldg.freight_code, ldg.delivery_date, ldg.comments, ldg.quantity, ldg.disposition
 , ldg.supplier_pack_size, ldg.create_time,ldg.create_time_tz, ldg.latest_update_time,ldg.latest_update_time_tz, ldg.latest_cancel_time,ldg.latest_cancel_time_tz, ldg.dw_batch_id, ldg.dw_batch_date
 , ldg.dw_sys_load_tmstp,ldg.dw_sys_load_tmstp_tz, ldg.dw_sys_updt_tmstp,ldg.dw_sys_updt_tmstp_tz, ldg.canceled_quantity);


/***********************************************************************************
-- DATA_TIMELINESS_METRIC
************************************************************************************/
CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('RMS_COST_TRANSFERS_FACT',  '{{params.dbenv}}_NAP_FCT',  'ascp_rms_cost_transfers',  'load_05_teradata',  1,  'LOAD_END',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  '{{params.subject_area}}');

/***********************************************************************************
-- Collect stats on fact tables
************************************************************************************/
--COLLECT STATS ON `{{params.gcp_project_id}}`.{{params.dbenv}}_fct_table_name INDEX ( transfer_num, rms_sku_num );

/***********************************************************************************
-- Perform audit between stg and fct tables
************************************************************************************/
CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DQ_PIPELINE_AUDIT_V1('RMS_COST_TRANSFERS_LDG_TO_BASE',  '{{params.subject_area}}',  '{{params.dbenv}}_NAP_BASE_VWS',  'RMS_COST_TRANSFERS_DQ_VW_LDG',  '{{params.dbenv}}_NAP_FCT',  'RMS_COST_TRANSFERS_FACT',  'Count_Distinct',  0,  'T-S',  'concat(transfer_number, detail_product_id)',  'concat(transfer_num, rms_sku_num)',  NULL,  NULL,  'Y');



/***********************************************************************************
-- Update control metrics table with temp table
************************************************************************************/

INSERT INTO isf_dag_control_log_wrk
(SELECT '{{params.dag_name}}' AS isf_dag_nm,
  CAST(NULL AS STRING) AS step_nm,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
  SUBSTR('{{params.dbenv}}_fct_table_name', 1, 100) AS tbl_nm,
  SUBSTR('TERADATA_JOB_END_TMSTP', 1, 100) AS metric_nm,
  SUBSTR(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS STRING), 1, 60) AS metric_value,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS STRING)) metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date);


UPDATE isf_dag_control_log_wrk AS wrk SET
    metric_value = wrk.metric_value,
    metric_tmstp = wrk.metric_tmstp FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.isf_dag_control_log
WHERE LOWER(isf_dag_control_log.isf_dag_nm) = LOWER(wrk.isf_dag_nm) AND LOWER(COALESCE(isf_dag_control_log.tbl_nm, '')) = LOWER(COALESCE(wrk.tbl_nm, '')) AND LOWER(isf_dag_control_log.metric_nm) = LOWER(wrk.metric_nm) AND isf_dag_control_log.batch_id = wrk.batch_id;

END;