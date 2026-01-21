--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File              : read_rms_cost_rtv_v2_from_kafka_to_teradata.sql
-- Author            : Kunal Lalwani
-- Description       : Read from Source Kakfa Topic to RMS_COST_RTV_SHIPMENT_DETAILS_V2_LDG tables
-- Object model     : inventory-merchandise-return-to-vendor-v2-analytical-avro
-- ETL Run Frequency : Every day
-- Version :         : 0.2
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2024-03-06 Kunal Lalwani  FA-11695   NSK Migration
-- 2024-03-11 Andrew Ivchuk  FA-11147   Adding step sending NewRelic metrics
--************************************************************************************************************************************


/***********************************************************************************
-- Load temp table with metrics
************************************************************************************/
CREATE TEMP TABLE isf_dag_control_log_wrk 
AS
( SELECT *
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.isf_dag_control_log
 ) 
;



INSERT INTO isf_dag_control_log_wrk(isf_dag_nm,step_nm,batch_id,tbl_nm,metric_nm,metric_value,metric_tmstp,metric_tmstp_tz,metric_date)
WITH dummy AS (SELECT 1 AS num)
SELECT
	'{{params.dag_name}}' AS isf_dag_nm,
	CAST(NULL AS STRING) AS step_nm,
	(SELECT batch_id FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
	CAST('{{params.dbenv}}_NAP_FCT.RMS_COST_RTV_SHIPMENT_DETAILS_V2_FACT' AS STRING) AS tbl_nm,
	CAST('TERADATA_JOB_START_TMSTP' AS STRING) AS metric_nm,
	CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS  TIMESTAMP) AS STRING) AS metric_value,
	CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS metric_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS  TIMESTAMP) AS STRING)) AS metric_tmstp_tz,
	CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT
  '{{params.dag_name}}' as isf_dag_nm,
   CAST(null AS STRING) as step_nm,
  (SELECT batch_id FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) =LOWER('{{params.subject_area}}')) as batch_id,
  CAST('{{params.dbenv}}_NAP_STG.RMS_COST_RTV_SHIPMENT_DETAILS_V2_LDG' as STRING) as tbl_nm,
  CAST('LOADED_TO_LDG_CNT' AS STRING) as metric_nm,
  CAST((SELECT metric_value FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.processed_data_spark_log
                   WHERE LOWER(isf_dag_nm) = LOWER('{{params.dag_name}}')
                         and LOWER(tbl_nm) = LOWER('RMS_COST_RTV_SHIPMENT_DETAILS_V2_LDG')
                         and LOWER(metric_nm) = LOWER('PROCESSED_ROWS_CNT')) as STRING) as metric_value,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS metric_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS  TIMESTAMP) AS STRING)) AS metric_tmstp_tz,
  CURRENT_DATE('GMT') as metric_date
FROM dummy
UNION ALL
SELECT
	'{{params.dag_name}}' AS isf_dag_nm,
	CAST(NULL AS STRING) AS step_nm,
	(SELECT batch_id FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
	CAST(NULL AS STRING) AS tbl_nm,
	CAST('SPARK_PROCESSED_MESSAGE_CNT' AS STRING) AS metric_nm,
	CAST((SELECT metric_value FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.processed_data_spark_log
			WHERE LOWER(isf_dag_nm) = LOWER('{{params.dag_name}}')
			AND metric_nm = 'SPARK_PROCESSED_MESSAGE_CNT') AS STRING) AS metric_value,
	CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS metric_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS  TIMESTAMP) AS STRING)) AS metric_tmstp_tz,
	CURRENT_DATE('GMT') AS metric_date
FROM dummy;



/***********************************************************************************
-- DATA_TIMELINESS_METRIC
************************************************************************************/

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD
  ('RMS_COST_RTV_SHIPMENT_DETAILS_V2_FACT',
   '{{params.dbenv}}_NAP_FCT',
   '{{params.dag_name}}',
   'teradata_load',
   0,'LOAD_START', '',
   CURRENT_DATETIME('PST8PDT'),
   '{{params.subject_area}}');



/***********************************************************************************
-- DATA UPSERT
************************************************************************************/


MERGE INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_fct.rms_cost_rtv_shipment_details_v2_fact fact
USING (
  SELECT
    TRIM(return_to_vendor_number) AS return_to_vendor_num,
    TRIM(shipment_detail_product_id) AS rms_sku_num,
    TRIM(correlation_id) AS correlation_id,
    from_location_facility AS from_location_facility,
    from_location_logical AS from_location_logical,
    shipment_detail_quantity AS quantity,
    shipment_detail_product_disposition AS disposition,
    shipment_detail_time_utc AS shipment_time,
    shipment_detail_time_tz AS shipment_time_tz,
    (SELECT batch_id FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) =LOWER('{{params.subject_area}}')) AS dw_batch_id,
    (SELECT curr_batch_date FROM  {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) =LOWER('{{params.subject_area}}')) AS dw_batch_date,
    TIMESTAMP(CURRENT_DATETIME('PST8PDT')) AS dw_sys_load_tmstp,
    `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(TIMESTAMP(CURRENT_DATETIME('PST8PDT')) AS STRING)) AS dw_sys_load_tmstp_tz,
    TIMESTAMP(CURRENT_DATETIME('PST8PDT')) AS dw_sys_updt_tmstp,
    `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(TIMESTAMP(CURRENT_DATETIME('PST8PDT')) AS STRING)) AS dw_sys_updt_tmstp_tz,
  FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.rms_cost_rtv_shipment_details_v2_ldg
  QUALIFY ROW_NUMBER() OVER(
	PARTITION BY
	  return_to_vendor_num,
      rms_sku_num,
      correlation_id
      ORDER BY shipment_time desc) = 1
) ldg
ON LOWER(ldg.return_to_vendor_num) = LOWER(fact.return_to_vendor_num)
  AND LOWER(ldg.rms_sku_num) = LOWER(fact.rms_sku_num)
  AND LOWER(ldg.correlation_id) = LOWER(fact.correlation_id)
WHEN MATCHED THEN
  UPDATE SET
    from_location_facility = ldg.from_location_facility,
    from_location_logical = ldg.from_location_logical,
    quantity = ldg.quantity,
    disposition = ldg.disposition,
    shipment_time = ldg.shipment_time,
    dw_batch_id = ldg.dw_batch_id,
    dw_batch_date = ldg.dw_batch_date,
    dw_sys_updt_tmstp = ldg.dw_sys_updt_tmstp
WHEN NOT MATCHED THEN
  INSERT (
    return_to_vendor_num,
    rms_sku_num,
    correlation_id,
    from_location_facility,
    from_location_logical,
    quantity,
    disposition,
    shipment_time,
    shipment_time_tz,
    dw_batch_id,
    dw_batch_date,
    dw_sys_load_tmstp,
    dw_sys_load_tmstp_tz,
    dw_sys_updt_tmstp,
    dw_sys_updt_tmstp_tz
  )
  VALUES (
    ldg.return_to_vendor_num,
    ldg.rms_sku_num,
    ldg.correlation_id,
    ldg.from_location_facility,
    ldg.from_location_logical,
    ldg.quantity,
    ldg.disposition,
    ldg.shipment_time,
    ldg.shipment_time_tz,
    ldg.dw_batch_id,
    ldg.dw_batch_date,
    ldg.dw_sys_load_tmstp,
    ldg.dw_sys_load_tmstp_tz,
    ldg.dw_sys_updt_tmstp,
    ldg.dw_sys_updt_tmstp_tz
  );

-- COLLECT STATS ON PRD_NAP_FCT.RMS_COST_RTV_SHIPMENT_DETAILS_V2_FACT INDEX ( return_to_vendor_num ,rms_sku_num ,correlation_id );

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DQ_PIPELINE_AUDIT_V1
(
  'RMS_COST_RTV_SHIPMENT_DETAILS_V2_LDG_TO_BASE',
  '{{params.subject_area}}',
  '{{params.dbenv}}_NAP_STG',
  'RMS_COST_RTV_SHIPMENT_DETAILS_V2_LDG',
  '{{params.dbenv}}_NAP_FCT',
  'RMS_COST_RTV_SHIPMENT_DETAILS_V2_FACT',
  'Count_Distinct',
  0,
  'T-S',
  'concat( return_to_vendor_number, correlation_id, shipment_detail_product_id )',
  'concat( return_to_vendor_num, rms_sku_num, correlation_id )',
  NULL,
  NULL,
  'Y'
);



/***********************************************************************************
-- DATA_TIMELINESS_METRIC
************************************************************************************/
 CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD
  ('RMS_COST_RTV_SHIPMENT_DETAILS_V2_FACT',
   '{{params.dbenv}}_NAP_FCT',
   '{{params.dag_name}}',
   'teradata_load',
   1,'LOAD_END', '',
   CURRENT_DATETIME('PST8PDT'),
   '{{params.subject_area}}');



/***********************************************************************************
-- Update control metrics table with temp table
************************************************************************************/
INSERT INTO isf_dag_control_log_wrk(isf_dag_nm,step_nm,batch_id,tbl_nm,metric_nm,metric_value,metric_tmstp,metric_tmstp_tz,metric_date)
select
  '{{params.dag_name}}' AS isf_dag_nm,
  CAST(NULL AS STRING) AS step_nm,
  (SELECT batch_id FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) =LOWER('{{params.subject_area}}')) as batch_id,
  CAST('{{params.dbenv}}_NAP_FCT.RMS_COST_RTV_SHIPMENT_DETAILS_V2_FACT' AS STRING) AS tbl_nm,
  CAST('TERADATA_JOB_END_TMSTP' AS STRING) AS metric_nm,
  CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS  TIMESTAMP) AS STRING) AS metric_value,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS metric_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS  TIMESTAMP) AS STRING)) AS metric_tmstp_tz,
  CURRENT_DATE('GMT') AS metric_date;



UPDATE {{params.gcp_project_id}}.{{params.dbenv}}_nap_utl.isf_dag_control_log AS tgt
SET metric_value=wrk.metric_value,
    metric_tmstp=CAST(wrk.metric_tmstp AS TIMESTAMP),
    metric_tmstp_tz=wrk.metric_tmstp_tz
FROM isf_dag_control_log_wrk wrk
WHERE LOWER(tgt.isf_dag_nm)=LOWER(wrk.isf_dag_nm)
  AND LOWER(coalesce(tgt.tbl_nm, '')) = LOWER(coalesce(wrk.tbl_nm, ''))
  AND LOWER(tgt.metric_nm)=LOWER(wrk.metric_nm)
  AND tgt.batch_id=wrk.batch_id;
