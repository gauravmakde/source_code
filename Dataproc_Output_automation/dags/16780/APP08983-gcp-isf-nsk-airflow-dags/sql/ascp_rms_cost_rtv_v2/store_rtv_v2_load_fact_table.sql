--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File              : store_rtv_v2_load_fact_table.sql
-- Author            : Kunal Lalwani
-- Description       : Merge COST_RTV_V2 table with delta from staging tables
-- Source topic      : inventory-merchandise-return-to-vendor-v2-analytical-avro
-- Object model      : Alfred RMS RTV V2
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
	CAST('{{params.dbenv}}_NAP_FCT.RMS_COST_RTV_V2_FACT' AS STRING) AS tbl_nm,
	CAST('TERADATA_JOB_START_TMSTP' AS STRING) AS metric_nm,
	CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS  TIMESTAMP) AS STRING) AS metric_value,
	CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS metric_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS  TIMESTAMP) AS STRING)) AS metric_tmstp_tz,
	CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT
  '{{params.dag_name}}' AS isf_dag_nm,
   CAST(NULL AS STRING) AS step_nm,
  (SELECT batch_id FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) =LOWER('{{params.subject_area}}')) AS batch_id,
  cast('{{params.dbenv}}_NAP_STG.RMS_COST_RTV_V2_LDG' AS STRING) AS tbl_nm,
  cast('LOADED_TO_LDG_CNT' AS STRING) AS metric_nm,
  cast((select metric_value from {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.processed_data_spark_log
                   where LOWER(isf_dag_nm) = LOWER('{{params.dag_name}}')
                         and LOWER(tbl_nm) = LOWER('RMS_COST_RTV_V2_LDG')
                         and LOWER(metric_nm) = LOWER('PROCESSED_ROWS_CNT')) AS STRING) AS metric_value,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS metric_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS  TIMESTAMP) AS STRING)) AS metric_tmstp_tz,
  CURRENT_DATE('GMT') AS metric_date
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
			AND LOWER(metric_nm) = LOWER('SPARK_PROCESSED_MESSAGE_CNT')) AS STRING) AS metric_value,
	CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS metric_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS  TIMESTAMP) AS STRING)) AS metric_tmstp_tz,
	CURRENT_DATE('GMT') AS metric_date
FROM dummy;



/***********************************************************************************
-- DATA_TIMELINESS_METRIC
************************************************************************************/

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD
  ('RMS_COST_RTV_V2_FACT',
   '{{params.dbenv}}_NAP_FCT',
   '{{params.dag_name}}',
   'teradata_load',
   0,'LOAD_START', '',
   CURRENT_DATETIME('PST8PDT'),
   '{{params.subject_area}}');



/***********************************************************************************
-- DATA UPSERT
************************************************************************************/

MERGE INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_fct.rms_cost_rtv_v2_fact fact
USING (
  SELECT
    trim(details_ldg.return_to_vendor_number) as return_to_vendor_num,
    trim(details_ldg.create_detail_product_id) as rms_sku_num,
    trim(details_ldg.correlation_id) as correlation_id,
    trim(details_ldg.external_reference_number) as external_reference_num,
    details_ldg.from_location_facility as from_location_facility,
    details_ldg.from_location_logical as from_location_logical,
    trim(details_ldg.vendor_number) as vendor_num,
    trim(details_ldg.vendor_address_line1_tokenized) as vendor_address_line1_tokenized,
    trim(details_ldg.vendor_address_line2_tokenized) as vendor_address_line2_tokenized,
    trim(details_ldg.vendor_address_line3_tokenized) as vendor_address_line3_tokenized,
    trim(details_ldg.vendor_address_city) as vendor_address_city,
    trim(details_ldg.vendor_address_state) as vendor_address_state,
    trim(details_ldg.vendor_address_postal_code_tokenized) as vendor_address_postal_code_tokenized,
    trim(details_ldg.vendor_address_country_code) as vendor_address_country_code,
    trim(details_ldg.returns_authorization_number) as returns_authorization_num,
    details_ldg.create_system_time as create_system_time,
    details_ldg.create_system_time_tz as create_system_time_tz,
    details_ldg.latest_updated_system_time as latest_updated_system_time,
    details_ldg.latest_updated_system_time_tz as latest_updated_system_time_tz,
    details_ldg.last_triggering_event_type as last_triggering_event_type,
    details_ldg.create_detail_quantity as quantity,
    details_ldg.create_detail_product_disposition as disposition,
    trim(details_ldg.create_detail_product_reason_code) as reason_code,
    trim(details_ldg.create_comments) as comments,
    details_ldg.create_detail_time as create_detail_time,
    details_ldg.create_detail_time_tz as create_detail_time_tz,
    (SELECT batch_id FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) =LOWER('{{params.subject_area}}')) as dw_batch_id,
    (SELECT curr_batch_date FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) =LOWER('{{params.subject_area}}')) as dw_batch_date,
    TIMESTAMP(CURRENT_DATETIME('PST8PDT')) AS dw_sys_load_tmstp,
    `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(TIMESTAMP(CURRENT_DATETIME('PST8PDT')) AS STRING)) AS dw_sys_load_tmstp_tz,
    TIMESTAMP(CURRENT_DATETIME('PST8PDT')) AS dw_sys_updt_tmstp,
    `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(TIMESTAMP(CURRENT_DATETIME('PST8PDT')) AS STRING)) AS dw_sys_updt_tmstp_tz,
  FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.rms_cost_rtv_v2_ldg details_ldg
  QUALIFY ROW_NUMBER() OVER(
	PARTITION BY
	  return_to_vendor_num,
      rms_sku_num,
      correlation_id
      ORDER BY latest_updated_system_time desc) = 1
) ldg
ON LOWER(ldg.return_to_vendor_num) = LOWER(fact.return_to_vendor_num)
  AND LOWER(ldg.rms_sku_num) = LOWER(fact.rms_sku_num)
  AND LOWER(ldg.correlation_id) = LOWER(fact.correlation_id)
WHEN MATCHED THEN
  UPDATE SET
    external_reference_num = ldg.external_reference_num,
    from_location_facility = ldg.from_location_facility,
    from_location_logical = ldg.from_location_logical,
    vendor_num = ldg.vendor_num,
    vendor_address_line1_tokenized = ldg.vendor_address_line1_tokenized,
    vendor_address_line2_tokenized = ldg.vendor_address_line2_tokenized,
    vendor_address_line3_tokenized = ldg.vendor_address_line3_tokenized,
    vendor_address_city = ldg.vendor_address_city,
    vendor_address_state = ldg.vendor_address_state,
    vendor_address_postal_code_tokenized = ldg.vendor_address_postal_code_tokenized,
    vendor_address_country_code = ldg.vendor_address_country_code,
    returns_authorization_num = ldg.returns_authorization_num,
    comments = ldg.comments,
    quantity = ldg.quantity,
    disposition = ldg.disposition,
    reason_code = ldg.reason_code,
    create_system_time = CAST(ldg.create_system_time AS TIMESTAMP),
    create_system_time_tz = ldg.create_system_time_tz,
    latest_updated_system_time = CAST(ldg.latest_updated_system_time AS TIMESTAMP),
    latest_updated_system_time_tz = ldg.latest_updated_system_time_tz,
    create_detail_time = CAST(ldg.create_detail_time AS TIMESTAMP),
    create_detail_time_tz = ldg.create_detail_time_tz,
    last_triggering_event_type = ldg.last_triggering_event_type,
    dw_batch_id = ldg.dw_batch_id,
    dw_batch_date = ldg.dw_batch_date,
    dw_sys_updt_tmstp = ldg.dw_sys_updt_tmstp,
    dw_sys_updt_tmstp_tz = ldg.dw_sys_updt_tmstp_tz
WHEN NOT MATCHED THEN
  INSERT (
    return_to_vendor_num,
    rms_sku_num,
    correlation_id,
    external_reference_num,
    from_location_facility,
    from_location_logical,
    vendor_num,
    vendor_address_line1_tokenized,
    vendor_address_line2_tokenized,
    vendor_address_line3_tokenized,
    vendor_address_city,
    vendor_address_state,
    vendor_address_postal_code_tokenized,
    vendor_address_country_code,
    returns_authorization_num,
    comments,
    quantity,
    disposition,
    reason_code,
    create_system_time,
    create_system_time_tz,
    latest_updated_system_time,
    latest_updated_system_time_tz,
    create_detail_time,
    create_detail_time_tz,
    last_triggering_event_type,
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
    ldg.external_reference_num,
    ldg.from_location_facility,
    ldg.from_location_logical,
    ldg.vendor_num,
    ldg.vendor_address_line1_tokenized,
    ldg.vendor_address_line2_tokenized,
    ldg.vendor_address_line3_tokenized,
    ldg.vendor_address_city,
    ldg.vendor_address_state,
    ldg.vendor_address_postal_code_tokenized,
    ldg.vendor_address_country_code,
    ldg.returns_authorization_num,
    ldg.comments,
    ldg.quantity,
    ldg.disposition,
    ldg.reason_code,
    CAST(ldg.create_system_time AS TIMESTAMP),
    ldg.create_system_time_tz,
    CAST(ldg.latest_updated_system_time AS TIMESTAMP),
    ldg.latest_updated_system_time_tz,
    CAST(ldg.create_detail_time AS TIMESTAMP),
    ldg.create_detail_time_tz,
    ldg.last_triggering_event_type,
    ldg.dw_batch_id,
    ldg.dw_batch_date,
    ldg.dw_sys_load_tmstp,
    ldg.dw_sys_load_tmstp_tz,
    ldg.dw_sys_updt_tmstp,
    ldg.dw_sys_updt_tmstp_tz
  );



CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DQ_PIPELINE_AUDIT_V1
(
  'RMS_COST_RTV_V2_LDG_TO_BASE',
  '{{params.subject_area}}',
  '{{params.dbenv}}_NAP_STG',
  'RMS_COST_RTV_V2_LDG',
  '{{params.dbenv}}_NAP_FCT',
  'RMS_COST_RTV_V2_FACT',
  'Count_Distinct',
  0,
  'T-S',
  'concat( return_to_vendor_number, correlation_id, create_detail_product_id )',
  'concat( return_to_vendor_num, rms_sku_num, correlation_id )',
  null,
  null,
  'Y'
);



/***********************************************************************************
-- DATA_TIMELINESS_METRIC
************************************************************************************/
 CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD
  ('RMS_COST_RTV_V2_FACT',
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
  (SELECT batch_id FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) =LOWER('{{params.subject_area}}')) AS batch_id,
  CAST('{{params.dbenv}}_NAP_FCT.RMS_COST_RTV_V2_FACT' AS STRING) AS tbl_nm,
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
  AND LOWER(COALESCE(tgt.tbl_nm, '')) = LOWER(COALESCE(wrk.tbl_nm, ''))
  AND LOWER(tgt.metric_nm)=LOWER(wrk.metric_nm)
  AND tgt.batch_id=wrk.batch_id;
