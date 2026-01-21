--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File              : read_wm_inbound_carton_events_fact_load.sql
-- Author            : Dmytro Utte
-- Description       : Merge WM_INBOUND_CARTON_EVENTS_FACT table with delta from staging table
-- Source topic      : inventory-distribution-center-inbound-carton-v2-analytical-avro
-- Object model      : DistributionCenterInboundCartonV2
-- ETL Run Frequency : Every hour
-- Version :         : 0.3
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2023-08-17  Utte Dmytro   FA-9913: Migrate NRLT DAG for WM_INBOUND_CARTON to IsF
-- 2023-12-18  Utte Dmytro   FA-10833: selling_brands_logical_locations_array field is added
-- 2024-02-07  Andrii Skyba  FA-11561: Optimize NRLT DAGs
--*************************************************************************************************************************************
/***********************************************************************************
-- Load temp table with metrics
************************************************************************************/



CREATE TEMPORARY TABLE IF NOT EXISTS isf_dag_control_log_wrk AS
SELECT *
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.isf_dag_control_log
LIMIT 0;


INSERT INTO isf_dag_control_log_wrk
(
isf_dag_nm,
step_nm,
batch_id,
tbl_nm,
metric_nm,
metric_value,
metric_tmstp_utc,
metric_date
)
WITH dummy AS (SELECT 'nrlt_wm_inbound_carton_lifecycle_16753_TECH_SC_NAP_insights' AS isf_dag_nm,
  (SELECT batch_id AS isf_dag_nm
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_WM_INBOUND_CARTON')) AS batch_id,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS TIMESTAMP) AS metric_tmstp,
 CURRENT_DATE('GMT') AS metric_date)
SELECT isf_dag_nm,
 CAST(NULL AS STRING) AS step_nm,
 batch_id,
 SUBSTR('{{params.dbenv}}_NAP_FCT.WM_INBOUND_CARTON_EVENTS_FACT', 1, 100) AS tbl_nm,
 SUBSTR('TERADATA_JOB_START_TMSTP', 1, 100) AS metric_nm,
 SUBSTR(CAST(metric_tmstp AS STRING), 1, 60) AS metric_value,
 metric_tmstp,
 metric_date
FROM dummy AS d
UNION ALL
SELECT isf_dag_nm,
 NULL AS step_nm,
 batch_id,
 SUBSTR('{{params.dbenv}}_NAP_STG.WM_INBOUND_CARTON_EVENTS_LDG', 1, 100) AS tbl_nm,
 SUBSTR('LOADED_TO_LDG_CNT', 1, 100) AS metric_nm,
 SUBSTR((SELECT metric_value
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.processed_data_spark_log
   WHERE LOWER(isf_dag_nm) = LOWER('nrlt_wm_inbound_carton_lifecycle_16753_TECH_SC_NAP_insights')
    AND LOWER(tbl_nm) = LOWER('WM_INBOUND_CARTON_EVENTS_LDG')
    AND LOWER(metric_nm) = LOWER('PROCESSED_ROWS_CNT')), 1, 60) AS metric_value,
 metric_tmstp,
 metric_date
FROM dummy AS d;


MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.wm_inbound_carton_events_fact AS fact
USING (SELECT event_source,
  inbound_carton_id,
  warehouse_event_type,
  audit_event_type,
  CAST(concat(event_start_tmstp, '+00:00') as timestamp ) as event_start_tmstp,
  '+00:00' AS event_start_tmstp_tz,
  CAST(concat(event_tmstp, '+00:00') as timestamp) as event_tmstp,
  '+00:00' AS event_tmstp_tz,
  user_id,
  user_type,
  originating_event_id,
  event_location_num,
  audit_result,
  all_audit_completed,
  CAST(concat(audit_completed_tmstp, '+00:00') as timestamp) as audit_completed_tmstp,
  '+00:00' AS audit_completed_tmstp_tz,
  CAST(concat(last_updt_tmstp, '+00:00') as timestamp) as last_updt_tmstp,
  '+00:00' AS last_updt_tmstp_tz,
  selling_brands_logical_locations_array,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_WM_INBOUND_CARTON')) AS dw_batch_id,
   (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_WM_INBOUND_CARTON')) AS dw_batch_date
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.wm_inbound_carton_events_ldg
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY inbound_carton_id, originating_event_id ORDER BY event_tmstp DESC)) = 1) AS t4

ON LOWER(fact.inbound_carton_id) = LOWER(t4.inbound_carton_id) AND LOWER(fact.originating_event_id) = LOWER(t4.originating_event_id
    ) AND fact.event_tmstp = CAST(t4.event_tmstp AS TIMESTAMP)
WHEN MATCHED THEN UPDATE SET
 event_source = t4.event_source,
 warehouse_event_type = t4.warehouse_event_type,
 audit_event_type = t4.audit_event_type,
 user_id = t4.user_id,
 user_type = t4.user_type,
 event_location_num = t4.event_location_num,
 event_start_tmstp = CAST(t4.event_start_tmstp AS TIMESTAMP),
 event_start_tmstp_tz = t4.event_start_tmstp_tz,
 audit_result_status = t4.audit_result,
 last_updt_tmstp = CAST(t4.last_updt_tmstp AS TIMESTAMP),
 last_updt_tmstp_tz = t4.last_updt_tmstp_tz,
 all_audit_completed_ind = t4.all_audit_completed,
 audit_completed_tmstp = CAST(t4.audit_completed_tmstp AS TIMESTAMP),
 audit_completed_tmstp_tz = t4.audit_completed_tmstp_tz,
 selling_brands_logical_locations_array = t4.selling_brands_logical_locations_array,
 dw_batch_id = t4.dw_batch_id,
 dw_batch_date = t4.dw_batch_date,
 dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP),
 dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
WHEN NOT MATCHED THEN INSERT 
VALUES(t4.inbound_carton_id, 
t4.originating_event_id, 
t4.user_id, 
t4.user_type, 
t4.event_source , 
 t4.warehouse_event_type, 
 t4.audit_event_type, 
 CAST(t4.event_start_tmstp AS TIMESTAMP), 
 t4.event_start_tmstp_tz,
 CAST(t4.event_tmstp AS TIMESTAMP) , 
 t4.event_tmstp_tz,
 t4.event_location_num, 
 t4.audit_result, 
 t4.all_audit_completed, 
 CAST(t4.audit_completed_tmstp AS TIMESTAMP) , 
 t4.audit_completed_tmstp_tz,
 CAST(t4.last_updt_tmstp AS TIMESTAMP), 
 t4.last_updt_tmstp_tz,
 t4.dw_batch_id, 
 t4.dw_batch_date, 
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP) , 
 `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST(),
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP),
 `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST(),
 t4.selling_brands_logical_locations_array);


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.dq_pipeline_audit_v1
(
  'WM_INBOUND_CARTON_EVENTS_LDG_TO_BASE',
  'NAP_ASCP_WM_INBOUND_CARTON',
  '{{params.dbenv}}_NAP_STG',
  'WM_INBOUND_CARTON_EVENTS_LDG',
  '{{params.dbenv}}_NAP_FCT',
  'WM_INBOUND_CARTON_EVENTS_FACT',
  'Count_Distinct',
  0,
  'T-S',
  'concat(originating_event_id, "-", inbound_carton_id)',
  'concat(originating_event_id, "-"", inbound_carton_id)',
  null,
  null,
  'Y'
);


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
(SELECT 'nrlt_wm_inbound_carton_lifecycle_16753_TECH_SC_NAP_insights' AS isf_dag_nm,
  CAST(NULL AS STRING) AS step_nm,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_WM_INBOUND_CARTON')) AS batch_id,
  SUBSTR('{{params.dbenv}}_NAP_FCT.WM_INBOUND_CARTON_EVENTS_FACT', 1, 100) AS tbl_nm,
  SUBSTR('TERADATA_JOB_END_TMSTP', 1, 100) AS metric_nm,
  SUBSTR(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS STRING), 1, 60) AS metric_value,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  'GMT' as metric_tmstp_tz,
  CURRENT_DATE('GMT') AS metric_date);


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.isf_dag_control_log SET
 metric_value = wrk.metric_value,
 metric_tmstp = CAST(wrk.metric_tmstp_utc AS TIMESTAMP) ,
 metric_tmstp_tz = wrk.metric_tmstp_tz
 FROM isf_dag_control_log_wrk AS wrk
WHERE LOWER(isf_dag_control_log.isf_dag_nm) = LOWER('{{params.dag_name}}') AND LOWER(COALESCE(isf_dag_control_log.tbl_nm, '')) =
    LOWER(COALESCE(wrk.tbl_nm, '')) AND LOWER(isf_dag_control_log.metric_nm) = LOWER(wrk.metric_nm) AND
  isf_dag_control_log.batch_id = (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}'));