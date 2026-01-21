--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File              : wm_inbound_carton_received_fact_load.sql
-- Author            : Dmytro Utte
-- Description       : Merge WM_INBOUND_CARTON_RECEIVED_FACT table with delta from staging table
-- Source topic      : inventory-distribution-center-inbound-carton-v2-analytical-avro
-- Object model      : DistributionCenterInboundCartonV2
-- ETL Run Frequency : Every hour
-- Version :         : 0.2
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2023-12-26  Utte Dmytro   FA-10508: Omni Hub data
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
 CAST(CURRENT_DATETIME('GMT') AS TIMESTAMP) AS metric_tmstp,
 CURRENT_DATE('GMT') AS metric_date)
SELECT isf_dag_nm,
 CAST(NULL AS STRING) AS step_nm,
 batch_id,
 SUBSTR('{{params.dbenv}}_NAP_FCT.WM_INBOUND_CARTON_RECEIVED_FACT', 1, 100) AS tbl_nm,
 SUBSTR('TERADATA_JOB_START_TMSTP', 1, 100) AS metric_nm,
 SUBSTR(CAST(metric_tmstp AS STRING), 1, 60) AS metric_value,
 CAST(CURRENT_DATETIME('GMT') AS TIMESTAMP) as metric_tmstp,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy AS d;


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.wm_inbound_carton_received_fact
WHERE EXISTS (SELECT *
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.wm_inbound_carton_ldg
 WHERE LOWER(wm_inbound_carton_received_fact.inbound_carton_id) = LOWER(inbound_carton_id)
  AND LOWER(wm_inbound_carton_received_fact.sku_id) = LOWER(sku_id));




INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.wm_inbound_carton_received_fact
WITH x AS (
  SELECT
    CONCAT(inbound_carton_id, sku_id) AS key_id,
    JSON_EXTRACT_ARRAY(initial_inventory_states_array) AS inventory_state
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.wm_inbound_carton_ldg
  WHERE
    initial_inventory_states_array IS NOT NULL
),
parsed_json AS (
  SELECT
    x.key_id,
    JSON_EXTRACT_SCALAR(element, '$.inventoryState') AS sku_inventory_state_code,
    JSON_EXTRACT_SCALAR(element, '$.quality') AS sku_quality_code,
    CAST(JSON_EXTRACT_SCALAR(element, '$.quantity') AS INT64) AS sku_qty
  FROM
    x,
    UNNEST(x.inventory_state) AS element
)
SELECT
  a.inbound_carton_id,
  CAST(a.received_time AS TIMESTAMP),
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(a.received_time) AS  received_time_tz,
  a.carton_lpn,
  a.purchase_order_number,
  a.location_id,
  a.vendor_number AS vendor_bill_of_Lading_number,
  a.vendor_number_type AS vendor_bill_of_Lading_type,
  a.carrier_number AS carrier_bill_of_Lading_number,
  a.carrier_number_type AS carrier_bill_of_Lading_type,
  a.sku_id,
  a.sku_type,
  parsed_json.sku_inventory_state_code,
  parsed_json.sku_quality_code,
  parsed_json.sku_qty,
  a.reason_codes,
  (SELECT BATCH_ID FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE SUBJECT_AREA_NM = 'NAP_ASCP_WM_INBOUND_CARTON') AS dw_batch_id,
  (SELECT CURR_BATCH_DATE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE SUBJECT_AREA_NM = 'NAP_ASCP_WM_INBOUND_CARTON') AS dw_batch_date,
  CAST(CURRENT_DATETIME('PST8PDT') AS TIMESTAMP) AS dw_sys_load_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS dw_sys_load_tmstp_tz,
  CAST(CURRENT_DATETIME('PST8PDT') AS TIMESTAMP) AS dw_sys_updt_tmstp_tz,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS dw_sys_updt_tmstp_tz
FROM
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.wm_inbound_carton_ldg a
JOIN
  parsed_json
ON
  CONCAT(a.inbound_carton_id, a.sku_id) = parsed_json.key_id;


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.dq_pipeline_audit_v1
(
  'WM_INBOUND_CARTON_RECEIVED_LDG_TO_BASE',
  'NAP_ASCP_WM_INBOUND_CARTON',
  '{{params.dbenv}}_NAP_BASE_VWS',
  'WM_INBOUND_CARTON_LDG',
  '{{params.dbenv}}_NAP_BASE_VWS',
  'WM_INBOUND_CARTON_RECEIVED_FACT',
  'Count_Distinct',
  0,
  'T-S',
  'concat(inbound_carton_id, "-", sku_id)',
  'concat(inbound_carton_id, "-", sku_id)',
  ' sku_id is not null and initial_inventory_states_array is not null',
  ' sku_id is not null',
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
  SUBSTR('{{params.dbenv}}_NAP_FCT.WM_INBOUND_CARTON_RECEIVED_FACT', 1, 100) AS tbl_nm,
  SUBSTR('TERADATA_JOB_END_TMSTP', 1, 100) AS metric_nm,
  SUBSTR(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS STRING), 1, 60) AS metric_value,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS
  metric_tmstp_utc,
   'GMT' AS metric_tmstp_tz,
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