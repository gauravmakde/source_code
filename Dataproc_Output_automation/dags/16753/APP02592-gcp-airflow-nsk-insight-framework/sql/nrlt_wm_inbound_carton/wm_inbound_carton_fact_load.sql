--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File              : read_wm_inbound_carton_fact_load.sql
-- Author            : Dmytro Utte
-- Description       : Merge WM_INBOUND_CARTON_FACT table with delta from staging table
-- Source topic      : inventory-distribution-center-inbound-carton-v2-analytical-avro
-- Object model      : DistributionCenterInboundCartonV2
-- ETL Run Frequency : Every hour
-- Version :         : 0.3
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2023-08-17  Utte Dmytro   FA-9913: Migrate NRLT DAG for WM_INBOUND_CARTON to IsF
-- 2023-12-18  Utte Dmytro   FA-10833: inventory_state field was removed, initial_inventory_states_array field is added
-- 2024-02-07  Andrii Skyba  FA-11561: Optimize NRLT DAGs
--*************************************************************************************************************************************
/***********************************************************************************
-- Load temp table with metrics
************************************************************************************/



CREATE TEMPORARY TABLE IF NOT EXISTS isf_dag_control_log_wrk  AS
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
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS metric_tmstp,
 CURRENT_DATE('GMT') AS metric_date)
SELECT isf_dag_nm,
 CAST(NULL AS STRING) AS step_nm,
 batch_id,
 SUBSTR('{{params.dbenv}}_NAP_FCT.WM_INBOUND_CARTON_FACT', 1, 100) AS tbl_nm,
 SUBSTR('TERADATA_JOB_START_TMSTP', 1, 100) AS metric_nm,
 SUBSTR(CAST(metric_tmstp AS STRING), 1, 60) AS metric_value,
 CAST(metric_tmstp as timestamp) as metric_tmstp,
 metric_date
FROM dummy AS d
UNION ALL
SELECT isf_dag_nm,
 NULL AS step_nm,
 batch_id,
 SUBSTR('{{params.dbenv}}_NAP_STG.WM_INBOUND_CARTON_LDG', 1, 100) AS tbl_nm,
 SUBSTR('LOADED_TO_LDG_CNT', 1, 100) AS metric_nm,
 SUBSTR((SELECT metric_value
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.processed_data_spark_log
   WHERE LOWER(isf_dag_nm) = LOWER('nrlt_wm_inbound_carton_lifecycle_16753_TECH_SC_NAP_insights')
    AND LOWER(tbl_nm) = LOWER('WM_INBOUND_CARTON_LDG')
    AND LOWER(metric_nm) = LOWER('PROCESSED_ROWS_CNT')), 1, 60) AS metric_value,
 CAST(metric_tmstp as timestamp) as metric_tmstp,
 metric_date
FROM dummy AS d
UNION ALL
SELECT isf_dag_nm,
 NULL AS step_nm,
 batch_id,
 SUBSTR(NULL, 1, 100) AS tbl_nm,
 SUBSTR('SPARK_PROCESSED_MESSAGE_CNT', 1, 100) AS metric_nm,
 SUBSTR((SELECT metric_value
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.processed_data_spark_log
   WHERE LOWER(isf_dag_nm) = LOWER('nrlt_wm_inbound_carton_lifecycle_16753_TECH_SC_NAP_insights')
    AND LOWER(metric_nm) = LOWER('SPARK_PROCESSED_MESSAGE_CNT')), 1, 60) AS metric_value,
 CAST(metric_tmstp as timestamp) as metric_tmstp,
 metric_date
FROM dummy AS d;


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.wm_inbound_carton_fact
WHERE EXISTS (SELECT *
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.wm_inbound_carton_ldg
 WHERE LOWER(wm_inbound_carton_fact.inbound_carton_id) = LOWER(inbound_carton_id)
  AND wm_inbound_carton_fact.sku_id IS NULL);


MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.wm_inbound_carton_fact AS fact
USING (SELECT inbound_carton_id,
   CAST(CONCAT(received_time, '+00:00') AS TIMESTAMP) AS received_time,
  '+00:00' AS received_time_tz,
  carton_lpn,
  purchase_order_number,
  location_id,
  vendor_number,
  vendor_number_type,
  carrier_number,
  carrier_number_type,
  sku_id,
  sku_type,
  initial_qty,
  latest_qty,
  reason_codes,
  initial_inventory_states_array,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_WM_INBOUND_CARTON')) AS dw_batch_id,
   (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_WM_INBOUND_CARTON')) AS dw_batch_date
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.wm_inbound_carton_ldg
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY inbound_carton_id, sku_id ORDER BY received_time DESC)) = 1) AS t4
ON LOWER(fact.inbound_carton_id) = LOWER(t4.inbound_carton_id) AND LOWER(fact.sku_id) = LOWER(t4.sku_id)
WHEN MATCHED THEN UPDATE SET
 received_time = t4.received_time,
 received_time_tz = t4.received_time_tz,
 carton_lpn = t4.carton_lpn,
 purchase_order_number = t4.purchase_order_number,
 location_id = t4.location_id,
 vendor_bill_of_lading_number = t4.vendor_number,
 vendor_bill_of_lading_type = t4.vendor_number_type,
 carrier_bill_of_lading_number = t4.carrier_number,
 carrier_bill_of_lading_type = t4.carrier_number_type,
 sku_type = t4.sku_type,
 initial_qty = CAST(TRUNC(CAST(t4.initial_qty AS FLOAT64)) AS INTEGER),
 latest_qty = CAST(TRUNC(CAST(t4.latest_qty AS FLOAT64)) AS INTEGER),
 reason_codes = TO_JSON(t4.reason_codes),
 initial_inventory_states_array = t4.initial_inventory_states_array,
 dw_batch_id = t4.dw_batch_id,
 dw_batch_date = t4.dw_batch_date,
 dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME()) AS TIMESTAMP),
 dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
WHEN NOT MATCHED THEN INSERT 
VALUES(t4.inbound_carton_id, 
t4.received_time,
t4.received_time_tz, 
t4.carton_lpn, 
t4.purchase_order_number, 
t4.location_id, 
 t4.vendor_number, 
 t4.vendor_number_type, 
 t4.carrier_number, 
 t4.carrier_number_type, 
 t4.sku_id, 
 t4.sku_type , 
 CAST(TRUNC(CAST(t4.initial_qty AS FLOAT64)) AS INTEGER),
  CAST(TRUNC(CAST(t4.latest_qty AS FLOAT64)) AS INTEGER),
   TO_JSON(CASE
   WHEN LOWER(t4.reason_codes) = LOWER('[]')
   THEN NULL
   ELSE t4.reason_codes
   END), t4.dw_batch_id, t4.dw_batch_date, 
   CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP),
   `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST(),
   CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP), 
   `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST(),
 t4.initial_inventory_states_array);


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.wm_inbound_carton_fact
(SELECT inbound_carton_id,
   CAST(CONCAT(received_time, '+00:00') AS TIMESTAMP) AS received_time,
  '+00:00' as received_time_tz,
  carton_lpn,
  purchase_order_number,
  location_id,
  vendor_number,
  vendor_number_type,
  carrier_number,
  carrier_number_type,
  sku_id,
  sku_type,
  CAST(TRUNC(NULL) AS INTEGER) AS initial_qty,
  CAST(TRUNC(NULL) AS INTEGER) AS latest_qty,
  TO_JSON(reason_codes) AS reason_codes,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_WM_INBOUND_CARTON')) AS dw_batch_id,
   (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_WM_INBOUND_CARTON')) AS dw_batch_date,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as  dw_sys_load_tmstp_tz,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as dw_sys_updt_tmstp_tz,
  CAST(NULL AS STRING) AS initial_inventory_states_array
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.wm_inbound_carton_ldg AS ldg
 WHERE sku_id IS NULL);


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.dq_pipeline_audit_v1
(
  'WM_INBOUND_CARTON_LDG_TO_BASE',
  'NAP_ASCP_WM_INBOUND_CARTON',
  '{{params.dbenv}}_NAP_BASE_VWS',
  'WM_INBOUND_CARTON_LDG',
  '{{params.dbenv}}_NAP_FCT',
  'WM_INBOUND_CARTON_FACT',
  'Count_Distinct',
  0,
  'T-S',
  'concat(inbound_carton_id, "-", sku_id)',
  'concat(inbound_carton_id, "-", sku_id)',
  ' sku_id is not null',
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
  SUBSTR('{{params.dbenv}}_NAP_FCT.WM_INBOUND_CARTON_FACT', 1, 100) AS tbl_nm,
  SUBSTR('TERADATA_JOB_END_TMSTP', 1, 100) AS metric_nm,
  SUBSTR(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS STRING), 1, 60) AS metric_value,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS
  metric_tmstp_utc,
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