--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File              : read_wm_inbound_carton_sku_adkustment_events_fact_load.sql
-- Author            : Dmytro Utte
-- Description       : Merge WM_INBOUND_CARTON_SKU_ADJUSTMENT_EVENTS_FACT table with delta from staging table
-- Source topic      : inventory-distribution-center-inbound-carton-v2-analytical-avro
-- Object model      : DistributionCenterInboundCartonV2
-- ETL Run Frequency : Every hour
-- Version :         : 0.3
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2023-08-17  Utte Dmytro   FA-9913: Migrate NRLT DAG for WM_INBOUND_CARTON to IsF
-- 2023-12-18  Utte Dmytro   FA-10833: sku_quality_code field is added
-- 2024-02-07  Andrii Skyba  FA-11561: Optimize NRLT DAGs
--*************************************************************************************************************************************
/***********************************************************************************
-- Load temp table with metrics
************************************************************************************/



CREATE TEMPORARY TABLE IF NOT EXISTS isf_dag_control_log_wrk AS
SELECT isf_dag_nm			
,step_nm		
,batch_id			
,tbl_nm			
,metric_nm			
,metric_value						
,metric_tmstp_utc			
,metric_tmstp_tz			
,metric_date
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
metric_tmstp_tz,
metric_date
)
WITH dummy AS (SELECT 'nrlt_wm_inbound_carton_lifecycle_16753_TECH_SC_NAP_insights' AS isf_dag_nm,
  (SELECT batch_id AS isf_dag_nm
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_WM_INBOUND_CARTON')) AS batch_id,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp_utc,
'GMT'  as metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date)
SELECT isf_dag_nm,
 cast(NULL as string) AS step_nm,
 batch_id,
 SUBSTR('{{params.dbenv}}_NAP_FCT.WM_INBOUND_CARTON_SKU_ADJUSTMENT_EVENTS_FACT', 1, 100) AS tbl_nm,
 SUBSTR('TERADATA_JOB_START_TMSTP', 1, 100) AS metric_nm,
 SUBSTR(CAST(metric_tmstp_utc AS STRING), 1, 60) AS metric_value,
 metric_tmstp_utc,
 metric_tmstp_tz,
 metric_date
FROM dummy AS d
UNION ALL
SELECT isf_dag_nm,
 NULL AS step_nm,
 batch_id,
 SUBSTR('{{params.dbenv}}_NAP_STG.WM_INBOUND_CARTON_SKU_ADJUSTMENT_EVENTS_LDG', 1, 100) AS tbl_nm,
 SUBSTR('LOADED_TO_LDG_CNT', 1, 100) AS metric_nm,
 SUBSTR((SELECT metric_value
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.processed_data_spark_log
   WHERE LOWER(isf_dag_nm) = LOWER('nrlt_wm_inbound_carton_lifecycle_16753_TECH_SC_NAP_insights')
    AND LOWER(tbl_nm) = LOWER('WM_INBOUND_CARTON_SKU_ADJUSTMENT_EVENTS_LDG')
    AND LOWER(metric_nm) = LOWER('PROCESSED_ROWS_CNT')), 1, 60) AS metric_value,
 metric_tmstp_utc,
 metric_tmstp_tz,
 metric_date
FROM dummy AS d;


/***********************************************************************************
-- Merge into fact table
************************************************************************************/


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.wm_inbound_carton_sku_adjustment_events_fact
WHERE inbound_carton_id IN (SELECT DISTINCT inbound_carton_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.wm_inbound_carton_sku_adjustment_events_ldg);




INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.wm_inbound_carton_sku_adjustment_events_fact
SELECT
    a.inbound_carton_id,
	case when(adjustment_event_type = 'WAREHOUSE_PURCHASE_ORDER_CARTON_SPLIT' and cast(qty as int64) >= 0) then 'null'
	     when instr(inbound_carton_id, '-PO-', -1) > 0 then substring(a.inbound_carton_id, 1, instr(inbound_carton_id, '-PO-', -1) -1)
         else substring(a.inbound_carton_id, 1, instr(a.inbound_carton_id, '-', -1) -1) end as original_carton_id,
	a.location_id,
    a.adjustment_event_type,
    a.sku_id,
    a.sku_type,
    a.inventory_state,
    cast(a.qty as int64) as qty,
    parse_timestamp('%Y-%m-%d %H:%M:%E6S%Ez', concat(a.adjustment_date, '+00:00')) as adjustment_timestamp,
    '+00:00' as adjustment_timestamp_tz,
	CAST(SUBSTRING(a.adjustment_date,1, 10) as DATE ) as adjustment_date,
    a.user_id,
    a.user_type,
    a.to_carton_lpn,
    a.to_carton_purchase_order_number,
    (SELECT BATCH_ID FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE SUBJECT_AREA_NM ='NAP_ASCP_WM_INBOUND_CARTON') as dw_batch_id,
    (SELECT CURR_BATCH_DATE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE SUBJECT_AREA_NM ='NAP_ASCP_WM_INBOUND_CARTON') as dw_batch_date,
	timestamp(current_datetime('PST8PDT')) as dw_sys_load_tmstp,
     `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as dw_sys_load_tmstp_tz,
    timestamp(current_datetime('PST8PDT')) as dw_sys_updt_tmstp,
    `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as dw_sys_updt_tmstp_tz,
	a.sku_quality_code
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.wm_inbound_carton_sku_adjustment_events_ldg a
QUALIFY ROW_NUMBER() OVER(PARTITION BY inbound_carton_id, to_carton_lpn, sku_id, adjustment_date  ORDER BY adjustment_date desc) = 1
;




CREATE TEMPORARY TABLE IF NOT EXISTS wm_inbound_carton_sku_adjustment_events_origin (
inbound_carton_id STRING NOT NULL,
original_carton_id STRING,
sku_id STRING NOT NULL,
adjustment_timestamp_utc TIMESTAMP NOT NULL,
adjustment_timestamp_tz STRING NOT NULL,
user_id STRING,
to_carton_lpn STRING,
to_carton_purchase_order_number STRING
);


INSERT INTO wm_inbound_carton_sku_adjustment_events_origin
SELECT DISTINCT a.inbound_carton_id,
  a.original_carton_id,
  a.location_id AS sku_id,
  cast(a.adjustment_event_type as timestamp) AS adjustment_timestamp_utc,
  a.adjustment_timestamp_tz as adjustment_timestamp_tz,
  a.sku_id AS user_id,
  a.sku_type AS to_carton_lpn,
  a.inventory_state AS to_carton_purchase_order_number
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.wm_inbound_carton_sku_adjustment_events_fact AS a
  INNER JOIN (SELECT DISTINCT 
    inbound_carton_id,
    sku_id,
    to_carton_lpn,
    adjustment_date,
    user_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.wm_inbound_carton_sku_adjustment_events_fact
   WHERE LOWER(adjustment_event_type) = LOWER('WAREHOUSE_PURCHASE_ORDER_CARTON_SPLIT')
    AND qty > 0
    AND original_carton_id IS NULL) AS b 
    ON LOWER(a.to_carton_lpn) = LOWER(b.to_carton_lpn) AND a.adjustment_date = b.adjustment_date
       AND LOWER(a.sku_id) = LOWER(b.sku_id) AND LOWER(a.user_id) = LOWER(b.user_id)
 WHERE LOWER(a.adjustment_event_type) = LOWER('WAREHOUSE_PURCHASE_ORDER_CARTON_SPLIT')
  AND a.qty < 0
 EXCEPT DISTINCT
 SELECT inbound_carton_id,
  original_carton_id,
  sku_id,
  adjustment_timestamp_utc,
  adjustment_timestamp_tz,
  user_id,
  to_carton_lpn,
  to_carton_purchase_order_number
 FROM wm_inbound_carton_sku_adjustment_events_origin;


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.wm_inbound_carton_sku_adjustment_events_fact SET
 original_carton_id = wrk.original_carton_id FROM wm_inbound_carton_sku_adjustment_events_origin AS wrk
WHERE LOWER(wm_inbound_carton_sku_adjustment_events_fact.to_carton_purchase_order_number) = LOWER(wrk.to_carton_purchase_order_number
       ) AND LOWER(wm_inbound_carton_sku_adjustment_events_fact.sku_id) = LOWER(wrk.sku_id) AND LOWER(wm_inbound_carton_sku_adjustment_events_fact
      .to_carton_lpn) = LOWER(wrk.to_carton_lpn) AND wm_inbound_carton_sku_adjustment_events_fact.adjustment_timestamp =
    wrk.adjustment_timestamp_utc AND LOWER(wm_inbound_carton_sku_adjustment_events_fact.user_id) = LOWER(wrk.user_id) AND
 wm_inbound_carton_sku_adjustment_events_fact.original_carton_id IS NULL;


/***********************************************************************************
-- Collect stats on fact tables
************************************************************************************/


--COLLECT STATS ON `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.WM_INBOUND_CARTON_SKU_ADJUSTMENT_EVENTS_FACT INDEX (inbound_carton_id, sku_id, inventory_state, adjustment_timestamp, adjustment_event_type)


/***********************************************************************************
-- Perform audit between stg and fct tables
************************************************************************************/


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.dq_pipeline_audit_v1
(
  'WM_INBOUND_CARTON_SKU_ADJUSTMENT_EVENTS_LDG_TO_BASE',
  'NAP_ASCP_WM_INBOUND_CARTON',
  '{{params.dbenv}}_NAP_STG',
  'WM_INBOUND_CARTON_SKU_ADJUSTMENT_EVENTS_LDG',
  '{{params.dbenv}}_NAP_FCT',
  'WM_INBOUND_CARTON_SKU_ADJUSTMENT_EVENTS_FACT',
  'Count_Distinct',
  0,
  'T-S',
  "concat(inbound_carton_id, '-', sku_id, '-', inventory_state,  '-', cast(adjustment_date as STRING), '-', adjustment_event_type)",
  "concat(inbound_carton_id, '-', sku_id, '-', inventory_state,  '-', cast(adjustment_date as STRING), '-', adjustment_event_type)",
  null,
  null,
  'Y'
);


/***********************************************************************************
-- Update control metrics table with temp table
************************************************************************************/


INSERT INTO isf_dag_control_log_wrk
(SELECT 'nrlt_wm_inbound_carton_lifecycle_16753_TECH_SC_NAP_insights' AS isf_dag_nm,
  CAST(NULL AS STRING) AS step_nm,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_WM_INBOUND_CARTON')) AS batch_id,
  SUBSTR('{{params.dbenv}}_NAP_FCT.WM_INBOUND_CARTON_SKU_ADJUSTMENT_EVENTS_FACT', 1, 100) AS tbl_nm,
  SUBSTR('TERADATA_JOB_END_TMSTP', 1, 100) AS metric_nm,
  SUBSTR(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS STRING), 1, 60) AS metric_value,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP)  AS
  metric_tmstp_utc,
  'GMT' AS metric_tmstp_tz,
  CURRENT_DATE('GMT') AS metric_date);


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.isf_dag_control_log SET
 metric_value = wrk.metric_value,
 metric_tmstp = wrk.metric_tmstp_utc ,
 metric_tmstp_tz = wrk.metric_tmstp_tz 
 FROM isf_dag_control_log_wrk AS wrk
WHERE LOWER(isf_dag_control_log.isf_dag_nm) = LOWER('{{params.dag_name}}') AND LOWER(COALESCE(isf_dag_control_log.tbl_nm, '')) =
    LOWER(COALESCE(wrk.tbl_nm, '')) AND LOWER(isf_dag_control_log.metric_nm) = LOWER(wrk.metric_nm) AND
  isf_dag_control_log.batch_id = (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}'));