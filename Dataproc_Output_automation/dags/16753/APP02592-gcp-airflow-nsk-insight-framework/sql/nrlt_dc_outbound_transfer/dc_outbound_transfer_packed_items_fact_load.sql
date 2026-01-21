/*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File              : dc_outbound_transfer_packed_items_fact_load.sql
-- Author            : Dmytro Utte
-- Description       : Merge WM_OUTBOUND_CARTON_PACKED_ITEMS_FACT table with delta from the staging table
-- Source topic      : inventory-distribution-center-outbound-carton-analytical-avro
-- Object model      : DistributionCenterOutboundCarton
-- ETL Run Frequency : Every hour
-- Version           : 0.2
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2023-08-21  Utte Dmytro    FA-9944:  Migrate NRLT_inventory_distribution_center_outbound_carton DAG to IsF
-- 2024-03-06  Andrii Skyba   FA-11137: IsF NSK migration
-- 2023-03-12  Tetiana Ivchyk FA-11580: Bulk sort issue: to change the ongoing flow
-- 2024-06-06  Roman Tymchik  FA-13067: IsF nrlt_dc_outbound_transfer: remove nlsFlag & SystemTime
--************************************************************************************************************************************/
/***********************************************************************************
-- Load temp table with metrics
************************************************************************************/
CREATE TEMPORARY TABLE IF NOT EXISTS isf_dag_control_log_wrk AS 
SELECT *
FROM `{{params.gcp_project_id}}`.{{params.db_env}}_nap_base_vws.isf_dag_control_log
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
WITH dummy AS (SELECT 'nrlt_dc_outbound_transfer_lifecycle_16753_TECH_SC_NAP_insights' AS isf_dag_nm,
  (SELECT batch_id AS isf_dag_nm
  FROM `{{params.gcp_project_id}}`.{{params.db_env}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('INVENTORY_DISTRIBUTION_CENTER_OUTBOUND_CARTON')) AS batch_id,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS metric_tmstp,
 CURRENT_DATE('GMT') AS metric_date)
SELECT isf_dag_nm,
 NULL AS step_nm,
 batch_id,
 SUBSTR('{{params.db_env}}_NAP_FCT.WM_OUTBOUND_CARTON_PACKED_ITEMS_FACT', 1, 100) AS tbl_nm,
 SUBSTR('TERADATA_JOB_START_TMSTP', 1, 100) AS metric_nm,
 SUBSTR(CAST(metric_tmstp AS STRING), 1, 60) AS metric_value,
 CAST(metric_tmstp as timestamp) as metric_tmstp,
 metric_date
FROM dummy AS d
UNION ALL
SELECT isf_dag_nm,
 CAST(NULL AS STRING) AS step_nm,
 batch_id,
 SUBSTR('{{params.db_env}}_NAP_STG.WM_OUTBOUND_CARTON_PACKED_ITEMS_LDG', 1, 100) AS tbl_nm,
 SUBSTR('LOADED_TO_LDG_CNT', 1, 100) AS metric_nm,
 SUBSTR((SELECT metric_value
   FROM `{{params.gcp_project_id}}`.{{params.db_env}}_nap_base_vws.processed_data_spark_log
   WHERE LOWER(isf_dag_nm) = LOWER(isf_dag_nm)
    AND LOWER(tbl_nm) = LOWER('WM_OUTBOUND_CARTON_PACKED_ITEMS_LDG')
    AND LOWER(metric_nm) = LOWER('PROCESSED_ROWS_CNT')), 1, 60) AS metric_value,
 CAST(metric_tmstp as timestamp) as metric_tmstp,
 metric_date
FROM dummy AS d;


/***********************************************************************************
-- Merge into fact table
************************************************************************************/
MERGE INTO `{{params.gcp_project_id}}`.{{params.db_env}}_nap_fct.wm_outbound_carton_packed_items_fact AS uoc_fct
USING (SELECT cartonlpn,
  parse_timestamp('%Y-%m-%d %H:%M:%E6S%Ez', concat(uoc.packtime, '+00:00')) AS packtime,
  '+00:00' as pack_tmstp_tz,
  packtype,
  NULLIF(TRIM(destinationlocationid), '') AS destinationlocationid,
  NULLIF(TRIM(masterpacknumber), '') AS masterpacknumber,
  NULLIF(TRIM(lanenumber), '') AS lanenumber,
  NULLIF(TRIM(routenumber), '') AS routenumber,
  NULLIF(TRIM(dockdoorlpn), '') AS dockdoorlpn,
  NULLIF(TRIM(palletlpn), '') AS palletlpn,
  NULLIF(TRIM(trailerlpn), '') AS trailerlpn,
  NULLIF(TRIM(loadnumber), '') AS loadnumber,
  sku,
  sku_type,
  quantity,
  multiplier,
  NULLIF(TRIM(originalcartonlpn), '') AS originalcartonlpn,
  warehouseorderid,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.db_env}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('INVENTORY_DISTRIBUTION_CENTER_OUTBOUND_CARTON')) AS dw_batch_id,
   (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.db_env}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('INVENTORY_DISTRIBUTION_CENTER_OUTBOUND_CARTON')) AS dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP) AS dw_sys_load_tmstp,
  `{{params.gcp_project_id}}`.jwn_udf.default_tz_pst() as dw_sys_load_tmstp_tz,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP) AS dw_sys_updt_tmstp,
  `{{params.gcp_project_id}}`.jwn_udf.default_tz_pst() as dw_sys_updt_tmstp_tz
 FROM `{{params.gcp_project_id}}`.{{params.db_env}}_nap_base_vws.wm_outbound_carton_packed_items_ldg AS uoc
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY cartonlpn, sku, NULLIF(TRIM(originalcartonlpn), ''), packtype, packtime
    ORDER BY packtime DESC)) = 1) AS qry
ON LOWER(qry.cartonlpn) = LOWER(uoc_fct.carton_num) AND LOWER(qry.sku) = LOWER(uoc_fct.rms_sku_num) AND LOWER(COALESCE(qry
      .originalcartonlpn, '')) = LOWER(COALESCE(uoc_fct.original_carton_num, '')) AND LOWER(qry.packtype) = LOWER(uoc_fct
    .pack_type) AND CAST(qry.packtime AS TIMESTAMP) = uoc_fct.pack_tmstp
WHEN MATCHED THEN UPDATE SET
 destination_location_num = CAST(TRUNC(CAST(qry.destinationlocationid AS FLOAT64)) AS INTEGER),
 master_pack_num = qry.masterpacknumber,
 lane_num = qry.lanenumber,
 route_num = qry.routenumber,
 dock_door_lpn = qry.dockdoorlpn,
 pallet_lpn = qry.palletlpn,
 trailer_lpn = qry.trailerlpn,
 load_num = qry.loadnumber,
 rms_sku_type = qry.sku,
 sku_qty = CAST(TRUNC(CAST(qry.quantity AS FLOAT64)) AS INTEGER),
 multiplier = qry.multiplier,
 original_carton_num = qry.cartonlpn,
 warehouse_order_id = qry.warehouseorderid,
 dw_batch_id = qry.dw_batch_id,
 dw_batch_date = qry.dw_batch_date,
 dw_sys_updt_tmstp = CAST(qry.dw_sys_updt_tmstp AS TIMESTAMP),
 dw_sys_updt_tmstp_tz = qry.dw_sys_updt_tmstp_tz
WHEN NOT MATCHED THEN INSERT VALUES(qry.cartonlpn, CAST(qry.packtime AS TIMESTAMP),qry.pack_tmstp_tz, qry.packtype, CAST(TRUNC(CAST(qry.destinationlocationid AS FLOAT64)) AS INTEGER)
 , qry.masterpacknumber, qry.lanenumber, qry.routenumber, qry.dockdoorlpn, qry.palletlpn, qry.trailerlpn, qry.loadnumber
 , qry.sku, qry.sku_type, CAST(TRUNC(CAST(qry.quantity AS FLOAT64)) AS INTEGER), qry.multiplier, qry.originalcartonlpn, qry.warehouseorderid,
 qry.dw_batch_id, qry.dw_batch_date, qry.dw_sys_load_tmstp,qry.dw_sys_updt_tmstp_tz, qry.dw_sys_updt_tmstp,qry.dw_sys_updt_tmstp_tz);


--COLLECT STATS ON PRD_NAP_FCT.WM_OUTBOUND_CARTON_PACKED_ITEMS_FACT INDEX (carton_num, rms_sku_num);


/***********************************************************************************
-- Perform audit between stg and fct tables
************************************************************************************/
CALL `{{params.gcp_project_id}}`.{{params.db_env}}_nap_utl.dq_pipeline_audit_v1
(
	'WM_OUTBOUND_CARTON_PACKED_ITEMS_LDG_TO_BASE',
	'INVENTORY_DISTRIBUTION_CENTER_OUTBOUND_CARTON',
	'{{params.db_env}}_NAP_STG',
	'WM_OUTBOUND_CARTON_PACKED_ITEMS_LDG',
	'{{params.db_env}}_NAP_FCT',
	'WM_OUTBOUND_CARTON_PACKED_ITEMS_FACT',
	'Count_Distinct',
	0,
	'T-S', 
	'concat(cartonLpn,sku)', 
	'concat(carton_num,rms_sku_num)', 
	NULL,
	NULL,
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
(SELECT 'nrlt_dc_outbound_transfer_lifecycle_16753_TECH_SC_NAP_insights' AS isf_dag_nm,
  CAST(NULL AS STRING) AS step_nm,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.db_env}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('INVENTORY_DISTRIBUTION_CENTER_OUTBOUND_CARTON')) AS batch_id,
  SUBSTR('{{params.db_env}}_NAP_FCT.WM_OUTBOUND_CARTON_PACKED_ITEMS_FACT', 1, 100) AS tbl_nm,
  SUBSTR('TERADATA_JOB_END_TMSTP', 1, 100) AS metric_nm,
  SUBSTR(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS STRING), 1, 60) AS metric_value,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS
   metric_tmstp,
  'GMT' as metric_tmstp_tz,
  CURRENT_DATE('GMT') AS metric_date); 


UPDATE `{{params.gcp_project_id}}`.{{params.db_env}}_nap_utl.isf_dag_control_log SET
 metric_value = wrk.metric_value,
 metric_tmstp = CAST(wrk.metric_tmstp_utc AS TIMESTAMP) ,
 metric_tmstp_tz = wrk.metric_tmstp_tz
 FROM isf_dag_control_log_wrk AS wrk
WHERE LOWER(isf_dag_control_log.isf_dag_nm) = LOWER('{{params.dag_name}}') AND LOWER(COALESCE(isf_dag_control_log.tbl_nm, '')) =
    LOWER(COALESCE(wrk.tbl_nm, '')) AND LOWER(isf_dag_control_log.metric_nm) = LOWER(wrk.metric_nm) AND
  isf_dag_control_log.batch_id = (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.db_env}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}'));