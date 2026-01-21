/*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File              : dc_outbound_transfer_carton_events_fact_load.sql
-- Author            : Dmytro Utte
-- Description       : Merge INVENTORY_DISTRIBUTION_CENTER_OUTBOUND_CARTON_EVENTS_FACT table with delta from staging table
-- Source topic      : inventory-distribution-center-outbound-carton-analytical-avro
-- Object model      : DistributionCenterOutboundCarton
-- ETL Run Frequency : Every hour
-- Version           : 0.2
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2023-08-21  Utte Dmytro    FA-9944:  Migrate NRLT_inventory_distribution_center_outbound_carton DAG to IsF
-- 2024-03-06  Andrii Skyba   FA-11137: IsF NSK migration
-- 2024-03-12  Tetiana Ivchyk FA-11580: Bulk sort issue: to change the ongoing flow (for this table - just some optimization)
-- 2024-06-06  Roman Tymchik  FA-13067: IsF nrlt_dc_outbound_transfer: remove nlsFlag & SystemTime
--************************************************************************************************************************************/
/***********************************************************************************
-- Load temp table with metrics
************************************************************************************/


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
metric_tmstp,
metric_tmstp_tz,
metric_date
)
WITH dummy AS (SELECT 'nrlt_dc_outbound_transfer_lifecycle_16753_TECH_SC_NAP_insights' AS isf_dag_nm,
  (SELECT batch_id AS isf_dag_nm
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('INVENTORY_DISTRIBUTION_CENTER_OUTBOUND_CARTON')) AS batch_id,
   CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS   metric_tmstp,
  jwn_udf.udf_time_zone(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS STRING)) metric_tmstp_tz,
  CURRENT_DATE('GMT') AS metric_date)
SELECT isf_dag_nm,
 cast(NULL as string) AS step_nm,
 batch_id,
 SUBSTR('{{params.dbenv}}_NAP_FCT.INVENTORY_DISTRIBUTION_CENTER_OUTBOUND_CARTON_EVENTS_FACT', 1, 100) AS tbl_nm,
 SUBSTR('TERADATA_JOB_START_TMSTP', 1, 100) AS metric_nm,
 SUBSTR(CAST(metric_tmstp AS STRING), 1, 60) AS metric_value,
 metric_tmstp,
 metric_tmstp_tz,
 metric_date
FROM dummy AS d
UNION ALL
SELECT isf_dag_nm,
 cast(NULL as string) AS step_nm,
 batch_id,
 SUBSTR('{{params.dbenv}}_NAP_STG.INVENTORY_DISTRIBUTION_CENTER_OUTBOUND_CARTON_EVENTS_LDG', 1, 100) AS tbl_nm,
 SUBSTR('LOADED_TO_LDG_CNT', 1, 100) AS metric_nm,
 SUBSTR((SELECT metric_value
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.processed_data_spark_log
   WHERE LOWER(isf_dag_nm) = LOWER(d.isf_dag_nm)
    AND LOWER(tbl_nm) = LOWER('INVENTORY_DISTRIBUTION_CENTER_OUTBOUND_CARTON_EVENTS_LDG')
    AND LOWER(metric_nm) = LOWER('PROCESSED_ROWS_CNT')), 1, 60) AS metric_value,
 metric_tmstp,
 metric_tmstp_tz,
 metric_date
FROM dummy AS d
UNION ALL
SELECT isf_dag_nm,
 cast(NULL as string) AS step_nm,
 batch_id,
 SUBSTR(NULL, 1, 100) AS tbl_nm,
 SUBSTR('SPARK_PROCESSED_MESSAGE_CNT', 1, 100) AS metric_nm,
 SUBSTR((SELECT metric_value
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.processed_data_spark_log
   WHERE LOWER(isf_dag_nm) = LOWER(d.isf_dag_nm)
    AND LOWER(metric_nm) = LOWER('SPARK_PROCESSED_MESSAGE_CNT')), 1, 60) AS metric_value,
 metric_tmstp,
 metric_tmstp_tz,
 metric_date
FROM dummy AS d;


/***********************************************************************************
-- Merge into the target fact table
************************************************************************************/


-- CAST(concat(uoc.packTime, '+00:00') AS TIMESTAMP WITH TIME ZONE FORMAT 'YYYY-MM-DDBHH:MI:SS.S(F)Z') AS packTime,


-- CAST(concat(uoc.event_time, '+00:00') AS TIMESTAMP WITH TIME ZONE FORMAT 'YYYY-MM-DDBHH:MI:SS.S(F)Z') AS event_time,


-- avoid records with errors


MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.inventory_distribution_center_outbound_carton_events_fact AS uoc_fct
USING (SELECT cartonlpn,
  CAST(parse_timestamp('%Y-%m-%d %H:%M:%E6S%Ez', concat(packTime, '+00:00')) AS TIMESTAMP) AS packtime ,
   jwn_udf.default_tz_pst() as packtime_tz,
  packtype,
  NULLIF(TRIM(destinationlocationid), '') AS destinationlocationid,
  NULLIF(TRIM(masterpacknumber), '') AS masterpacknumber,
  NULLIF(TRIM(lanenumber), '') AS lanenumber,
  NULLIF(TRIM(routenumber), '') AS routenumber,
  NULLIF(TRIM(dockdoorlpn), '') AS dockdoorlpn,
  NULLIF(TRIM(palletlpn), '') AS palletlpn,
  NULLIF(TRIM(trailerlpn), '') AS trailerlpn,
  NULLIF(TRIM(loadnumber), '') AS loadnumber,
  event_id,
  location_id,
  user_id,
  event_type,
  shipmentnumber_number AS shipment_num,
  shipmentnumber_shipmentnumbertype AS shipment_type_code,
  CAST(parse_timestamp('%Y-%m-%d %H:%M:%E6S%Ez', concat(event_time, '+00:00')) AS TIMESTAMP) AS event_time ,
  jwn_udf.default_tz_pst()  as event_time_tz,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('INVENTORY_DISTRIBUTION_CENTER_OUTBOUND_CARTON')) AS dw_batch_id,
   (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('INVENTORY_DISTRIBUTION_CENTER_OUTBOUND_CARTON')) AS dw_batch_date,
   timestamp(current_datetime('PST8PDT'))  as dw_sys_load_tmstp,
   jwn_udf.default_tz_pst()  as dw_sys_load_tmstp_tz,
   timestamp(current_datetime('PST8PDT')) as dw_sys_updt_tmstp,
   jwn_udf.default_tz_pst() as dw_sys_updt_tmstp_tz
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.inventory_distribution_center_outbound_carton_events_ldg AS uoc
 WHERE cartonlpn IS NOT NULL
  AND event_id IS NOT NULL
  AND packtime IS NOT NULL
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY cartonlpn, event_id ORDER BY CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME()) AS DATETIME)
      DESC, CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME()) AS DATETIME) DESC)) = 1) AS t6
ON LOWER(t6.cartonlpn) = LOWER(uoc_fct.carton_lpn) AND LOWER(t6.event_id) = LOWER(uoc_fct.event_id)
WHEN MATCHED THEN UPDATE SET
 pack_time = t6.packtime,
 pack_type = t6.packtype,
 destination_location_id = CAST(t6.destinationlocationid AS INTEGER),
 master_pack_number = t6.masterpacknumber,
 lane_number = t6.lanenumber,
 route_number = t6.routenumber,
 dock_door_lpn = t6.dockdoorlpn,
 pallet_lpn = t6.palletlpn,
 trailer_lpn = t6.trailerlpn,
 load_number = t6.loadnumber,
 location_id = CAST(t6.location_id AS INTEGER),
 user_id = t6.user_id,
 event_type = t6.event_type,
 event_time = t6.event_time,
 shipment_num = t6.shipment_num,
 shipment_type_code = t6.shipment_type_code,
 dw_batch_id = t6.dw_batch_id,
 dw_batch_date = t6.dw_batch_date,
 dw_sys_updt_tmstp = t6.dw_sys_updt_tmstp
WHEN NOT MATCHED THEN INSERT VALUES(t6.cartonlpn, t6.packtime, t6.packtime_tz, t6.packtype, CAST(t6.destinationlocationid AS INTEGER),
 t6.masterpacknumber, t6.lanenumber, t6.routenumber, t6.dockdoorlpn, t6.palletlpn, t6.trailerlpn, t6.loadnumber, t6.event_id
 , CAST(t6.location_id AS INTEGER), t6.user_id, t6.event_type, t6.event_time, t6.event_time_tz, t6.dw_batch_id, t6.dw_batch_date, t6.dw_sys_load_tmstp
 , t6.dw_sys_load_tmstp_tz, t6.dw_sys_updt_tmstp, t6.dw_sys_updt_tmstp_tz, t6.shipment_num, t6.shipment_type_code);


--COLLECT STATS ON PRD_NAP_FCT.INVENTORY_DISTRIBUTION_CENTER_OUTBOUND_CARTON_EVENTS_FACT INDEX (carton_lpn, event_id)


/***********************************************************************************
-- Perform audit between stg and fct tables
************************************************************************************/


-- Audit_Type


-- Mtch_Threshold


-- Mtch_Type_Code


-- SRC_Key_Expr


-- TGT_Key_Expr


-- SRC_Filter


-- TGT_Filter


-- Mtch_Batch_Id_Ind


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.dq_pipeline_audit_v1
(
	'INVENTORY_DISTRIBUTION_CENTER_OUTBOUND_CARTON_EVENTS_LDG_TO_BASE',
	'INVENTORY_DISTRIBUTION_CENTER_OUTBOUND_CARTON',
	'{{params.dbenv}}_NAP_STG',
	'INVENTORY_DISTRIBUTION_CENTER_OUTBOUND_CARTON_EVENTS_LDG',
	'{{params.dbenv}}_NAP_FCT',
	'INVENTORY_DISTRIBUTION_CENTER_OUTBOUND_CARTON_EVENTS_FACT',
	'Count_Distinct', 
	0, 
	'T-S', 
	'concat(cartonLpn,event_id)', 
	'concat(carton_lpn,event_id)', 
	NULL,
	NULL, 
	'Y'
);


/***********************************************************************************
-- Update control metrics table with temp table
************************************************************************************/


INSERT INTO isf_dag_control_log_wrk
(SELECT 'nrlt_dc_outbound_transfer_lifecycle_16753_TECH_SC_NAP_insights' AS isf_dag_nm,
  CAST(NULL AS STRING) AS step_nm,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('INVENTORY_DISTRIBUTION_CENTER_OUTBOUND_CARTON')) AS batch_id,
  SUBSTR('{{params.dbenv}}_NAP_FCT.INVENTORY_DISTRIBUTION_CENTER_OUTBOUND_CARTON_EVENTS_FACT', 1, 100) AS tbl_nm,
  SUBSTR('TERADATA_JOB_END_TMSTP', 1, 100) AS metric_nm,
  SUBSTR(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS STRING), 1, 60) AS metric_value,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS   metric_tmstp,
  jwn_udf.udf_time_zone(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS STRING)) metric_tmstp_tz,
  CURRENT_DATE('GMT') AS metric_date);


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.isf_dag_control_log SET
 metric_value = wrk.metric_value,
 metric_tmstp = wrk.metric_tmstp FROM isf_dag_control_log_wrk AS wrk
WHERE LOWER(isf_dag_control_log.isf_dag_nm) = LOWER('{{params.dag_name}}') AND LOWER(COALESCE(isf_dag_control_log.tbl_nm, '')) =
    LOWER(COALESCE(wrk.tbl_nm, '')) AND LOWER(isf_dag_control_log.metric_nm) = LOWER(wrk.metric_nm) AND
  isf_dag_control_log.batch_id = (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}'));
END