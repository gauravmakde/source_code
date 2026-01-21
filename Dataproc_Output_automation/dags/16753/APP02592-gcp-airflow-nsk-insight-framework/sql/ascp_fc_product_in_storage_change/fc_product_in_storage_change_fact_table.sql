BEGIN


CREATE TEMPORARY TABLE IF NOT EXISTS isf_dag_control_log_wrk 
 AS
SELECT 
isf_dag_nm,			
step_nm,
batch_id,
tbl_nm,
metric_nm,
metric_value,
metric_tmstp_utc,
metric_tmstp_tz,
metric_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.isf_dag_control_log
LIMIT 0;


CREATE TEMPORARY TABLE IF NOT EXISTS fc_product_in_storage_change_ldg_tpt_info AS
SELECT jobstarttime,
 jobendtime,
 rowsinserted
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.tpt_job_log
WHERE LOWER(job_name) = LOWER('{{params.dbenv}}_NAP_STG_FC_PRODUCT_IN_STORAGE_CHANGE_LDG_JOB')
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
WITH dummy AS (SELECT 'ascp_fc_product_in_storage_change_16753_TECH_SC_NAP_insights' AS isf_dag_nm,
  (SELECT batch_id AS isf_dag_nm
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_FC_PRODUCT_IN_STORAGE_CHANGE')) AS batch_id,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS TIMESTAMP) AS metric_tmstp_utc,
 `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
 )
SELECT isf_dag_nm,
 cast(NULL as string) AS step_nm,
 batch_id,
 SUBSTR('{{params.dbenv}}_NAP_FCT.FC_PRODUCT_IN_STORAGE_CHANGE_FACT', 1, 100) AS tbl_nm,
 SUBSTR('TERADATA_JOB_START_TMSTP', 1, 100) AS metric_nm,
 SUBSTR(CAST(metric_tmstp_utc AS STRING), 1, 60) AS metric_value,
 metric_tmstp_utc,
 metric_tmstp_tz,
 metric_date
FROM dummy AS d
UNION ALL
SELECT 
isf_dag_nm,
 NULL AS step_nm,
 batch_id,
 SUBSTR('{{params.dbenv}}_NAP_STG.FC_PRODUCT_IN_STORAGE_CHANGE_LDG', 1, 100) AS tbl_nm,
 SUBSTR('LOADED_TO_CSV_CNT', 1, 100) AS metric_nm,
 SUBSTR((SELECT metric_value
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.processed_data_spark_log
   WHERE LOWER(isf_dag_nm) = LOWER(d.isf_dag_nm)
    AND LOWER(tbl_nm) = LOWER('FC_PRODUCT_IN_STORAGE_CHANGE_LDG')
    AND LOWER(metric_nm) = LOWER('PROCESSED_ROWS_CNT')), 1, 60) AS metric_value,
 metric_tmstp_utc,
 metric_tmstp_tz,
 metric_date
FROM dummy AS d
UNION ALL
SELECT 
isf_dag_nm,
 NULL AS step_nm,
 batch_id,
 SUBSTR(NULL, 1, 100) AS tbl_nm,
 SUBSTR('SPARK_PROCESSED_MESSAGE_CNT', 1, 100) AS metric_nm,
 SUBSTR((SELECT metric_value
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.processed_data_spark_log
   WHERE LOWER(isf_dag_nm) = LOWER(d.isf_dag_nm)
    AND LOWER(metric_nm) = LOWER('SPARK_PROCESSED_MESSAGE_CNT')), 1, 60) AS metric_value,
 metric_tmstp_utc,
 metric_tmstp_tz,
 metric_date
FROM dummy AS d
UNION ALL
SELECT isf_dag_nm,
 NULL AS step_nm,
 batch_id,
 SUBSTR('{{params.dbenv}}_NAP_STG.FC_PRODUCT_IN_STORAGE_CHANGE_LDG', 1, 100) AS tbl_nm,
 SUBSTR('LOADED_TO_LDG_CNT', 1, 100) AS metric_nm,
 SUBSTR(CAST((SELECT rowsinserted
    FROM fc_product_in_storage_change_ldg_tpt_info) AS STRING), 1, 60) AS metric_value,
 metric_tmstp_utc,
 metric_tmstp_tz,
 metric_date
FROM dummy AS d
UNION ALL
SELECT 
isf_dag_nm,
 NULL AS step_nm,
 batch_id,
 SUBSTR('{{params.dbenv}}_NAP_STG.FC_PRODUCT_IN_STORAGE_CHANGE_LDG', 1, 100) AS tbl_nm,
 SUBSTR('TPT_JOB_START_TMSTP', 1, 100) AS metric_nm,
 SUBSTR(CAST((SELECT jobstarttime
    FROM fc_product_in_storage_change_ldg_tpt_info) AS STRING), 1, 60) AS metric_value,
 metric_tmstp_utc,
 metric_tmstp_tz,
 metric_date
FROM dummy AS d
UNION ALL
SELECT isf_dag_nm,
 NULL AS step_nm,
 batch_id,
 SUBSTR('{{params.dbenv}}_NAP_STG.FC_PRODUCT_IN_STORAGE_CHANGE_LDG', 1, 100) AS tbl_nm,
 SUBSTR('TPT_JOB_END_TMSTP', 1, 100) AS metric_nm,
 SUBSTR(CAST((SELECT jobendtime
    FROM fc_product_in_storage_change_ldg_tpt_info) AS STRING), 1, 60) AS metric_value,
 metric_tmstp_utc,
 metric_tmstp_tz,
 metric_date
FROM dummy AS d;


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('FC_PRODUCT_IN_STORAGE_CHANGE_FACT',  '{{params.dbenv}}_NAP_FCT',  'ascp_fc_product_in_storage_change_16753_TECH_SC_NAP_insights',  'teradata_load_teradata',  0,  'LOAD_START',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  'NAP_ASCP_FC_PRODUCT_IN_STORAGE_CHANGE');



UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.fc_product_in_storage_change_ldg 
SET
 carton_lpn = CASE
  WHEN fc_product_in_storage_change_ldg.carton_lpn IS NULL
  THEN ''
  ELSE fc_product_in_storage_change_ldg.carton_lpn
  END,
 to_storage_id = CASE
  WHEN fc_product_in_storage_change_ldg.to_storage_id IS NULL
  THEN ''
  ELSE fc_product_in_storage_change_ldg.to_storage_id
  END,
 selling_brands_logical_locations_array = TRIM(REPLACE(fc_product_in_storage_change_ldg.selling_brands_logical_locations_array
   , '\\', ''), '"'),
 inventory_states_array = TRIM(REPLACE(fc_product_in_storage_change_ldg.inventory_states_array, '\\', ''), '"'),
 reason_code = TRIM(REPLACE(fc_product_in_storage_change_ldg.reason_code, '\\', ''), '"')
WHERE TRUE;

-- Merge into fact table



MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.fc_product_in_storage_change_fact fact
USING (
SELECT
originating_event_id,
sku_id,
sku_type,
carton_lpn,
sku_inventory_state_code,
sku_quality_code,
cast(trunc(cast(sku_qty as float64)) as int64) as sku_qty,
to_storage_id,
to_storage_type,
event_location_id,
logical_location_id,
user_id,
user_id_type,
event_type,
inbound_order_id,
inbound_order_id_type_code,
cast( REPLACE(REPLACE(event_time, 'Z', '+00:00'), 'T', ' ') as timestamp) as event_time,
'+00:00' as event_time_tz,
cast( cast(REPLACE(REPLACE(event_time, 'Z', '+00:00'), 'T', ' ') as timestamp)as date)as  event_date,
case when inventory_states_array = '[]' then null else inventory_states_array end as inventory_states_array,
case when reason_code = '[]' then null else reason_code end as reason_code,
case when selling_brands_logical_locations_array = '[]' then null else selling_brands_logical_locations_array end as selling_brands_logical_locations_array,
(SELECT BATCH_ID FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE SUBJECT_AREA_NM ='NAP_ASCP_FC_PRODUCT_IN_STORAGE_CHANGE') as dw_batch_id,
(SELECT CURR_BATCH_DATE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE SUBJECT_AREA_NM ='NAP_ASCP_FC_PRODUCT_IN_STORAGE_CHANGE') as dw_batch_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.fc_product_in_storage_change_ldg
QUALIFY ROW_NUMBER() OVER(PARTITION BY originating_event_id, sku_id, sku_inventory_state_code, carton_lpn  ORDER BY event_time desc) = 1
) ldg
ON fact.originating_event_id = ldg.originating_event_id
AND fact.sku_id = ldg.sku_id
AND fact.sku_inventory_state_code = ldg.sku_inventory_state_code
AND fact.carton_lpn = ldg.carton_lpn
and fact.event_date = ldg.event_date
WHEN MATCHED THEN
UPDATE SET
sku_type = ldg.sku_type,
sku_quality_code = ldg.sku_quality_code,
sku_qty = ldg.sku_qty,
selling_brands_logical_locations_array =to_json(ldg.selling_brands_logical_locations_array),
to_storage_id = ldg.to_storage_id,
to_storage_type = ldg.to_storage_type,
event_location_id = ldg.event_location_id,
user_id = ldg.user_id,
user_id_type = ldg.user_id_type,
event_type = ldg.event_type,
event_time = ldg.event_time,
inventory_states_array = to_json(ldg.inventory_states_array),
reason_code = ldg.reason_code,
inbound_order_id = ldg.inbound_order_id,
inbound_order_id_type_code = ldg.inbound_order_id_type_code,
dw_batch_id = ldg.dw_batch_id,
dw_batch_date = ldg.dw_batch_date,
dw_sys_updt_tmstp = timestamp(current_datetime('PST8PDT'))
WHEN NOT MATCHED THEN
INSERT (
        originating_event_id,
	    sku_id,
        carton_lpn,
		sku_inventory_state_code,
	    sku_quality_code,
	    sku_qty,
        to_storage_id,
        to_storage_type,
        event_location_id,
		logical_location_id,
        event_type,
        event_time,
        event_time_tz,
        event_date,
		selling_brands_logical_locations_array,
        inventory_states_array,
        reason_code,
		sku_type,
        user_id,
        user_id_type,
        inbound_order_id,
        inbound_order_id_type_code,
        dw_batch_id,
        dw_batch_date,
        dw_sys_load_tmstp,
        dw_sys_load_tmstp_tz,
        dw_sys_updt_tmstp,
        dw_sys_updt_tmstp_tz
    )
VALUES (
ldg.originating_event_id,
ldg.sku_id,
ldg.carton_lpn,
ldg.sku_inventory_state_code,
ldg.sku_quality_code,
ldg.sku_qty,
ldg.to_storage_id,
ldg.to_storage_type,
ldg.event_location_id,
ldg.logical_location_id,
ldg.event_type,
ldg.event_time,
ldg.event_time_tz,
ldg.event_date,
to_json(ldg.selling_brands_logical_locations_array),
to_json(ldg.inventory_states_array),
ldg.reason_code,
ldg.sku_type,
ldg.user_id,
ldg.user_id_type,
ldg.inbound_order_id,
ldg.inbound_order_id_type_code,
ldg.dw_batch_id,
ldg.dw_batch_date,
timestamp(current_datetime('PST8PDT'))  ,
 `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() ,
timestamp(current_datetime('PST8PDT')) ,
 `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() 
);




-- Collect stats on fact tables

--COLLECT STATS INDEX (originating_event_id, sku_id, sku_inventory_state_code, carton_lpn), COLUMN (PARTITION), COLUMN (event_date), COLUMN (PARTITION, originating_event_id, sku_id, sku_inventory_state_code, carton_lpn), COLUMN (PARTITION, originating_event_id, sku_id, sku_inventory_state_code, carton_lpn, event_date) ON `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.FC_PRODUCT_IN_STORAGE_CHANGE_FACT;

-- Perform audit between stg and fct tables




CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DQ_PIPELINE_AUDIT_V1('FC_PRODUCT_IN_STORAGE_CHANGE_LDG_TO_BASE',  'NAP_ASCP_FC_PRODUCT_IN_STORAGE_CHANGE',  
'{{params.dbenv}}_NAP_BASE_VWS',  'FC_PRODUCT_IN_STORAGE_CHANGE_LDG', '{{params.dbenv}}_NAP_BASE_VWS',  'FC_PRODUCT_IN_STORAGE_CHANGE_FACT',  'Count_Distinct',  0,  'T-S',  'concat(originating_event_id, \'\'-\'\', sku_id, \'\'-\'\', sku_inventory_state_code, \'\'-\'\', carton_lpn)',  'concat(originating_event_id, \'\'-\'\', sku_id, \'\'-\'\', sku_inventory_state_code, \'\'-\'\', carton_lpn)',  NULL,  NULL,  'N');

/*

-- Update control metrics table with temp table

*/



INSERT INTO isf_dag_control_log_wrk
(SELECT 'ascp_fc_product_in_storage_change_16753_TECH_SC_NAP_insights' AS isf_dag_nm,
  CAST(NULL AS STRING) AS step_nm,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_FC_PRODUCT_IN_STORAGE_CHANGE')) AS batch_id,
  SUBSTR('`{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.FC_PRODUCT_IN_STORAGE_CHANGE_FACT', 1, 100) AS tbl_nm,
  SUBSTR('TERADATA_JOB_END_TMSTP', 1, 100) AS metric_nm,
  SUBSTR(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS STRING), 1, 60) AS metric_value,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS
  metric_tmstp_utc,
 `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as metric_tmstp_tz,
  CURRENT_DATE('GMT') AS metric_date);
  

UPDATE isf_dag_control_log_wrk AS wrk SET
    metric_value = wrk.metric_value,
    metric_tmstp_utc = wrk.metric_tmstp_utc ,
    metric_tmstp_tz=wrk.metric_tmstp_tz
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.isf_dag_control_log
WHERE 
LOWER(isf_dag_control_log.isf_dag_nm) = LOWER('{{params.dag_name}}') 
AND LOWER(COALESCE(isf_dag_control_log.tbl_nm, '')) = LOWER(COALESCE(wrk.tbl_nm, '')) 
AND LOWER(isf_dag_control_log.metric_nm) = LOWER(wrk.metric_nm) 
AND isf_dag_control_log.batch_id = (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}'));

            


END;
