CREATE TEMPORARY TABLE isf_dag_control_log_wrk AS
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
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.isf_dag_control_log;



CREATE TEMPORARY TABLE IF NOT EXISTS rms_cost_rtv_ldg_tpt_info AS
SELECT jobstarttime,
 jobendtime,
 rowsinserted
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.tpt_job_log
WHERE LOWER(job_name) = LOWER('{{params.dbenv}}_nap_stg_rms_cost_rtv_ldg_job')
QUALIFY (ROW_NUMBER() OVER (PARTITION BY job_name ORDER BY rcd_load_tmstp DESC)) = 1;



INSERT INTO isf_dag_control_log_wrk
WITH dummy AS (SELECT 1 as num)
select
  '{{params.dag_name}}' as isf_dag_nm,
  'null' as STEP_NM,
  (SELECT BATCH_ID FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE SUBJECT_AREA_NM ='{{params.subject_area}}') as batch_id,
  cast('{{params.dbenv}}_{{params.fct_table_name}}' as STRING) as tbl_nm,
  cast('TERADATA_JOB_START_TMSTP' as STRING) as metric_nm,
  CAST(timestamp(current_datetime('GMT')) AS STRING) AS metric_value,
  timestamp(current_datetime('GMT')) AS metric_tmstp_utc,
  'GMT' AS metric_tmstp_tz,
  CURRENT_DATE('GMT') AS metric_date
union all
select
  '{{params.dag_name}}' as isf_dag_nm,
   'null' as STEP_NM,
  (SELECT BATCH_ID FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE SUBJECT_AREA_NM ='{{params.subject_area}}') as batch_id,
  cast('{{params.dbenv}}_{params.ldg_table_name}' as STRING) as tbl_nm,
  cast('LOADED_TO_CSV_CNT' as STRING) as metric_nm,
  cast((select metric_value from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.processed_data_spark_log 
                   where isf_dag_nm = '{{params.dag_name}}'
                         and tbl_nm = 'RMS_COST_RTV_LDG'
                         and metric_nm = 'PROCESSED_ROWS_CNT') as STRiNG) as metric_value,
  timestamp(current_datetime('GMT')) AS metric_tmstp_utc,
  'GMT' AS metric_tmstp_tz,
  CURRENT_DATE('GMT') AS metric_date
from dummy
union all
select
  '{{params.dag_name}}' as isf_dag_nm,
   'null' as STEP_NM,
  (SELECT BATCH_ID FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE lower(SUBJECT_AREA_NM) =lower('{{params.subject_area}}')) as batch_id,
  cast(null as STRING) as tbl_nm,
  cast('SPARK_PROCESSED_MESSAGE_CNT' AS STRING) as metric_nm,
  cast((select metric_value from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.processed_data_spark_log 
                   where isf_dag_nm = '{{params.dag_name}}'
                         and metric_nm = 'SPARK_PROCESSED_MESSAGE_CNT') as STRING) as metric_value,
  timestamp(current_datetime('GMT')) AS metric_tmstp_utc,
  'GMT' AS metric_tmstp_tz,
  CURRENT_DATE('GMT') AS metric_date
from dummy
union all
select
  '{{params.dag_name}}' as isf_dag_nm,
   'null' as STEP_NM,
  (SELECT BATCH_ID FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE lower(SUBJECT_AREA_NM) =lower('{{params.subject_area}}')) as batch_id,
  cast('{{params.dbenv}}_{params.ldg_table_name}' as STRING) as tbl_nm,
  cast('LOADED_TO_LDG_CNT' as STRING) as metric_nm,
  cast((select RowsInserted from rms_cost_rtv_ldg_tpt_info) as STRING) as metric_value,
  timestamp(current_datetime('GMT')) AS metric_tmstp_utc,
  'GMT' AS metric_tmstp_tz,
  CURRENT_DATE('GMT') AS metric_date
from dummy
union all
select
  '{{params.dag_name}}' as isf_dag_nm,
  'null' as STEP_NM,
  (SELECT BATCH_ID FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE lower(SUBJECT_AREA_NM) =lower('{{params.subject_area}}')) as batch_id,
  cast('{{params.dbenv}}_{params.ldg_table_name}' as STRING) as tbl_nm,
  cast('TPT_JOB_START_TMSTP' as STRING) as metric_nm,
  cast((select JobStartTime from rms_cost_rtv_ldg_tpt_info) as STRING) as metric_value,
  timestamp(current_datetime('GMT')) AS metric_tmstp_utc,
  'GMT' AS metric_tmstp_tz,
  CURRENT_DATE('GMT') AS metric_date
from dummy
union all
select
  '{{params.dag_name}}' as isf_dag_nm,
   'null' as STEP_NM,
  (SELECT BATCH_ID FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE lower(SUBJECT_AREA_NM) =lower('{{params.subject_area}}')) as batch_id,
  cast('{{params.dbenv}}_{params.ldg_table_name}' as STRING) as tbl_nm,
  cast('TPT_JOB_END_TMSTP' as STRING) as metric_nm,
  cast((select JobEndTime from rms_cost_rtv_ldg_tpt_info) as STRING) as metric_value,
  timestamp(current_datetime('GMT')) AS metric_tmstp_utc,
  'GMT' AS metric_tmstp_tz,
  CURRENT_DATE('GMT') AS metric_date
from dummy;


--MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.RMS_COST_RTV_FACT
MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.rms_cost_rtv_fact AS fact
USING (SELECT TRIM(return_to_vendor_number) AS return_to_vendor_num,
  TRIM(detail_product_id) AS rms_sku_num,
  TRIM(external_reference_number) AS external_reference_num,
  CAST(TRIM(from_location_facility) AS INTEGER) AS from_location_facility,
  CAST(TRIM(from_location_logical) AS INTEGER) AS from_location_logical,
  TRIM(vendor_number) AS vendor_num,
  TRIM(vendor_address_line1_tokenized) AS vendor_address_line1_tokenized,
  TRIM(vendor_address_line2_tokenized) AS vendor_address_line2_tokenized,
  TRIM(vendor_address_line3_tokenized) AS vendor_address_line3_tokenized,
  TRIM(vendor_address_city) AS vendor_address_city,
  TRIM(vendor_address_state) AS vendor_address_state,
  TRIM(vendor_address_postal_code_tokenized) AS vendor_address_postal_code_tokenized,
  TRIM(vendor_address_country_code) AS vendor_address_country_code,
  TRIM(returns_authorization_number) AS returns_authorization_num,
  TRIM(comments) AS comments,
  CAST((detail_quantity) AS INTEGER) AS quantity,
  TRIM(detail_disposition) AS disposition,
  TRIM(detail_reason_code) AS reason_code,
   CAST(REPLACE(REPLACE(cast(create_time as string), 'Z', '+00:00'), 'T', ' ') as TIMESTAMP ) AS create_time,
   '+00:00' as create_time_tz,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(REPLACE(REPLACE(cast(detail_latest_update_time as string), 'Z', '+00:00'), 'T', ' ') AS DATETIME)
    ) AS DATETIME) AS TIMESTAMP) AS latest_update_time,
    '+00:00' as latest_update_time_tz,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(REPLACE(REPLACE(cast(detail_latest_cancel_time as string), 'Z', '+00:00'), 'T', ' ') AS DATETIME)
    ) AS DATETIME) AS TIMESTAMP) AS latest_cancel_time,
   '+00:00' as latest_cancel_time_tz,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(REPLACE(REPLACE(cast(detail_shipped_time as string), 'Z', '+00:00'), 'T', ' ') AS DATETIME)
    ) AS DATETIME) AS TIMESTAMP) AS shipped_time,
    '+00:00' as shipped_time_tz,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS dw_batch_id,
   (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('GMT')) AS TIMESTAMP) AS dw_sys_load_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as dw_sys_load_tmstp_tz,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('GMT')) AS TIMESTAMP) AS dw_sys_updt_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS dw_sys_updt_tmstp_tz
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_{{params.ldg_table_name}}--{{params.dbenv}}_nap_stg.rms_cost_rtv_ldg
 ) AS ldg
ON LOWER(ldg.return_to_vendor_num) = LOWER(fact.return_to_vendor_num) AND LOWER(ldg.rms_sku_num) = LOWER(fact.rms_sku_num
   )
WHEN MATCHED THEN UPDATE SET
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
 create_time = ldg.create_time,
 create_time_tz= ldg.create_time_tz,
 latest_update_time = ldg.latest_update_time,
 latest_update_time_tz= ldg.latest_update_time_tz,
 latest_cancel_time = ldg.latest_cancel_time,
 latest_cancel_time_tz= ldg.latest_cancel_time_tz,
 shipped_time = ldg.shipped_time,
 shipped_time_tz = ldg.shipped_time_tz,
 dw_batch_id = ldg.dw_batch_id,
 dw_batch_date = ldg.dw_batch_date,
 dw_sys_updt_tmstp = ldg.dw_sys_updt_tmstp,
 dw_sys_updt_tmstp_tz = ldg.dw_sys_updt_tmstp_tz
WHEN NOT MATCHED THEN INSERT (return_to_vendor_num, rms_sku_num, external_reference_num, from_location_facility,
 from_location_logical, vendor_num, vendor_address_line1_tokenized, vendor_address_line2_tokenized,
 vendor_address_line3_tokenized, vendor_address_city, vendor_address_state, vendor_address_postal_code_tokenized,
 vendor_address_country_code, returns_authorization_num, comments, quantity, disposition, reason_code, create_time,create_time_tz,
 latest_update_time,latest_update_time_tz, latest_cancel_time,latest_cancel_time_tz, shipped_time,shipped_time_tz, dw_batch_id, dw_batch_date, dw_sys_load_tmstp,dw_sys_load_tmstp_tz, dw_sys_updt_tmstp, dw_sys_updt_tmstp_tz
 ) VALUES(ldg.return_to_vendor_num, ldg.rms_sku_num, ldg.external_reference_num, ldg.from_location_facility, ldg.from_location_logical
 , ldg.vendor_num, ldg.vendor_address_line1_tokenized, ldg.vendor_address_line2_tokenized, ldg.vendor_address_line3_tokenized
 , ldg.vendor_address_city, ldg.vendor_address_state, ldg.vendor_address_postal_code_tokenized, ldg.vendor_address_country_code
 , ldg.returns_authorization_num, ldg.comments, ldg.quantity, ldg.disposition, ldg.reason_code, ldg.create_time,ldg.create_time_tz, ldg.latest_update_time
 , ldg.latest_update_time_tz, ldg.latest_cancel_time,ldg.latest_cancel_time_tz, ldg.shipped_time, ldg.shipped_time_tz ,ldg.dw_batch_id, ldg.dw_batch_date, ldg.dw_sys_load_tmstp,ldg.dw_sys_load_tmstp_tz, ldg.dw_sys_updt_tmstp, ldg.dw_sys_updt_tmstp_tz
 );


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.dq_pipeline_audit_v1(
  'RMS_COST_RTV_LDG_TO_BASE',
  '{{params.subject_area}}',
  '{{params.dbenv}}_NAP_STG',
  'RMS_COST_RTV_LDG',
  '{{params.dbenv}}_NAP_FCT',
  'RMS_COST_RTV_FACT',
  'Count_Distinct',
  0,
  'T-S',
  'CONCAT(return_to_vendor_number, detail_product_id)',
  'CONCAT(return_to_vendor_num, rms_sku_num)',
  NULL,
  NULL,
  'Y'
);


INSERT INTO isf_dag_control_log_wrk
SELECT
  '{{params.dag_name}}' AS isf_dag_nm,
  NULL AS STEP_NM,
  (SELECT BATCH_ID FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE lower(SUBJECT_AREA_NM) = lower('{{params.subject_area}}')) AS batch_id,
  CAST('{{params.dbenv}}_{{params.fct_table_name}}' AS STRING) AS tbl_nm,
  CAST('TERADATA_JOB_END_TMSTP' AS STRING) AS metric_nm,
  CAST(CAST(timestamp(current_datetime('GMT')) AS STRING) AS STRING) AS metric_value,
  timestamp(current_datetime('GMT')) AS metric_tmstp_utc,
  'GMT' AS metric_tmstp_tz,
  CURRENT_DATE('GMT') AS metric_date;





UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.isf_dag_control_log  AS log
SET 
  log.metric_value = wrk.metric_value,
  log.metric_tmstp = wrk.metric_tmstp_utc,
  log.metric_tmstp_tz = wrk.metric_tmstp_tz
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY isf_dag_nm, tbl_nm, metric_nm, batch_id ORDER BY metric_tmstp_utc DESC) as rn
    FROM isf_dag_control_log_wrk
) AS wrk
WHERE LOWER(log.isf_dag_nm) = LOWER(wrk.isf_dag_nm)
  AND LOWER(COALESCE(log.tbl_nm, '')) = LOWER(COALESCE(wrk.tbl_nm, ''))
  AND LOWER(log.metric_nm) = LOWER(wrk.metric_nm)
  AND log.batch_id = wrk.batch_id
  AND wrk.rn = 1; 
