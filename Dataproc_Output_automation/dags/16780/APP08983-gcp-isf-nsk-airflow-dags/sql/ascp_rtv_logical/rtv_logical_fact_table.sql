/*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File              : rtv_logical_fact_table.sql
-- Author            : Alexander Doroshevich
-- Description       : Merge RTV_LOGICAL_FACT table with delta from staging table
-- Source topic      : ascp-rms-rtv-avro
-- Object model      : RTV
-- ETL Run Frequency : Every day
-- Version :         : 0.1
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2023-07-31  Alexander Doroshevich   FA-9653:  Migrate TECH_SC_NAP_Engg_Metamorph_rtv_logical to ISF
-- 2023-10-17  Tetiana Ivchyk          FA-10219: Add metrics to released rtv_logical
-- 2023-11-10 Josh Roter               FA-10503: Migrate DAG to new AppId 08983
-- 2024-02-26  Oleksandr Chaichenko  FA-11960: Migration to Airflow NSK
--************************************************************************************************************************************/


/***********************************************************************************
-- Load temp table with metrics
************************************************************************************/
/************************************************************************************/


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
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.isf_dag_control_log;


CREATE TEMPORARY TABLE IF NOT EXISTS rtv_logical_ldg_tpt_info AS
SELECT jobstarttime,
 jobendtime,
 rowsinserted
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.tpt_job_log
WHERE LOWER(job_name) = LOWER('{{params.dbenv}}_NAP_STG_RTV_LOGICAL_LDG_JOB')
QUALIFY (ROW_NUMBER() OVER (PARTITION BY job_name ORDER BY rcd_load_tmstp DESC)) = 1;


CREATE TEMPORARY TABLE IF NOT EXISTS rtv_logical_item_ldg_tpt_info AS
SELECT jobstarttime,
 jobendtime,
 rowsinserted
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.tpt_job_log
WHERE LOWER(job_name) = LOWER('{{params.dbenv}}_NAP_STG_RTV_LOGICAL_ITEM_LDG_JOB')
QUALIFY (ROW_NUMBER() OVER (PARTITION BY job_name ORDER BY rcd_load_tmstp DESC)) = 1;


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
WITH dummy AS (SELECT 1 AS num)
SELECT 'ascp_rtv_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
 cast(NULL as STRING)  AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_RTV_LOGICAL')) AS batch_id,
 SUBSTR('{{params.dbenv}}_NAP_FCT.RTV_LOGICAL_FACT', 1, 100) AS tbl_nm,
 SUBSTR('TERADATA_JOB_START_TMSTP', 1, 100) AS metric_nm,
 SUBSTR(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS STRING), 1, 60) AS metric_value,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS STRING)) metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT 'ascp_rtv_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
 cast(NULL as STRING)  AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_RTV_LOGICAL')) AS batch_id,
 SUBSTR('{{params.dbenv}}_NAP_STG.RTV_LOGICAL_LDG', 1, 100) AS tbl_nm,
 SUBSTR('LOADED_TO_CSV_CNT', 1, 100) AS metric_nm,
 SUBSTR((SELECT metric_value
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.processed_data_spark_log
   WHERE LOWER(isf_dag_nm) = LOWER('ascp_rtv_logical_16780_TECH_SC_NAP_insights')
    AND LOWER(tbl_nm) = LOWER('RTV_LOGICAL_LDG')
    AND LOWER(metric_nm) = LOWER('PROCESSED_ROWS_CNT')), 1, 60) AS metric_value,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS STRING)) metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT 'ascp_rtv_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
 cast(NULL as STRING)  AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_RTV_LOGICAL')) AS batch_id,
 SUBSTR('{{params.dbenv}}_NAP_STG.RTV_LOGICAL_ITEM_LDG', 1, 100) AS tbl_nm,
 SUBSTR('LOADED_TO_CSV_CNT', 1, 100) AS metric_nm,
 SUBSTR((SELECT metric_value
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.processed_data_spark_log
   WHERE LOWER(isf_dag_nm) = LOWER('ascp_rtv_logical_16780_TECH_SC_NAP_insights')
    AND LOWER(tbl_nm) = LOWER('RTV_LOGICAL_ITEM_LDG')
    AND LOWER(metric_nm) = LOWER('PROCESSED_ROWS_CNT')), 1, 60) AS metric_value,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS STRING)) metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT 'ascp_rtv_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
 cast(NULL as STRING)  AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_RTV_LOGICAL')) AS batch_id,
 SUBSTR(NULL, 1, 100) AS tbl_nm,
 SUBSTR('SPARK_PROCESSED_MESSAGE_CNT', 1, 100) AS metric_nm,
 SUBSTR((SELECT metric_value
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.processed_data_spark_log
   WHERE LOWER(isf_dag_nm) = LOWER('ascp_rtv_logical_16780_TECH_SC_NAP_insights')
    AND LOWER(metric_nm) = LOWER('SPARK_PROCESSED_MESSAGE_CNT')), 1, 60) AS metric_value,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS STRING)) metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT 'ascp_rtv_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
 cast(NULL as STRING)  AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_RTV_LOGICAL')) AS batch_id,
 SUBSTR('{{params.dbenv}}_NAP_STG.RTV_LOGICAL_LDG', 1, 100) AS tbl_nm,
 SUBSTR('LOADED_TO_LDG_CNT', 1, 100) AS metric_nm,
 SUBSTR(CAST((SELECT rowsinserted
    FROM rtv_logical_ldg_tpt_info) AS STRING), 1, 60) AS metric_value,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS STRING)) metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT 'ascp_rtv_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
 cast(NULL as STRING)  AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_RTV_LOGICAL')) AS batch_id,
 SUBSTR('{{params.dbenv}}_NAP_STG.RTV_LOGICAL_ITEM_LDG', 1, 100) AS tbl_nm,
 SUBSTR('LOADED_TO_LDG_CNT', 1, 100) AS metric_nm,
 SUBSTR(CAST((SELECT rowsinserted
    FROM rtv_logical_item_ldg_tpt_info) AS STRING), 1, 60) AS metric_value,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS STRING)) metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT 'ascp_rtv_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
 cast(NULL as STRING)  AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_RTV_LOGICAL')) AS batch_id,
 SUBSTR('{{params.dbenv}}_NAP_STG.RTV_LOGICAL_LDG', 1, 100) AS tbl_nm,
 SUBSTR('TPT_JOB_START_TMSTP', 1, 100) AS metric_nm,
 SUBSTR(CAST((SELECT jobstarttime
    FROM rtv_logical_ldg_tpt_info) AS STRING), 1, 60) AS metric_value,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS STRING)) metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT 'ascp_rtv_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
 cast(NULL as STRING)  AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_RTV_LOGICAL')) AS batch_id,
 SUBSTR('{{params.dbenv}}_NAP_STG.RTV_LOGICAL_ITEM_LDG', 1, 100) AS tbl_nm,
 SUBSTR('TPT_JOB_START_TMSTP', 1, 100) AS metric_nm,
 SUBSTR(CAST((SELECT jobstarttime
    FROM rtv_logical_item_ldg_tpt_info) AS STRING), 1, 60) AS metric_value,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS STRING)) metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT 'ascp_rtv_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
 cast(NULL as STRING)  AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_RTV_LOGICAL')) AS batch_id,
 SUBSTR('{{params.dbenv}}_NAP_STG.RTV_LOGICAL_LDG', 1, 100) AS tbl_nm,
 SUBSTR('TPT_JOB_END_TMSTP', 1, 100) AS metric_nm,
 SUBSTR(CAST((SELECT jobendtime
    FROM rtv_logical_ldg_tpt_info) AS STRING), 1, 60) AS metric_value,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS STRING)) metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT 'ascp_rtv_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
 cast(NULL as STRING)  AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_RTV_LOGICAL')) AS batch_id,
 SUBSTR('{{params.dbenv}}_NAP_STG.RTV_LOGICAL_ITEM_LDG', 1, 100) AS tbl_nm,
 SUBSTR('TPT_JOB_END_TMSTP', 1, 100) AS metric_nm,
 SUBSTR(CAST((SELECT jobendtime
    FROM rtv_logical_item_ldg_tpt_info) AS STRING), 1, 60) AS metric_value,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS STRING)) metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy;

/***********************************************************************************
-- DATA_TIMELINESS_METRIC
************************************************************************************/
 CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.data_timeliness_metric_fact_ld('RTV_LOGICAL_FACT',  '{{params.dbenv}}_NAP_FCT',  'ascp_rtv_logical',  'load_05_teradata',  0,  'LOAD_START',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  'NAP_ASCP_RTV_LOGICAL');

-- Merge into target FACT table
/* * curr.market_rate */






-- Compare timestamp fields to match versions


-- and rtv.version_ts = rtvi.version_ts


--LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.CURRENCY_EXCHG_RATE_DIM curr on rtv.shiptime BETWEEN curr.start_date AND curr.end_date


-- qry.return_to_vendor_num = tgt.return_to_vendor_num


-- qry.rms_sku_num = tgt.rms_sku_num


-- qry.product_id = tgt.product_id


--dw_sys_load_tmstp = qry.dw_sys_load_tmstp,


MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.rtv_logical_fact AS tgt
USING (SELECT rtv.return_to_vendor_num,
  rtv.vendor_num,
  rtvi.rms_sku_num,
  rtv.create_date,
  rtv.createtime,
  rtv.createtime_tz,
  rtv.ship_date,
  rtv.shiptime,
  rtv.shiptime_tz,
  rtv.rtv_source,
  rtv.from_location_id,
  rtv.from_location_type,
  rtv.from_logical_location_id,
  rtv.from_logical_location_type,
  rtv.external_reference_number,
  rtv.returns_authorization_number,
  rtvi.upc_num,
  rtvi.rtv_qty,
  rtvi.from_disposition,
  rtvi.to_disposition,
  rtvi.reason_code,
   CAST(rtvi.unitcost AS FLOAT64) * rtvi.rtv_qty AS unit_cost_amt,
  '' AS unit_cost_currency_code,
   CAST(rtvi.unitcost AS FLOAT64) * rtvi.rtv_qty AS unit_cost_amt_usd,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_RTV_LOGICAL')) AS dw_batch_id,
   (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_RTV_LOGICAL')) AS dw_batch_date,
   timestamp(current_datetime('PST8PDT')) AS dw_sys_load_tmstp,
  `{{params.gcp_project_id}}`.jwn_udf.default_tz_pst() AS dw_sys_load_tmstp_tz,
   timestamp(current_datetime('PST8PDT')) AS dw_sys_updt_tmstp,
  `{{params.gcp_project_id}}`.jwn_udf.default_tz_pst() AS dw_sys_updt_tmstp_tz,
 FROM (SELECT returntovendornumber AS return_to_vendor_num,
    vendornumber AS vendor_num,
    PARSE_DATE('%F', SUBSTR(createtime, 1, 10)) AS create_date,
 cast(parse_timestamp('%Y-%m-%d %H:%M:%E6S%Ez', replace(replace(rtv.createtime, 'Z', '+00:00'), 'T', ' ')) as TIMESTAMP) AS createtime,
 '+00:00' AS createtime_tz,
    PARSE_DATE('%F', SUBSTR(shiptime, 1, 10)) AS ship_date, 
 cast(parse_timestamp('%Y-%m-%d %H:%M:%E6S%Ez', replace(replace(rtv.shiptime, 'Z', '+00:00'), 'T', ' ')) as TIMESTAMP) AS shiptime,
 '+00:00' AS shiptime_tz,
    rtv_source,
    fromlocation_id AS from_location_id,
    fromlocation_type AS from_location_type,
    fromlogical_location_id AS from_logical_location_id,
    fromlogical_location_type AS from_logical_location_type,
    externalreferencenumber AS external_reference_number,
    returnsauthorizationnumber AS returns_authorization_number
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.rtv_logical_ldg AS rtv

QUALIFY row_number() OVER (PARTITION BY return_to_vendor_num, vendor_num ORDER BY greatest(createtime, coalesce(shiptime, TIMESTAMP '1970-01-01 00:00:00'), CAST(trim(coalesce(rtv.canceltime, '1970-01-01 00:00:00'), ' ') as TIMESTAMP), CAST(trim(coalesce(rtv.updatetime, '1970-01-01 00:00:00'), ' ') as TIMESTAMP)) DESC, shiptime DESC, rtv.canceltime DESC, rtv.updatetime DESC) = 1
         ) AS rtv
  INNER JOIN (SELECT returntovendornumber AS return_to_vendor_num,
    product_id AS rms_sku_num,
    upc AS upc_num,
    CAST(trunc(cast(quantity as float64)) AS INTEGER) AS rtv_qty,
    fromdisposition AS from_disposition,
    todisposition AS to_disposition,
    reasoncode AS reason_code,
    unitcost
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.rtv_logical_item_ldg AS rtvi
   QUALIFY (ROW_NUMBER() OVER (PARTITION BY returntovendornumber, product_id ORDER BY GREATEST(createtime, COALESCE(shiptime
          , '1970-01-01 00:00:00'), COALESCE(canceltime, '1970-01-01 00:00:00'), COALESCE(updatetime,
          '1970-01-01 00:00:00')) DESC, shiptime DESC, canceltime DESC, updatetime DESC)) = 1) AS rtvi ON LOWER(rtv.return_to_vendor_num
    ) = LOWER(rtvi.return_to_vendor_num)) AS qry
ON LOWER(qry.return_to_vendor_num) = LOWER(tgt.return_to_vendor_num) AND LOWER(qry.vendor_num) = LOWER(tgt.vendor_num)
 AND LOWER(qry.rms_sku_num) = LOWER(tgt.rms_sku_num)
WHEN MATCHED THEN UPDATE SET
 create_date = qry.create_date,
 createtime = qry.createtime,
 createtime_tz = qry.createtime_tz,
 ship_date = qry.ship_date,
 shiptime = qry.shiptime,
 shiptime_tz = qry.shiptime_tz,
 rtv_source = qry.rtv_source,
 from_location_id = CAST(trunc(cast(qry.from_location_id as float64)) AS INTEGER),
 from_location_type = qry.from_location_type,
 from_logical_location_id = CAST(trunc(cast(qry.from_logical_location_id as float64)) AS INTEGER),
 from_logical_location_type = qry.from_logical_location_type,
 external_reference_number = qry.external_reference_number,
 returns_authorization_number = qry.returns_authorization_number,
 upc_num = qry.upc_num,
 rtv_qty = qry.rtv_qty,
 from_disposition = qry.from_disposition,
 to_disposition = qry.to_disposition,
 reason_code = qry.reason_code,
 unit_cost_amt = cast(qry.unit_cost_amt as numeric),
 unit_cost_currency_code = qry.unit_cost_currency_code,
 unit_cost_amt_usd = cast(qry.unit_cost_amt_usd as numeric),
 dw_batch_id = qry.dw_batch_id,
 dw_batch_date = qry.dw_batch_date,
 dw_sys_updt_tmstp = qry.dw_sys_updt_tmstp,
 dw_sys_updt_tmstp_tz = qry.dw_sys_updt_tmstp_tz
WHEN NOT MATCHED THEN INSERT VALUES(qry.return_to_vendor_num, qry.vendor_num, qry.rms_sku_num, qry.create_date, qry.createtime,createtime_tz
 , qry.ship_date, qry.shiptime,qry.shiptime_tz, qry.rtv_source, CAST(trunc(cast(qry.from_location_id as float64)) AS INTEGER), qry.from_location_type, CAST(trunc(cast(qry.from_logical_location_id as float64)) AS INTEGER)
 , qry.from_logical_location_type, qry.external_reference_number, qry.returns_authorization_number, qry.upc_num, qry.rtv_qty
 , qry.from_disposition, qry.to_disposition, qry.reason_code, cast(qry.unit_cost_amt as numeric), qry.unit_cost_currency_code, cast(qry.unit_cost_amt_usd as numeric)
 , qry.dw_batch_id, qry.dw_batch_date, qry.dw_sys_load_tmstp,qry.dw_sys_load_tmstp_tz, qry.dw_sys_updt_tmstp, qry.dw_sys_updt_tmstp_tz);


--COLLECT STATISTICS ON `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.RTV_LOGICAL_FACT COLUMN( return_to_vendor_num, vendor_num, rms_sku_num )





/***********************************************************************************
-- Perform audit between stg and fct tables
************************************************************************************/

-- RTV_LOGICAL
CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.dq_pipeline_audit_v1
(
  'RTV_LOGICAL_LDG_TO_BASE',
  'NAP_ASCP_RTV_LOGICAL',
  '{{params.dbenv}}_NAP_STG',
  'RTV_LOGICAL_LDG',
  '{{params.dbenv}}_NAP_FCT',
  'RTV_LOGICAL_FACT',
  'Count_Distinct', 
  0, 
  'T-S', 
  'concat(returntovendornumber, vendornumber)', 
  'concat(return_to_vendor_num, vendor_num)', 
  null,
  null, 
  'Y'
);

-- RTV_LOGICAL_ITEM
CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.dq_pipeline_audit_v1
(
  'RTV_LOGICAL_ITEM_LDG_TO_BASE',
  'NAP_ASCP_RTV_LOGICAL',
  '{{params.dbenv}}_NAP_STG',
  'RTV_LOGICAL_ITEM_LDG',
  '{{params.dbenv}}_NAP_FCT',
  'RTV_LOGICAL_FACT',
  'Count_Distinct', 
  0, 
  'T-S', 
  'concat(returntovendornumber, product_id)', 
  'concat(return_to_vendor_num, vendor_num, rms_sku_num)', 
  null,
  null, 
  'Y'
);


/***********************************************************************************
-- DATA_TIMELINESS_METRIC
************************************************************************************/
CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.data_timeliness_metric_fact_ld('RTV_LOGICAL_FACT',  '{{params.dbenv}}_NAP_FCT',  'ascp_rtv_logical',  'load_05_teradata',  1,  'LOAD_END',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  'NAP_ASCP_RTV_LOGICAL');

 /***********************************************************************************
-- Update control metrics table with temp table
************************************************************************************/
INSERT INTO isf_dag_control_log_wrk
(SELECT 'ascp_rtv_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
  CAST(NULL AS STRING) AS step_nm,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_RTV_LOGICAL')) AS batch_id,
  SUBSTR('{{params.dbenv}}_NAP_FCT.RTV_LOGICAL_FACT', 1, 100) AS tbl_nm,
  SUBSTR('TERADATA_JOB_END_TMSTP', 1, 100) AS metric_nm,
  SUBSTR(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS STRING), 1, 60) AS metric_value,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS STRING)) metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date);


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.isf_dag_control_log SET
 metric_value = wrk.metric_value,
 metric_tmstp = wrk.metric_tmstp FROM isf_dag_control_log_wrk AS wrk
WHERE LOWER(isf_dag_control_log.isf_dag_nm) = LOWER(wrk.isf_dag_nm) AND LOWER(COALESCE(isf_dag_control_log.tbl_nm, ''))
    = LOWER(COALESCE(wrk.tbl_nm, '')) AND LOWER(isf_dag_control_log.metric_nm) = LOWER(wrk.metric_nm) AND
  isf_dag_control_log.batch_id = wrk.batch_id;
