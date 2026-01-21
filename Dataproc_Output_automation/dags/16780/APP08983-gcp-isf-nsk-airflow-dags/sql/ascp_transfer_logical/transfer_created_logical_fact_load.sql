BEGIN
CREATE TEMPORARY TABLE IF NOT EXISTS isf_dag_control_log_wrk  AS
SELECT *
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.isf_dag_control_log;


CREATE TEMPORARY TABLE IF NOT EXISTS transfer_created_logical_ldg_tpt_info AS
SELECT jobstarttime,
 jobendtime,
 rowsinserted
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.tpt_job_log
WHERE LOWER(job_name) = LOWER('{{params.dbenv}}_NAP_STG_TRANSFER_CREATED_LOGICAL_LDG_JOB')
QUALIFY (ROW_NUMBER() OVER (PARTITION BY job_name ORDER BY rcd_load_tmstp DESC)) = 1;


INSERT INTO isf_dag_control_log_wrk
WITH dummy AS (SELECT 1 AS num) 
SELECT 'ascp_transfer_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
   'NULL' as STEP_NM,
    (SELECT batch_id
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
    WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
   RPAD('{{params.dbenv}}_NAP_FCT.TRANSFER_CREATED_LOGICAL_FACT', 1, ' ') AS tbl_nm,
   RPAD('TERADATA_JOB_START_TMSTP', 1, ' ') AS metric_nm,
   RPAD(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('GMT')) AS DATETIME) AS STRING), 1, ' ') AS metric_value
   ,
   CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('GMT')) AS DATETIME) AS metric_tmstp,
   CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp_utc,
   'GMT' as metric_tmstp_tz,
   CURRENT_DATE('GMT') AS metric_date
  FROM dummy
union all
SELECT 
    'ascp_transfer_logical_16780_TECH_SC_NAP_insights' as isf_dag_nm,
    'NULL' as STEP_NM,
    (SELECT batch_id
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
    WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
   RPAD('{{params.dbenv}}_NAP_STG.TRANSFER_CREATED_LOGICAL_LDG', 1, ' ') AS tbl_nm,
   RPAD('LOADED_TO_CSV_CNT', 1, ' ') AS metric_nm,
   RPAD((SELECT metric_value
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.processed_data_spark_log
     WHERE LOWER(isf_dag_nm) = LOWER('ascp_transfer_logical_16780_TECH_SC_NAP_insights')
      AND LOWER(tbl_nm) = LOWER('TRANSFER_CREATED_LOGICAL_LDG')
      AND LOWER(metric_nm) = LOWER('PROCESSED_ROWS_CNT')), 1, ' ') AS metric_value,
   CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('GMT')) AS DATETIME) AS metric_tmstp,
   CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp_utc,
   'GMT' as metric_tmstp_tz,
   CURRENT_DATE('GMT') AS metric_date
  FROM dummy
union all
select 
    'ascp_transfer_logical_16780_TECH_SC_NAP_insights' as isf_dag_nm,
    'NULL' as STEP_NM,
    (SELECT batch_id
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
    WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
   RPAD(NULL, 1, ' ') AS tbl_nm,
   RPAD('SPARK_PROCESSED_MESSAGE_CNT', 1, ' ') AS metric_nm,
   RPAD((SELECT metric_value
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.processed_data_spark_log
     WHERE LOWER(isf_dag_nm) = LOWER('ascp_transfer_logical_16780_TECH_SC_NAP_insights')
      AND LOWER(metric_nm) = LOWER('SPARK_PROCESSED_MESSAGE_CNT')), 1, ' ') AS metric_value,
   CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('GMT')) AS DATETIME) AS metric_tmstp,
   CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp_utc,
   'GMT' as metric_tmstp_tz,
   CURRENT_DATE('GMT') AS metric_date
  FROM dummy
union all
SELECT 'ascp_transfer_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
   'NULL' as STEP_NM,
    (SELECT batch_id
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
    WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
   RPAD('{{params.dbenv}}_NAP_STG.TRANSFER_CREATED_LOGICAL_LDG', 1, ' ') AS tbl_nm,
   RPAD('LOADED_TO_LDG_CNT', 1, ' ') AS metric_nm,
   RPAD(CAST((SELECT rowsinserted
      FROM transfer_created_logical_ldg_tpt_info) AS STRING), 1, ' ') AS metric_value,
   CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('GMT')) AS DATETIME) AS metric_tmstp,
   CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp_utc,
   'GMT' as metric_tmstp_tz,
   CURRENT_DATE('GMT') AS metric_date
FROM dummy
union all
SELECT 'ascp_transfer_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
   'NULL' as STEP_NM,
    (SELECT batch_id
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
    WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
   RPAD('{{params.dbenv}}_NAP_STG.TRANSFER_CREATED_LOGICAL_LDG', 1, ' ') AS tbl_nm,
   RPAD('TPT_JOB_START_TMSTP', 1, ' ') AS metric_nm,
   RPAD(CAST((SELECT jobstarttime
      FROM transfer_created_logical_ldg_tpt_info) AS STRING), 1, ' ') AS metric_value,
   CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('GMT')) AS DATETIME) AS metric_tmstp,
   CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp_utc,
   'GMT' as metric_tmstp_tz,
   CURRENT_DATE('GMT') AS metric_date
FROM dummy
union all
SELECT 'ascp_transfer_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
   'NULL' as STEP_NM,
    (SELECT batch_id
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
    WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
   RPAD('{{params.dbenv}}_NAP_STG.TRANSFER_CREATED_LOGICAL_LDG', 1, ' ') AS tbl_nm,
   RPAD('TPT_JOB_END_TMSTP', 1, ' ') AS metric_nm,
   RPAD(CAST((SELECT jobendtime
      FROM transfer_created_logical_ldg_tpt_info) AS STRING), 1, ' ') AS metric_value,
   CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('GMT')) AS DATETIME) AS metric_tmstp,
   CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp_utc,
   'GMT' as metric_tmstp_tz,
   CURRENT_DATE('GMT') AS metric_date
FROM dummy;



CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('TRANSFER_CREATED_LOGICAL_FACT',  '{{params.dbenv}}_NAP_FCT',  'ascp_transfer_logical',  'load_05_teradata',  0,  'LOAD_START',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  'NAP_ASCP_TRANSFER_LOGICAL');




MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.transfer_created_logical_fact AS fact
USING (SELECT SUBSTR(TRIM(operationno), 1, 16) AS operationno, 
RPAD(TRIM(operationtype), 1, ' ') AS operationtype, 
RPAD(TRIM(transfer_source), 5, ' ') AS transfer_source, 
SUBSTR(TRIM(transfercreated_status), 1, 20) AS transfercreated_status, CAST(SUBSTR(transfercreated_createtimestamp, 1, 10) AS DATE) AS createtimestamp_dt, 
REPLACE(
  REPLACE(transfercreated_createtimestamp, 'Z', '+00:00'),
  'T', ' ') 
 AS createtimestamp_ts, 
'+00:00' as createtimestamp_tz,
SUBSTR(TRIM(transfercreated_requestuserid), 1, 10) AS transfercreated_requestuserid, 
RPAD(TRIM(transfercreated_transfertype), 2, ' ') AS transfercreated_transfertype, 
SUBSTR(TRIM(transfercreated_transfercontextvalue), 1, 10) AS transfercreated_transfercontextvalue, 
transfercreated_transactionid, 
SUBSTR(TRIM(transfercreated_routingcode), 1, 10) AS transfercreated_routingcode, 
RPAD(TRIM(transfercreated_freightcode), 1, ' ') AS transfercreated_freightcode, 
CAST(SUBSTR(transfercreated_deliverydate, 1, 10) AS DATE) AS deliverydate, 
transfercreated_dept, 
CAST(SUBSTR(transfercreated_eventtimestamp, 1, 10) AS DATE) AS eventtimestamp_dt, 
REPLACE(
  REPLACE(transfercreated_createtimestamp, 'Z', '+00:00'),
  'T', ' ') 
 AS eventtimestamp_ts,
'+00:00' as eventtimestamp_tz,
CASE WHEN transfercreated_fromlocation_id IS NULL THEN NULL
WHEN CAST(TRUNC(CAST(TRIM(transfercreated_fromlocation_id) as bigint)) AS INTEGER) IS NULL THEN 0
ELSE CAST(TRUNC(CAST(TRIM(transfercreated_fromlocation_id) AS BIGINT)) AS INTEGER) END AS fromlocation_id, 
RPAD(TRIM(transfercreated_fromlocation_typ), 1, ' ') AS transfercreated_fromlocation_typ, 
CASE WHEN transfercreated_fromlogicallocation_id IS NULL THEN NULL 
WHEN CAST(TRUNC(CAST(TRIM(transfercreated_fromlogicallocation_id) as bigint)) AS INTEGER) IS NULL THEN 0
ELSE CAST(TRUNC(CAST(TRIM(transfercreated_fromlogicallocation_id) AS BIGINT)) AS INTEGER) END AS fromlogicallocation_id, 
RPAD(TRIM(transfercreated_fromlogicallocation_typ), 1, ' ') AS transfercreated_fromlogicallocation_typ, 
CASE WHEN transfercreated_tolocation_id IS NULL THEN NULL 
WHEN CAST(TRUNC(CAST(TRIM(transfercreated_tolocation_id) as bigint)) AS INTEGER) is null then 0
ELSE CAST(TRUNC(CAST(TRIM(transfercreated_tolocation_id) AS BIGINT)) AS INTEGER) END AS tolocation_id, 
RPAD(TRIM(transfercreated_tolocation_typ), 1, ' ') AS transfercreated_tolocation_typ, 
CASE WHEN transfercreated_tologicallocation_id IS NULL THEN NULL 
WHEN CAST(TRUNC(CAST(TRIM(transfercreated_tologicallocation_id) as bigint)) AS INTEGER) is null then 0
ELSE CAST(TRUNC(CAST(TRIM(transfercreated_tologicallocation_id) AS BIGINT)) AS INTEGER) END AS tologicallocation_id, 
RPAD(TRIM(transfercreated_tologicallocation_typ), 1, ' ') AS transfercreated_tologicallocation_typ, 
SUBSTR(TRIM(transfercreated_items_product_id), 1, 16) AS items_product_id, 
transfercreated_items_product_upc, 
transfercreated_items_quantity, 
transfercreated_items_cancelledquantity, 
(SELECT batch_id
            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
            WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS dw_batch_id, 
(SELECT curr_batch_date
            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
            WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS dw_batch_date,
CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP) as dw_sys_load_tmstp,
`{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as dw_sys_load_tmstp_tz, 
CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP) as dw_sys_updt_tmstp,
`{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as dw_sys_updt_tmstp_tz
    
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.transfer_created_logical_ldg
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY SUBSTR(TRIM(operationno), 1, 16), RPAD(TRIM(operationtype), 1, ' '), transfercreated_items_product_id ORDER BY transfercreated_eventtimestamp DESC)) = 1) AS ldg
ON LOWER(ldg.operationno) = LOWER(fact.operation_num) AND LOWER(ldg.operationtype) = LOWER(fact.operation_type) AND LOWER(ldg.items_product_id) = LOWER(fact.rms_sku_num)
WHEN MATCHED THEN UPDATE SET
    transfer_source = ldg.transfer_source,
    status = ldg.transfercreated_status,
    create_date = ldg.createtimestamp_dt,
    create_tmstp = CAST(ldg.createtimestamp_ts AS TIMESTAMP),
    request_userid = ldg.transfercreated_requestuserid,
    transfer_type = ldg.transfercreated_transfertype,
    transfer_context_value = ldg.transfercreated_transfercontextvalue,
    transaction_id = ldg.transfercreated_transactionid,
    routing_code = ldg.transfercreated_routingcode,
    freight_code = ldg.transfercreated_freightcode,
    delivery_date = ldg.deliverydate,
    department_number = CAST(ldg.transfercreated_dept AS INTEGER),
    event_date = ldg.eventtimestamp_dt,
    event_timestamp = CAST(ldg.eventtimestamp_ts AS TIMESTAMP),
    from_location_id = ldg.fromlocation_id,
    from_location_type = ldg.transfercreated_fromlocation_typ,
    from_logical_location_id = ldg.fromlogicallocation_id,
    from_logical_location_type = ldg.transfercreated_fromlogicallocation_typ,
    to_location_id = ldg.tolocation_id,
    to_location_type = ldg.transfercreated_tolocation_typ,
    to_logical_location_id = ldg.tologicallocation_id,
    to_logical_location_type = ldg.transfercreated_tologicallocation_typ,
    upc_num = ldg.transfercreated_items_product_upc,
    transfer_qty = ldg.transfercreated_items_quantity,
    cancelled_qty = ldg.transfercreated_items_cancelledquantity,
    dw_batch_id = ldg.dw_batch_id,
    dw_batch_date = ldg.dw_batch_date,
    dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP),
	dw_sys_updt_tmstp_tz = ldg.dw_sys_updt_tmstp_tz
WHEN NOT MATCHED THEN INSERT VALUES(ldg.operationno, ldg.operationtype, ldg.items_product_id, ldg.transfer_source, ldg.transfercreated_status, ldg.createtimestamp_dt, CAST(ldg.createtimestamp_ts AS TIMESTAMP), ldg.createtimestamp_tz, ldg.transfercreated_requestuserid, ldg.transfercreated_transfertype, ldg.transfercreated_transfercontextvalue, ldg.transfercreated_transactionid, ldg.transfercreated_routingcode, ldg.transfercreated_freightcode, ldg.deliverydate, CAST(ldg.transfercreated_dept AS INTEGER), ldg.eventtimestamp_dt, CAST(ldg.eventtimestamp_ts AS TIMESTAMP),ldg.eventtimestamp_tz, ldg.fromlocation_id, ldg.transfercreated_fromlocation_typ, ldg.fromlogicallocation_id, ldg.transfercreated_fromlogicallocation_typ, ldg.tolocation_id, ldg.transfercreated_tolocation_typ, ldg.tologicallocation_id, ldg.transfercreated_tologicallocation_typ, ldg.transfercreated_items_product_upc, ldg.transfercreated_items_quantity, ldg.transfercreated_items_cancelledquantity, ldg.dw_batch_id, ldg.dw_batch_date, ldg.dw_sys_load_tmstp,ldg.dw_sys_load_tmstp_tz, dw_sys_updt_tmstp,dw_sys_updt_tmstp_tz);


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DQ_PIPELINE_AUDIT_V1('TRANSFER_CREATED_LOGICAL_LDG_TO_BASE',  'NAP_ASCP_TRANSFER_LOGICAL',  '{{params.dbenv}}_NAP_STG',  'TRANSFER_CREATED_LOGICAL_LDG',  '{{params.dbenv}}_NAP_FCT',  'TRANSFER_CREATED_LOGICAL_FACT',  'Count_Distinct',  0,  'T-S',  'concat(operationno, operationtype, transfercreated_items_product_id)',  'concat(operation_num, operation_type, rms_sku_num)',  NULL,  NULL,  'Y');


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('TRANSFER_CREATED_LOGICAL_FACT',  '{{params.dbenv}}_NAP_FCT',  'ascp_transfer_logical',  'load_05_teradata',  1,  'LOAD_END',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  'NAP_ASCP_TRANSFER_LOGICAL');

INSERT INTO isf_dag_control_log_wrk
(SELECT 'ascp_transfer_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
  CAST(NULL AS STRING) AS step_nm,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
  SUBSTR('{{params.dbenv}}_NAP_FCT.TRANSFER_CREATED_LOGICAL_FACT', 1, 100) AS tbl_nm,
  SUBSTR('TERADATA_JOB_END_TMSTP', 1, 100) AS metric_nm,
  SUBSTR(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS STRING), 1, 60) AS metric_value,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS metric_tmstp,
  timestamp(CURRENT_DATETIME('GMT')) AS metric_tmstp_utc,
  'GMT' as metric_tmstp_tz,
  CURRENT_DATE('GMT') AS metric_date);

  
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.isf_dag_control_log 
SET
    metric_value = wrk.metric_value,
    metric_tmstp = CAST(wrk.metric_tmstp AS TIMESTAMP),
    metric_tmstp_tz = wrk.metric_tmstp_tz

	FROM isf_dag_control_log_wrk as wrk
WHERE LOWER(isf_dag_control_log.isf_dag_nm) = LOWER(wrk.isf_dag_nm) AND LOWER(COALESCE(isf_dag_control_log.tbl_nm, '')) = LOWER(COALESCE(wrk.tbl_nm, '')) AND LOWER(isf_dag_control_log.metric_nm) = LOWER(wrk.metric_nm) AND isf_dag_control_log.batch_id = wrk.batch_id;

END;


