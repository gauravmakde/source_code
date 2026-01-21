CREATE TEMPORARY TABLE IF NOT EXISTS isf_dag_control_log_wrk 
--(
--metric_tmstp_tz STRING NOT NULL
--) 
AS
SELECT *
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.isf_dag_control_log;


CREATE TEMPORARY TABLE IF NOT EXISTS po_receipt_ldg_tpt_info AS
SELECT jobstarttime,
 jobendtime,
 rowsinserted
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.tpt_job_log
WHERE LOWER(job_name) = LOWER('{{params.gcp_project_id}}.{{params.dbenv}}_nap_stg_po_receipt_v1_ldg_job')
QUALIFY (ROW_NUMBER() OVER (PARTITION BY job_name ORDER BY rcd_load_tmstp DESC)) = 1;



INSERT INTO isf_dag_control_log_wrk
WITH dummy AS (SELECT 1 AS num) 
(SELECT '{{params.dag_name}}' AS isf_dag_nm,
   cast(NULL as string) AS step_nm,
    (SELECT batch_id
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
    WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
   RPAD('{{params.gcp_project_id}}.{{params.dbenv}}_nap_fct.po_receipt_raw_fact', 1, ' ') AS tbl_nm,
   RPAD('TERADATA_JOB_START_TMSTP', 1, ' ') AS metric_nm,
   RPAD(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS STRING), 1, ' ') AS metric_value
   ,
   CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS metric_tmstp,
   current_timestamp as metric_tmstp_utc,
   JWN_UDF.UDF_TIME_ZONE(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS STRING)) AS metric_tmstp_tz,

   current_date('PST8PDT') AS metric_date
  FROM dummy)
UNION ALL
(SELECT '{{params.dag_name}}' AS isf_dag_nm,
   NULL AS step_nm,
    (SELECT batch_id
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
    WHERE LOWER(subject_area_nm) = LOWER( '{{params.subject_area}}' )) AS batch_id,
   RPAD('{{params.dbenv}}_NAP_STG.PO_RECEIPT_V1_LDG', 1, ' ') AS tbl_nm,
   RPAD('LOADED_TO_CSV_CNT', 1, ' ') AS metric_nm,
   RPAD((SELECT metric_value
     FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.processed_data_spark_log
     WHERE LOWER(isf_dag_nm) = LOWER('{{params.dag_name}}')
      AND LOWER(tbl_nm) = LOWER('PO_RECEIPT_V1_LDG')
      AND LOWER(metric_nm) = LOWER('PROCESSED_ROWS_CNT')), 1, ' ') AS metric_value,
   CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS metric_tmstp,
   current_timestamp as metric_tmstp_utc,
   JWN_UDF.UDF_TIME_ZONE(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS STRING)) AS metric_tmstp_tz,
   current_date('PST8PDT') AS metric_date
  FROM dummy)
UNION ALL
(SELECT '{{params.dag_name}}'AS isf_dag_nm,
   NULL AS step_nm,
    (SELECT batch_id
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
    WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
   RPAD(NULL, 1, ' ') AS tbl_nm,
   RPAD('SPARK_PROCESSED_MESSAGE_CNT', 1, ' ') AS metric_nm,
   RPAD((SELECT metric_value
     FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.processed_data_spark_log
     WHERE LOWER(isf_dag_nm) = LOWER('{{params.dag_name}}')
      AND LOWER(metric_nm) = LOWER('SPARK_PROCESSED_MESSAGE_CNT')), 1, ' ') AS metric_value,
   CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS metric_tmstp,
   current_timestamp as metric_tmstp_utc,
   JWN_UDF.UDF_TIME_ZONE(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS STRING)) AS metric_tmstp_tz,
   current_date('PST8PDT') AS metric_date
  FROM dummy)
UNION ALL
(SELECT '{{params.dag_name}}' AS isf_dag_nm,
   NULL AS step_nm,
    (SELECT batch_id
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
    WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
   RPAD('{{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.po_receipt_v1_ldg', 1, ' ') AS tbl_nm,
   RPAD('LOADED_TO_LDG_CNT', 1, ' ') AS metric_nm,
   RPAD(CAST((SELECT rowsinserted
      FROM po_receipt_ldg_tpt_info) AS STRING), 1, ' ') AS metric_value,
   CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S',current_datetime('PST8PDT')) AS DATETIME) AS metric_tmstp,
   current_timestamp as metric_tmstp_utc,
   JWN_UDF.UDF_TIME_ZONE(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S',current_datetime('PST8PDT')) AS DATETIME) AS STRING)) AS metric_tmstp_tz,
   current_date('PST8PDT') AS metric_date
  FROM dummy)
UNION ALL
(SELECT '{{params.dag_name}}' AS isf_dag_nm,
   NULL AS step_nm,
    (SELECT batch_id
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
    WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
   RPAD('{{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.po_receipt_v1_ldg', 1, ' ') AS tbl_nm,
   RPAD('TPT_JOB_START_TMSTP', 1, ' ') AS metric_nm,
   RPAD(CAST((SELECT jobstarttime
      FROM po_receipt_ldg_tpt_info) AS STRING), 1, ' ') AS metric_value,
   CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S',current_datetime('PST8PDT')) AS DATETIME) AS metric_tmstp,
   current_timestamp as metric_tmstp_utc,
   JWN_UDF.UDF_TIME_ZONE(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS STRING)) AS metric_tmstp_tz,
   current_date('PST8PDT') AS metric_date
  FROM dummy)
UNION ALL
 (SELECT '{{params.dag_name}}' AS isf_dag_nm,
   NULL AS step_nm,
    (SELECT batch_id
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
    WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
   RPAD('{{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.po_receipt_v1_ldg', 1, ' ') AS tbl_nm,
   RPAD('TPT_JOB_END_TMSTP', 1, ' ') AS metric_nm,
   RPAD(CAST((SELECT jobendtime
      FROM po_receipt_ldg_tpt_info) AS STRING), 1, ' ') AS metric_value,
   CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS metric_tmstp,
   current_timestamp as metric_tmstp_utc,
   JWN_UDF.UDF_TIME_ZONE(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS STRING)) AS metric_tmstp_tz,
   current_date('PST8PDT') AS metric_date
  FROM dummy);

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('PO_RECEIPT_RAW_FACT',  '{{params.dbenv}}_NAP_FCT',  'ascp_po_receipt',  'load_04_teradata',  0,  'LOAD_START',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME), `{{params.subject_area}}`);




CREATE TEMPORARY TABLE IF NOT EXISTS delta_latest_items_ldg
CLUSTER BY shipmentid, purchaseordernumber, items_product_id, items_seq_num
AS
SELECT SUBSTR(TRIM(shipmentid), 1, 32) AS shipmentid,
 SUBSTR(TRIM(purchaseordernumber), 1, 16) AS purchaseordernumber,
  CASE
  WHEN LOWER(externalreferenceid) = LOWER('""')
  THEN NULL
  ELSE SUBSTR(TRIM(externalreferenceid), 1, 40)
  END AS externalreferenceid,
 CAST(SUBSTR(receivedat, 1, 10) AS DATE) AS receivedat_dt,
 PARSE_DATETIME('%F%H:%M:%S', REGEXP_REPLACE(REPLACE(REPLACE(receivedat, 'Z', '+00:00'), 'T', ' '), '(?i)b|T', ' ')) AS
 receivedat_ts,
 SUBSTR(TRIM(items_product_id), 1, 16) AS items_product_id,
 items_product_upc,
 SUBSTR(items_seq_num, 1, 5) AS items_seq_num,
 items_quantity,
  CASE
  WHEN LOWER(cartonid) = LOWER('""')
  THEN 'null'
  ELSE SUBSTR(TRIM(cartonid), 1, 40)
  END AS cartonid,
 vendoradvanceshipnotice,
 SUBSTR(TRIM(operationdetails_operationnumber), 1, 16) AS operationdetails_operationnumber,
 RPAD(TRIM(operationdetails_operationtype), 1, ' ') AS operationdetails_operationtype,
 CAST(TRIM(tofacility_id) AS INTEGER) AS tofacility_id,
 RPAD(TRIM(tofacility_type), 1, ' ') AS tofacility_type,
 CAST(TRIM(tological_id) AS INTEGER) AS tological_id,
 RPAD(TRIM(tological_type), 1, ' ') AS tological_type,
 SUBSTR(TRIM(csn), 1, 60) AS csn,
 SUBSTR(TRIM(op_seq_no), 1, 60) AS op_seq_no,
 SUBSTR(TRIM(position_no), 1, 60) AS position_no,
 PARSE_DATETIME('%F%H:%M:%S', REGEXP_REPLACE(REPLACE(REPLACE(headersystemtime, 'Z', '+00:00'), 'T', ' '), '(?i)b|T', ' '
   )) AS header_sys_tmstp,
  (SELECT batch_id
  FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS dw_batch_id,
  (SELECT curr_batch_date
  FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS dw_batch_date
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.po_receipt_v1_ldg
WHERE shipmentid IS NOT NULL
 AND purchaseordernumber IS NOT NULL
QUALIFY (ROW_NUMBER() OVER (PARTITION BY SUBSTR(TRIM(shipmentid), 1, 32), CASE
      WHEN LOWER(cartonid) = LOWER('""')
      THEN 'null'
      ELSE SUBSTR(TRIM(cartonid), 1, 40)
      END, SUBSTR(TRIM(items_product_id), 1, 16), SUBSTR(items_seq_num, 1, 5) ORDER BY SUBSTR(TRIM(purchaseordernumber)
      , 1, 16), SUBSTR(TRIM(csn), 1, 60) DESC, CAST(SUBSTR(TRIM(op_seq_no), 1, 60) AS BIGINT) DESC, SUBSTR(TRIM(position_no
       ), 1, 60) DESC)) = 1;

UPDATE {{params.gcp_project_id}}.{{params.dbenv}}_nap_fct.po_receipt_raw_fact 
SET purchase_order_num = wrk.purchaseordernumber,
    external_reference_id = wrk.externalreferenceid,
    received_date = wrk.receivedat_dt,
    received_tmstp = CAST(wrk.receivedat_ts AS TIMESTAMP),
    upc_num = wrk.items_product_upc,
    shipment_qty = wrk.items_quantity,
    vendor_advance_ship_notice = wrk.vendoradvanceshipnotice,
    operation_detail_operation_num = wrk.operationdetails_operationnumber,
    operation_detail_operation_type = wrk.operationdetails_operationtype,
    tofacility_id = wrk.tofacility_id,
    tofacility_type = wrk.tofacility_type,
    tological_id = wrk.tological_id,
    tological_type = wrk.tological_type,
    csn = wrk.csn,
    op_seq_no = wrk.op_seq_no,
    header_sys_tmstp = CAST(wrk.header_sys_tmstp AS TIMESTAMP),
    position_no = wrk.position_no,
    dw_batch_id = wrk.dw_batch_id,
    dw_batch_date = wrk.dw_batch_date,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS TIMESTAMP) FROM (
  SELECT
      ldg.*
    FROM
      delta_latest_items_ldg AS ldg
      INNER JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_fct.po_receipt_raw_fact AS fct ON ldg.shipmentid = fct.shipment_id
       AND ldg.cartonid = fct.carton_id
       AND ldg.items_product_id = fct.rms_sku_num
       AND ldg.items_seq_num = fct.product_seq_num
       AND (coalesce(CAST( ldg.csn as INT64), 0) > coalesce(CAST( fct.csn as INT64), 0)
       OR coalesce(CAST( ldg.csn as INT64), 0) = coalesce(CAST( fct.csn as INT64), 0)
       AND CAST( ldg.op_seq_no as INT64) > CAST( fct.op_seq_no as INT64)
       OR coalesce(CAST( ldg.csn as INT64), 0) = coalesce(CAST( fct.csn as INT64), 0)
       AND CAST( ldg.op_seq_no as INT64) = CAST( fct.op_seq_no as INT64)
       AND CAST( ldg.position_no as INT64) > CAST( fct.position_no as INT64))
) AS wrk
WHERE LOWER(wrk.shipmentid) = LOWER(po_receipt_raw_fact.shipment_id) AND LOWER(wrk.cartonid) = LOWER(po_receipt_raw_fact.carton_id) AND LOWER(wrk.items_product_id) = LOWER(po_receipt_raw_fact.rms_sku_num) AND LOWER(wrk.items_seq_num) = LOWER(po_receipt_raw_fact.product_seq_num);




INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_fct.po_receipt_raw_fact
(SELECT ldg.shipmentid,
  ldg.cartonid,
  ldg.items_product_id,
  ldg.items_seq_num,
  ldg.purchaseordernumber,
  ldg.externalreferenceid,
  ldg.receivedat_dt,
  CAST(ldg.receivedat_ts AS TIMESTAMP) AS receivedat_ts,
   JWN_UDF.UDF_TIME_ZONE(JWN_UDF.ISO8601_TMSTP(CAST(ldg.receivedat_ts AS STRING))) as received_tmstp_tz,
  ldg.items_product_upc,
  ldg.items_quantity,
  ldg.vendoradvanceshipnotice,
  ldg.operationdetails_operationnumber,
  ldg.operationdetails_operationtype,
  ldg.tofacility_id,
  ldg.tofacility_type,
  ldg.tological_id,
  ldg.tological_type,
  'RMS',
  ldg.csn,
  ldg.op_seq_no,
  ldg.position_no,
  ldg.dw_batch_id,
  ldg.dw_batch_date,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS TIMESTAMP),
JWN_UDF.DEFAULT_TZ_PST(),
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS TIMESTAMP),
JWN_UDF.DEFAULT_TZ_PST(),
  CAST(ldg.header_sys_tmstp AS TIMESTAMP) AS header_sys_tmstp,
JWN_UDF.DEFAULT_TZ_PST()

 FROM delta_latest_items_ldg AS ldg
  LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_fct.po_receipt_raw_fact AS fact ON LOWER(ldg.shipmentid) = LOWER(fact.shipment_id) AND LOWER(ldg.cartonid
       ) = LOWER(fact.carton_id) AND LOWER(ldg.items_product_id) = LOWER(fact.rms_sku_num) AND LOWER(ldg.items_seq_num)
    = LOWER(fact.product_seq_num)
 WHERE fact.shipment_id IS NULL
  AND fact.carton_id IS NULL
  AND fact.rms_sku_num IS NULL
  AND fact.product_seq_num IS NULL);



CREATE TEMPORARY TABLE IF NOT EXISTS delta_header_wrk_ldg
CLUSTER BY shipment_id, purchase_order_num
AS
SELECT fct.shipment_id,
 fct.purchase_order_num,
 fct.received_date,
 fct.received_tmstp
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.po_receipt_raw_fact AS fct
 INNER JOIN (SELECT shipmentid,
   purchaseordernumber
  FROM delta_latest_items_ldg
  GROUP BY shipmentid,
   purchaseordernumber) AS ldg ON LOWER(ldg.shipmentid) = LOWER(fct.shipment_id) AND LOWER(ldg.purchaseordernumber) =
   LOWER(fct.purchase_order_num)
QUALIFY (ROW_NUMBER() OVER (PARTITION BY fct.shipment_id, fct.purchase_order_num ORDER BY fct.csn DESC, CAST(fct.op_seq_no AS BIGINT)
     DESC, fct.position_no DESC)) = 1;



UPDATE delta_header_wrk_ldg AS wrk SET
    received_date = wrk.received_date,
    received_tmstp  = CAST(wrk.received_tmstp AS DATETIME) FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_fct.po_receipt_raw_fact
WHERE LOWER(po_receipt_raw_fact.shipment_id) = LOWER(wrk.shipment_id) AND LOWER(po_receipt_raw_fact.purchase_order_num) = LOWER(wrk.purchase_order_num) AND (po_receipt_raw_fact.received_date <> wrk.received_date OR po_receipt_raw_fact.received_tmstp <> CAST(wrk.received_tmstp AS TIMESTAMP));



CREATE TEMPORARY TABLE IF NOT EXISTS delta_raw_data
CLUSTER BY shipment_id, carton_id, rms_sku_num
AS
SELECT shipment_id,
 carton_id,
 rms_sku_num
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.po_receipt_raw_fact
WHERE dw_batch_id = (SELECT batch_id
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}'))
GROUP BY shipment_id,
 carton_id,
 rms_sku_num;



MERGE INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_fct.po_receipt_fact AS fact
USING (SELECT fct.shipment_id, fct.carton_id, fct.rms_sku_num, fct.purchase_order_num, fct.external_reference_id, fct.received_date, fct.received_tmstp,
JWN_UDF.UDF_TIME_ZONE(JWN_UDF.ISO8601_TMSTP(CAST(fct.received_tmstp AS STRING))) AS received_tmstp_tz,
 fct.upc_num, SUM(fct.shipment_qty) AS shipment_qty, fct.vendor_advance_ship_notice, fct.operation_detail_operation_num, fct.operation_detail_operation_type, fct.tofacility_id, fct.tofacility_type, fct.tological_id, fct.tological_type, fct.source_system, (SELECT batch_id
            FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
            WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS dw_batch_id, (SELECT curr_batch_date
            FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
            WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS dw_batch_date
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_fct.po_receipt_raw_fact AS fct
        INNER JOIN delta_raw_data AS wrk ON LOWER(fct.shipment_id) = LOWER(wrk.shipment_id) AND LOWER(fct.carton_id) = LOWER(wrk.carton_id) AND LOWER(fct.rms_sku_num) = LOWER(wrk.rms_sku_num)
    GROUP BY fct.shipment_id, fct.carton_id, fct.rms_sku_num, fct.purchase_order_num, fct.external_reference_id, fct.received_date, fct.received_tmstp, fct.upc_num, fct.vendor_advance_ship_notice, fct.operation_detail_operation_num, fct.operation_detail_operation_type, fct.tofacility_id, fct.tofacility_type, fct.tological_id, fct.tological_type, fct.source_system) AS ldg
ON LOWER(fact.shipment_id) = LOWER(ldg.shipment_id) AND LOWER(fact.carton_id) = LOWER(ldg.carton_id) AND LOWER(fact.rms_sku_num) = LOWER(ldg.rms_sku_num) AND LOWER(fact.purchase_order_num) = LOWER(ldg.purchase_order_num)
WHEN MATCHED THEN UPDATE SET
    external_reference_id = ldg.external_reference_id,
    received_date = ldg.received_date,
    received_tmstp = ldg.received_tmstp,
    upc_num = ldg.upc_num,
    shipment_qty = ldg.shipment_qty,
    vendor_advance_ship_notice = ldg.vendor_advance_ship_notice,
    operation_detail_operation_num = ldg.operation_detail_operation_num,
    operation_detail_operation_type = ldg.operation_detail_operation_type,
    tofacility_id = ldg.tofacility_id,
    tofacility_type = ldg.tofacility_type,
    tological_id = ldg.tological_id,
    tological_type = ldg.tological_type,
    source_system = ldg.source_system,
    dw_batch_id = ldg.dw_batch_id,
    dw_batch_date = ldg.dw_batch_date,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS TIMESTAMP)
WHEN NOT MATCHED THEN INSERT VALUES(ldg.shipment_id, ldg.carton_id, ldg.rms_sku_num, ldg.purchase_order_num, ldg.external_reference_id, ldg.received_date, ldg.received_tmstp,received_tmstp_tz, ldg.upc_num, ldg.shipment_qty, ldg.vendor_advance_ship_notice, ldg.operation_detail_operation_num, ldg.operation_detail_operation_type, ldg.tofacility_id, ldg.tofacility_type, ldg.tological_id, ldg.tological_type, ldg.source_system, ldg.dw_batch_id, ldg.dw_batch_date, 
CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS TIMESTAMP),
JWN_UDF.DEFAULT_TZ_PST(),
CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS TIMESTAMP),
JWN_UDF.DEFAULT_TZ_PST());




CALL  `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('PO_RECEIPT_RAW_FACT',  '{{params.dbenv}}_NAP_FCT',  'ascp_po_receipt',  'load_04_teradata',  1,  'LOAD_END',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME),  '{{params.subject_area}}');


INSERT INTO isf_dag_control_log_wrk
(SELECT '{{params.dag_name}}' AS isf_dag_nm,
  CAST(NULL AS STRING) AS step_nm,
   (SELECT batch_id
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,

  SUBSTR('{{params.gcp_project_id}}.{{params.dbenv}}_NAP_FCT.PO_RECEIPT_RAW_FACT', 1, 100) AS tbl_nm,
  SUBSTR('TERADATA_JOB_END_TMSTP', 1, 100) AS metric_nm,
  SUBSTR(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', current_datetime('PST8PDT')) AS DATETIME) AS STRING), 1, 60) AS metric_value,
  cast(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', current_datetime('PST8PDT')) AS DATETIME) AS TIMESTAMP) as datetime) AS metric_tmstp,
   current_timestamp,
   JWN_UDF.DEFAULT_TZ_PST(),
  current_date('PST8PDT') AS metric_date);
  
UPDATE isf_dag_control_log_wrk AS wrk SET
    metric_value = wrk.metric_value,
    metric_tmstp = wrk.metric_tmstp FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.isf_dag_control_log
WHERE LOWER(isf_dag_control_log.isf_dag_nm) = LOWER(wrk.isf_dag_nm) AND LOWER(COALESCE(isf_dag_control_log.tbl_nm, '')) = LOWER(COALESCE(wrk.tbl_nm, '')) AND LOWER(isf_dag_control_log.metric_nm) = LOWER(wrk.metric_nm) AND isf_dag_control_log.batch_id = wrk.batch_id;

